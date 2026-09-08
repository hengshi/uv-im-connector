package uvim

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

type EventSink interface {
	Emit(context.Context, Event) error
}

type EventSinkFunc func(context.Context, Event) error

func (f EventSinkFunc) Emit(ctx context.Context, event Event) error {
	if f == nil {
		return nil
	}
	return f(ctx, event)
}

type Provider interface {
	ID() string
	ConnectorID() string
	Capabilities() Capabilities
	Run(context.Context, EventSink) error
	Send(context.Context, OutboundMessage) (SendResult, error)
	Download(context.Context, ResourceDownloadRequest) (ResourceRef, error)
	Health(context.Context) Health
}

// SendResourceSequence sends text followed by each resource through a provider's
// single-message path. Successful parts are never replayed after a later failure.
func SendResourceSequence(ctx context.Context, msg OutboundMessage, send func(context.Context, OutboundMessage) (SendResult, error)) (SendResult, error) {
	var textParts []string
	var collectText func([]Element) error
	collectText = func(elements []Element) error {
		for _, element := range elements {
			if (element.Type != "" && element.Type != ElementText) || element.Resource != nil {
				return fmt.Errorf("send: rich elements are not supported")
			}
			if element.Type == ElementText && strings.TrimSpace(element.Text) != "" {
				textParts = append(textParts, element.Text)
			}
			if err := collectText(element.Children); err != nil {
				return err
			}
		}
		return nil
	}
	if err := collectText(msg.Elements); err != nil {
		return SendResult{}, err
	}
	if strings.TrimSpace(msg.Text) == "" {
		msg.Text = strings.Join(textParts, "\n")
	}
	var parts []OutboundMessage
	if strings.TrimSpace(msg.Text) != "" {
		part := msg
		part.Resources, part.Elements = nil, nil
		parts = append(parts, part)
	}
	for _, ref := range msg.Resources {
		part := msg
		part.Text, part.Elements = "", nil
		part.Resources = []ResourceRef{ref}
		parts = append(parts, part)
	}
	var result SendResult
	var ids []string
	for i, part := range parts {
		// Matrix uses ID as its transaction key; every part needs a distinct,
		// stable key when the caller supplies an idempotency ID.
		if len(parts) > 1 && msg.ID != "" {
			part.ID = fmt.Sprintf("%s-part-%d", msg.ID, i+1)
		}
		next, err := send(ctx, part)
		if err != nil {
			if i == 0 {
				return result, err
			}
			failure, ok := ProviderSendFailure(err)
			if !ok {
				failure = classifyProviderSendFailure(err)
			}
			failure.DeliveryState, failure.Retryable = DeliveryUnknown, false
			failure.DeliveredCount, failure.DeliveredMessageIDs = i, append([]string(nil), ids...)
			return result, NewProviderSendFailure(failure, fmt.Sprintf("send stopped after %d of %d messages; do not retry the whole sequence", i, len(parts)), err)
		}
		ids = append(ids, next.MessageID)
		result = next
		result.MessageIDs = ids
	}
	return result, nil
}

type WebhookProvider interface {
	Provider
	ServeWebhook(http.ResponseWriter, *http.Request, EventSink)
}

type ResourceDownloadRequest struct {
	Resource ResourceRef `json:"resource"`
	Message  Message     `json:"message,omitempty"`
	Event    Event       `json:"event,omitempty"`
	Dir      string      `json:"dir,omitempty"`
}

type ProviderRegistry struct {
	byKey      map[string]Provider
	byProvider map[string][]Provider
}

func NewProviderRegistry(providers ...Provider) *ProviderRegistry {
	r := &ProviderRegistry{byKey: map[string]Provider{}, byProvider: map[string][]Provider{}}
	for _, provider := range providers {
		r.Add(provider)
	}
	return r
}

func (r *ProviderRegistry) Add(provider Provider) {
	if r == nil || provider == nil || provider.ID() == "" {
		return
	}
	providerID := canonicalKeyPart(provider.ID())
	connectorID := canonicalKeyPart(provider.ConnectorID())
	if connectorID == "" {
		connectorID = providerID
	}
	key := ProviderKey(providerID, connectorID)
	if existing := r.byKey[key]; existing != nil {
		r.removeFromProviderIndex(existing)
	}
	r.byKey[key] = provider
	r.byProvider[providerID] = append(r.byProvider[providerID], provider)
}

func (r *ProviderRegistry) Get(provider string, connector ...string) Provider {
	if r == nil {
		return nil
	}
	providerID := canonicalKeyPart(provider)
	connectorID := ""
	if len(connector) > 0 {
		connectorID = canonicalKeyPart(connector[0])
	}
	if connectorID != "" {
		return r.byKey[ProviderKey(providerID, connectorID)]
	}
	matches := r.byProvider[providerID]
	if len(matches) == 1 {
		return matches[0]
	}
	return nil
}

func (r *ProviderRegistry) List() []Provider {
	if r == nil || len(r.byKey) == 0 {
		return nil
	}
	out := make([]Provider, 0, len(r.byKey))
	for _, provider := range r.byKey {
		out = append(out, provider)
	}
	sort.Slice(out, func(i, j int) bool {
		return ProviderKey(out[i].ID(), out[i].ConnectorID()) < ProviderKey(out[j].ID(), out[j].ConnectorID())
	})
	return out
}

func ProviderKey(provider, connector string) string {
	return canonicalKeyPart(provider) + "/" + canonicalKeyPart(connector)
}

func canonicalKeyPart(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func (r *ProviderRegistry) removeFromProviderIndex(provider Provider) {
	providerID := canonicalKeyPart(provider.ID())
	matches := r.byProvider[providerID]
	for i, candidate := range matches {
		if candidate == provider {
			r.byProvider[providerID] = append(matches[:i], matches[i+1:]...)
			if len(r.byProvider[providerID]) == 0 {
				delete(r.byProvider, providerID)
			}
			return
		}
	}
}
