package uvim

import (
	"context"
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
