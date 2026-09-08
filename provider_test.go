package uvim

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSendResourceSequencePreservesPartsAndPartialFailure(t *testing.T) {
	for _, failAt := range []int{0, 1, 2, 3} {
		calls := 0
		cause := errors.New("provider rejected")
		result, err := SendResourceSequence(t.Context(), OutboundMessage{
			ID: "batch", Text: "caption", Resources: []ResourceRef{{ID: "a"}, {ID: "b"}},
		}, func(_ context.Context, part OutboundMessage) (SendResult, error) {
			index := calls
			calls++
			if index == 0 && (part.Text != "caption" || len(part.Resources) != 0) {
				t.Fatalf("text part = %+v", part)
			}
			if index > 0 && (part.Text != "" || len(part.Resources) != 1 || part.Resources[0].ID != []string{"a", "b"}[index-1]) {
				t.Fatalf("resource part = %+v", part)
			}
			if index == failAt {
				return SendResult{}, NewProviderSendFailure(SendFailure{Category: SendFailureRateLimited, DeliveryState: DeliveryRejected, Retryable: true}, "", cause)
			}
			return SendResult{MessageID: part.ID}, nil
		})
		if failAt == 3 {
			if err != nil || strings.Join(result.MessageIDs, ",") != "batch-part-1,batch-part-2,batch-part-3" {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			continue
		}
		failure, ok := ProviderSendFailure(err)
		if !errors.Is(err, cause) || !ok || calls != failAt+1 || failure.DeliveredCount != failAt {
			t.Fatalf("calls=%d failure=%+v err=%v", calls, failure, err)
		}
		if failAt > 0 && (failure.Retryable || failure.DeliveryState != DeliveryUnknown || len(failure.DeliveredMessageIDs) != failAt) {
			t.Fatalf("partial failure = %+v", failure)
		}
	}
}

func TestProviderRegistryRoutesByProviderAndConnector(t *testing.T) {
	first := registryTestProvider{provider: "lark", connector: "main"}
	second := registryTestProvider{provider: "lark", connector: "sandbox"}
	registry := NewProviderRegistry(first, second)
	if got := registry.Get("lark", "main"); got != first {
		t.Fatalf("main connector = %#v", got)
	}
	if got := registry.Get("lark", "sandbox"); got != second {
		t.Fatalf("sandbox connector = %#v", got)
	}
	if got := registry.Get("lark"); got != nil {
		t.Fatalf("ambiguous provider lookup = %#v, want nil", got)
	}
}

func TestProviderRegistrySingleProviderFallback(t *testing.T) {
	provider := registryTestProvider{provider: "wecom", connector: "prod"}
	registry := NewProviderRegistry(provider)
	if got := registry.Get("wecom"); got != provider {
		t.Fatalf("fallback provider = %#v", got)
	}
}

type registryTestProvider struct {
	provider  string
	connector string
}

func (p registryTestProvider) ID() string          { return p.provider }
func (p registryTestProvider) ConnectorID() string { return p.connector }
func (p registryTestProvider) Capabilities() Capabilities {
	return Capabilities{Inbound: true, Outbound: true}
}
func (p registryTestProvider) Run(ctx context.Context, sink EventSink) error {
	<-ctx.Done()
	return ctx.Err()
}
func (p registryTestProvider) Send(context.Context, OutboundMessage) (SendResult, error) {
	return SendResult{Provider: p.provider, Connector: p.connector, Time: time.Now().UTC()}, nil
}
func (p registryTestProvider) Download(context.Context, ResourceDownloadRequest) (ResourceRef, error) {
	return ResourceRef{}, nil
}
func (p registryTestProvider) Health(context.Context) Health {
	return Health{Provider: p.provider, Connector: p.connector, State: "ok", CheckedAt: time.Now().UTC()}
}
