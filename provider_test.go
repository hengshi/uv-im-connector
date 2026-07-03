package uvim

import (
	"context"
	"testing"
	"time"
)

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
