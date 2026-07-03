package conformance

import (
	"context"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
)

func AssertProvider(t *testing.T, provider uvim.Provider) {
	t.Helper()
	AssertProviderMetadata(t, provider)
	caps := provider.Capabilities()
	if caps.Outbound {
		result, err := provider.Send(context.Background(), uvim.OutboundMessage{
			Provider:  provider.ID(),
			ChannelID: "conformance-channel",
			Text:      "conformance message",
		})
		if err != nil {
			t.Fatalf("Send() error = %v", err)
		}
		if result.Provider != provider.ID() {
			t.Fatalf("SendResult provider = %q, want %q", result.Provider, provider.ID())
		}
	}
	if caps.DownloadResource {
		ref, err := provider.Download(context.Background(), uvim.ResourceDownloadRequest{
			Resource: uvim.ResourceRef{Kind: uvim.ElementFile, Name: "contract.txt", Key: "contract"},
		})
		if err != nil {
			t.Fatalf("Download() error = %v", err)
		}
		if ref.Kind == "" {
			t.Fatalf("downloaded resource must preserve kind: %+v", ref)
		}
	}
}

func AssertProviderMetadata(t *testing.T, provider uvim.Provider) {
	t.Helper()
	if provider == nil {
		t.Fatal("provider is nil")
	}
	if provider.ID() == "" {
		t.Fatal("provider ID is empty")
	}
	caps := provider.Capabilities()
	if !caps.Inbound && !caps.Outbound {
		t.Fatalf("provider %s must declare inbound or outbound capability", provider.ID())
	}
	health := provider.Health(context.Background())
	if health.Provider != provider.ID() {
		t.Fatalf("health provider = %q, want %q", health.Provider, provider.ID())
	}
}
