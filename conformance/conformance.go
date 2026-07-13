package conformance

import (
	"context"
	"slices"
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
	if caps.Outbound && !caps.ReplyMessage && !caps.ProactiveDirect && !caps.ProactiveGroup {
		t.Fatalf("provider %s must declare at least one outbound mode", provider.ID())
	}
	if caps.Outbound && len(caps.TargetKinds) == 0 {
		t.Fatalf("provider %s must declare outbound target kinds", provider.ID())
	}
	for _, kind := range caps.TargetKinds {
		if !slices.Contains([]string{uvim.TargetUser, uvim.TargetGroup, uvim.TargetChannel, uvim.TargetConversation}, kind) {
			t.Fatalf("provider %s has unsupported target kind %q", provider.ID(), kind)
		}
	}
	health := provider.Health(context.Background())
	if health.Provider != provider.ID() {
		t.Fatalf("health provider = %q, want %q", health.Provider, provider.ID())
	}
}
