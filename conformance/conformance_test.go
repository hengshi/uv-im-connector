package conformance

import (
	"testing"

	"github.com/hengshi/uv-im-connector/providers/memory"
)

func TestMemoryProviderConformance(t *testing.T) {
	provider := memory.New("memory")
	AssertProvider(t, provider)
	sent := provider.Sent()
	if len(sent) != 1 || sent[0].Provider != "memory" || sent[0].ChannelID != "conformance-channel" || sent[0].Text != "conformance message" {
		t.Fatalf("sent messages = %+v", sent)
	}
}
