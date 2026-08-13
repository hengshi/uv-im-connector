package memory

import (
	"context"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
)

func TestSendMarksFixedLocalFailure(t *testing.T) {
	provider := New("memory")
	_, err := provider.Send(context.Background(), uvim.OutboundMessage{})
	if err == nil {
		t.Fatal("Send() error = nil")
	}
	if got := uvim.ProviderSendErrorLogDetail(err); got != "memory send: invalid target" {
		t.Fatalf("private log detail = %q", got)
	}
}
