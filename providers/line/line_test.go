package line

import (
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
)

func TestDecodeSetsSingleUseReplyHandleDeadline(t *testing.T) {
	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	event, ok, err := Decode([]byte(`{
  "events": [{
    "replyToken": "reply-1",
    "source": {"type": "user", "userId": "u1"},
    "message": {"id": "m1", "type": "text", "text": "hello"}
  }]
}`), httpchannel.Config{ConnectorID: "main", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("decode ok = false")
	}
	if event.Referrer.ExpiresAt == nil || !event.Referrer.ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("reply expiry = %v", event.Referrer.ExpiresAt)
	}
	if event.Referrer.Target == nil || *event.Referrer.Target != (uvim.OutboundTarget{ID: "u1", Kind: uvim.TargetUser}) {
		t.Fatalf("reply target = %+v", event.Referrer.Target)
	}
}

func TestCapabilitiesDeclareSingleUseReplyToken(t *testing.T) {
	provider, err := New(Config{BaseURL: "http://127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := provider.Capabilities().ReplyMaxUses; got != 1 {
		t.Fatalf("reply max uses = %d, want 1", got)
	}
}

func TestDefaultDownloadUsesLineContentHost(t *testing.T) {
	if got := lineContentBaseURL("https://api.line.me"); got != "https://api-data.line.me" {
		t.Fatalf("content base = %q", got)
	}
	if got := lineContentBaseURL("http://127.0.0.1:1234"); got != "http://127.0.0.1:1234" {
		t.Fatalf("custom content base = %q", got)
	}
}
