package dingtalk

import (
	"fmt"
	"testing"
	"time"

	"github.com/hengshi/uv-im-connector/providers/httpchannel"
)

func TestDecodeUsesSessionWebhookExpiry(t *testing.T) {
	expiresAt := time.Date(2026, 7, 16, 11, 30, 0, 0, time.UTC)
	event, ok, err := Decode([]byte(`{
  "msgId": "m1",
  "msgtype": "text",
  "senderStaffId": "u1",
  "conversationId": "g1",
  "conversationType": "2",
  "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?session=secret",
  "sessionWebhookExpiredTime": `+formatUnixMilli(expiresAt)+`,
  "text": {"content": "hello"}
}`), httpchannel.Config{BaseURL: "https://oapi.dingtalk.com", ConnectorID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("decode ok = false")
	}
	if event.Referrer.ExpiresAt == nil || !event.Referrer.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("reply expiry = %v", event.Referrer.ExpiresAt)
	}
}

func formatUnixMilli(value time.Time) string {
	return fmt.Sprintf("%d", value.UnixMilli())
}
