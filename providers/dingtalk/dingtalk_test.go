package dingtalk

import (
	"context"
	"fmt"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"
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

func TestNewRejectsPartialStreamCredentials(t *testing.T) {
	for _, config := range []Config{
		{ClientID: "client-id"},
		{ClientSecret: "client-secret"},
	} {
		if _, err := New(config); err == nil {
			t.Fatal("New() accepted a partial DingTalk Stream credential pair")
		}
	}
}

func TestStreamRunEmitsNormalizedEvent(t *testing.T) {
	provider, err := New(Config{
		ConnectorID:  "main",
		ClientID:     "client-id",
		ClientSecret: "client-secret",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeStreamClient{
		message: &chatbot.BotCallbackDataModel{
			MsgId:            "m-stream",
			Msgtype:          "text",
			SenderStaffId:    "u1",
			SenderNick:       "Actor",
			ConversationId:   "c1",
			ConversationType: "1",
			SessionWebhook:   "https://oapi.dingtalk.com/robot/sendBySession?session=secret",
			Text:             chatbot.BotCallbackDataTextModel{Content: "/start JARVIS-IM-REAL-E2E-1"},
		},
		afterMessage: cancel,
	}
	provider.newStreamClient = func(string, string) streamClient { return fake }

	var events []uvim.Event
	err = provider.Run(ctx, uvim.EventSinkFunc(func(_ context.Context, event uvim.Event) error {
		events = append(events, event)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	event := events[0]
	if event.Provider != "dingtalk" || event.Connector != "main" || event.ID != "m-stream" {
		t.Fatalf("unexpected event identity: %#v", event)
	}
	if event.User.ID != "u1" || event.Channel.ID != "c1" || event.Message.Text != "/start JARVIS-IM-REAL-E2E-1" {
		t.Fatalf("unexpected normalized event: %#v", event)
	}
	if event.Referrer.ReplyToken == "" {
		t.Fatal("stream event lost session webhook reply token")
	}
}

type fakeStreamClient struct {
	handler      chatbot.IChatBotMessageHandler
	message      *chatbot.BotCallbackDataModel
	afterMessage func()
}

func (f *fakeStreamClient) RegisterChatBotCallbackRouter(handler chatbot.IChatBotMessageHandler) {
	f.handler = handler
}

func (f *fakeStreamClient) Start(ctx context.Context) error {
	if f.handler == nil {
		return fmt.Errorf("chatbot handler was not registered")
	}
	if _, err := f.handler(ctx, f.message); err != nil {
		return err
	}
	if f.afterMessage != nil {
		f.afterMessage()
	}
	return nil
}

func (f *fakeStreamClient) Close() {}
