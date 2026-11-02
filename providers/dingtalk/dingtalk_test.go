package dingtalk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
	"github.com/hengshi/uv-im-connector/server"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/handler"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/payload"
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

func TestDecodeContentRichTextAndDownloadCodes(t *testing.T) {
	event, ok, err := Decode([]byte(`{
  "msgId": "m-rich",
  "msgtype": "richText",
  "robotCode": "robot-1",
  "senderStaffId": "u1",
  "senderNick": "Ada",
  "conversationId": "g1",
  "conversationType": "2",
  "content": {
    "richText": [
      {"text": "hello"},
      {"type": "picture", "downloadCode": "pic-code", "fileName": "pic.png"},
      {"text": "world"},
      {"type": "file", "downloadCode": "file-code", "fileName": "report.pdf", "fileSize": 12}
    ]
  }
}`), httpchannel.Config{ConnectorID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("decode ok = false")
	}
	if event.Message.Text != "hello\nworld" {
		t.Fatalf("text = %q", event.Message.Text)
	}
	if len(event.Message.Resources) != 2 {
		t.Fatalf("resources = %+v", event.Message.Resources)
	}
	if event.Message.Resources[0].Kind != uvim.ElementImage || event.Message.Resources[0].Key != "pic-code" {
		t.Fatalf("image resource = %+v", event.Message.Resources[0])
	}
	if event.Message.Resources[1].Kind != uvim.ElementFile || event.Message.Resources[1].Key != "file-code" || event.Message.Resources[1].SizeBytes != 12 {
		t.Fatalf("file resource = %+v", event.Message.Resources[1])
	}
	if event.Message.Resources[0].Private["robot_code"] != "robot-1" {
		t.Fatalf("private metadata = %+v", event.Message.Resources[0].Private)
	}
}

func TestDownloadExchangesDownloadCodeForTemporaryURL(t *testing.T) {
	var sawTokenRequest, sawDownloadRequest bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			sawTokenRequest = true
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["appKey"] != "client-id" || body["appSecret"] != "client-secret" {
				t.Fatalf("token body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": "access-token"})
		case "/v1.0/robot/messageFiles/download":
			sawDownloadRequest = true
			if got := r.Header.Get("x-acs-dingtalk-access-token"); got != "access-token" {
				t.Fatalf("download token = %q", got)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["robotCode"] != "robot-1" || body["downloadCode"] != "download-code" {
				t.Fatalf("download body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"downloadUrl": apiURL(r) + "/file"})
		case "/file":
			_, _ = io.WriteString(w, "file bytes")
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer api.Close()

	provider, err := New(Config{BaseURL: api.URL, ClientID: "client-id", ClientSecret: "client-secret"})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := provider.Download(context.Background(), uvim.ResourceDownloadRequest{
		Dir: t.TempDir(),
		Resource: uvim.ResourceRef{
			Provider: "dingtalk",
			Kind:     uvim.ElementImage,
			Name:     "pic.png",
			Key:      "download-code",
			URL:      api.URL + "/legacy-file",
			Private:  map[string]string{"robot_code": "robot-1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawTokenRequest || !sawDownloadRequest {
		t.Fatalf("token=%t download=%t", sawTokenRequest, sawDownloadRequest)
	}
	if ref.InternalURL == "" || ref.Key != "" {
		t.Fatalf("downloaded ref = %+v", ref)
	}
}

func apiURL(r *http.Request) string {
	return "http://" + r.Host
}

func TestSendMarkdownThroughWebhookAndAppAPI(t *testing.T) {
	const text = "# 任务结论\n\n**已完成**\n- [文档](https://example.com/guide)\n\n```go\nfmt.Println(\"ok\")\n```"
	for _, scenario := range []struct {
		name, kind, path string
		app, reply       bool
	}{
		{"session reply", uvim.TargetUser, "/robot/sendBySession", false, true},
		{"app session reply", uvim.TargetGroup, "/robot/sendBySession", true, true},
		{"group webhook", uvim.TargetGroup, "/robot/send", false, false},
		{"app direct", uvim.TargetUser, "/v1.0/robot/oToMessages/batchSend", true, false},
		{"app group", uvim.TargetGroup, "/v1.0/robot/groupMessages/send", true, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var sends atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1.0/oauth2/accessToken" {
					_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": "access-token"})
					return
				}
				sends.Add(1)
				if r.Method != http.MethodPost || r.URL.Path != scenario.path {
					t.Errorf("send route = %s %s", r.Method, r.URL.Path)
				}
				var body struct {
					MsgType, MsgKey, MsgParam, RobotCode, OpenConversationID string
					UserIDs                                                  []string
					Markdown                                                 map[string]string
					Text                                                     json.RawMessage
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				content := body.Markdown
				if scenario.app && !scenario.reply {
					if body.MsgKey != "sampleMarkdown" || body.RobotCode != "client-id" || r.Header.Get("x-acs-dingtalk-access-token") != "access-token" {
						t.Errorf("app message envelope = %+v", body)
					}
					if scenario.kind == uvim.TargetUser && (len(body.UserIDs) != 1 || body.UserIDs[0] != "recipient") || scenario.kind == uvim.TargetGroup && body.OpenConversationID != "recipient" {
						t.Errorf("app recipient = %+v", body)
					}
					if err := json.Unmarshal([]byte(body.MsgParam), &content); err != nil {
						t.Error(err)
					}
					_ = json.NewEncoder(w).Encode(map[string]string{"processQueryKey": "delivered"})
				} else {
					if body.MsgType != "markdown" || len(body.Text) != 0 || r.Header.Get("Authorization") != "" {
						t.Errorf("webhook message envelope = %+v", body)
					}
					_ = json.NewEncoder(w).Encode(map[string]int{"errcode": 0})
				}
				if content["title"] != "# 任务结论" || content["text"] != text {
					t.Errorf("Markdown content = %+v", content)
				}
			}))
			defer api.Close()
			config := Config{BaseURL: api.URL, Token: "group-token"}
			if scenario.app {
				config.ClientID, config.ClientSecret = "client-id", "client-secret"
			}
			provider, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			msg := uvim.OutboundMessage{Text: text, Target: &uvim.OutboundTarget{Kind: scenario.kind, ID: "recipient"}}
			if scenario.reply {
				msg.Referrer.ReplyToken = api.URL + scenario.path + "?session=reply-token"
			}
			if _, err := provider.Send(t.Context(), msg); err != nil {
				t.Fatal(err)
			}
			if sends.Load() != 1 {
				t.Fatalf("message send count = %d, want 1", sends.Load())
			}
		})
	}
}

// The SDK is stubbed; the real DingTalk adapter, Hub and event log execute.
func TestStreamRunEmitsNormalizedEventWithSDKStub(t *testing.T) {
	for _, failStartup := range []bool{false, true} {
		t.Run(fmt.Sprintf("startup-failure=%t", failStartup), func(t *testing.T) {
			provider, err := New(Config{
				ConnectorID:  "main",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
			})
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			fake := &fakeStreamClient{
				data: `{
  "msgId": "m-stream",
  "msgtype": "text",
  "senderStaffId": "u1",
  "senderNick": "Actor",
  "conversationId": "c1",
  "conversationType": "1",
  "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?session=secret",
  "text": {"content": "/start JARVIS-IM-REAL-E2E-1"}
}`,
				afterMessage: cancel,
			}
			provider.newStreamClient = func(string, string) streamClient {
				if failStartup {
					failStartup = false
					return &fakeStreamClient{startErr: errors.New("temporary connection failure")}
				}
				return fake
			}
			log, err := uvim.NewEventLog(t.TempDir() + "/events.jsonl")
			if err != nil {
				t.Fatal(err)
			}
			hub := server.NewHub(uvim.NewProviderRegistry(provider), log, nil)
			if err := hub.RunProviders(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("runner did not recover and emit before cancellation: %v", err)
			}
			events, err := log.ReadAfter(context.Background(), 0)
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
		})
	}
}

func TestStreamRunPreservesDownloadCodeRobotCode(t *testing.T) {
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
		data: `{
  "msgId": "m-stream-picture",
  "msgtype": "picture",
  "robotCode": "robot-1",
  "senderStaffId": "u1",
  "conversationId": "c1",
  "conversationType": "2",
  "content": {"downloadCode": "download-code", "fileName": "image.png"}
}`,
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
	resources := events[0].Message.Resources
	if len(resources) != 1 {
		t.Fatalf("resources = %+v", resources)
	}
	if resources[0].Kind != uvim.ElementImage || resources[0].Key != "download-code" || resources[0].Name != "image.png" {
		t.Fatalf("resource = %+v", resources[0])
	}
	if resources[0].Private["robot_code"] != "robot-1" {
		t.Fatalf("private metadata = %+v", resources[0].Private)
	}
}

func TestStreamRunDownloadsDownloadCodeThroughHub(t *testing.T) {
	var sawDownloadRequest bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["appKey"] != "client-id" || body["appSecret"] != "client-secret" {
				t.Fatalf("token body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": "access-token"})
		case "/v1.0/robot/messageFiles/download":
			sawDownloadRequest = true
			if got := r.Header.Get("x-acs-dingtalk-access-token"); got != "access-token" {
				t.Fatalf("download token = %q", got)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["robotCode"] != "robot-1" || body["downloadCode"] != "download-code" {
				t.Fatalf("download body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"downloadUrl": apiURL(r) + "/file"})
		case "/file":
			_, _ = io.WriteString(w, "hub file bytes")
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer api.Close()

	provider, err := New(Config{
		ConnectorID:  "main",
		BaseURL:      api.URL,
		ClientID:     "client-id",
		ClientSecret: "client-secret",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &fakeStreamClient{
		data: `{
  "msgId": "m-stream-picture",
  "msgtype": "picture",
  "robotCode": "robot-1",
  "senderStaffId": "u1",
  "conversationId": "c1",
  "conversationType": "2",
  "content": {"downloadCode": "download-code", "fileName": "image.png"}
}`,
		afterMessage: cancel,
	}
	provider.newStreamClient = func(string, string) streamClient { return fake }

	store := &uvim.ResourceStore{Dir: t.TempDir()}
	log, err := uvim.NewEventLog(t.TempDir() + "/events.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	hub := server.NewHub(uvim.NewProviderRegistry(provider), log, store)
	if err := provider.Run(ctx, hub); err != nil {
		t.Fatal(err)
	}
	events, err := log.ReadAfter(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if !sawDownloadRequest {
		t.Fatal("Hub did not trigger the DingTalk downloadCode exchange")
	}
	resources := events[0].Message.Resources
	if len(resources) != 1 {
		t.Fatalf("resources = %+v", resources)
	}
	ref := resources[0]
	if ref.Kind != uvim.ElementImage || ref.Name != "image.png" || ref.InternalURL == "" || ref.Error != "" {
		t.Fatalf("persisted resource = %+v", ref)
	}
	if ref.Key != "" || ref.URL != "" || ref.Private != nil {
		t.Fatalf("persisted resource was not sanitized: %+v", ref)
	}
	file, _, err := store.Open(ref.InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hub file bytes" {
		t.Fatalf("downloaded body = %q", data)
	}
}

type fakeStreamClient struct {
	startErr     error
	handler      handler.IFrameHandler
	data         string
	afterMessage func()
}

func (f *fakeStreamClient) RegisterCallbackRouter(topic string, handler handler.IFrameHandler) {
	if topic != payload.BotMessageCallbackTopic {
		return
	}
	f.handler = handler
}

func (f *fakeStreamClient) Start(ctx context.Context) error {
	if f.startErr != nil {
		return f.startErr
	}
	if f.handler == nil {
		return fmt.Errorf("chatbot handler was not registered")
	}
	_, err := f.handler(ctx, &payload.DataFrame{
		Headers: payload.DataFrameHeader{
			payload.DataFrameHeaderKTopic: payload.BotMessageCallbackTopic,
		},
		Data: f.data,
	})
	if err != nil {
		return err
	}
	if f.afterMessage != nil {
		f.afterMessage()
	}
	return nil
}

func (f *fakeStreamClient) Close() {}
