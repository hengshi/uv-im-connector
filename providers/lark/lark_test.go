package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/conformance"
)

func TestDecodePayloadTextMention(t *testing.T) {
	event := map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   "evt-1",
			"event_type": "im.message.receive_v1",
			"app_id":     "app",
		},
		"event": map[string]any{
			"sender": map[string]any{
				"sender_id": map[string]any{"open_id": "ou_sender"},
			},
			"message": map[string]any{
				"message_id":   "om_msg",
				"parent_id":    "om_parent",
				"root_id":      "om_root",
				"chat_id":      "oc_chat",
				"chat_type":    "group",
				"message_type": "text",
				"content":      `{"text":"@_user_1 帮我看一下"}`,
				"mentions": []any{
					map[string]any{
						"key":  "@_user_1",
						"name": "Bot",
						"id":   map[string]any{"open_id": "ou_bot"},
					},
				},
				"create_time": "1700000000000",
			},
		},
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := DecodePayload(raw, DecoderConfig{BotOpenID: "ou_bot", Connector: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("ok = false")
	}
	if got.Provider != "lark" || got.Connector != "main" || got.Message.ID != "om_msg" {
		t.Fatalf("event = %+v", got)
	}
	if got.Channel.Type != uvim.ChannelGroup {
		t.Fatalf("channel = %+v", got.Channel)
	}
	if got.Message.Text != "帮我看一下" {
		t.Fatalf("text = %q", got.Message.Text)
	}
	if got.Referrer.MessageID != "om_msg" || got.Referrer.ParentMessageID != "om_parent" || got.Referrer.RootMessageID != "om_root" {
		t.Fatalf("referrer = %+v", got.Referrer)
	}
}

func TestDecodePayloadFileResource(t *testing.T) {
	event := map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   "evt-1",
			"event_type": "im.message.receive_v1",
		},
		"event": map[string]any{
			"sender": map[string]any{
				"sender_id": map[string]any{"open_id": "ou_sender"},
			},
			"message": map[string]any{
				"message_id":   "om_msg",
				"chat_id":      "oc_chat",
				"chat_type":    "p2p",
				"message_type": "file",
				"content":      `{"file_key":"file_v3_abc","file_name":"log.txt"}`,
			},
		},
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := DecodePayload(raw, DecoderConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("ok = false")
	}
	if len(got.Message.Resources) != 1 {
		t.Fatalf("resources = %+v", got.Message.Resources)
	}
	ref := got.Message.Resources[0]
	if ref.Kind != uvim.ElementFile || ref.Key != "file_v3_abc" || ref.Name != "log.txt" {
		t.Fatalf("resource = %+v", ref)
	}
	if ref.Metadata["message_id"] != "om_msg" {
		t.Fatalf("metadata = %+v", ref.Metadata)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	in := &wsFrame{SeqID: 1, Service: 2, Method: frameMethodData, Headers: []frameHeader{{Key: "message_id", Value: "m1"}}, Payload: []byte("payload")}
	out, err := unmarshalFrame(in.marshal())
	if err != nil {
		t.Fatal(err)
	}
	if out.SeqID != 1 || out.Service != 2 || out.Method != frameMethodData || string(out.Payload) != "payload" {
		t.Fatalf("frame = %+v", out)
	}
	if out.headerValue("message_id") != "m1" {
		t.Fatalf("headers = %+v", out.Headers)
	}
}

func TestProviderMetadataConformance(t *testing.T) {
	provider, err := New(Config{AppID: "app", AppSecret: "secret", ResourceStore: &uvim.ResourceStore{Dir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	conformance.AssertProviderMetadata(t, provider)
	if !provider.Capabilities().UploadResource {
		t.Fatal("UploadResource = false")
	}
}

func TestSendRejectsMixedTextAndResource(t *testing.T) {
	provider, err := New(Config{AppID: "app", AppSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(nil, uvim.OutboundMessage{
		Provider:  "lark",
		ChannelID: "chat",
		Text:      "file",
		Resources: []uvim.ResourceRef{{Kind: uvim.ElementFile, InternalURL: "internal://r1"}},
	})
	if err == nil {
		t.Fatal("Send() error = nil, want unsupported resource error")
	}
}

func TestSendResourceUploadsThenReplies(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	var sentBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/files":
			if err := req.ParseMultipartForm(maxFileBytes); err != nil {
				t.Error(err)
			}
			file, header, err := req.FormFile("file")
			if err != nil {
				t.Error(err)
			} else {
				defer file.Close()
				var data bytes.Buffer
				_, _ = data.ReadFrom(file)
				if header.Filename != "report.txt" || data.String() != "report" || req.FormValue("file_type") != "stream" {
					t.Errorf("upload filename=%q data=%q type=%q", header.Filename, data.String(), req.FormValue("file_type"))
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"file_key": "file-1"}})
		case "/open-apis/im/v1/messages/om_in/reply":
			if err := json.NewDecoder(req.Body).Decode(&sentBody); err != nil {
				t.Error(err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"message_id": "om_out"}})
		default:
			t.Errorf("unexpected path %s", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL, ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Send(context.Background(), uvim.OutboundMessage{Resources: []uvim.ResourceRef{ref}, Referrer: uvim.Referrer{MessageID: "om_in"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "om_out" || sentBody["msg_type"] != uvim.ElementFile {
		t.Fatalf("result=%+v body=%+v", result, sentBody)
	}
	var content map[string]string
	if err := json.Unmarshal([]byte(sentBody["content"].(string)), &content); err != nil {
		t.Fatal(err)
	}
	if content["file_key"] != "file-1" || len(content) != 1 {
		t.Fatalf("content = %+v", content)
	}
}

func TestSendImageUsesImageUpload(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewReader([]byte("png")), uvim.ResourceRef{Kind: uvim.ElementImage, Name: "chart.png", MIME: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	var sentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/images":
			if err := req.ParseMultipartForm(maxImageBytes); err != nil {
				t.Error(err)
			}
			if req.FormValue("image_type") != "message" {
				t.Errorf("image_type = %q", req.FormValue("image_type"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"image_key": "img-1"}})
		case "/open-apis/im/v1/messages":
			var body map[string]any
			_ = json.NewDecoder(req.Body).Decode(&body)
			sentType, _ = body["msg_type"].(string)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"message_id": "om_out"}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL, ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "oc_chat", Resources: []uvim.ResourceRef{ref}})
	if err != nil {
		t.Fatal(err)
	}
	if sentType != uvim.ElementImage {
		t.Fatalf("msg_type = %q", sentType)
	}
}

func TestProactiveSendSelectsRecipientIDType(t *testing.T) {
	tests := []struct {
		name       string
		target     uvim.OutboundTarget
		wantIDType string
	}{
		{name: "user open id", target: uvim.OutboundTarget{ID: "ou_user", Kind: uvim.TargetUser}, wantIDType: "open_id"},
		{name: "conversation", target: uvim.OutboundTarget{ID: "oc_chat", Kind: uvim.TargetConversation}, wantIDType: "chat_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotBody map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/open-apis/auth/v3/tenant_access_token/internal":
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
				case "/open-apis/im/v1/messages":
					if got := req.URL.Query().Get("receive_id_type"); got != tt.wantIDType {
						t.Errorf("receive_id_type = %q, want %q", got, tt.wantIDType)
					}
					if err := json.NewDecoder(req.Body).Decode(&gotBody); err != nil {
						t.Error(err)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "msg": "ok", "data": map[string]any{"message_id": "om_sent"}})
				default:
					t.Errorf("unexpected path %s", req.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			result, err := provider.Send(context.Background(), uvim.OutboundMessage{Target: &tt.target, Text: "hello"})
			if err != nil {
				t.Fatal(err)
			}
			if gotBody["receive_id"] != tt.target.ID || result.MessageID != "om_sent" {
				t.Fatalf("body=%+v result=%+v", gotBody, result)
			}
		})
	}
}

func TestLegacyDirectChannelRemainsChatID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/messages":
			if got := req.URL.Query().Get("receive_id_type"); got != "chat_id" {
				t.Errorf("receive_id_type = %q, want chat_id", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"message_id": "om_sent"}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "oc_chat", ChannelType: uvim.ChannelDirect, Text: "hello"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestProactiveSendRejectsNonOpenIDUserTarget(t *testing.T) {
	provider, err := New(Config{AppID: "app", AppSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Target: &uvim.OutboundTarget{ID: "user_123", Kind: uvim.TargetUser},
		Text:   "hello",
	})
	if err == nil || err.Error() != "lark send: user target id must be an Open ID" {
		t.Fatalf("Send() error = %v", err)
	}
}
