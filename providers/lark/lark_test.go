package lark

import (
	"encoding/json"
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
	provider, err := New(Config{AppID: "app", AppSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	conformance.AssertProviderMetadata(t, provider)
	if provider.Capabilities().UploadResource {
		t.Fatal("UploadResource must stay false until provider supports upload")
	}
}

func TestSendRejectsUnsupportedResources(t *testing.T) {
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
