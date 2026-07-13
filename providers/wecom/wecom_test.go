package wecom

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/conformance"
)

func TestProviderConformanceShape(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret", Now: func() time.Time { return time.Unix(1, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	caps := provider.Capabilities()
	if !caps.Inbound || !caps.Outbound || !caps.DownloadResource {
		t.Fatalf("capabilities = %+v", caps)
	}
	if provider.ID() != "wecom" {
		t.Fatalf("ID = %q", provider.ID())
	}
	conformance.AssertProviderMetadata(t, provider)
}

func TestDecodeMessageFile(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	event, ok := provider.decodeMessage(frame{
		Cmd:     cmdCallback,
		Headers: headers{ReqID: "req-1"},
		Body: map[string]any{
			"msgid":    "msg-1",
			"msgtype":  "file",
			"chattype": "group",
			"chatid":   "chat-1",
			"from":     map[string]any{"userid": "u1"},
			"file":     map[string]any{"file_name": "log.txt", "url": "https://download.test/file", "aeskey": "secret"},
		},
	})
	if !ok {
		t.Fatal("decode ok = false")
	}
	if event.Type != uvim.EventMessageCreate || event.Channel.Type != uvim.ChannelGroup {
		t.Fatalf("event = %+v", event)
	}
	if event.Referrer.Target == nil || event.Referrer.Target.ID != "chat-1" || event.Referrer.Target.Kind != uvim.TargetGroup {
		t.Fatalf("reply target = %+v", event.Referrer.Target)
	}
	if len(event.Message.Resources) != 1 || event.Message.Resources[0].Name != "log.txt" {
		t.Fatalf("resources = %+v", event.Message.Resources)
	}
	if got := event.Sanitized().Message.Resources[0].URL; got != "" {
		t.Fatalf("sanitized URL = %q", got)
	}
}

func TestDecodeMessageIgnoresKeyOnlyAttachment(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	event, ok := provider.decodeMessage(frame{
		Cmd:     cmdCallback,
		Headers: headers{ReqID: "req-1"},
		Body: map[string]any{
			"msgid":    "msg-1",
			"msgtype":  "file",
			"chattype": "single",
			"from":     map[string]any{"userid": "u1"},
			"file":     map[string]any{"file_name": "log.txt", "media_id": "media-key"},
		},
	})
	if !ok {
		t.Fatal("decode ok = false")
	}
	if len(event.Message.Resources) != 0 {
		t.Fatalf("key-only resource should not be emitted: %+v", event.Message.Resources)
	}
}

func TestDecodeAESKeyAcceptsRawBase64(t *testing.T) {
	key := "0123456789abcdef0123456789abcdef"
	encoded := "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY"
	got, err := DecodeAESKey(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != key {
		t.Fatalf("key = %q", string(got))
	}
}

func TestConformanceNonNetworkParts(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	health := provider.Health(nil)
	if health.Provider != "wecom" {
		t.Fatalf("health = %+v", health)
	}
	if provider.Capabilities().Inbound == false {
		t.Fatal("provider must declare inbound")
	}
	conformance.AssertProviderMetadata(t, provider)
}

func TestSendRejectsUnsupportedResources(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(nil, uvim.OutboundMessage{
		Provider:  "wecom",
		ChannelID: "chat",
		Text:      "file",
		Resources: []uvim.ResourceRef{{Kind: uvim.ElementFile, InternalURL: "internal://r1"}},
	})
	if err == nil {
		t.Fatal("Send() error = nil, want unsupported resource error")
	}
}

func TestProactiveSendUsesMarkdownForDirectUser(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	conn := &sendTestConn{}
	conn.onWrite = func(raw []byte) {
		var sent frame
		if err := json.Unmarshal(raw, &sent); err != nil {
			t.Error(err)
			return
		}
		conn.sent = sent
		code := 0
		provider.resolvePending(sent.Headers.ReqID, frame{Headers: sent.Headers, ErrCode: &code})
	}
	provider.activeMu.Lock()
	provider.activeConn = conn
	provider.activeWrite = &sync.Mutex{}
	provider.activeMu.Unlock()

	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Provider: "wecom",
		Target:   &uvim.OutboundTarget{ID: "ChenJunHao", Kind: uvim.TargetUser},
		Text:     "upgrade complete",
	})
	if err != nil {
		t.Fatal(err)
	}
	if conn.sent.Cmd != cmdSend || conn.sent.Body["chatid"] != "ChenJunHao" || conn.sent.Body["msgtype"] != "markdown" {
		t.Fatalf("sent frame = %+v", conn.sent)
	}
	markdown, _ := conn.sent.Body["markdown"].(map[string]any)
	if markdown["content"] != "upgrade complete" {
		t.Fatalf("markdown = %+v", markdown)
	}
}

func TestSendReturnsProviderAckError(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	conn := &sendTestConn{}
	conn.onWrite = func(raw []byte) {
		var sent frame
		_ = json.Unmarshal(raw, &sent)
		code := 40058
		provider.resolvePending(sent.Headers.ReqID, frame{Headers: sent.Headers, ErrCode: &code, ErrMsg: "invalid msgtype"})
	}
	provider.activeMu.Lock()
	provider.activeConn = conn
	provider.activeWrite = &sync.Mutex{}
	provider.activeMu.Unlock()

	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "u1", ChannelType: uvim.ChannelDirect, Text: "hello"})
	if err == nil || !strings.Contains(err.Error(), "errcode=40058") || !strings.Contains(err.Error(), "invalid msgtype") {
		t.Fatalf("Send() error = %v", err)
	}
}

type sendTestConn struct {
	onWrite func([]byte)
	sent    frame
}

func (c *sendTestConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("not implemented")
}
func (c *sendTestConn) WriteMessage(_ int, raw []byte) error {
	if c.onWrite != nil {
		c.onWrite(raw)
	}
	return nil
}
func (c *sendTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *sendTestConn) SetWriteDeadline(time.Time) error { return nil }
func (c *sendTestConn) Close() error                     { return nil }
