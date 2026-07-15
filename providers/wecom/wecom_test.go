package wecom

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/conformance"
)

func TestProviderConformanceShape(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: &uvim.ResourceStore{Dir: t.TempDir()}, Now: func() time.Time { return time.Unix(1, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	caps := provider.Capabilities()
	if !caps.Inbound || !caps.Outbound || !caps.UploadResource || !caps.DownloadResource {
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

func TestSendRejectsMixedTextAndResource(t *testing.T) {
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

func TestSendResourceReplyUploadsChunksAndSendsMedia(t *testing.T) {
	data := bytes.Repeat([]byte("x"), uploadChunkSize+1)
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewReader(data), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	conn := &sendTestConn{}
	var frames []frame
	conn.onWrite = func(raw []byte) {
		var sent frame
		if err := json.Unmarshal(raw, &sent); err != nil {
			t.Error(err)
			return
		}
		frames = append(frames, sent)
		code := 0
		ack := frame{Headers: sent.Headers, ErrCode: &code}
		switch sent.Cmd {
		case cmdUploadInit:
			ack.Body = map[string]any{"upload_id": "upload-1"}
		case cmdUploadFinish:
			ack.Body = map[string]any{"media_id": "media-1"}
		}
		provider.resolvePending(sent.Headers.ReqID, ack)
	}
	activateSendTestConn(provider, conn)

	result, err := provider.Send(context.Background(), uvim.OutboundMessage{
		Provider:  "wecom",
		ChannelID: "chat-1",
		Resources: []uvim.ResourceRef{ref},
		Referrer:  uvim.Referrer{MessageID: "msg-1", ReplyToken: "reply-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "reply-1" {
		t.Fatalf("result = %+v", result)
	}
	wantCommands := []string{cmdUploadInit, cmdUploadChunk, cmdUploadChunk, cmdUploadFinish, cmdRespond}
	if len(frames) != len(wantCommands) {
		t.Fatalf("frames = %+v", frames)
	}
	for i, command := range wantCommands {
		if frames[i].Cmd != command {
			t.Fatalf("frame %d command = %q, want %q", i, frames[i].Cmd, command)
		}
	}
	if frames[0].Body["filename"] != "report.txt" || int(frames[0].Body["total_chunks"].(float64)) != 2 {
		t.Fatalf("init body = %+v", frames[0].Body)
	}
	if frames[0].Body["md5"] != fmt.Sprintf("%x", md5.Sum(data)) {
		t.Fatalf("init md5 = %v", frames[0].Body["md5"])
	}
	var uploaded []byte
	for index, sent := range frames[1:3] {
		if got := int(sent.Body["chunk_index"].(float64)); got != index+1 {
			t.Fatalf("chunk index = %d, want %d", got, index+1)
		}
		decoded, err := base64.StdEncoding.DecodeString(sent.Body["base64_data"].(string))
		if err != nil {
			t.Fatal(err)
		}
		uploaded = append(uploaded, decoded...)
	}
	if !bytes.Equal(uploaded, data) {
		t.Fatal("uploaded chunks do not match source data")
	}
	media := frames[4].Body[uvim.ElementFile].(map[string]any)
	if frames[4].Headers.ReqID != "reply-1" || frames[4].Body["msgtype"] != uvim.ElementFile || media["media_id"] != "media-1" {
		t.Fatalf("media frame = %+v", frames[4])
	}
}

func TestWeComUploadChunkCountBoundaries(t *testing.T) {
	for _, test := range []struct {
		size int
		want int
		ok   bool
	}{
		{size: 0, ok: false},
		{size: 1, want: 1, ok: true},
		{size: uploadChunkSize, want: 1, ok: true},
		{size: uploadChunkSize + 1, want: 2, ok: true},
		{size: uploadChunkSize * uploadMaxChunks, want: uploadMaxChunks, ok: true},
		{size: uploadChunkSize*uploadMaxChunks + 1, ok: false},
	} {
		got, err := wecomUploadChunkCount(test.size)
		if test.ok && (err != nil || got != test.want) {
			t.Fatalf("size %d: chunks=%d error=%v, want %d", test.size, got, err, test.want)
		}
		if !test.ok && err == nil {
			t.Fatalf("size %d: error=nil", test.size)
		}
	}
}

func TestWeComMediaKindMapsProtocolAudioToVoice(t *testing.T) {
	if got := wecomMediaKind(uvim.ElementAudio); got != mediaVoice {
		t.Fatalf("wecomMediaKind(audio) = %q, want %q", got, mediaVoice)
	}
}

func TestSendResourceReturnsUploadStageErrors(t *testing.T) {
	for _, failCommand := range []string{cmdUploadInit, cmdUploadChunk, cmdUploadFinish, cmdRespond} {
		t.Run(failCommand, func(t *testing.T) {
			store := &uvim.ResourceStore{Dir: t.TempDir()}
			ref, err := store.Save(context.Background(), strings.NewReader("file"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "file.txt"})
			if err != nil {
				t.Fatal(err)
			}
			provider, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: store})
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
				code := 0
				ack := frame{Headers: sent.Headers, ErrCode: &code}
				if sent.Cmd == cmdUploadInit {
					ack.Body = map[string]any{"upload_id": "upload-1"}
				}
				if sent.Cmd == cmdUploadFinish {
					ack.Body = map[string]any{"media_id": "media-1"}
				}
				if sent.Cmd == failCommand {
					failure := 50001
					ack.ErrCode = &failure
					ack.ErrMsg = "stage failed"
				}
				provider.resolvePending(sent.Headers.ReqID, ack)
			}
			activateSendTestConn(provider, conn)
			_, err = provider.Send(context.Background(), uvim.OutboundMessage{
				ChannelID: "chat-1",
				Resources: []uvim.ResourceRef{ref},
				Referrer:  uvim.Referrer{MessageID: "msg-1", ReplyToken: "reply-1"},
			})
			if err == nil || !strings.Contains(err.Error(), "stage failed") {
				t.Fatalf("Send() error = %v", err)
			}
		})
	}
}

func TestSendResourceProactivelyUsesTarget(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), strings.NewReader("file"), uvim.ResourceRef{Kind: uvim.ElementImage, Name: "chart.png"})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	conn := &sendTestConn{}
	var last frame
	conn.onWrite = func(raw []byte) {
		var sent frame
		if err := json.Unmarshal(raw, &sent); err != nil {
			t.Error(err)
			return
		}
		last = sent
		code := 0
		ack := frame{Headers: sent.Headers, ErrCode: &code}
		if sent.Cmd == cmdUploadInit {
			ack.Body = map[string]any{"upload_id": "upload-1"}
		}
		if sent.Cmd == cmdUploadFinish {
			ack.Body = map[string]any{"media_id": "media-1"}
		}
		provider.resolvePending(sent.Headers.ReqID, ack)
	}
	activateSendTestConn(provider, conn)

	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Provider:  "wecom",
		Target:    &uvim.OutboundTarget{ID: "user-1", Kind: uvim.TargetUser},
		Resources: []uvim.ResourceRef{ref},
	})
	if err != nil {
		t.Fatal(err)
	}
	image := last.Body[uvim.ElementImage].(map[string]any)
	if last.Cmd != cmdSend || last.Body["chatid"] != "user-1" || last.Body["msgtype"] != uvim.ElementImage || image["media_id"] != "media-1" {
		t.Fatalf("media frame = %+v", last)
	}
}

func TestUploadResourceRejectsEmptyAndOversizeBeforeWriting(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
	}{
		{name: "empty"},
		{name: "oversize", data: make([]byte, uploadChunkSize*uploadMaxChunks+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &uvim.ResourceStore{Dir: t.TempDir(), MaxBytes: int64(len(test.data)) + 1}
			ref, err := store.Save(context.Background(), bytes.NewReader(test.data), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "large.bin"})
			if err != nil {
				t.Fatal(err)
			}
			provider, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: store})
			if err != nil {
				t.Fatal(err)
			}
			conn := &sendTestConn{}
			activateSendTestConn(provider, conn)
			_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "chat", Resources: []uvim.ResourceRef{ref}})
			if err == nil {
				t.Fatal("Send() error = nil")
			}
			if conn.sent.Cmd != "" {
				t.Fatalf("unexpected frame = %+v", conn.sent)
			}
		})
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

func activateSendTestConn(provider *Provider, conn *sendTestConn) {
	provider.activeMu.Lock()
	provider.activeConn = conn
	provider.activeWrite = &sync.Mutex{}
	provider.activeMu.Unlock()
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
