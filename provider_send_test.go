package uvim_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/dingtalk"
	"github.com/hengshi/uv-im-connector/providers/discord"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
	"github.com/hengshi/uv-im-connector/providers/kook"
	"github.com/hengshi/uv-im-connector/providers/line"
	"github.com/hengshi/uv-im-connector/providers/matrix"
	"github.com/hengshi/uv-im-connector/providers/onebot"
	"github.com/hengshi/uv-im-connector/providers/qq"
	"github.com/hengshi/uv-im-connector/providers/qqguild"
	"github.com/hengshi/uv-im-connector/providers/slack"
	"github.com/hengshi/uv-im-connector/providers/telegram"
	"github.com/hengshi/uv-im-connector/providers/wechatofficial"
	"github.com/hengshi/uv-im-connector/providers/whatsapp"
	"github.com/hengshi/uv-im-connector/providers/zulip"
)

func TestSlackSendUsesBearerJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/chat.postMessage" {
			t.Fatalf("path = %s", req.URL.Path)
		}
		if got := req.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("authorization = %q", got)
		}
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["channel"] != "c1" || body["text"] != "hello" || body["thread_ts"] != "m1" {
			t.Fatalf("body = %+v", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "channel": "c1", "ts": "m2"})
	}))
	defer server.Close()
	provider, err := slack.New(slack.Config{BaseURL: server.URL, Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Send(context.Background(), uvim.OutboundMessage{Provider: "slack", ChannelID: "c1", Text: "hello", Referrer: uvim.Referrer{MessageID: "m1"}}); err != nil {
		t.Fatal(err)
	}
}

func TestSlackSendUploadsInternalResource(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/api/conversations.open":
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if req.Header.Get("Authorization") != "Bearer token" || body["users"] != "U1" {
				t.Fatalf("conversation auth=%q body=%v", req.Header.Get("Authorization"), body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "channel": map[string]string{"id": "D1"}})
		case "/api/files.getUploadURLExternal":
			if req.Header.Get("Authorization") != "Bearer token" || req.FormValue("filename") != "report.txt" || req.FormValue("length") != "6" {
				t.Fatalf("init auth=%q form=%v", req.Header.Get("Authorization"), req.Form)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "upload_url": server.URL + "/upload", "file_id": "F1"})
		case "/upload":
			data, readErr := io.ReadAll(req.Body)
			if readErr != nil || string(data) != "report" || req.Header.Get("Authorization") != "" {
				t.Fatalf("upload data=%q auth=%q err=%v", data, req.Header.Get("Authorization"), readErr)
			}
		case "/api/files.completeUploadExternal":
			if req.Header.Get("Authorization") != "Bearer token" {
				t.Fatalf("complete authorization = %q", req.Header.Get("Authorization"))
			}
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["channel_id"] != "D1" || body["thread_ts"] != "T1" || body["initial_comment"] != "caption" {
				t.Fatalf("complete body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "files": []map[string]string{{"id": "F1"}}})
		default:
			t.Fatalf("unexpected path = %s", req.URL.Path)
		}
	}))
	defer server.Close()
	provider, err := slack.New(slack.Config{BaseURL: server.URL, Token: "token", ResourceStore: store, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Send(context.Background(), uvim.OutboundMessage{Target: &uvim.OutboundTarget{ID: "U1", Kind: uvim.TargetUser}, Text: "caption", Resources: []uvim.ResourceRef{ref}, Referrer: uvim.Referrer{ThreadID: "T1"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "F1" {
		t.Fatalf("result = %+v", result)
	}
}

func TestDiscordSendUsesBotAuthorization(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/v10/channels/c1/messages" {
			t.Fatalf("path = %s", req.URL.Path)
		}
		if got := req.Header.Get("Authorization"); got != "Bot token" {
			t.Fatalf("authorization = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "m1"})
	}))
	defer server.Close()
	provider, err := discord.New(discord.Config{BaseURL: server.URL, Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Send(context.Background(), uvim.OutboundMessage{Provider: "discord", ChannelID: "c1", ChannelType: uvim.ChannelDirect, Text: "hello"}); err != nil {
		t.Fatal(err)
	}
}

func TestDiscordSendUploadsInternalResource(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/v10/channels/c1/messages" {
			t.Fatalf("path = %s", req.URL.Path)
		}
		if got := req.Header.Get("Authorization"); got != "Bot token" {
			t.Fatalf("authorization = %q", got)
		}
		if err := req.ParseMultipartForm(20 << 20); err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(req.FormValue("payload_json")), &payload); err != nil {
			t.Fatal(err)
		}
		if payload["content"] != "caption" {
			t.Fatalf("payload = %+v", payload)
		}
		file, header, err := req.FormFile("files[0]")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		data, _ := io.ReadAll(file)
		if header.Filename != "report.txt" || string(data) != "report" {
			t.Fatalf("filename=%q data=%q", header.Filename, data)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "m1"})
	}))
	defer server.Close()
	provider, err := discord.New(discord.Config{BaseURL: server.URL, Token: "token", ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "c1", Text: "caption", Resources: []uvim.ResourceRef{ref}}); err != nil {
		t.Fatal(err)
	}
}

func TestDiscordSendAcceptsPrefixedBotAuthorization(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if got := req.Header.Get("Authorization"); got != "Bot token" {
			t.Fatalf("authorization = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "m1"})
	}))
	defer server.Close()
	provider, err := discord.New(discord.Config{BaseURL: server.URL, Token: "Bot token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Send(context.Background(), uvim.OutboundMessage{Provider: "discord", ChannelID: "c1", Text: "hello"}); err != nil {
		t.Fatal(err)
	}
}

func TestDiscordProactiveUserCreatesDMThenSends(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		paths = append(paths, req.URL.Path)
		if got := req.Header.Get("Authorization"); got != "Bot token" {
			t.Fatalf("authorization = %q", got)
		}
		switch req.URL.Path {
		case "/api/v10/users/@me/channels":
			var body map[string]any
			_ = json.NewDecoder(req.Body).Decode(&body)
			if body["recipient_id"] != "u1" {
				t.Fatalf("create dm body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "dm1"})
		case "/api/v10/channels/dm1/messages":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "m1"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := discord.New(discord.Config{BaseURL: server.URL, Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Send(context.Background(), targetMessage(uvim.TargetUser, "u1"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(paths, ",") != "/api/v10/users/@me/channels,/api/v10/channels/dm1/messages" || result.MessageID != "m1" {
		t.Fatalf("paths=%v result=%+v", paths, result)
	}
}

func TestKOOKSendUploadsInternalResource(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bot token" {
			t.Fatalf("authorization = %q", req.Header.Get("Authorization"))
		}
		switch req.URL.Path {
		case "/api/v3/asset/create":
			if err := req.ParseMultipartForm(110 << 20); err != nil {
				t.Fatal(err)
			}
			file, header, err := req.FormFile("file")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			data, _ := io.ReadAll(file)
			if header.Filename != "report.txt" || string(data) != "report" {
				t.Fatalf("filename=%q data=%q", header.Filename, data)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]string{"url": "https://assets.kook/report"}})
		case "/api/v3/message/create":
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["target_id"] != "channel1" || body["type"] != float64(10) {
				t.Fatalf("message body = %+v", body)
			}
			var cards []map[string]any
			if err := json.Unmarshal([]byte(body["content"].(string)), &cards); err != nil {
				t.Fatal(err)
			}
			modules, _ := cards[0]["modules"].([]any)
			module, _ := modules[0].(map[string]any)
			if module["type"] != "file" || module["src"] != "https://assets.kook/report" || module["title"] != "report.txt" {
				t.Fatalf("cards = %+v", cards)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]string{"msg_id": "message1"}})
		default:
			t.Fatalf("unexpected path = %s", req.URL.Path)
		}
	}))
	defer server.Close()
	provider, err := kook.New(kook.Config{BaseURL: server.URL, Token: "token", ResourceStore: store, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Send(context.Background(), uvim.OutboundMessage{Target: &uvim.OutboundTarget{ID: "channel1", Kind: uvim.TargetChannel}, Resources: []uvim.ResourceRef{ref}})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "message1" {
		t.Fatalf("result = %+v", result)
	}
}

func TestMatrixSendUploadsInternalResource(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	var uploadSeen, sendSeen bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if got := req.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case req.Method == http.MethodPost && req.URL.Path == "/_matrix/media/v3/upload":
			uploadSeen = true
			if req.URL.Query().Get("filename") != "report.txt" || req.Header.Get("Content-Type") != "text/plain" {
				t.Fatalf("upload request = %s %s content-type=%q", req.URL.Path, req.URL.RawQuery, req.Header.Get("Content-Type"))
			}
			data, readErr := io.ReadAll(req.Body)
			if readErr != nil || string(data) != "report" {
				t.Fatalf("upload data=%q err=%v", data, readErr)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"content_uri": "mxc://example.org/media1"})
		case req.Method == http.MethodPut && strings.Contains(req.URL.Path, "/send/m.room.message/"):
			sendSeen = true
			if !strings.Contains(req.URL.Path, "rooms/!room:example.org/") {
				t.Fatalf("send path = %q", req.URL.Path)
			}
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["msgtype"] != "m.file" || body["body"] != "report.txt" || body["url"] != "mxc://example.org/media1" {
				t.Fatalf("send body = %+v", body)
			}
			relation, _ := body["m.relates_to"].(map[string]any)
			reply, _ := relation["m.in_reply_to"].(map[string]any)
			if reply["event_id"] != "$original" {
				t.Fatalf("reply relation = %+v", relation)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"event_id": "$sent"})
		default:
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
		}
	}))
	defer server.Close()
	provider, err := matrix.New(matrix.Config{BaseURL: server.URL, Token: "token", ResourceStore: store, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Send(context.Background(), uvim.OutboundMessage{
		ID:        "txn1",
		ChannelID: "!room:example.org",
		Resources: []uvim.ResourceRef{ref},
		Referrer:  uvim.Referrer{MessageID: "$original"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !uploadSeen || !sendSeen || result.MessageID != "$sent" {
		t.Fatalf("upload=%v send=%v result=%+v", uploadSeen, sendSeen, result)
	}
}

func TestWhatsAppSendRequiresPhoneNumberID(t *testing.T) {
	provider, err := whatsapp.New(whatsapp.Config{BaseURL: "http://127.0.0.1", Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Send(context.Background(), uvim.OutboundMessage{Provider: "whatsapp", ChannelID: "u1", Text: "hello"}); err == nil {
		t.Fatal("Send() error = nil, want phone_number_id error")
	}
}

func TestWeChatOfficialSendUploadsInternalMedia(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("image"), uvim.ResourceRef{Kind: uvim.ElementImage, Name: "chart.png", MIME: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/cgi-bin/media/upload":
			if req.URL.Query().Get("access_token") != "token" || req.URL.Query().Get("type") != "image" {
				t.Fatalf("upload query = %v", req.URL.Query())
			}
			if err := req.ParseMultipartForm(12 << 20); err != nil {
				t.Fatal(err)
			}
			file, header, err := req.FormFile("media")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			data, _ := io.ReadAll(file)
			if header.Filename != "chart.png" || string(data) != "image" {
				t.Fatalf("filename=%q data=%q", header.Filename, data)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"type": "image", "media_id": "MEDIA1"})
		case "/cgi-bin/message/custom/send":
			if req.URL.Query().Get("access_token") != "token" {
				t.Fatalf("send query = %v", req.URL.Query())
			}
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			image, _ := body["image"].(map[string]any)
			if body["touser"] != "user1" || body["msgtype"] != "image" || image["media_id"] != "MEDIA1" {
				t.Fatalf("send body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"errcode": 0, "msgid": "MESSAGE1"})
		default:
			t.Fatalf("unexpected path = %s", req.URL.Path)
		}
	}))
	defer server.Close()
	provider, err := wechatofficial.New(wechatofficial.Config{BaseURL: server.URL, Token: "token", ResourceStore: store, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Send(context.Background(), uvim.OutboundMessage{Target: &uvim.OutboundTarget{ID: "user1", Kind: uvim.TargetUser}, Resources: []uvim.ResourceRef{ref}})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "MESSAGE1" {
		t.Fatalf("result = %+v", result)
	}
}

func TestWhatsAppDownloadMediaLookupPreservesPrefixedAuthorization(t *testing.T) {
	var sawMetadata bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/media1":
			sawMetadata = true
			if got := req.Header.Get("Authorization"); got != "Bearer token" {
				t.Fatalf("metadata authorization = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"url": "http://" + req.Host + "/download", "mime_type": "image/png"})
		case "/download":
			if got := req.Header.Get("Authorization"); got != "Bearer token" {
				t.Fatalf("download authorization = %q", got)
			}
			_, _ = w.Write([]byte("image"))
		default:
			t.Fatalf("unexpected path = %s", req.URL.Path)
		}
	}))
	defer server.Close()
	provider, err := whatsapp.New(whatsapp.Config{BaseURL: server.URL, Token: "Bearer token"})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := provider.Download(context.Background(), uvim.ResourceDownloadRequest{
		Dir: filepath.Join(t.TempDir(), "resources"),
		Resource: uvim.ResourceRef{
			Provider: "whatsapp",
			Kind:     uvim.ElementImage,
			Key:      "media1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawMetadata || ref.InternalURL == "" {
		t.Fatalf("download did not resolve media: sawMetadata=%v ref=%+v", sawMetadata, ref)
	}
}

func TestWhatsAppSendUploadsInternalResource(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("authorization = %q", req.Header.Get("Authorization"))
		}
		switch req.URL.Path {
		case "/phone1/media":
			if err := req.ParseMultipartForm(110 << 20); err != nil {
				t.Fatal(err)
			}
			if req.FormValue("messaging_product") != "whatsapp" {
				t.Fatalf("messaging_product = %q", req.FormValue("messaging_product"))
			}
			file, header, err := req.FormFile("file")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			data, _ := io.ReadAll(file)
			if header.Filename != "report.txt" || string(data) != "report" {
				t.Fatalf("filename=%q data=%q", header.Filename, data)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "MEDIA1"})
		case "/phone1/messages":
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			document, _ := body["document"].(map[string]any)
			contextBody, _ := body["context"].(map[string]any)
			if body["type"] != "document" || body["to"] != "user1" || document["id"] != "MEDIA1" || document["filename"] != "report.txt" || contextBody["message_id"] != "original" {
				t.Fatalf("message body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"messages": []map[string]string{{"id": "MESSAGE1"}}})
		default:
			t.Fatalf("unexpected path = %s", req.URL.Path)
		}
	}))
	defer server.Close()
	provider, err := whatsapp.New(whatsapp.Config{BaseURL: server.URL, Token: "token", PhoneNumberID: "phone1", ResourceStore: store, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "user1", Resources: []uvim.ResourceRef{ref}, Referrer: uvim.Referrer{MessageID: "original"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "MESSAGE1" {
		t.Fatalf("result = %+v", result)
	}
}

func TestTelegramSendUsesTokenPathWithoutBearer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/bottoken/sendMessage" {
			t.Fatalf("path = %s", req.URL.Path)
		}
		if got := req.Header.Get("Authorization"); got != "" {
			t.Fatalf("authorization = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 42}})
	}))
	defer server.Close()
	provider, err := telegram.New(telegram.Config{BaseURL: server.URL, Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Send(context.Background(), uvim.OutboundMessage{Provider: "telegram", ChannelID: "c1", Text: "hello"}); err != nil {
		t.Fatal(err)
	}
}

func TestZulipSendUsesFormEncoding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/v1/messages" {
			t.Fatalf("path = %s", req.URL.Path)
		}
		if got := req.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/x-www-form-urlencoded") {
			t.Fatalf("content-type = %q", got)
		}
		raw, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		if body := string(raw); !strings.Contains(body, "type=stream") || !strings.Contains(body, "topic=general") {
			t.Fatalf("body = %q", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"result": "success", "msg": "", "id": 42})
	}))
	defer server.Close()
	provider, err := zulip.New(zulip.Config{BaseURL: server.URL, Token: "Basic token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Send(context.Background(), uvim.OutboundMessage{
		Provider: "zulip",
		Target:   &uvim.OutboundTarget{Kind: uvim.TargetGroup, ID: "c1"},
		Text:     "hello",
		Referrer: uvim.Referrer{ThreadID: "general"},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestZulipSendUploadsInternalResource(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Basic credentials" {
			t.Fatalf("authorization = %q", req.Header.Get("Authorization"))
		}
		switch req.URL.Path {
		case "/api/v1/user_uploads":
			if err := req.ParseMultipartForm(30 << 20); err != nil {
				t.Fatal(err)
			}
			file, header, err := req.FormFile("filename")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			data, _ := io.ReadAll(file)
			if header.Filename != "report.txt" || string(data) != "report" {
				t.Fatalf("filename=%q data=%q", header.Filename, data)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"result": "success", "url": "/user_uploads/report", "filename": "report.txt"})
		case "/api/v1/messages":
			if err := req.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if req.FormValue("type") != "stream" || req.FormValue("to") != "engineering" || !strings.Contains(req.FormValue("content"), "[report.txt](/user_uploads/report)") {
				t.Fatalf("message form = %v", req.Form)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"result": "success", "id": 42})
		default:
			t.Fatalf("unexpected path = %s", req.URL.Path)
		}
	}))
	defer server.Close()
	provider, err := zulip.New(zulip.Config{BaseURL: server.URL, Token: "Basic credentials", ResourceStore: store, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Send(context.Background(), uvim.OutboundMessage{Target: &uvim.OutboundTarget{ID: "engineering", Kind: uvim.TargetGroup}, Resources: []uvim.ResourceRef{ref}})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "42" {
		t.Fatalf("result = %+v", result)
	}
}

func TestProviderOutboundTargetRequestShapes(t *testing.T) {
	tests := []struct {
		name  string
		build func() (httpchannel.Request, error)
		path  string
		check func(*testing.T, httpchannel.Request)
	}{
		{name: "dingtalk group", path: "/robot/send?access_token=token", build: func() (httpchannel.Request, error) {
			return dingtalk.Send(targetMessage(uvim.TargetGroup, "g1"), httpchannel.Config{Token: "token"})
		}},
		{name: "dingtalk session reply", path: "/robot/sendBySession?session=s1", build: func() (httpchannel.Request, error) {
			msg := targetMessage(uvim.TargetUser, "u1")
			msg.Referrer.ReplyToken = "https://oapi.dingtalk.com/robot/sendBySession?session=s1"
			return dingtalk.Send(msg, httpchannel.Config{BaseURL: "https://oapi.dingtalk.com"})
		}},
		{name: "discord channel", path: "/api/v10/channels/c1/messages", build: func() (httpchannel.Request, error) {
			return discord.Send(targetMessage(uvim.TargetChannel, "c1"), httpchannel.Config{})
		}},
		{name: "kook direct", path: "/api/v3/direct-message/create", build: func() (httpchannel.Request, error) {
			return kook.Send(targetMessage(uvim.TargetUser, "u1"), httpchannel.Config{})
		}, check: checkBodyValue("target_id", "u1")},
		{name: "line push", path: "/v2/bot/message/push", build: func() (httpchannel.Request, error) {
			return line.Send(targetMessage(uvim.TargetUser, "u1"), httpchannel.Config{})
		}, check: checkBodyValue("to", "u1")},
		{name: "line reply", path: "/v2/bot/message/reply", build: func() (httpchannel.Request, error) {
			msg := targetMessage(uvim.TargetUser, "u1")
			msg.Referrer.ReplyToken = "reply-token"
			return line.Send(msg, httpchannel.Config{})
		}, check: checkBodyValue("replyToken", "reply-token")},
		{name: "matrix room", path: "/_matrix/client/v3/rooms/%21room:example/send/m.room.message/", build: func() (httpchannel.Request, error) {
			msg := targetMessage(uvim.TargetConversation, "!room:example")
			msg.Referrer.MessageID = "$event"
			return matrix.Send(msg, httpchannel.Config{})
		}, check: func(t *testing.T, req httpchannel.Request) {
			body := req.Body.(map[string]any)
			relates := body["m.relates_to"].(map[string]any)
			reply := relates["m.in_reply_to"].(map[string]string)
			if reply["event_id"] != "$event" {
				t.Fatalf("body = %+v", body)
			}
		}},
		{name: "onebot direct", path: "/send_msg", build: func() (httpchannel.Request, error) {
			return onebot.Send(targetMessage(uvim.TargetUser, "1001"), httpchannel.Config{})
		}, check: checkBodyValue("user_id", "1001")},
		{name: "onebot group", path: "/send_msg", build: func() (httpchannel.Request, error) {
			return onebot.Send(targetMessage(uvim.TargetGroup, "2001"), httpchannel.Config{})
		}, check: checkBodyValue("group_id", "2001")},
		{name: "onebot reply", path: "/send_msg", build: func() (httpchannel.Request, error) {
			msg := targetMessage(uvim.TargetUser, "1001")
			msg.Referrer.MessageID = "42"
			return onebot.Send(msg, httpchannel.Config{})
		}, check: func(t *testing.T, req httpchannel.Request) {
			body := req.Body.(map[string]any)
			segments := body["message"].([]map[string]any)
			if segments[0]["type"] != "reply" || segments[0]["data"].(map[string]string)["id"] != "42" {
				t.Fatalf("body = %+v", body)
			}
		}},
		{name: "qq group", path: "/send_msg", build: func() (httpchannel.Request, error) {
			return qq.Send(targetMessage(uvim.TargetGroup, "2001"), httpchannel.Config{})
		}, check: checkBodyValue("group_id", "2001")},
		{name: "qq c2c", path: "/v2/users/u1/messages", build: func() (httpchannel.Request, error) {
			return qqguild.Send(targetMessage(uvim.TargetUser, "u1"), httpchannel.Config{})
		}},
		{name: "qq group openid", path: "/v2/groups/g1/messages", build: func() (httpchannel.Request, error) {
			return qqguild.Send(targetMessage(uvim.TargetGroup, "g1"), httpchannel.Config{})
		}},
		{name: "qq guild channel", path: "/channels/c1/messages", build: func() (httpchannel.Request, error) {
			return qqguild.Send(targetMessage(uvim.TargetChannel, "c1"), httpchannel.Config{})
		}},
		{name: "slack user", path: "/api/chat.postMessage", build: func() (httpchannel.Request, error) {
			return slack.Send(targetMessage(uvim.TargetUser, "U1"), httpchannel.Config{})
		}, check: checkBodyValue("channel", "U1")},
		{name: "telegram chat", path: "/bottoken/sendMessage", build: func() (httpchannel.Request, error) {
			return telegram.Send(targetMessage(uvim.TargetUser, "1001"), httpchannel.Config{Token: "token"})
		}, check: checkBodyValue("chat_id", "1001")},
		{name: "wechat official user", path: "/cgi-bin/message/custom/send?access_token=token", build: func() (httpchannel.Request, error) {
			return wechatofficial.Send(targetMessage(uvim.TargetUser, "open1"), httpchannel.Config{Token: "token"})
		}, check: checkBodyValue("touser", "open1")},
		{name: "whatsapp user", path: "/phone1/messages", build: func() (httpchannel.Request, error) {
			return whatsapp.Send(targetMessage(uvim.TargetUser, "8613800000000"), httpchannel.Config{}, "phone1")
		}, check: func(t *testing.T, req httpchannel.Request) {
			body := req.Body.(map[string]any)
			if body["to"] != "8613800000000" || body["recipient_type"] != "individual" {
				t.Fatalf("body = %+v", body)
			}
		}},
		{name: "whatsapp group reply", path: "/phone1/messages", build: func() (httpchannel.Request, error) {
			msg := targetMessage(uvim.TargetGroup, "group1")
			msg.Referrer.MessageID = "wamid.1"
			return whatsapp.Send(msg, httpchannel.Config{}, "phone1")
		}, check: func(t *testing.T, req httpchannel.Request) {
			body := req.Body.(map[string]any)
			context := body["context"].(map[string]string)
			if body["recipient_type"] != "group" || context["message_id"] != "wamid.1" {
				t.Fatalf("body = %+v", body)
			}
		}},
		{name: "zulip direct", path: "/api/v1/messages", build: func() (httpchannel.Request, error) {
			return zulip.Send(targetMessage(uvim.TargetUser, "ada@example.test"), httpchannel.Config{})
		}, check: func(t *testing.T, req httpchannel.Request) {
			if req.Form.Get("type") != "private" || req.Form.Get("to") != "ada@example.test" {
				t.Fatalf("form = %v", req.Form)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := tt.build()
			if err != nil {
				t.Fatal(err)
			}
			if tt.name == "matrix room" {
				if !strings.HasPrefix(req.Path, tt.path) {
					t.Fatalf("path = %q, want prefix %q", req.Path, tt.path)
				}
			} else if req.Path != tt.path {
				t.Fatalf("path = %q, want %q", req.Path, tt.path)
			}
			if tt.check != nil {
				tt.check(t, req)
			}
		})
	}
}

func TestProviderSendResponseParsers(t *testing.T) {
	tests := []struct {
		name       string
		parse      httpchannel.ParseSendResponseFunc
		successRaw string
		wantID     string
		failureRaw string
	}{
		{name: "dingtalk", parse: dingtalk.ParseSendResponse, successRaw: `{"errcode":0,"errmsg":"ok"}`, failureRaw: `{"errcode":310000,"errmsg":"invalid token"}`},
		{name: "discord", parse: discord.ParseSendResponse, successRaw: `{"id":"m1"}`, wantID: "m1", failureRaw: `{"code":50035,"message":"Invalid Form Body"}`},
		{name: "kook", parse: kook.ParseSendResponse, successRaw: `{"code":0,"data":{"msg_id":"m1"}}`, wantID: "m1", failureRaw: `{"code":40000,"message":"invalid target"}`},
		{name: "line", parse: line.ParseSendResponse, successRaw: `{"sentMessages":[{"id":"m1"}]}`, wantID: "m1", failureRaw: `{"message":"invalid user"}`},
		{name: "matrix", parse: matrix.ParseSendResponse, successRaw: `{"event_id":"m1"}`, wantID: "m1", failureRaw: `{"errcode":"M_FORBIDDEN","error":"forbidden"}`},
		{name: "onebot", parse: onebot.ParseSendResponse, successRaw: `{"status":"ok","retcode":0,"data":{"message_id":1}}`, wantID: "1", failureRaw: `{"status":"failed","retcode":100,"wording":"blocked"}`},
		{name: "qq", parse: qq.ParseSendResponse, successRaw: `{"status":"ok","retcode":0,"data":{"message_id":1}}`, wantID: "1", failureRaw: `{"status":"failed","retcode":100,"wording":"blocked"}`},
		{name: "qqguild", parse: qqguild.ParseSendResponse, successRaw: `{"id":"m1"}`, wantID: "m1", failureRaw: `{"code":11255,"message":"invalid request"}`},
		{name: "slack", parse: slack.ParseSendResponse, successRaw: `{"ok":true,"ts":"m1"}`, wantID: "m1", failureRaw: `{"ok":false,"error":"channel_not_found"}`},
		{name: "telegram", parse: telegram.ParseSendResponse, successRaw: `{"ok":true,"result":{"message_id":1}}`, wantID: "1", failureRaw: `{"ok":false,"error_code":403,"description":"bot blocked"}`},
		{name: "wechat-official", parse: wechatofficial.ParseSendResponse, successRaw: `{"errcode":0,"errmsg":"ok","msgid":"m1"}`, wantID: "m1", failureRaw: `{"errcode":45015,"errmsg":"response out of time limit"}`},
		{name: "whatsapp", parse: whatsapp.ParseSendResponse, successRaw: `{"messages":[{"id":"m1"}]}`, wantID: "m1", failureRaw: `{"error":{"code":131047,"message":"re-engagement message"}}`},
		{name: "zulip", parse: zulip.ParseSendResponse, successRaw: `{"result":"success","id":1}`, wantID: "1", failureRaw: `{"result":"error","msg":"invalid email"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.parse([]byte(tt.successRaw))
			if err != nil || got != tt.wantID {
				t.Fatalf("success: id=%q err=%v", got, err)
			}
			if _, err := tt.parse([]byte(tt.failureRaw)); err == nil {
				t.Fatal("failure response accepted")
			}
		})
	}
}

func TestDingTalkSessionReplyRejectsDifferentOrigin(t *testing.T) {
	msg := targetMessage(uvim.TargetUser, "u1")
	msg.Referrer.ReplyToken = "https://attacker.example/robot/sendBySession?session=s1"
	if _, err := dingtalk.Send(msg, httpchannel.Config{BaseURL: "https://oapi.dingtalk.com"}); err == nil {
		t.Fatal("Send() error = nil")
	}
}

func targetMessage(kind, id string) uvim.OutboundMessage {
	return uvim.OutboundMessage{Target: &uvim.OutboundTarget{Kind: kind, ID: id}, Text: "hello"}
}

func checkBodyValue(key string, want any) func(*testing.T, httpchannel.Request) {
	return func(t *testing.T, req httpchannel.Request) {
		body, ok := req.Body.(map[string]any)
		if !ok || body[key] != want {
			t.Fatalf("body = %+v, want %s=%v", req.Body, key, want)
		}
	}
}

func TestHTTPChannelDownloadPreservesPrefixedAuthorization(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if got := req.Header.Get("Authorization"); got != "Basic token" {
			t.Fatalf("authorization = %q", got)
		}
		_, _ = w.Write([]byte("resource"))
	}))
	defer server.Close()
	provider, err := httpchannel.New(httpchannel.Config{ProviderID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := provider.Download(context.Background(), uvim.ResourceDownloadRequest{
		Dir: filepath.Join(t.TempDir(), "resources"),
		Resource: uvim.ResourceRef{
			Provider: "test",
			Kind:     uvim.ElementFile,
			Name:     "resource.txt",
			URL:      server.URL + "/resource",
			Secret:   "Basic token",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if ref.InternalURL == "" {
		t.Fatalf("downloaded ref missing internal URL: %+v", ref)
	}
}
