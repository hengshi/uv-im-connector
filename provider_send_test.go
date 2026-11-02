package uvim_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
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
		if len(payload["attachments"].([]any)) != 2 {
			t.Fatalf("attachments = %+v", payload)
		}
		for _, field := range []string{"files[0]", "files[1]"} {
			file, header, err := req.FormFile(field)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			data, _ := io.ReadAll(file)
			if header.Filename != "report.txt" || string(data) != "report" {
				t.Fatalf("filename=%q data=%q", header.Filename, data)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "m1"})
	}))
	defer server.Close()
	provider, err := discord.New(discord.Config{BaseURL: server.URL, Token: "token", ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "c1", Text: "caption", Resources: []uvim.ResourceRef{ref, ref}}); err != nil {
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
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["content"] != "hello" {
				t.Errorf("DM message body = %+v", body)
			}
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
	var uploads int
	var transactions []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if got := req.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case req.Method == http.MethodPost && req.URL.Path == "/_matrix/media/v3/upload":
			uploads++
			if req.URL.Query().Get("filename") != "report.txt" || req.Header.Get("Content-Type") != "text/plain" {
				t.Fatalf("upload request = %s %s content-type=%q", req.URL.Path, req.URL.RawQuery, req.Header.Get("Content-Type"))
			}
			data, readErr := io.ReadAll(req.Body)
			if readErr != nil || string(data) != "report" {
				t.Fatalf("upload data=%q err=%v", data, readErr)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"content_uri": "mxc://example.org/media1"})
		case req.Method == http.MethodPut && strings.Contains(req.URL.Path, "/send/m.room.message/"):
			transactions = append(transactions, req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:])
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
			_ = json.NewEncoder(w).Encode(map[string]any{"event_id": req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:]})
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
		Resources: []uvim.ResourceRef{ref, ref},
		Referrer:  uvim.Referrer{MessageID: "$original"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if uploads != 2 || strings.Join(transactions, ",") != "txn1-part-1,txn1-part-2" || strings.Join(result.MessageIDs, ",") != "txn1-part-1,txn1-part-2" {
		t.Fatalf("uploads=%v transactions=%v result=%+v", uploads, transactions, result)
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
	expectedField := "to"
	var uploads int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("authorization = %q", req.Header.Get("Authorization"))
		}
		switch req.URL.Path {
		case "/phone1/media":
			uploads++
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
			if body["type"] != "document" || body[expectedField] != "user1" || document["id"] != "MEDIA1" || document["filename"] != "report.txt" || contextBody["message_id"] != "original" {
				t.Fatalf("message body = %+v", body)
			}
			if expectedField == "recipient" && body["to"] != nil {
				t.Fatalf("BSUID also sent as phone: %+v", body)
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
	for _, idType := range []string{"", "whatsapp.wa_id", "whatsapp.bsuid", "whatsapp.parent_bsuid"} {
		t.Run("resource "+idType, func(t *testing.T) {
			expectedField = "to"
			if idType == "whatsapp.bsuid" || idType == "whatsapp.parent_bsuid" {
				expectedField = "recipient"
			}
			msg := uvim.OutboundMessage{ChannelID: "user1", Resources: []uvim.ResourceRef{ref}, Referrer: uvim.Referrer{MessageID: "original"}}
			if idType != "" {
				msg.Target = &uvim.OutboundTarget{ID: "user1", Kind: uvim.TargetUser, IDType: idType}
			}
			result, err := provider.Send(context.Background(), msg)
			if err != nil || result.MessageID != "MESSAGE1" {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
	before := uploads
	for _, target := range []uvim.OutboundTarget{{ID: "user1", Kind: uvim.TargetUser, IDType: "unknown"}, {ID: "group", Kind: uvim.TargetGroup, IDType: "whatsapp.bsuid"}} {
		if _, err := provider.Send(context.Background(), uvim.OutboundMessage{Target: &target, Resources: []uvim.ResourceRef{ref}}); err == nil {
			t.Fatal("invalid native address accepted")
		}
	}
	if uploads != before {
		t.Fatal("invalid target uploaded resource before rejection")
	}
}

func TestWhatsAppTypedTextAddressing(t *testing.T) {
	for _, tt := range []struct{ idType, kind, field string }{
		{"", uvim.TargetUser, "to"}, {"whatsapp.wa_id", uvim.TargetUser, "to"},
		{"whatsapp.bsuid", uvim.TargetUser, "recipient"}, {"whatsapp.parent_bsuid", uvim.TargetUser, "recipient"},
		{"whatsapp.group_id", uvim.TargetGroup, "to"}, {"whatsapp.bsuid", uvim.TargetGroup, ""},
		{"whatsapp.group_id", uvim.TargetUser, ""}, {"unknown", uvim.TargetUser, ""},
	} {
		t.Run(tt.idType+"/"+tt.kind, func(t *testing.T) {
			// Use the same opaque address to prove dispatch uses the declared type.
			msg := uvim.OutboundMessage{Referrer: uvim.Referrer{Target: &uvim.OutboundTarget{ID: "opaque", Kind: tt.kind, IDType: tt.idType}}, Text: "hi"}
			req, err := whatsapp.Send(msg, httpchannel.Config{}, "phone1")
			if tt.field == "" {
				if err == nil {
					t.Fatal("invalid kind/type accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			body := req.Body.(map[string]any)
			if body[tt.field] != "opaque" || (tt.field == "recipient" && body["to"] != nil) || (tt.field == "to" && body["recipient"] != nil) {
				t.Fatalf("body=%+v", body)
			}
		})
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
			if req.FormValue("type") != "stream" || req.FormValue("to") != "engineering" || strings.Count(req.FormValue("content"), "[report.txt](/user_uploads/report)") != 2 {
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
	result, err := provider.Send(context.Background(), uvim.OutboundMessage{Target: &uvim.OutboundTarget{ID: "engineering", Kind: uvim.TargetGroup}, Resources: []uvim.ResourceRef{ref, ref}})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "42" {
		t.Fatalf("result = %+v", result)
	}
}

func TestProviderOutboundTargetRequestShapes(t *testing.T) {
	// Exercise Provider.Send, including HTTP serialization, authentication and receipts.
	providers := map[string]struct {
		new                 func(string, string) (uvim.Provider, error)
		response, messageID string
	}{
		"discord": {func(baseURL, token string) (uvim.Provider, error) {
			return discord.New(discord.Config{BaseURL: baseURL, Token: token})
		}, `{"id":"m1"}`, "m1"},
		"kook": {func(baseURL, token string) (uvim.Provider, error) {
			return kook.New(kook.Config{BaseURL: baseURL, Token: token})
		}, `{"code":0,"data":{"msg_id":"m1"}}`, "m1"},
		"line": {func(baseURL, token string) (uvim.Provider, error) {
			return line.New(line.Config{BaseURL: baseURL, Token: token})
		}, `{"sentMessages":[{"id":"m1"}]}`, "m1"},
		"matrix": {func(baseURL, token string) (uvim.Provider, error) {
			return matrix.New(matrix.Config{BaseURL: baseURL, Token: token})
		}, `{"event_id":"m1"}`, "m1"},
		"onebot": {func(baseURL, token string) (uvim.Provider, error) {
			return onebot.New(onebot.Config{BaseURL: baseURL, Token: token})
		}, `{"status":"ok","retcode":0,"data":{"message_id":1}}`, "1"},
		"qq": {func(baseURL, token string) (uvim.Provider, error) {
			return qq.New(qq.Config{BaseURL: baseURL, Token: token})
		}, `{"status":"ok","retcode":0,"data":{"message_id":1}}`, "1"},
		"qqguild": {func(baseURL, token string) (uvim.Provider, error) {
			return qqguild.New(qqguild.Config{BaseURL: baseURL, Token: token})
		}, `{"id":"m1"}`, "m1"},
		"slack": {func(baseURL, token string) (uvim.Provider, error) {
			return slack.New(slack.Config{BaseURL: baseURL, Token: token})
		}, `{"ok":true,"ts":"m1"}`, "m1"},
		"telegram": {func(baseURL, token string) (uvim.Provider, error) {
			return telegram.New(telegram.Config{BaseURL: baseURL, Token: token})
		}, `{"ok":true,"result":{"message_id":1}}`, "1"},
		"wechat-official": {func(baseURL, token string) (uvim.Provider, error) {
			return wechatofficial.New(wechatofficial.Config{BaseURL: baseURL, Token: token})
		}, `{"errcode":0,"msgid":"m1"}`, "m1"},
		"whatsapp": {func(baseURL, token string) (uvim.Provider, error) {
			return whatsapp.New(whatsapp.Config{BaseURL: baseURL, Token: token, PhoneNumberID: "phone1"})
		}, `{"messages":[{"id":"m1"}]}`, "m1"},
		"zulip": {func(baseURL, token string) (uvim.Provider, error) {
			return zulip.New(zulip.Config{BaseURL: baseURL, Token: token})
		}, `{"result":"success","id":1}`, "1"},
	}
	tests := []struct {
		name, provider, kind, id, path, body string
		reply, thread, token, auth, method   string
	}{
		{"discord channel", "discord", uvim.TargetChannel, "c1", "/api/v10/channels/c1/messages", `{"content":"hello"}`, "", "", "token", "Bot token", "POST"},
		{"discord prefixed token", "discord", uvim.TargetChannel, "c1", "/api/v10/channels/c1/messages", `{"content":"hello"}`, "", "", "Bot token", "Bot token", "POST"},
		{"kook direct", "kook", uvim.TargetUser, "u1", "/api/v3/direct-message/create", `{"target_id":"u1","content":"hello","type":1}`, "", "", "token", "Bot token", "POST"},
		{"line push", "line", uvim.TargetUser, "u1", "/v2/bot/message/push", `{"to":"u1","messages":[{"type":"text","text":"hello"}]}`, "", "", "token", "Bearer token", "POST"},
		{"line reply", "line", uvim.TargetUser, "u1", "/v2/bot/message/reply", `{"replyToken":"reply-token","messages":[{"type":"text","text":"hello"}]}`, "reply-token", "", "token", "Bearer token", "POST"},
		{"matrix room", "matrix", uvim.TargetConversation, "!room:example", "/_matrix/client/v3/rooms/%21room:example/send/m.room.message/txn-1", `{"msgtype":"m.text","body":"hello","m.relates_to":{"m.in_reply_to":{"event_id":"$event"}}}`, "$event", "", "token", "Bearer token", "PUT"},
		{"onebot direct", "onebot", uvim.TargetUser, "1001", "/send_msg", `{"user_id":"1001","message":"hello"}`, "", "", "token", "Bearer token", "POST"},
		{"onebot group", "onebot", uvim.TargetGroup, "2001", "/send_msg", `{"group_id":"2001","message":"hello"}`, "", "", "token", "Bearer token", "POST"},
		{"onebot reply", "onebot", uvim.TargetUser, "1001", "/send_msg", `{"user_id":"1001","message":[{"type":"reply","data":{"id":"42"}},{"type":"text","data":{"text":"hello"}}]}`, "42", "", "token", "Bearer token", "POST"},
		{"qq group", "qq", uvim.TargetGroup, "2001", "/send_msg", `{"group_id":"2001","message":"hello"}`, "", "", "token", "Bearer token", "POST"},
		{"qq c2c", "qqguild", uvim.TargetUser, "u1", "/v2/users/u1/messages", `{"content":"hello","msg_type":0,"msg_id":"m0"}`, "m0", "", "token", "Bot token", "POST"},
		{"qq group openid", "qqguild", uvim.TargetGroup, "g1", "/v2/groups/g1/messages", `{"content":"hello","msg_type":0}`, "", "", "token", "Bot token", "POST"},
		{"qq guild channel", "qqguild", uvim.TargetChannel, "c1", "/channels/c1/messages", `{"content":"hello"}`, "", "", "token", "Bot token", "POST"},
		{"slack user", "slack", uvim.TargetUser, "U1", "/api/chat.postMessage", `{"channel":"U1","text":"hello"}`, "", "", "token", "Bearer token", "POST"},
		{"slack reply", "slack", uvim.TargetChannel, "c1", "/api/chat.postMessage", `{"channel":"c1","text":"hello","thread_ts":"m0"}`, "m0", "", "token", "Bearer token", "POST"},
		{"slack thread", "slack", uvim.TargetChannel, "c1", "/api/chat.postMessage", `{"channel":"c1","text":"hello","thread_ts":"thread1"}`, "m0", "thread1", "token", "Bearer token", "POST"},
		{"telegram chat", "telegram", uvim.TargetUser, "1001", "/bottoken/sendMessage", `{"chat_id":"1001","text":"hello","reply_parameters":{"message_id":42}}`, "42", "", "token", "", "POST"},
		{"wechat official user", "wechat-official", uvim.TargetUser, "open1", "/cgi-bin/message/custom/send?access_token=token", `{"touser":"open1","msgtype":"text","text":{"content":"hello"}}`, "", "", "token", "", "POST"},
		{"whatsapp user", "whatsapp", uvim.TargetUser, "8613800000000", "/phone1/messages", `{"messaging_product":"whatsapp","recipient_type":"individual","to":"8613800000000","type":"text","text":{"body":"hello"}}`, "", "", "token", "Bearer token", "POST"},
		{"whatsapp group reply", "whatsapp", uvim.TargetGroup, "group1", "/phone1/messages", `{"messaging_product":"whatsapp","recipient_type":"group","to":"group1","type":"text","text":{"body":"hello"},"context":{"message_id":"wamid.1"}}`, "wamid.1", "", "token", "Bearer token", "POST"},
		{"zulip direct", "zulip", uvim.TargetUser, "ada@example.test", "/api/v1/messages", `content=hello&to=ada%40example.test&type=private`, "", "", "Basic token", "Basic token", "POST"},
		{"zulip stream", "zulip", uvim.TargetGroup, "c1", "/api/v1/messages", `content=hello&to=c1&topic=general&type=stream`, "", "general", "Basic token", "Basic token", "POST"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sends atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sends.Add(1)
				if r.Method != tt.method || r.URL.RequestURI() != tt.path {
					t.Errorf("request = %s %s, want %s %s", r.Method, r.URL.RequestURI(), tt.method, tt.path)
				}
				if got := r.Header.Get("Authorization"); got != tt.auth {
					t.Errorf("authorization = %q, want %q", got, tt.auth)
				}
				if strings.Contains(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
					if err := r.ParseForm(); err != nil {
						t.Error(err)
					}
					if r.PostForm.Encode() != tt.body {
						t.Errorf("form = %s, want %s", r.PostForm.Encode(), tt.body)
					}
				} else {
					if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
						t.Errorf("content type = %s", r.Header.Get("Content-Type"))
					}
					var got, want any
					if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
						t.Error(err)
					}
					if err := json.Unmarshal([]byte(tt.body), &want); err != nil {
						t.Error(err)
						return
					}
					if !reflect.DeepEqual(got, want) {
						t.Errorf("body = %#v, want %#v", got, want)
					}
				}
				_, _ = io.WriteString(w, providers[tt.provider].response)
			}))
			defer api.Close()
			provider, err := providers[tt.provider].new(api.URL, tt.token)
			if err != nil {
				t.Fatal(err)
			}
			msg := targetMessage(tt.kind, tt.id)
			msg.ID = "txn-1"
			msg.Referrer.MessageID, msg.Referrer.ThreadID = tt.reply, tt.thread
			// LINE replies carry their native token rather than a message ID.
			if tt.reply == "reply-token" {
				msg.Referrer.ReplyToken = tt.reply
			}
			result, err := provider.Send(t.Context(), msg)
			if err != nil {
				t.Fatal(err)
			}
			if result.Provider != tt.provider || result.MessageID != providers[tt.provider].messageID || result.Time.IsZero() || sends.Load() != 1 {
				t.Fatalf("result = %+v, sends = %d", result, sends.Load())
			}
		})
	}

	const text = "# 任务结论\n\n**已完成**\n- [文档](https://example.com/guide)\n\n```go\nfmt.Println(\"ok\")\n```"
	for _, scenario := range []struct {
		name, kind, path string
		app, reply       bool
	}{
		{"dingtalk session reply", uvim.TargetUser, "/robot/sendBySession", false, true},
		{"dingtalk app session reply", uvim.TargetGroup, "/robot/sendBySession", true, true},
		{"dingtalk group", uvim.TargetGroup, "/robot/send", false, false},
		{"dingtalk app direct", uvim.TargetUser, "/v1.0/robot/oToMessages/batchSend", true, false},
		{"dingtalk app group", uvim.TargetGroup, "/v1.0/robot/groupMessages/send", true, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var sends atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1.0/oauth2/accessToken" {
					var credentials map[string]string
					if err := json.NewDecoder(r.Body).Decode(&credentials); err != nil {
						t.Error(err)
					}
					if r.Method != http.MethodPost || credentials["appKey"] != "client-id" || credentials["appSecret"] != "client-secret" {
						t.Errorf("token request = %s %v", r.Method, credentials)
					}
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
					if scenario.reply && r.URL.Query().Get("session") != "reply-token" || !scenario.reply && r.URL.Query().Get("access_token") != "group-token" {
						t.Errorf("webhook query = %s", r.URL.RawQuery)
					}
					_ = json.NewEncoder(w).Encode(map[string]int{"errcode": 0})
				}
				if content["title"] != "# 任务结论" || content["text"] != text {
					t.Errorf("Markdown content = %+v", content)
				}
			}))
			defer api.Close()
			config := dingtalk.Config{BaseURL: api.URL, Token: "group-token"}
			if scenario.app {
				config.ClientID, config.ClientSecret = "client-id", "client-secret"
			}
			provider, err := dingtalk.New(config)
			if err != nil {
				t.Fatal(err)
			}
			msg := uvim.OutboundMessage{Text: text, Target: &uvim.OutboundTarget{Kind: scenario.kind, ID: "recipient"}}
			if scenario.reply {
				msg.Referrer.ReplyToken = api.URL + scenario.path + "?session=reply-token"
			}
			result, err := provider.Send(t.Context(), msg)
			if err != nil {
				t.Fatal(err)
			}
			wantID := ""
			if scenario.app && !scenario.reply {
				wantID = "delivered"
			}
			if sends.Load() != 1 || result.Provider != "dingtalk" || result.MessageID != wantID || result.Time.IsZero() {
				t.Fatalf("result = %+v, sends = %d", result, sends.Load())
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
		wantCode   string
	}{
		{name: "dingtalk", parse: dingtalk.ParseSendResponse, successRaw: `{"errcode":0,"errmsg":"ok"}`, failureRaw: `{"errcode":310000,"errmsg":"invalid token"}`, wantCode: "310000"},
		{name: "discord", parse: discord.ParseSendResponse, successRaw: `{"id":"m1"}`, wantID: "m1", failureRaw: `{"code":50035,"message":"Invalid Form Body"}`, wantCode: "50035"},
		{name: "kook", parse: kook.ParseSendResponse, successRaw: `{"code":0,"data":{"msg_id":"m1"}}`, wantID: "m1", failureRaw: `{"code":40000,"message":"invalid target"}`, wantCode: "40000"},
		{name: "line", parse: line.ParseSendResponse, successRaw: `{"sentMessages":[{"id":"m1"}]}`, wantID: "m1", failureRaw: `{"message":"invalid user"}`},
		{name: "matrix", parse: matrix.ParseSendResponse, successRaw: `{"event_id":"m1"}`, wantID: "m1", failureRaw: `{"errcode":"M_FORBIDDEN","error":"forbidden"}`, wantCode: "M_FORBIDDEN"},
		{name: "onebot", parse: onebot.ParseSendResponse, successRaw: `{"status":"ok","retcode":0,"data":{"message_id":1}}`, wantID: "1", failureRaw: `{"status":"failed","retcode":100,"wording":"blocked"}`, wantCode: "100"},
		{name: "qq", parse: qq.ParseSendResponse, successRaw: `{"status":"ok","retcode":0,"data":{"message_id":1}}`, wantID: "1", failureRaw: `{"status":"failed","retcode":100,"wording":"blocked"}`, wantCode: "100"},
		{name: "qqguild", parse: qqguild.ParseSendResponse, successRaw: `{"id":"m1"}`, wantID: "m1", failureRaw: `{"code":11255,"message":"invalid request"}`, wantCode: "11255"},
		{name: "slack", parse: slack.ParseSendResponse, successRaw: `{"ok":true,"ts":"m1"}`, wantID: "m1", failureRaw: `{"ok":false,"error":"channel_not_found"}`, wantCode: "channel_not_found"},
		{name: "telegram", parse: telegram.ParseSendResponse, successRaw: `{"ok":true,"result":{"message_id":1}}`, wantID: "1", failureRaw: `{"ok":false,"error_code":403,"description":"bot blocked"}`, wantCode: "403"},
		{name: "wechat-official", parse: wechatofficial.ParseSendResponse, successRaw: `{"errcode":0,"errmsg":"ok","msgid":"m1"}`, wantID: "m1", failureRaw: `{"errcode":45015,"errmsg":"response out of time limit"}`, wantCode: "45015"},
		{name: "whatsapp", parse: whatsapp.ParseSendResponse, successRaw: `{"messages":[{"id":"m1"}]}`, wantID: "m1", failureRaw: `{"error":{"code":131047,"message":"re-engagement message"}}`, wantCode: "131047"},
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
			} else if detail := uvim.ProviderSendErrorDetail(err); detail != "" {
				t.Fatalf("business failure exposed provider detail %q", detail)
			} else if failure, ok := uvim.ProviderSendFailure(err); !ok || failure.Category != uvim.SendFailureProviderRejected || failure.Retryable || failure.DeliveryState != uvim.DeliveryRejected || failure.ProviderCode != tt.wantCode {
				t.Fatalf("business failure is not normalized: %+v, ok=%v", failure, ok)
			}
		})
	}
}

func TestCustomHTTPSendStagesPreserveStatusAcrossProviders(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("image"), uvim.ResourceRef{Kind: uvim.ElementImage, Name: "chart.png", MIME: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: providerSendRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Body:       io.NopCloser(providerSendErrorReader{err: errors.New("access_token=secret")}),
			Header:     make(http.Header),
		}, nil
	})}
	baseURL := "https://api.example.test"
	tests := []struct {
		name       string
		build      func() (uvim.Provider, error)
		message    uvim.OutboundMessage
		wantLog    string
		wantPublic string
	}{
		{
			name: "discord create dm",
			build: func() (uvim.Provider, error) {
				return discord.New(discord.Config{BaseURL: baseURL, Token: "token", HTTPClient: client})
			},
			message: targetMessage(uvim.TargetUser, "user1"),
			wantLog: "discord create dm: http 502",
		},
		{
			name: "kook upload",
			build: func() (uvim.Provider, error) {
				return kook.New(kook.Config{BaseURL: baseURL, Token: "token", ResourceStore: store, HTTPClient: client})
			},
			message:    uvim.OutboundMessage{Target: &uvim.OutboundTarget{ID: "channel1", Kind: uvim.TargetChannel}, Resources: []uvim.ResourceRef{ref}},
			wantLog:    "kook upload: http 502",
			wantPublic: "kook upload: http 502",
		},
		{
			name: "matrix upload",
			build: func() (uvim.Provider, error) {
				return matrix.New(matrix.Config{BaseURL: baseURL, Token: "token", ResourceStore: store, HTTPClient: client})
			},
			message:    uvim.OutboundMessage{Target: &uvim.OutboundTarget{ID: "!room:example.org", Kind: uvim.TargetConversation}, Resources: []uvim.ResourceRef{ref}},
			wantLog:    "matrix upload: http 502",
			wantPublic: "matrix send: http 502",
		},
		{
			name: "slack upload init",
			build: func() (uvim.Provider, error) {
				return slack.New(slack.Config{BaseURL: baseURL, Token: "token", ResourceStore: store, HTTPClient: client})
			},
			message:    uvim.OutboundMessage{Target: &uvim.OutboundTarget{ID: "C1", Kind: uvim.TargetChannel}, Resources: []uvim.ResourceRef{ref}},
			wantLog:    "slack upload init: http 502",
			wantPublic: "slack send: http 502",
		},
		{
			name: "telegram send",
			build: func() (uvim.Provider, error) {
				return telegram.New(telegram.Config{BaseURL: baseURL, Token: "token", ResourceStore: store, HTTPClient: client})
			},
			message:    uvim.OutboundMessage{Target: &uvim.OutboundTarget{ID: "user1", Kind: uvim.TargetUser}, Resources: []uvim.ResourceRef{ref}},
			wantLog:    "telegram send: http 502",
			wantPublic: "telegram send: http 502",
		},
		{
			name: "wechat official upload",
			build: func() (uvim.Provider, error) {
				return wechatofficial.New(wechatofficial.Config{BaseURL: baseURL, Token: "token", ResourceStore: store, HTTPClient: client})
			},
			message:    uvim.OutboundMessage{Target: &uvim.OutboundTarget{ID: "user1", Kind: uvim.TargetUser}, Resources: []uvim.ResourceRef{ref}},
			wantLog:    "wechat-official upload: http 502",
			wantPublic: "wechat-official upload: http 502",
		},
		{
			name: "whatsapp upload",
			build: func() (uvim.Provider, error) {
				return whatsapp.New(whatsapp.Config{BaseURL: baseURL, Token: "token", PhoneNumberID: "phone1", ResourceStore: store, HTTPClient: client})
			},
			message:    uvim.OutboundMessage{Target: &uvim.OutboundTarget{ID: "user1", Kind: uvim.TargetUser}, Resources: []uvim.ResourceRef{ref}},
			wantLog:    "whatsapp upload: http 502",
			wantPublic: "whatsapp send: http 502",
		},
		{
			name: "zulip upload",
			build: func() (uvim.Provider, error) {
				return zulip.New(zulip.Config{BaseURL: baseURL, Token: "Basic token", ResourceStore: store, HTTPClient: client})
			},
			message:    uvim.OutboundMessage{Target: &uvim.OutboundTarget{ID: "engineering", Kind: uvim.TargetGroup}, Resources: []uvim.ResourceRef{ref}, Referrer: uvim.Referrer{ThreadID: "general"}},
			wantLog:    "zulip upload: http 502",
			wantPublic: "zulip upload: http 502",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, err := tt.build()
			if err != nil {
				t.Fatal(err)
			}
			_, err = provider.Send(context.Background(), tt.message)
			if err == nil {
				t.Fatal("Send() error = nil")
			}
			if got := uvim.ProviderSendErrorLogDetail(err); got != tt.wantLog {
				t.Fatalf("private log detail = %q, want %q", got, tt.wantLog)
			}
			if got := uvim.ProviderSendErrorDetail(err); got != tt.wantPublic {
				t.Fatalf("public detail = %q, want %q", got, tt.wantPublic)
			}
			failure, ok := uvim.ProviderSendFailure(err)
			if !ok || failure.Category != uvim.SendFailureProviderUnavailable || !failure.Retryable || failure.DeliveryState != uvim.DeliveryUnknown || failure.HTTPStatus != http.StatusBadGateway {
				t.Fatalf("normalized HTTP failure = %+v, ok=%v", failure, ok)
			}
			if strings.Contains(uvim.ProviderSendErrorLogDetail(err), "access_token") {
				t.Fatalf("private log detail leaked response body: %q", uvim.ProviderSendErrorLogDetail(err))
			}
		})
	}
}

func TestSlackUploadDecodeFailureKeepsSafeStage(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: providerSendRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"ok":`)), Header: make(http.Header)}, nil
	})}
	provider, err := slack.New(slack.Config{BaseURL: "https://api.example.test", Token: "token", ResourceStore: store, HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Target:    &uvim.OutboundTarget{ID: "C1", Kind: uvim.TargetChannel},
		Resources: []uvim.ResourceRef{ref},
	})
	if err == nil {
		t.Fatal("Send() error = nil")
	}
	if got := uvim.ProviderSendErrorLogDetail(err); got != "slack upload: invalid JSON response" {
		t.Fatalf("private log detail = %q", got)
	}
	if got := uvim.ProviderSendErrorDetail(err); got != "" {
		t.Fatalf("public detail = %q", got)
	}
}

type providerSendRoundTripFunc func(*http.Request) (*http.Response, error)

func (f providerSendRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type providerSendErrorReader struct{ err error }

func (r providerSendErrorReader) Read([]byte) (int, error) { return 0, r.err }

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
