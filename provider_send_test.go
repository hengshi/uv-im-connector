package uvim_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/discord"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
	"github.com/hengshi/uv-im-connector/providers/slack"
	"github.com/hengshi/uv-im-connector/providers/telegram"
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
		w.WriteHeader(http.StatusOK)
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

func TestDiscordSendUsesBotAuthorization(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/v10/channels/c1/messages" {
			t.Fatalf("path = %s", req.URL.Path)
		}
		if got := req.Header.Get("Authorization"); got != "Bot token" {
			t.Fatalf("authorization = %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	provider, err := discord.New(discord.Config{BaseURL: server.URL, Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Send(context.Background(), uvim.OutboundMessage{Provider: "discord", ChannelID: "c1", Text: "hello"}); err != nil {
		t.Fatal(err)
	}
}

func TestDiscordSendAcceptsPrefixedBotAuthorization(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if got := req.Header.Get("Authorization"); got != "Bot token" {
			t.Fatalf("authorization = %q", got)
		}
		w.WriteHeader(http.StatusOK)
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

func TestWhatsAppSendRequiresPhoneNumberID(t *testing.T) {
	provider, err := whatsapp.New(whatsapp.Config{BaseURL: "http://127.0.0.1", Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Send(context.Background(), uvim.OutboundMessage{Provider: "whatsapp", ChannelID: "u1", Text: "hello"}); err == nil {
		t.Fatal("Send() error = nil, want phone_number_id error")
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

func TestTelegramSendUsesTokenPathWithoutBearer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/bottoken/sendMessage" {
			t.Fatalf("path = %s", req.URL.Path)
		}
		if got := req.Header.Get("Authorization"); got != "" {
			t.Fatalf("authorization = %q", got)
		}
		w.WriteHeader(http.StatusOK)
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
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	provider, err := zulip.New(zulip.Config{BaseURL: server.URL, Token: "Basic token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Send(context.Background(), uvim.OutboundMessage{Provider: "zulip", ChannelID: "c1", Text: "hello", Referrer: uvim.Referrer{ThreadID: "general"}}); err != nil {
		t.Fatal(err)
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
