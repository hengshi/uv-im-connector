package slack

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
)

func TestSendUsesTypedSlackErrorCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/chat.postMessage" {
			t.Errorf("path = %q", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"ok":false,"error":"channel_not_found"}`))
	}))
	defer server.Close()

	provider, err := New(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Target: &uvim.OutboundTarget{ID: "C123", Kind: uvim.TargetChannel},
		Text:   "hello",
	})
	if err == nil {
		t.Fatal("Send() error = nil")
	}
	failure, ok := uvim.ProviderSendFailure(err)
	if !ok || failure.ProviderCode != "channel_not_found" {
		t.Fatalf("failure = %+v, ok=%v", failure, ok)
	}
}

func TestResourceChannelIDUsesSlackResponseErrorCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/conversations.open" {
			t.Errorf("path = %q", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"ok":false,"error":"channel_not_found"}`))
	}))
	defer server.Close()

	provider, err := New(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.resourceChannelID(context.Background(), uvim.OutboundTarget{ID: "U123", Kind: uvim.TargetUser})
	if err == nil {
		t.Fatal("resourceChannelID() error = nil")
	}
	failure, ok := uvim.ProviderSendFailure(err)
	if !ok || failure.ProviderCode != "channel_not_found" {
		t.Fatalf("failure = %+v, ok=%v", failure, ok)
	}
}

func TestResourceChannelIDUsesSlackResponseRawMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/conversations.open" {
			t.Errorf("path = %q", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"ok":false,"error":"channel not found","code":429}`))
	}))
	defer server.Close()

	provider, err := New(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.resourceChannelID(context.Background(), uvim.OutboundTarget{ID: "U123", Kind: uvim.TargetUser})
	if err == nil {
		t.Fatal("resourceChannelID() error = nil")
	}
	failure, ok := uvim.ProviderSendFailure(err)
	if !ok || failure.ProviderCode != "429" {
		t.Fatalf("failure = %+v, ok=%v", failure, ok)
	}
}
