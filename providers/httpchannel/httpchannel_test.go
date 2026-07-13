package httpchannel

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
)

func TestSendRejectsProviderBusinessErrorOnHTTP200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":false,"error":"channel_not_found"}`))
	}))
	defer server.Close()
	provider, err := New(Config{
		ProviderID: "test",
		BaseURL:    server.URL,
		Capabilities: uvim.Capabilities{
			Outbound:       true,
			ProactiveGroup: true,
			TargetKinds:    []string{uvim.TargetConversation},
		},
		Send: func(uvim.OutboundMessage, Config) (Request, error) {
			return Request{Path: "/send"}, nil
		},
		ParseSendResponse: func([]byte) (string, error) {
			businessErr := errors.New("channel_not_found")
			return "", uvim.NewProviderSendError(businessErr.Error(), businessErr)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "c1", Text: "hello"})
	if err == nil || !strings.Contains(err.Error(), "channel_not_found") {
		t.Fatalf("Send() error = %v", err)
	}
	if got := uvim.ProviderSendErrorDetail(err); !strings.Contains(got, "channel_not_found") {
		t.Fatalf("public detail = %q", got)
	}
}

func TestSendIncludesBoundedHTTPErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid recipient"}`))
	}))
	defer server.Close()
	provider, err := New(Config{
		ProviderID: "test",
		BaseURL:    server.URL,
		Capabilities: uvim.Capabilities{
			Outbound:       true,
			ProactiveGroup: true,
			TargetKinds:    []string{uvim.TargetConversation},
		},
		Send: func(uvim.OutboundMessage, Config) (Request, error) {
			return Request{Path: "/send"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "c1", Text: "hello"})
	if err == nil || !strings.Contains(err.Error(), "http 400") || !strings.Contains(err.Error(), "invalid recipient") {
		t.Fatalf("Send() error = %v", err)
	}
	if got := uvim.ProviderSendErrorDetail(err); got != "test send: http 400" {
		t.Fatalf("public detail = %q", got)
	}
}

func TestSendRejectsUnsupportedExplicitTarget(t *testing.T) {
	provider, err := New(Config{
		ProviderID: "test",
		Capabilities: uvim.Capabilities{
			Outbound:        true,
			ProactiveDirect: true,
			TargetKinds:     []string{uvim.TargetUser},
		},
		Send: func(uvim.OutboundMessage, Config) (Request, error) {
			return Request{Path: "/send"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Target: &uvim.OutboundTarget{ID: "g1", Kind: uvim.TargetGroup},
		Text:   "hello",
	})
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("Send() error = %v", err)
	}
}
