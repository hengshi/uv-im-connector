package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
)

func TestWatchEventsReturnsOnContextCancelWhileIdle(t *testing.T) {
	connected := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events/ws" {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		close(connected)
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- New(server.URL).WatchEvents(ctx, 0, nil)
	}()
	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("websocket did not connect")
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WatchEvents error = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WatchEvents did not return after context cancel")
	}
}

func TestWatchEventsWithConnectRunsAfterDial(t *testing.T) {
	connected := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events/ws" {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- New(server.URL).WatchEventsWithConnect(ctx, 0, func() error {
			close(connected)
			return nil
		}, nil)
	}()
	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("onConnect was not called")
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WatchEventsWithConnect error = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WatchEventsWithConnect did not return after context cancel")
	}
}

func TestMetaPreservesRawMapCompatibility(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/meta" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("Authorization = %q", got)
		}
		_ = json.NewEncoder(w).Encode(uvim.ServiceMeta{
			Service:          uvim.ServiceName,
			ConnectorVersion: "v0.0.4",
			ProtocolVersion:  uvim.ProtocolVersion,
		})
	}))
	defer server.Close()

	client := New(server.URL)
	client.Token = "token"
	meta, err := client.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if meta["service"] != uvim.ServiceName || meta["connector_version"] != "v0.0.4" || meta["protocol_version"] != uvim.ProtocolVersion {
		t.Fatalf("meta = %+v", meta)
	}
}

func TestServiceMetaReturnsTypedServiceMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/meta" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(uvim.ServiceMeta{
			Service:          uvim.ServiceName,
			ConnectorVersion: "v0.0.4",
			ProtocolVersion:  uvim.ProtocolVersion,
		})
	}))
	defer server.Close()

	meta, err := New(server.URL).ServiceMeta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if meta.Service != uvim.ServiceName || meta.ConnectorVersion != "v0.0.4" || meta.ProtocolVersion != uvim.ProtocolVersion {
		t.Fatalf("meta = %+v", meta)
	}
}

func TestSendSurfacesStructuredProviderFailureDetail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":  "provider_send_failed",
			"detail": "stream message update expired",
		})
	}))
	defer server.Close()

	_, err := New(server.URL).Send(context.Background(), uvim.OutboundMessage{Provider: "wecom", Text: "done"})
	if err == nil || !strings.Contains(err.Error(), "http 502") || !strings.Contains(err.Error(), "stream message update expired") {
		t.Fatalf("Send error = %v", err)
	}
}

func TestSendDoesNotSurfaceUnstructuredFailureBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream token=secret"))
	}))
	defer server.Close()

	_, err := New(server.URL).Send(context.Background(), uvim.OutboundMessage{Provider: "wecom", Text: "done"})
	if err == nil || err.Error() != "http 502" {
		t.Fatalf("Send error = %v", err)
	}
}
