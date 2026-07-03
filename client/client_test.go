package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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
