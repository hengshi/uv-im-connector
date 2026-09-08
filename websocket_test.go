package uvim

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestDialWebSocketCancelsCustomAndProxyHandshakes(t *testing.T) {
	for _, mode := range []string{"NetDial", "NetDialContext", "NetDialTLSContext", "proxy"} {
		t.Run(mode, func(t *testing.T) {
			local, peer := net.Pipe()
			defer local.Close()
			defer peer.Close()
			dial := func(context.Context, string, string) (net.Conn, error) { return local, nil }
			d := &websocket.Dialer{NetDialContext: dial}
			address := "ws://example.test/ws"
			switch mode {
			case "NetDial":
				d.NetDialContext = nil
				d.NetDial = func(string, string) (net.Conn, error) { return local, nil }
			case "NetDialTLSContext":
				d.NetDialTLSContext = dial // A caller-supplied, already TLS-connected socket.
				address = "wss://example.test/ws"
			case "proxy":
				proxy, _ := url.Parse("http://proxy.test")
				d.Proxy = http.ProxyURL(proxy)
			}
			request := make(chan *http.Request, 1)
			go func() {
				req, _ := http.ReadRequest(bufio.NewReader(peer))
				request <- req
				_, _ = io.Copy(io.Discard, peer)
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, _, err := DialWebSocket(ctx, d, address, nil); done <- err }()
			select {
			case req := <-request:
				if req == nil || (mode == "proxy" && req.Method != http.MethodConnect) {
					t.Fatalf("unexpected handshake: %v", req)
				}
			case <-time.After(time.Second):
				t.Fatal("handshake did not start")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel result: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation did not close handshake socket")
			}
		})
	}
}

func TestDialWebSocketTransfersSuccessfulConnection(t *testing.T) {
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		kind, data, err := conn.ReadMessage()
		if err == nil {
			_ = conn.WriteMessage(kind, data)
		}
	}))
	defer api.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &websocket.Dialer{HandshakeTimeout: time.Second, TLSClientConfig: api.Client().Transport.(*http.Transport).TLSClientConfig}
	conn, _, err := DialWebSocket(ctx, d, strings.Replace(api.URL, "https", "wss", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Neither Gorilla's deferred timeout-context cancellation nor later caller
	// cancellation may close the socket after its ownership has transferred.
	cancel()
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	_, data, err := conn.ReadMessage()
	if err != nil || string(data) != "hello" || d.NetDialContext != nil {
		t.Fatalf("transferred connection: data=%q err=%v dialer mutated=%v", data, err, d.NetDialContext != nil)
	}
}
