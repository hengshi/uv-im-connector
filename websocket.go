package uvim

import (
	"context"
	"net"
	"net/http"

	"github.com/gorilla/websocket"
)

// DialWebSocket honors caller cancellation throughout proxy, TLS and HTTP
// handshakes, including contexts with no deadline. After success, the caller
// owns the connection and its cancellation lifecycle.
func DialWebSocket(ctx context.Context, dialer *websocket.Dialer, url string, header http.Header) (*websocket.Conn, *http.Response, error) {
	d := *dialer
	var stops []func() bool
	watch := func(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
		return func(dialCtx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dial(dialCtx, network, addr)
			if err == nil {
				// Watch the caller, not Gorilla's temporary timeout context, which
				// is canceled even when DialContext returns a successful connection.
				stops = append(stops, context.AfterFunc(ctx, func() { _ = conn.Close() }))
			}
			return conn, err
		}
	}
	dial := d.NetDialContext
	if dial == nil {
		if d.NetDial != nil {
			dial = func(_ context.Context, network, addr string) (net.Conn, error) { return d.NetDial(network, addr) }
		} else {
			dial = (&net.Dialer{}).DialContext
		}
	}
	d.NetDialContext = watch(dial)
	if d.NetDialTLSContext != nil {
		d.NetDialTLSContext = watch(d.NetDialTLSContext)
	}
	conn, resp, err := d.DialContext(ctx, url, header)
	// Disarm before the final cancellation check. If cancellation already won,
	// ctx.Err is set and no connection is handed off; otherwise no handshake
	// watcher can close the connection after ownership transfers to the caller.
	for _, stop := range stops {
		stop()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, resp, ctxErr
	}
	return conn, resp, err
}
