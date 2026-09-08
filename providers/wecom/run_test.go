package wecom

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/server"
)

func TestQuotedAttachmentKeepsHeartbeatReadable(t *testing.T) {
	const followingMessages = 64
	key := []byte("0123456789abcdef0123456789abcdef")
	plain := []byte("slow quoted attachment")
	padding := 32 - len(plain)%32
	encrypted := append(bytes.Clone(plain), bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(encrypted, encrypted)
	downloadStarted := make(chan struct{})
	releaseDownload := make(chan struct{})
	media := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(downloadStarted)
		select {
		case <-releaseDownload:
			_, _ = w.Write(encrypted)
		case <-r.Context().Done():
		}
	}))
	defer media.Close()

	callbacks := make([]frame, followingMessages+1)
	for i := range callbacks {
		callbacks[i] = runTestMessage(i)
	}
	callbacks[0].Body["quote"] = map[string]any{
		"msgtype": "file",
		"file": map[string]any{
			"url": media.URL, "aeskey": base64.StdEncoding.EncodeToString(key), "file_name": "report.bin",
		},
	}
	wsURL, pings, _ := newRunTestServer(t, callbacks, true)
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	provider, err := New(Config{
		BotID: "bot", Secret: "secret", WSURL: wsURL, ResourceStore: store,
		HeartbeatInterval: 25 * time.Millisecond, AckTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	log, err := uvim.NewEventLog(t.TempDir() + "/events.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	hub := server.NewHub(uvim.NewProviderRegistry(provider), log, store)
	emitted := make(chan string, len(callbacks))
	sink := uvim.EventSinkFunc(func(ctx context.Context, event uvim.Event) error {
		if err := hub.Emit(ctx, event); err != nil {
			return err
		}
		emitted <- event.Message.ID
		return nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- provider.Run(ctx, sink) }()
	select {
	case <-downloadStarted:
	case err := <-runDone:
		t.Fatalf("Run stopped before download: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("download did not start")
	}
	// Keep the download blocked for multiple ACK deadlines. ACKs must remain
	// readable even with a burst of callbacks queued behind the quoted file.
	timer := time.NewTimer(3 * provider.config.AckTimeout)
	defer timer.Stop()
	pingCount := 0
waiting:
	for {
		select {
		case <-pings:
			pingCount++
		case err := <-runDone:
			t.Fatalf("Run stopped during download: %v", err)
		case id := <-emitted:
			t.Fatalf("event %s overtook the blocked attachment", id)
		case <-timer.C:
			break waiting
		}
	}
	if pingCount < 3 {
		t.Fatalf("only %d heartbeats during download; ACK processing stalled", pingCount)
	}
	if _, err := provider.Send(ctx, uvim.OutboundMessage{
		Target: &uvim.OutboundTarget{Kind: uvim.TargetGroup, ID: "chat-1"}, Text: "still connected",
	}); err != nil {
		t.Fatalf("outbound ACK blocked by download: %v", err)
	}
	close(releaseDownload)
	for i := range callbacks {
		select {
		case id := <-emitted:
			if id != fmt.Sprintf("message-%d", i) {
				t.Fatalf("event %d = %s; delivery order changed", i, id)
			}
		case err := <-runDone:
			t.Fatalf("Run stopped before event %d: %v", i, err)
		case <-time.After(3 * time.Second):
			t.Fatalf("event %d was not emitted", i)
		}
	}
	events, err := log.ReadAfter(ctx, 0)
	if err != nil || len(events) != len(callbacks) {
		t.Fatalf("persisted events=%d err=%v", len(events), err)
	}
	refs := events[0].Message.Resources
	if len(refs) != 1 || refs[0].InternalURL == "" || refs[0].Error != "" || refs[0].URL != "" || refs[0].Secret != "" {
		t.Fatalf("persisted attachment = %+v", refs)
	}
	file, _, err := store.Open(refs[0].InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	got, err := io.ReadAll(file)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("downloaded=%q err=%v", got, err)
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run after cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func TestRunPropagatesSinkError(t *testing.T) {
	for _, disconnected := range []bool{false, true} {
		t.Run(fmt.Sprintf("disconnected=%t", disconnected), func(t *testing.T) {
			wsURL, _, peers := newRunTestServer(t, []frame{runTestMessage(0)}, true)
			provider, err := New(Config{BotID: "bot", Secret: "secret", WSURL: wsURL})
			if err != nil {
				t.Fatal(err)
			}
			want := errors.New("event persistence failed")
			started, release := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			runDone := make(chan error, 1)
			go func() {
				runDone <- provider.Run(ctx, uvim.EventSinkFunc(func(ctx context.Context, _ uvim.Event) error {
					close(started)
					select {
					case <-release:
						return want
					case <-ctx.Done():
						return ctx.Err()
					}
				}))
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("sink did not start")
			}
			if disconnected {
				select {
				case peer := <-peers:
					_ = peer.Close()
				case <-ctx.Done():
					t.Fatal("callback was not sent")
				}
				select {
				case err := <-runDone:
					t.Fatalf("Run returned before sink finished: %v", err)
				case <-time.After(100 * time.Millisecond):
				}
			}
			close(release)
			err = <-runDone
			if !errors.Is(err, want) || !strings.Contains(err.Error(), "emit event") {
				t.Fatalf("Run error = %v, want sink error", err)
			}
			if disconnected {
				var closeErr *websocket.CloseError
				if !errors.As(err, &closeErr) || closeErr.Code != websocket.CloseAbnormalClosure {
					t.Fatalf("Run lost connection error while reporting sink failure: %v", err)
				}
			}
		})
	}
}

func TestRunCancelsInFlightSink(t *testing.T) {
	for _, trigger := range []string{"caller cancellation", "caller cancellation after TCP close"} {
		t.Run(trigger, func(t *testing.T) {
			wsURL, _, peers := newRunTestServer(t, []frame{runTestMessage(0), runTestMessage(1)}, true)
			provider, err := New(Config{
				BotID: "bot", Secret: "secret", WSURL: wsURL,
				HeartbeatInterval: 25 * time.Millisecond, AckTimeout: 200 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan struct{}, 2)
			stopped := make(chan struct{}, 2)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			runDone := make(chan error, 1)
			go func() {
				runDone <- provider.Run(ctx, uvim.EventSinkFunc(func(ctx context.Context, _ uvim.Event) error {
					started <- struct{}{}
					<-ctx.Done()
					stopped <- struct{}{}
					return ctx.Err()
				}))
			}()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("sink did not start")
			}
			if trigger == "caller cancellation after TCP close" {
				select {
				case peer := <-peers:
					_ = peer.Close()
				case <-time.After(3 * time.Second):
					t.Fatal("callbacks were not sent")
				}
				select {
				case err := <-runDone:
					t.Fatalf("connection loss canceled the sink: %v", err)
				case <-time.After(100 * time.Millisecond):
				}
			}
			cancel()
			select {
			case err := <-runDone:
				if err != nil {
					t.Fatalf("Run error = %v for %s", err, trigger)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Run did not stop its in-flight sink")
			}
			if len(stopped) != 1 || len(started) != 0 {
				t.Fatalf("sink still running or queued callback emitted: stopped=%d queued=%d", len(stopped), len(started))
			}
			if conn, _ := provider.active(); conn != nil {
				t.Fatal("connection remained active after Run stopped")
			}
		})
	}
}

func TestRunDrainsEventsOnNormalClose(t *testing.T) {
	callbacks := []frame{runTestMessage(0), runTestMessage(1), runTestMessage(2)}
	wsURL, _, peers := newRunTestServer(t, callbacks, true)
	provider, err := New(Config{BotID: "bot", Secret: "secret", WSURL: wsURL})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	emitted := make(chan string, len(callbacks))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		runDone <- provider.Run(ctx, uvim.EventSinkFunc(func(ctx context.Context, event uvim.Event) error {
			if event.Message.ID == "message-0" {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			emitted <- event.Message.ID
			return nil
		}))
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("sink did not start")
	}
	var peer *websocket.Conn
	select {
	case peer = <-peers:
	case <-time.After(3 * time.Second):
		t.Fatal("callbacks were not sent")
	}
	if err := peer.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runDone:
		t.Fatalf("Run discarded pending events on normal close: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run after normal close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not drain events and stop")
	}
	for i := range callbacks {
		select {
		case id := <-emitted:
			if id != fmt.Sprintf("message-%d", i) {
				t.Fatalf("event %d = %s", i, id)
			}
		default:
			t.Fatalf("event %d was lost on normal close", i)
		}
	}
}

func TestRunPreservesReceivedEventsOnDisconnect(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	plain := []byte("attachment survives a lost WebSocket")
	padding := 32 - len(plain)%32
	encrypted := append(bytes.Clone(plain), bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(encrypted, encrypted)
	for _, trigger := range []string{"TCP close", "heartbeat timeout", "disconnected event", "read timeout"} {
		for _, quoted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/quoted=%t", trigger, quoted), func(t *testing.T) {
				started := make(chan struct{})
				media := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					close(started)
					select {
					case <-time.After(300 * time.Millisecond):
						_, _ = w.Write(encrypted)
					case <-r.Context().Done():
					}
				}))
				defer media.Close()
				callbacks := []frame{runTestMessage(0), runTestMessage(1), runTestMessage(2)}
				attachment := map[string]any{
					"msgtype": "file", "file": map[string]any{
						"url": media.URL, "aeskey": base64.StdEncoding.EncodeToString(key), "file_name": "report.bin",
					},
				}
				if quoted {
					callbacks[0].Body["quote"] = attachment
				} else {
					callbacks[0].Body["msgtype"] = "file"
					callbacks[0].Body["file"] = attachment["file"]
					delete(callbacks[0].Body, "text")
				}
				wsURL, _, peers := newRunTestServer(t, callbacks, trigger != "heartbeat timeout")
				store := &uvim.ResourceStore{Dir: t.TempDir()}
				config := Config{BotID: "bot", Secret: "secret", WSURL: wsURL, ResourceStore: store}
				if trigger == "heartbeat timeout" {
					config.HeartbeatInterval, config.AckTimeout = 25*time.Millisecond, 100*time.Millisecond
				}
				if trigger == "read timeout" {
					config.ReadDeadline = 100 * time.Millisecond
				}
				provider, err := New(config)
				if err != nil {
					t.Fatal(err)
				}
				log, err := uvim.NewEventLog(t.TempDir() + "/events.jsonl")
				if err != nil {
					t.Fatal(err)
				}
				hub := server.NewHub(uvim.NewProviderRegistry(provider), log, store)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				runDone := make(chan error, 1)
				go func() { runDone <- provider.Run(ctx, hub) }()
				select {
				case <-started:
				case <-time.After(3 * time.Second):
					t.Fatal("attachment download did not start")
				}
				var peer *websocket.Conn
				select {
				case peer = <-peers:
				case <-time.After(3 * time.Second):
					t.Fatal("callbacks were not sent")
				}
				switch trigger {
				case "TCP close":
					if err := peer.Close(); err != nil {
						t.Fatal(err)
					}
				case "disconnected event":
					if err := peer.WriteJSON(frame{Cmd: cmdEventCallback, Body: map[string]any{
						"event": map[string]any{"eventtype": "disconnected_event"},
					}}); err != nil {
						t.Fatal(err)
					}
				}
				select {
				case err := <-runDone:
					if err == nil || ctx.Err() != nil {
						t.Fatalf("connection error lost or caller canceled: Run=%v caller=%v", err, ctx.Err())
					}
					if trigger == "disconnected event" && !strings.Contains(err.Error(), "disconnected_event") {
						t.Fatalf("disconnect cause lost: %v", err)
					}
					if trigger == "TCP close" {
						var closeErr *websocket.CloseError
						if !errors.As(err, &closeErr) || closeErr.Code != websocket.CloseAbnormalClosure {
							t.Fatalf("TCP close cause lost: %v", err)
						}
					}
				case <-time.After(3 * time.Second):
					t.Fatal("Run did not finish after disconnect")
				}
				events, err := log.ReadAfter(ctx, 0)
				if err != nil || len(events) != 3 {
					t.Fatalf("persisted events=%+v err=%v; received callbacks were lost", events, err)
				}
				for i, event := range events {
					if event.Message.ID != fmt.Sprintf("message-%d", i) {
						t.Fatalf("event %d = %s; order changed", i, event.Message.ID)
					}
				}
				refs := events[0].Message.Resources
				if len(refs) != 1 || refs[0].InternalURL == "" || refs[0].Error != "" || refs[0].URL != "" || refs[0].Secret != "" {
					t.Fatalf("attachment canceled by connection loss: %+v", refs)
				}
				file, _, err := store.Open(refs[0].InternalURL)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				got, err := io.ReadAll(file)
				if err != nil || !bytes.Equal(got, plain) {
					t.Fatalf("attachment=%q err=%v", got, err)
				}
			})
		}
	}
}

func runTestMessage(i int) frame {
	return frame{Cmd: cmdCallback, Headers: headers{ReqID: fmt.Sprintf("request-%d", i)}, Body: map[string]any{
		"msgid": fmt.Sprintf("message-%d", i), "chattype": "group", "chatid": "chat-1",
		"from": map[string]any{"userid": "user-1"}, "msgtype": "text",
		"text": map[string]any{"content": "analyze this"},
	}}
}

func newRunTestServer(t *testing.T, callbacks []frame, acknowledgeHeartbeats bool) (string, <-chan struct{}, <-chan *websocket.Conn) {
	t.Helper()
	pings := make(chan struct{}, 128)
	peers := make(chan *websocket.Conn, 1)
	ws := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		stopClose := context.AfterFunc(t.Context(), func() { _ = conn.Close() })
		defer stopClose()
		for {
			var in frame
			if err := conn.ReadJSON(&in); err != nil {
				return
			}
			if in.Cmd == cmdHeartbeat && !acknowledgeHeartbeats {
				continue
			}
			code := 0
			if err := conn.WriteJSON(frame{Headers: in.Headers, ErrCode: &code}); err != nil {
				return
			}
			switch in.Cmd {
			case cmdSubscribe:
				for _, callback := range callbacks {
					if err := conn.WriteJSON(callback); err != nil {
						return
					}
				}
				peers <- conn
			case cmdHeartbeat:
				select {
				case pings <- struct{}{}:
				default:
				}
			}
		}
	}))
	t.Cleanup(ws.Close)
	return "ws" + strings.TrimPrefix(ws.URL, "http"), pings, peers
}
