package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/client"
	"github.com/hengshi/uv-im-connector/providers/lark"
	"github.com/hengshi/uv-im-connector/providers/line"
	"github.com/hengshi/uv-im-connector/providers/matrix"
	"github.com/hengshi/uv-im-connector/providers/memory"
	"github.com/hengshi/uv-im-connector/providers/slack"
	"github.com/hengshi/uv-im-connector/providers/wecom"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestUploadValidLongFilename(t *testing.T) {
	for _, tc := range []struct {
		label, name, mime, content, contentType string
	}{
		{"234-txt", strings.Repeat("a", 230) + ".txt", "", "hello", "text/plain"},
		{"255-txt", strings.Repeat("a", 251) + ".txt", "", "hello", "text/plain"},
		{"255-noext-plain", strings.Repeat("a", 255), "text/plain", "hello", "text/plain"},
		{"short-noext-json", "document", "application/json", `{}`, "application/json"},
		{"255-noext-json", strings.Repeat("a", 255), "application/json", `{}`, "application/json"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			name, data := tc.name, []byte(tc.content)
			// Establish that the original name is legal on this filesystem.
			if err := os.WriteFile(filepath.Join(t.TempDir(), name), data, 0o600); err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			hub := NewHub(nil, nil, &uvim.ResourceStore{Dir: dir})
			api := httptest.NewServer(hub.Handler())
			defer api.Close()
			c := client.New(api.URL)
			ref, err := c.Upload(context.Background(), uvim.ResourceRef{Name: name, MIME: tc.mime}, data)
			if err != nil {
				t.Fatal(err)
			}
			// Reopen using a fresh store: lookup must not depend on in-memory metadata.
			api.Close()
			restarted := httptest.NewServer(NewHub(nil, nil, &uvim.ResourceStore{Dir: dir}).Handler())
			defer restarted.Close()
			c = client.New(restarted.URL)
			resp, err := c.ResolveInternalURL(context.Background(), ref.InternalURL)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			got, err := io.ReadAll(resp.Body)
			if err != nil || !bytes.Equal(got, data) || ref.Name != name || ref.MIME != tc.mime || !strings.HasPrefix(resp.Header.Get("Content-Type"), tc.contentType) {
				t.Fatalf("round trip: name=%q body=%q type=%q err=%v", ref.Name, got, resp.Header.Get("Content-Type"), err)
			}
		})
	}
}

func TestWebSocketHandshakeCancellation(t *testing.T) {
	for _, entry := range []string{"wecom", "lark", "client"} {
		t.Run(entry, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/callback/ws/endpoint" {
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": "ws://" + r.Host + "/blocked?service_id=1"}})
					return
				}
				close(started)
				<-release // Deliberately never complete the upgrade until cleanup.
				http.Error(w, "closed", http.StatusServiceUnavailable)
			}))
			defer api.Close()
			defer close(release)
			ctx, cancel := context.WithCancel(context.Background()) // No deadline.
			defer cancel()
			var run func(context.Context) error
			if entry == "client" {
				run = func(ctx context.Context) error { return client.New(api.URL).WatchEvents(ctx, 0, nil) }
			} else {
				var p uvim.Provider
				var err error
				if entry == "wecom" {
					p, err = wecom.New(wecom.Config{BotID: "bot", Secret: "secret", WSURL: strings.Replace(api.URL, "http", "ws", 1)})
				} else {
					p, err = lark.New(lark.Config{AppID: "app", AppSecret: "secret", BotOpenID: "bot", CallbackBaseURL: api.URL})
				}
				if err != nil {
					t.Fatal(err)
				}
				run = NewHub(uvim.NewProviderRegistry(p), nil, nil).RunProviders
			}
			done := make(chan error, 1)
			go func() { done <- run(ctx) }()
			select {
			case <-started:
			case err := <-done:
				t.Fatalf("returned before handshake: %v", err)
			case <-time.After(2 * time.Second):
				t.Fatal("handshake did not start")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel result: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation did not interrupt pending upgrade")
			}
		})
	}
}

// Local provider transports, real adapters/Hub, and the public event API.
// This proves recovery and sibling isolation, not external platform authentication.
func TestHubRecoversWebSocketProvidersWithTransportStub(t *testing.T) {
	for _, providerID := range []string{"wecom", "lark"} {
		for _, normalClose := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/normal-close=%t", providerID, normalClose), func(t *testing.T) {
				t.Parallel()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var attempts atomic.Int32
				failed := make(chan struct{})
				transport := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/callback/ws/endpoint" {
						_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": "ws://" + r.Host + "/ws?service_id=1"}})
						return
					}
					attempt := attempts.Add(1)
					if attempt == 1 {
						http.Error(w, "temporary outage", http.StatusServiceUnavailable)
						close(failed)
						return
					}
					conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
					defer stop()
					_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
					if providerID == "wecom" {
						var subscribe map[string]any
						if err := conn.ReadJSON(&subscribe); err != nil {
							t.Error(err)
							return
						}
						_ = conn.WriteJSON(map[string]any{"headers": subscribe["headers"], "errcode": 0})
					}
					// The platform redelivers m1 after reconnect; it must stay deduplicated.
					ids := []string{"m1"}
					if attempt >= 3 {
						ids = append(ids, "m2")
					}
					for _, id := range ids {
						if providerID == "wecom" {
							_ = conn.WriteJSON(map[string]any{"cmd": "aibot_msg_callback", "headers": map[string]any{"req_id": id}, "body": map[string]any{
								"msgid": id, "msgtype": "text", "chattype": "single", "from": map[string]any{"userid": "user"}, "text": map[string]any{"content": id},
							}})
						} else {
							raw, _ := json.Marshal(map[string]any{"header": map[string]any{"event_type": "im.message.receive_v1"}, "event": map[string]any{"message": map[string]any{
								"message_id": id, "chat_id": "chat", "chat_type": "p2p", "message_type": "text", "content": `{"text":"` + id + `"}`,
							}}})
							frame := protowire.AppendVarint(protowire.AppendTag(nil, 4, protowire.VarintType), 1)
							frame = protowire.AppendBytes(protowire.AppendTag(frame, 8, protowire.BytesType), raw)
							_ = conn.WriteMessage(websocket.BinaryMessage, frame)
						}
					}
					if attempt == 2 {
						if normalClose {
							_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "rotate"), time.Now().Add(time.Second))
						}
						return
					}
					for {
						if _, _, err := conn.ReadMessage(); err != nil {
							return
						}
					}
				}))
				defer transport.Close()
				var provider uvim.Provider
				var err error
				if providerID == "wecom" {
					provider, err = wecom.New(wecom.Config{BotID: "bot", Secret: "secret", WSURL: strings.Replace(transport.URL, "http", "ws", 1)})
				} else {
					provider, err = lark.New(lark.Config{AppID: "app", AppSecret: "secret", BotOpenID: "bot", CallbackBaseURL: transport.URL})
				}
				if err != nil {
					t.Fatal(err)
				}
				sibling, err := slack.New(slack.Config{WebhookSecret: "secret"})
				if err != nil {
					t.Fatal(err)
				}
				hub := NewHub(uvim.NewProviderRegistry(provider, sibling), mustEventLog(t, filepath.Join(t.TempDir(), "events.jsonl")), nil)
				done := make(chan error, 1)
				go func() { done <- hub.RunProviders(ctx) }()
				defer func() {
					cancel()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Error("provider shutdown blocked")
					}
				}()
				api := httptest.NewServer(hub.Handler())
				defer api.Close()
				select {
				case <-failed:
				case <-time.After(time.Second):
					t.Fatal("initial connection attempt missing")
				}
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, api.URL+"/v1/webhook/slack/slack", strings.NewReader(`{"event":{"type":"message","user":"u","channel":"c","text":"sibling alive","ts":"s1"}}`))
				req.Header.Set("X-UV-Webhook-Secret", "secret")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("healthy sibling webhook status = %d", resp.StatusCode)
				}
				deadline := time.NewTimer(8 * time.Second)
				defer deadline.Stop()
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				sawDisconnected := false
				for {
					select {
					case err := <-done:
						done <- err // Keep teardown bounded after a pre-fix failure.
						t.Fatalf("one provider failure stopped the hub: %v", err)
					case <-deadline.C:
						t.Fatalf("provider did not recover; attempts=%d", attempts.Load())
					case <-ticker.C:
						resp, err := http.Get(api.URL + "/v1/meta")
						if err != nil {
							t.Fatal(err)
						}
						var meta uvim.ServiceMeta
						err = json.NewDecoder(resp.Body).Decode(&meta)
						resp.Body.Close()
						if err != nil {
							t.Fatal(err)
						}
						for _, item := range meta.Providers {
							if attempts.Load() == 2 && item.Provider == providerID && (item.Health.State == "error" || item.Health.State == "disconnected") {
								sawDisconnected = true
							}
						}
						events, err := client.New(api.URL).Events(ctx, 0)
						if err != nil {
							t.Fatal(err)
						}
						if len(events) < 3 {
							continue
						}
						if len(events) != 3 || events[0].Message.Text != "sibling alive" || events[1].Message.ID != "m1" || events[2].Message.ID != "m2" {
							t.Fatalf("events lost, duplicated, or reordered: %+v", events)
						}
						if !sawDisconnected {
							t.Fatal("metadata never reported the disconnected provider during backoff")
						}
						return
					}
				}
			})
		}
	}
}

func TestUploadBeyondLegacyResourceSizeLimit(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	hub := NewHub(nil, nil, store)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	c := client.New(server.URL)
	data := bytes.Repeat([]byte("x"), 100*1024*1024+1)
	ref, err := c.Upload(context.Background(), uvim.ResourceRef{Name: "report.txt"}, data)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.ResolveInternalURL(context.Background(), ref.InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(got, data) || ref.SizeBytes != int64(len(data)) {
		t.Fatalf("upload round trip: size=%d metadata=%d err=%v", len(got), ref.SizeBytes, err)
	}
}

func TestHubEventsAndOutbound(t *testing.T) {
	dir := t.TempDir()
	log, err := uvim.NewEventLog(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	provider := memory.New("test")
	hub := NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(dir, "resources")})
	mapper, err := uvim.NewUserIdentityMapper([]uvim.UserIdentityMapping{{Provider: "test", Connector: "main", UserID: "native", Identities: []uvim.UserIdentity{{Type: "company.employee_id", ID: "employee", Scope: "company"}}}})
	if err != nil {
		t.Fatal(err)
	}
	hub.SetUserIdentityMapper(mapper)
	if err := hub.Emit(context.Background(), uvim.Event{ID: "evt-1", Type: uvim.EventMessageCreate, Provider: "test", Connector: "main", User: uvim.User{ID: "native", Identities: []uvim.UserIdentity{{Type: "test.native", ID: "native", Source: uvim.IdentitySourceCallback}}}, Message: uvim.Message{ID: "m1", Text: "hello"}}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var events struct {
		Events []uvim.Event `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	if len(events.Events) != 1 || events.Events[0].Message.Text != "hello" {
		t.Fatalf("events = %+v", events.Events)
	}
	wantIdentities := []uvim.UserIdentity{{Type: "test.native", ID: "native", Source: uvim.IdentitySourceCallback}, {Type: "company.employee_id", ID: "employee", Scope: "company", Source: uvim.IdentitySourceConfigured}}
	if !reflect.DeepEqual(events.Events[0].User.Identities, wantIdentities) {
		t.Fatalf("HTTP identities=%+v", events.Events[0].User)
	}
	reopened, err := uvim.NewEventLog(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := reopened.ReadAfter(context.Background(), 0)
	if err != nil || len(replayed) != 1 || !reflect.DeepEqual(replayed[0].User.Identities, wantIdentities) {
		t.Fatalf("persisted identities=%+v err=%v", replayed, err)
	}

	raw, _ := json.Marshal(uvim.OutboundMessage{Provider: "test", ChannelID: "c1", Text: "reply"})
	resp, err = http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(provider.Sent()) != 1 || provider.Sent()[0].Text != "reply" {
		t.Fatalf("sent = %+v", provider.Sent())
	}
}

func TestHubRoutesOutboundByConnector(t *testing.T) {
	first := memory.NewConnector("lark", "main")
	second := memory.NewConnector("lark", "sandbox")
	hub := NewHub(uvim.NewProviderRegistry(first, second), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw, _ := json.Marshal(uvim.OutboundMessage{Provider: "lark", Connector: "sandbox", ChannelID: "c1", Text: "reply"})
	resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(first.Sent()) != 0 {
		t.Fatalf("main connector sent = %+v", first.Sent())
	}
	if len(second.Sent()) != 1 {
		t.Fatalf("sandbox connector sent = %+v", second.Sent())
	}
}

// The connector API must carry an explicitly requested, provider-confirmed
// publication locator end-to-end. This is intentionally an HTTP test: a
// caller should not need access to Lark provider internals to receive the
// receipt needed for a future quoted continuation.
func TestMessageCreateReturnsConfirmedPublicationLocatorWhenRequested(t *testing.T) {
	var sends, lookups int
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			if req.Method != http.MethodPost {
				t.Errorf("token method = %s", req.Method)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/messages":
			sends++
			if req.Method != http.MethodPost || req.URL.Query().Get("receive_id_type") != "open_id" || req.Header.Get("Authorization") != "Bearer token" {
				t.Errorf("send request = %s %s auth=%q", req.Method, req.URL.RawQuery, req.Header.Get("Authorization"))
			}
			var body map[string]string
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["receive_id"] != "ou_recipient" {
				t.Errorf("send body = %v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]string{"message_id": "om_sent"}})
		case "/open-apis/im/v1/messages/om_sent":
			lookups++
			if req.Method != http.MethodGet || req.Header.Get("Authorization") != "Bearer token" {
				t.Errorf("locator request = %s auth=%q", req.Method, req.Header.Get("Authorization"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []map[string]any{{
				"message_id": "om_sent", "chat_id": "oc_actual_conversation", "msg_type": "text",
				"sender": map[string]string{"id": "app", "id_type": "app_id", "sender_type": "app"},
			}}}})
		default:
			t.Errorf("unexpected Lark route %s", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer native.Close()
	provider, err := lark.New(lark.Config{ConnectorID: "main", AppID: "app", AppSecret: "secret", BaseURL: native.URL})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(NewHub(uvim.NewProviderRegistry(provider), nil, nil).Handler())
	defer api.Close()

	result, err := client.New(api.URL).Send(t.Context(), uvim.OutboundMessage{
		Provider: "lark", Connector: "main", Text: "notification",
		Target:                    &uvim.OutboundTarget{ID: "ou_recipient", Kind: uvim.TargetUser},
		RequestPublicationLocator: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sends != 1 || lookups != 1 || result.MessageID != "om_sent" {
		t.Fatalf("sends=%d lookups=%d result=%+v", sends, lookups, result)
	}
	if len(result.Publications) != 1 {
		t.Fatalf("publications = %+v", result.Publications)
	}
	publication := result.Publications[0]
	if publication.DeliveryState != uvim.PublicationDeliveryConfirmed || publication.PartOrdinal != 1 || publication.Locator.Provider != "lark" || publication.Locator.Connector != "main" || publication.Locator.MessageID != "om_sent" || publication.Locator.NativeConversationID != "oc_actual_conversation" || publication.Locator.BotAppID != "app" || publication.Locator.BotAppIDType != "app_id" || publication.Locator.NativeTarget != (uvim.OutboundTarget{ID: "oc_actual_conversation", Kind: uvim.TargetConversation}) {
		t.Fatalf("publication = %+v", publication)
	}
}

func TestHubMessageUpdateChecksCapabilityIdentityAndAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		capability, implementation bool
		change                     func(*uvim.MessageUpdate, *http.Request)
		wantStatus                 int
	}{
		{"supported", true, true, nil, 200},
		{"missing capability", false, true, nil, 501},
		{"missing implementation", true, false, nil, 501},
		{"unauthenticated", true, true, func(_ *uvim.MessageUpdate, r *http.Request) { r.Header.Del("Authorization") }, 401},
		{"wrong method", true, true, func(_ *uvim.MessageUpdate, r *http.Request) { r.Method = http.MethodGet }, 405},
		{"missing connector", true, true, func(m *uvim.MessageUpdate, _ *http.Request) { m.Connector = "" }, 400},
		{"unknown connector", true, true, func(m *uvim.MessageUpdate, _ *http.Request) { m.Connector = "other" }, 404},
		{"missing message", true, true, func(m *uvim.MessageUpdate, _ *http.Request) { m.MessageID = "" }, 400},
		{"unsupported target", true, true, func(m *uvim.MessageUpdate, _ *http.Request) { m.Target.Kind = "foreign-kind" }, 400},
		{"oversize", true, true, func(m *uvim.MessageUpdate, _ *http.Request) { m.Text = strings.Repeat("x", 16385) }, 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := memory.NewConnector("test", "sandbox")
			advertised := &updateMetadataProvider{Provider: base, edit: tc.capability}
			updater := &updateTestProvider{updateMetadataProvider: advertised}
			var registered uvim.Provider = advertised
			if tc.implementation {
				registered = updater
			}
			main := memory.NewConnector("test", "main")
			hub := NewHub(uvim.NewProviderRegistry(main, registered), nil, nil)
			hub.SetAuthToken("token")
			api := httptest.NewServer(hub.Handler())
			defer api.Close()
			msg := uvim.MessageUpdate{OutboundMessage: uvim.OutboundMessage{Provider: "test", Connector: "sandbox", Target: &uvim.OutboundTarget{ID: "conversation", Kind: uvim.TargetConversation}, Text: "replacement"}, MessageID: "outbound"}
			req, _ := http.NewRequest(http.MethodPost, api.URL+"/v1/message.update", nil)
			req.Header.Set("Authorization", "Bearer token")
			if tc.change != nil {
				tc.change(&msg, req)
			}
			raw, _ := json.Marshal(msg)
			req.Body = io.NopCloser(bytes.NewReader(raw))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status=%d want=%d", resp.StatusCode, tc.wantStatus)
			}
			if tc.wantStatus == 200 {
				var result uvim.SendResult
				if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
					t.Fatal(err)
				}
				if len(updater.updates) != 1 || !reflect.DeepEqual(updater.updates[0], msg) || result.MessageID != "outbound" || result.Connector != "sandbox" || result.Time.IsZero() {
					t.Fatalf("updates=%+v result=%+v", updater.updates, result)
				}
			} else if len(updater.updates) != 0 {
				t.Fatalf("unexpected updates=%+v", updater.updates)
			}
			if len(main.Sent()) != 0 || len(base.Sent()) != 0 {
				t.Fatal("update fell back to create")
			}
		})
	}
}

type updateMetadataProvider struct {
	uvim.Provider
	edit bool
}

func (p *updateMetadataProvider) Capabilities() uvim.Capabilities {
	caps := p.Provider.Capabilities()
	caps.EditMessage, caps.TextMaxBytes = p.edit, 16384
	return caps
}

type updateTestProvider struct {
	*updateMetadataProvider
	updates []uvim.MessageUpdate
}

func (p *updateTestProvider) Update(_ context.Context, msg uvim.MessageUpdate) (uvim.SendResult, error) {
	p.updates = append(p.updates, msg)
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: msg.MessageID, Time: time.Now().UTC()}, nil
}

func TestHubRejectsInvalidExplicitTarget(t *testing.T) {
	provider := memory.New("test")
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw, _ := json.Marshal(uvim.OutboundMessage{
		Provider: "test",
		Target:   &uvim.OutboundTarget{ID: "x", Kind: "invalid"},
		Text:     "hello",
	})
	resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(provider.Sent()) != 0 {
		t.Fatalf("provider sent = %+v", provider.Sent())
	}
}

func TestHubRejectsInvalidLegacyChannelType(t *testing.T) {
	provider := memory.New("test")
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw, _ := json.Marshal(uvim.OutboundMessage{
		Provider:    "test",
		ChannelID:   "legacy",
		ChannelType: "bogus",
		Text:        "hello",
	})
	resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(provider.Sent()) != 0 {
		t.Fatalf("provider sent = %+v", provider.Sent())
	}
}

func TestHubRejectsLegacyTargetWithoutRecipient(t *testing.T) {
	provider := memory.New("test")
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw, _ := json.Marshal(uvim.OutboundMessage{
		Provider:    "test",
		ChannelType: uvim.ChannelDirect,
		Text:        "hello",
	})
	resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(provider.Sent()) != 0 {
		t.Fatalf("provider sent = %+v", provider.Sent())
	}
}

func TestHubReturnsProviderSendFailureDetail(t *testing.T) {
	provider := &downloadProvider{
		id:        "test",
		connector: "main",
		sendErr: uvim.NewProviderSendError(
			"provider rejected message: invalid recipient",
			errors.New("internal request failed at a URL containing a secret"),
		),
	}
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw, _ := json.Marshal(uvim.OutboundMessage{Provider: "test", ChannelID: "c1", Text: "hello"})
	resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var body struct {
		Error   string           `json:"error"`
		Detail  string           `json:"detail"`
		Failure uvim.SendFailure `json:"failure"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error != "provider_send_failed" || body.Detail != "provider rejected message: invalid recipient" {
		t.Fatalf("body = %+v", body)
	}
	if body.Failure.Category != uvim.SendFailureUnknown || body.Failure.DeliveryState != uvim.DeliveryUnknown {
		t.Fatalf("failure = %+v", body.Failure)
	}
}

func TestHubLogsCredentialSafeProviderSendFailureDetail(t *testing.T) {
	tests := []struct {
		name       string
		sendErr    error
		wantReason string
	}{
		{
			name:       "marked private diagnostic",
			sendErr:    uvim.NewProviderSendLogError("decode lark send response: unexpected EOF", errors.New("access_token=secret")),
			wantReason: "decode lark send response: unexpected EOF",
		},
		{
			name:       "unmarked error",
			sendErr:    errors.New("POST https://user:password@example.test/private?access_token=secret failed"),
			wantReason: "unmarked provider error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			previousLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previousLogger) })

			provider := &downloadProvider{id: "lark", connector: "main", sendErr: tt.sendErr}
			hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
			server := httptest.NewServer(hub.Handler())
			defer server.Close()

			raw, _ := json.Marshal(uvim.OutboundMessage{Provider: "lark", Connector: "main", ChannelID: "c1", Text: "hello"})
			resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadGateway {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			var body struct {
				Error  string `json:"error"`
				Detail string `json:"detail"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Error != "provider_send_failed" || body.Detail != "" {
				t.Fatalf("body = %+v", body)
			}
			got := logs.String()
			if !strings.Contains(got, `provider=lark connector=main reason="`+tt.wantReason+`"`) || strings.Contains(got, "internal_error") || strings.Contains(got, "secret") {
				t.Fatalf("logs = %q", got)
			}
		})
	}
}

func TestHubDownloadsInboundResourcesBeforeEventLog(t *testing.T) {
	dir := t.TempDir()
	provider := &downloadProvider{id: "test", connector: "main"}
	log := mustEventLog(t, filepath.Join(dir, "events.jsonl"))
	hub := NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(dir, "resources")})
	err := hub.Emit(context.Background(), uvim.Event{
		ID:        "evt-1",
		Type:      uvim.EventMessageCreate,
		Provider:  "test",
		Connector: "main",
		Message: uvim.Message{
			ID:   "m1",
			Text: "file",
			Resources: []uvim.ResourceRef{{
				Provider:  "test",
				Connector: "main",
				Kind:      uvim.ElementFile,
				Name:      "secret.txt",
				Key:       "provider-key",
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	events, err := log.ReadAfter(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || len(events[0].Message.Resources) != 1 {
		t.Fatalf("events = %+v", events)
	}
	ref := events[0].Message.Resources[0]
	if ref.InternalURL == "" || ref.Key != "" || ref.Metadata != nil {
		t.Fatalf("resource was not resolved and sanitized: %+v", ref)
	}
}

func TestHubAuthProtectsAPIs(t *testing.T) {
	hub := NewHub(uvim.NewProviderRegistry(memory.New("test")), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	hub.SetAuthToken("token")
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	resp, err := http.Get(server.URL + "/v1/meta")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without token = %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/meta", nil)
	req.Header.Set("Authorization", "Bearer token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status with token = %d", resp.StatusCode)
	}
}

func TestHubMetaIncludesServiceAndProtocolVersion(t *testing.T) {
	provider := memory.NewConnector("memory", "main")
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/v1/meta")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var meta uvim.ServiceMeta
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		t.Fatal(err)
	}
	if meta.Service != uvim.ServiceName || meta.ProtocolVersion != uvim.ProtocolVersion || meta.ConnectorVersion == "" || meta.PublicationLocatorVersion != uvim.PublicationLocatorVersionV1 {
		t.Fatalf("meta = %+v", meta)
	}
	if len(meta.Providers) != 1 || meta.Providers[0].Provider != "memory" || meta.Providers[0].Connector != "main" {
		t.Fatalf("providers = %+v", meta.Providers)
	}
}

func TestHubWebhookRoutesToProviderAndStoresEvent(t *testing.T) {
	dir := t.TempDir()
	provider, err := slack.New(slack.Config{WebhookSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	log := mustEventLog(t, filepath.Join(dir, "events.jsonl"))
	hub := NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(dir, "resources")})
	hub.SetAuthToken("api-token")
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw := `{"type":"event_callback","event":{"type":"message","user":"u1","channel":"c1","text":"hello","ts":"m1"}}`
	resp, err := http.Post(server.URL+"/v1/webhook/slack/slack", "application/json", strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without provider secret = %d", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/webhook/slack/slack", strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-UV-Webhook-Secret", "secret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status with provider secret = %d", resp.StatusCode)
	}
	events, err := log.ReadAfter(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Provider != "slack" || events[0].Message.Text != "hello" {
		t.Fatalf("events = %+v", events)
	}
}

// This exercises the public connector protocol, durable restart, native Slack
// addressing and revocation together. It does not qualify Slack's live service.
func TestHubVerifiedIdentityLinkJourney(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var sends []map[string]any
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/chat.postMessage" || req.Header.Get("Authorization") != "Bearer native-token" {
			t.Errorf("unexpected native send: %s, auth=%t", req.URL.Path, req.Header.Get("Authorization") == "Bearer native-token")
			http.Error(w, "bad request", 400)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		sends = append(sends, body)
		writeJSON(w, 200, map[string]any{"ok": true, "channel": "D-private", "ts": "sent-1"})
	}))
	defer native.Close()
	provider, err := slack.New(slack.Config{ConnectorID: "work", BaseURL: native.URL, Token: "native-token", WebhookSecret: "hook-token"})
	if err != nil {
		t.Fatal(err)
	}
	var api *httptest.Server
	var hub *Hub
	var c *client.Client
	linkDir := filepath.Join(dir, "links")
	start := func() {
		hub = NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, filepath.Join(dir, "events.jsonl")), nil)
		hub.SetAuthToken("api-token")
		if err := hub.EnableIdentityLinks(filepath.Join(linkDir, "identity-links.json")); err != nil {
			t.Fatal(err)
		}
		api = httptest.NewServer(hub.Handler())
		c = client.New(api.URL)
		c.Token = "api-token"
	}
	start()
	defer func() { api.Close(); hub.CloseIdentityLinks() }()
	meta, err := c.ServiceMeta(ctx)
	if err != nil || !meta.IdentityLinks {
		t.Fatalf("identity capability: %+v %v", meta, err)
	}
	emit := func(user, channel, channelType, ts string) uvim.IdentityEventRef {
		raw, _ := json.Marshal(map[string]any{"type": "event_callback", "event": map[string]any{"type": "message", "user": user, "channel": channel, "channel_type": channelType, "text": "/bind gitlab thomas", "ts": ts}})
		req, _ := http.NewRequest(http.MethodPost, api.URL+"/v1/webhook/slack/work", bytes.NewReader(raw))
		req.Header.Set("X-UV-Webhook-Secret", "hook-token")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("native ingress status %d", resp.StatusCode)
		}
		events, err := c.Events(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if event.Message.ID == ts {
				return uvim.IdentityEventRef{Provider: "slack", Connector: "work", EventID: event.ID}
			}
		}
		t.Fatal("native event not observed")
		return uvim.IdentityEventRef{}
	}
	event := emit("U-employee", "D-private", "im", "1.0")
	identity := uvim.UserIdentity{Type: "gitlab.userid", Scope: "gitlab.example", ID: "17"}
	account := uvim.UserIdentity{Type: "gitlab.account", Scope: "gitlab.example", ID: "thomas"}
	request := uvim.IdentityLinkRequest{Identity: identity, Account: &account, Event: event, Proof: "gitlab:project:note-41"}
	linked, err := c.LinkIdentity(ctx, request)
	if err != nil || !linked.Found || linked.Link == nil {
		t.Fatalf("link: %+v %v", linked, err)
	}
	if linked.Link.UserID != "U-employee" || linked.Link.Target != (uvim.OutboundTarget{ID: "D-private", Kind: uvim.TargetChannel}) {
		t.Fatalf("native target: %+v", linked.Link)
	}
	repeated, err := c.LinkIdentity(ctx, request)
	if err != nil || repeated.Link.IdentityLinkRef != linked.Link.IdentityLinkRef {
		t.Fatalf("link must be idempotent: %+v %v", repeated, err)
	}
	aliasRequest := request
	aliasRequest.Identity, aliasRequest.Account = account, nil
	if _, err := c.LinkIdentity(ctx, aliasRequest); err == nil {
		t.Fatal("account alias became a second canonical identity")
	}
	func() {
		original, err := os.ReadFile(filepath.Join(linkDir, "identity-links.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(linkDir, linkDir+".parked"); err != nil {
			t.Fatal(err)
		}
		defer func() {
			os.Remove(linkDir)
			if err := os.Rename(linkDir+".parked", linkDir); err != nil {
				t.Fatal(err)
			}
		}()
		if err := os.WriteFile(linkDir, []byte("not a directory"), 0o600); err != nil {
			t.Fatal(err)
		}
		failed := request
		failed.Identity.ID, failed.Account = "19", nil
		if _, err := c.LinkIdentity(ctx, failed); err == nil {
			t.Fatal("unwritable identity store reported success")
		}
		result, err := c.ResolveIdentity(ctx, uvim.IdentityResolveRequest{Identity: failed.Identity, Provider: "slack", Connector: "work"})
		if err != nil || result.Found {
			t.Fatalf("failed link entered the official index: %+v %v", result, err)
		}
		retained, err := os.ReadFile(filepath.Join(linkDir+".parked", "identity-links.json"))
		if err != nil || !bytes.Equal(retained, original) {
			t.Fatalf("failed save changed old data: %v", err)
		}
	}()
	otherEvent := emit("U-other", "D-other", "im", "2.0")
	otherRequest := request
	otherRequest.Event = otherEvent
	if _, err := c.LinkIdentity(ctx, otherRequest); err == nil {
		t.Fatal("another user replaced the link")
	}
	if _, err := c.UnlinkIdentity(ctx, otherRequest); err == nil {
		t.Fatal("another user removed the link")
	}
	groupRequest := request
	groupRequest.Identity.ID = "18"
	groupRequest.Account = nil
	groupRequest.Event = emit("U-employee", "C-group", "channel", "3.0")
	if _, err := c.LinkIdentity(ctx, groupRequest); err == nil {
		t.Fatal("group audience accepted as private")
	}
	api.Close()
	if err := hub.CloseIdentityLinks(); err != nil {
		t.Fatal(err)
	}
	start()
	resolved, err := c.ResolveIdentity(ctx, uvim.IdentityResolveRequest{Identity: account, Provider: "slack", Connector: "work"})
	if err != nil || !resolved.Found || resolved.Link.IdentityLinkRef != linked.Link.IdentityLinkRef {
		t.Fatalf("persisted alias: %+v %v", resolved, err)
	}
	isolated, err := c.ResolveIdentity(ctx, uvim.IdentityResolveRequest{Identity: account, Provider: "slack", Connector: "other"})
	if err != nil || isolated.Found || isolated.Revoked {
		t.Fatalf("connector isolation: %+v %v", isolated, err)
	}
	message := uvim.OutboundMessage{Provider: "slack", Connector: "work", Target: &linked.Link.Target, IdentityLink: &linked.Link.IdentityLinkRef, Text: "Task completed", Final: true}
	if _, err := c.Send(ctx, message); err != nil {
		t.Fatal(err)
	}
	if len(sends) != 1 || sends[0]["channel"] != "D-private" || sends[0]["text"] != "Task completed" {
		t.Fatalf("native delivery: %+v", sends)
	}
	if _, err := c.UnlinkIdentity(ctx, request); err != nil {
		t.Fatal(err)
	}
	_, err = c.Send(ctx, message)
	if failure, ok := client.ProviderSendFailure(err); !ok || failure.DeliveryState != uvim.DeliveryNotAttempted || len(sends) != 1 {
		t.Fatalf("revoked send: %+v %v count=%d", failure, err, len(sends))
	}
	resolved, err = c.ResolveIdentity(ctx, uvim.IdentityResolveRequest{Identity: account, Provider: "slack", Connector: "work"})
	if err != nil || resolved.Found || !resolved.Revoked {
		t.Fatalf("revoked alias must prevent fallback: %+v %v", resolved, err)
	}
}

func TestHubLinkedMatrixRoomRechecksNativeAudience(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var joined atomic.Int32
	joined.Store(2)
	var sent atomic.Int32
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer native-token" {
			http.Error(w, "unauthorized", 401)
			return
		}
		switch {
		case req.URL.Path == "/_matrix/client/v3/account/whoami":
			writeJSON(w, 200, map[string]any{"user_id": "@bot:example"})
		case strings.HasSuffix(req.URL.Path, "/joined_members"):
			members := map[string]any{"@bot:example": map[string]any{}, "@employee:example": map[string]any{}}
			if joined.Load() > 2 {
				members["@third:example"] = map[string]any{}
			}
			writeJSON(w, 200, map[string]any{"joined": members})
		case req.Method == http.MethodPut && strings.Contains(req.URL.Path, "/send/m.room.message/"):
			sent.Add(1)
			writeJSON(w, 200, map[string]any{"event_id": "$sent"})
		default:
			http.Error(w, "unexpected native operation", 400)
		}
	}))
	defer native.Close()
	p, err := matrix.New(matrix.Config{BaseURL: native.URL, Token: "native-token", WebhookSecret: "hook-token"})
	if err != nil {
		t.Fatal(err)
	}
	hub := NewHub(uvim.NewProviderRegistry(p), mustEventLog(t, filepath.Join(dir, "events.jsonl")), nil)
	hub.SetAuthToken("api-token")
	if err := hub.EnableIdentityLinks(filepath.Join(dir, "identity-links.json")); err != nil {
		t.Fatal(err)
	}
	defer hub.CloseIdentityLinks()
	api := httptest.NewServer(hub.Handler())
	defer api.Close()
	req, _ := http.NewRequest(http.MethodPost, api.URL+"/v1/webhook/matrix/matrix", strings.NewReader(`{"event_id":"$message","room_id":"!private:example","sender":"@employee:example","type":"m.room.message","content":{"msgtype":"m.text","body":"/bind github employee"}}`))
	req.Header.Set("X-UV-Webhook-Secret", "hook-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("native ingress %d", resp.StatusCode)
	}
	c := client.New(api.URL)
	c.Token = "api-token"
	linked, err := c.LinkIdentity(ctx, uvim.IdentityLinkRequest{Identity: uvim.UserIdentity{Type: "github.userid", Scope: "github.com", ID: "42"}, Event: uvim.IdentityEventRef{Provider: "matrix", Connector: "matrix", EventID: "$message"}, Proof: "github:issue:comment-21"})
	if err != nil || !linked.Found {
		t.Fatalf("private room link %+v %v", linked, err)
	}
	if linked.Link.Target != (uvim.OutboundTarget{ID: "!private:example", Kind: uvim.TargetConversation}) {
		t.Fatalf("room target %+v", linked.Link.Target)
	}
	msg := uvim.OutboundMessage{Provider: "matrix", Connector: "matrix", Target: &linked.Link.Target, IdentityLink: &linked.Link.IdentityLinkRef, Text: "private result"}
	if _, err := c.Send(ctx, msg); err != nil {
		t.Fatal(err)
	}
	joined.Store(3)
	_, err = c.Send(ctx, msg)
	if failure, ok := client.ProviderSendFailure(err); !ok || failure.DeliveryState != uvim.DeliveryNotAttempted || sent.Load() != 1 {
		t.Fatalf("changed audience sent=%d failure=%+v err=%v", sent.Load(), failure, err)
	}
}

func TestHubWebhookRejectsMissingProviderSecret(t *testing.T) {
	provider, err := slack.New(slack.Config{})
	if err != nil {
		t.Fatal(err)
	}
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw := `{"type":"event_callback","event":{"type":"message","user":"u1","channel":"c1","text":"hello","ts":"m1"}}`
	resp, err := http.Post(server.URL+"/v1/webhook/slack/slack", "application/json", strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without configured provider secret = %d", resp.StatusCode)
	}
}

func TestHubWebhookEmitsBatchedProviderEvents(t *testing.T) {
	dir := t.TempDir()
	provider, err := line.New(line.Config{WebhookSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	log := mustEventLog(t, filepath.Join(dir, "events.jsonl"))
	hub := NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(dir, "resources")})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw := `{"events":[{"replyToken":"r1","source":{"type":"user","userId":"u1"},"message":{"id":"m1","type":"text","text":"one"}},{"replyToken":"r2","source":{"type":"user","userId":"u2"},"message":{"id":"m2","type":"text","text":"two"}}]}`
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/webhook/line/line", strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-UV-Webhook-Secret", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	events, err := log.ReadAfter(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Message.Text != "one" || events[1].Message.Text != "two" {
		t.Fatalf("events = %+v", events)
	}
}

func TestHubClosesSubscriberOnOverflow(t *testing.T) {
	hub := NewHub(uvim.NewProviderRegistry(memory.New("test")), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	ch := make(chan uvim.Event, 1)
	hub.mu.Lock()
	hub.subscribers[ch] = struct{}{}
	hub.mu.Unlock()
	if err := hub.Emit(context.Background(), uvim.Event{ID: "evt-1", Type: uvim.EventMessageCreate, Provider: "test", Message: uvim.Message{ID: "m1"}}); err != nil {
		t.Fatal(err)
	}
	if err := hub.Emit(context.Background(), uvim.Event{ID: "evt-2", Type: uvim.EventMessageCreate, Provider: "test", Message: uvim.Message{ID: "m2"}}); err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	_, stillSubscribed := hub.subscribers[ch]
	hub.mu.Unlock()
	if stillSubscribed {
		t.Fatal("subscriber remained registered after overflow")
	}
	<-ch
	if _, ok := <-ch; ok {
		t.Fatal("subscriber channel is not closed after overflow")
	}
}

func TestClientResolveInternalURL(t *testing.T) {
	dir := t.TempDir()
	store := &uvim.ResourceStore{Dir: dir}
	ref, err := store.Save(context.Background(), strings.NewReader("hello"), uvim.ResourceRef{ID: "r1", Kind: uvim.ElementFile, Name: "hello.txt"})
	if err != nil {
		t.Fatal(err)
	}
	hub := NewHub(uvim.NewProviderRegistry(memory.New("test")), mustEventLog(t, ""), store)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	resp, err := client.New(server.URL).ResolveInternalURL(context.Background(), ref.InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("body = %q", string(data))
	}
}

func mustEventLog(t *testing.T, path string) *uvim.EventLog {
	t.Helper()
	log, err := uvim.NewEventLog(path)
	if err != nil {
		t.Fatal(err)
	}
	return log
}

type downloadProvider struct {
	id        string
	connector string
	sendErr   error
}

func (p *downloadProvider) ID() string          { return p.id }
func (p *downloadProvider) ConnectorID() string { return p.connector }
func (p *downloadProvider) Capabilities() uvim.Capabilities {
	return uvim.Capabilities{
		Inbound:          true,
		Outbound:         true,
		ReplyMessage:     true,
		ProactiveGroup:   true,
		TargetKinds:      []string{uvim.TargetConversation},
		DownloadResource: true,
		ResourceKinds:    []string{uvim.ElementFile},
	}
}
func (p *downloadProvider) Run(ctx context.Context, sink uvim.EventSink) error {
	<-ctx.Done()
	return ctx.Err()
}
func (p *downloadProvider) Send(context.Context, uvim.OutboundMessage) (uvim.SendResult, error) {
	if p.sendErr != nil {
		return uvim.SendResult{}, p.sendErr
	}
	return uvim.SendResult{Provider: p.id, Connector: p.connector, Time: time.Now().UTC()}, nil
}
func (p *downloadProvider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	store := &uvim.ResourceStore{Dir: req.Dir}
	ref := req.Resource
	return store.Save(ctx, strings.NewReader("downloaded"), ref)
}
func (p *downloadProvider) Health(context.Context) uvim.Health {
	return uvim.Health{Provider: p.id, Connector: p.connector, State: "ok", CheckedAt: time.Now().UTC(), Capabilities: p.Capabilities()}
}
