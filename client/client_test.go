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

func TestConversationWindowUsesTypedAuthenticatedRoute(t *testing.T) {
	for _, statusCode := range []int{http.StatusOK, http.StatusUnauthorized} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodPost || req.URL.Path != "/v1/conversation.window" || req.Header.Get("Authorization") != "Bearer token" {
					t.Errorf("request=%s %s authorization=%q", req.Method, req.URL.Path, req.Header.Get("Authorization"))
				}
				var input uvim.ConversationWindowRequest
				if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
					t.Error(err)
				}
				if input != (uvim.ConversationWindowRequest{Provider: "lark", Connector: "main", TriggerEventID: "evt", TriggerMessageID: "om"}) {
					t.Errorf("input=%+v", input)
				}
				w.WriteHeader(statusCode)
				if statusCode == http.StatusOK {
					_ = json.NewEncoder(w).Encode(uvim.ConversationWindowResult{Version: uvim.ConversationWindowVersionV1, Source: "native", Status: "none", TriggerMessageID: "om"})
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
			}))
			defer api.Close()

			c := New(api.URL)
			c.Token = "token"
			result, err := c.ConversationWindow(t.Context(), uvim.ConversationWindowRequest{Provider: "lark", Connector: "main", TriggerEventID: "evt", TriggerMessageID: "om"})
			if statusCode == http.StatusOK {
				if err != nil || result.Status != "none" || result.Source != "native" || result.Version != uvim.ConversationWindowVersionV1 {
					t.Fatalf("result=%+v err=%v", result, err)
				}
				return
			}
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.StatusCode != statusCode {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestUpdatePreservesExactMessageAndFailureFacts(t *testing.T) {
	for _, status := range []int{200, 502, 404} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls++
				var msg uvim.MessageUpdate
				if err := json.NewDecoder(req.Body).Decode(&msg); err != nil {
					t.Error(err)
				}
				if req.Method != http.MethodPost || req.URL.Path != "/v1/message.update" || req.Header.Get("Authorization") != "Bearer token" || msg.Provider != "lark" || msg.Connector != "main" || msg.MessageID != "outbound" || msg.Text != "replacement" || msg.Referrer.MessageID != "incoming" {
					t.Errorf("request=%s %s msg=%+v", req.Method, req.URL.Path, msg)
				}
				w.WriteHeader(status)
				switch status {
				case 200:
					json.NewEncoder(w).Encode(uvim.SendResult{Provider: "lark", Connector: "main", MessageID: "outbound", Time: time.Now().UTC()})
				case 502:
					json.NewEncoder(w).Encode(map[string]any{"error": "provider_send_failed", "failure": uvim.SendFailure{Category: uvim.SendFailureRateLimited, DeliveryState: uvim.DeliveryRejected, Retryable: true, RetryAfterSeconds: 3, DeliveredCount: 1, DeliveredMessageIDs: []string{"acknowledged-prefix"}}})
				default:
					w.Write([]byte("legacy endpoint not found"))
				}
			}))
			defer api.Close()
			c := New(api.URL)
			c.Token = "token"
			result, err := c.Update(t.Context(), uvim.MessageUpdate{OutboundMessage: uvim.OutboundMessage{Provider: "lark", Connector: "main", Text: "replacement", Referrer: uvim.Referrer{MessageID: "incoming"}}, MessageID: "outbound"})
			if calls != 1 {
				t.Fatalf("calls=%d", calls)
			}
			if status == 200 {
				if err != nil || result.MessageID != "outbound" || result.Time.IsZero() {
					t.Fatalf("result=%+v err=%v", result, err)
				}
				return
			}
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.StatusCode != status {
				t.Fatalf("error=%v", err)
			}
			if status == 502 {
				failure, ok := ProviderSendFailure(err)
				if !ok || failure.Category != uvim.SendFailureRateLimited || failure.DeliveryState != uvim.DeliveryRejected || !failure.Retryable || failure.RetryAfterSeconds != 3 || failure.DeliveredCount != 1 || len(failure.DeliveredMessageIDs) != 1 || failure.DeliveredMessageIDs[0] != "acknowledged-prefix" {
					t.Fatalf("failure=%+v", failure)
				}
			} else if httpErr.Failure != nil {
				t.Fatal("legacy failure inferred delivery evidence")
			}
		})
	}
}

func TestSendSurfacesStructuredProviderFailureDetail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":  "provider_send_failed",
			"detail": "stream message update expired",
			"failure": uvim.SendFailure{
				Category:          uvim.SendFailureRateLimited,
				Retryable:         true,
				DeliveryState:     uvim.DeliveryRejected,
				HTTPStatus:        http.StatusTooManyRequests,
				ProviderCode:      "230020",
				RetryAfterSeconds: 3,
				DeliveredCount:    1,
				DeliveredPublications: []uvim.Publication{{
					PartOrdinal:   1,
					DeliveryState: uvim.PublicationDeliveryConfirmed,
					Locator: uvim.PublicationLocator{
						Provider: "lark", Connector: "main", MessageID: "om_confirmed", NativeConversationID: "oc_confirmed",
						BotAppID: "app", BotAppIDType: "app_id", NativeTarget: uvim.OutboundTarget{ID: "oc_confirmed", Kind: uvim.TargetConversation},
					},
				}},
			},
		})
	}))
	defer server.Close()

	_, err := New(server.URL).Send(context.Background(), uvim.OutboundMessage{Provider: "wecom", Text: "done"})
	if err == nil || !strings.Contains(err.Error(), "http 502") || !strings.Contains(err.Error(), "stream message update expired") {
		t.Fatalf("Send error = %v", err)
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Failure == nil {
		t.Fatalf("Send error = %T %v, want typed HTTPError failure", err, err)
	}
	if httpErr.Failure.Category != uvim.SendFailureRateLimited || !httpErr.Failure.Retryable || httpErr.Failure.ProviderCode != "230020" || len(httpErr.Failure.DeliveredPublications) != 1 || httpErr.Failure.DeliveredPublications[0].Locator.MessageID != "om_confirmed" {
		t.Fatalf("Send failure = %+v", httpErr.Failure)
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
