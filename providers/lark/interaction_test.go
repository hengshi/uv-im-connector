package lark

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/client"
	"github.com/hengshi/uv-im-connector/server"
)

func interactionTestCard() uvim.InteractionCard {
	return uvim.InteractionCard{SchemaVersion: uvim.InteractionSchemaV1, CardID: "presentation-a", Revision: 1, Title: "请选择目标版本", Summary: "<at id=all>literal</at>", Question: &uvim.CardQuestion{ID: "question-a", Prompt: "需要哪个版本？", AllowFreeText: true, FreeTextActionID: "free-answer"}, Actions: []uvim.InteractionAction{{ID: "stable", Label: "稳定版", Kind: uvim.InteractionAnswer, Token: strings.Repeat("a", 64)}, {ID: "free-answer", Label: "提交补充", Kind: uvim.InteractionAnswer, Token: strings.Repeat("b", 64)}}}
}

// Real Hub HTTP and Lark HTTP code exercise the transport ledger. The vendor
// endpoint is a protocol fixture, not live tenant/authentication qualification.
func TestInteractionCardSendUpdateRestartAndUnknownDoesNotResend(t *testing.T) {
	var patches atomic.Int32
	var dropACK atomic.Bool
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal":
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "native-secret", "expire": 3600})
		case r.URL.Path == "/open-apis/im/v1/messages" && r.Method == http.MethodPost:
			var request struct {
				Type    string `json:"msg_type"`
				Content string `json:"content"`
			}
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.Type != "interactive" {
				t.Error("card did not use native interactive send")
			}
			var card map[string]any
			if json.Unmarshal([]byte(request.Content), &card) != nil || card["schema"] != "2.0" || card["body"] == nil {
				t.Error("native Card JSON 2.0 body missing")
			}
			if strings.Contains(request.Content, `"tag":"div"`) || strings.Contains(request.Content, `"tag":"action"`) || strings.Contains(request.Content, "<at id=all>") {
				t.Error("card reused deprecated/unsafe presentation")
			}
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"message_id": "om-card"}})
		case r.URL.Path == "/open-apis/im/v1/messages/om-card" && r.Method == http.MethodGet:
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{"message_id": "om-card", "chat_id": "oc-group", "msg_type": "interactive", "sender": map[string]string{"id": "app", "id_type": "app_id", "sender_type": "app"}}}}})
		case r.URL.Path == "/open-apis/im/v1/messages/om-card" && r.Method == http.MethodPatch:
			patches.Add(1)
			var request map[string]any
			if json.NewDecoder(r.Body).Decode(&request) != nil || request["content"] == nil || request["msg_type"] != nil {
				t.Error("card PATCH must contain content only")
			}
			if dropACK.Load() {
				connection, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				connection.Close()
				return
			}
			// Official PATCH response has no message-id data echo.
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "msg": "success"})
		default:
			t.Errorf("unexpected native request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer native.Close()
	provider, err := New(Config{AppID: "app", AppSecret: "native-secret", BotOpenID: "bot", ConnectorID: "main", BaseURL: native.URL})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	log, err := uvim.NewEventLog(filepath.Join(root, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	hub := server.NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(root, "resources")})
	hub.SetAuthToken("connector-secret")
	api := httptest.NewServer(hub.Handler())
	defer func() { api.Close() }()
	c := client.New(api.URL)
	c.Token = "connector-secret"
	card := interactionTestCard()
	target := uvim.OutboundTarget{Kind: uvim.TargetConversation, ID: "oc-group"}
	referrer := uvim.Referrer{ChannelID: "oc-group"}
	sent, err := c.Send(t.Context(), uvim.OutboundMessage{Provider: "lark", Connector: "main", Target: &target, Referrer: referrer, Interaction: &card})
	if err != nil || sent.Interaction == nil || sent.MessageID != "om-card" {
		t.Fatalf("card send=%+v err=%v", sent, err)
	}
	card.Revision = 2
	digest, _ := card.Digest()
	request := uvim.InteractionMessageUpdate{Version: uvim.InteractionUpdateVersionV1, Provider: "lark", Connector: "main", NativeMessageID: "om-card", Target: target, Referrer: referrer, CardID: card.CardID, ExpectedRevision: 1, ReplacementRevision: 2, Card: card, PayloadSHA256: digest, OperationID: "operation-a"}
	wrong := request
	wrong.Target.ID = "oc-other"
	denied, err := c.UpdateInteraction(t.Context(), wrong)
	if err != nil || denied.DeliveryState != uvim.InteractionNotAttempted || patches.Load() != 0 {
		t.Fatalf("cross-scope update=%+v %v", denied, err)
	}
	time.Sleep(510 * time.Millisecond)
	updated, err := c.UpdateInteraction(t.Context(), request)
	if err != nil || updated.DeliveryState != uvim.InteractionDelivered || patches.Load() != 1 {
		t.Fatalf("card update=%+v patches=%d err=%v", updated, patches.Load(), err)
	}
	api.Close()
	hub = server.NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(root, "resources")})
	hub.SetAuthToken("connector-secret")
	api = httptest.NewServer(hub.Handler())
	c = client.New(api.URL)
	c.Token = "connector-secret"
	replayed, err := c.UpdateInteraction(t.Context(), request)
	if err != nil || replayed != updated || patches.Load() != 1 {
		t.Fatalf("restart replay=%+v patches=%d err=%v", replayed, patches.Load(), err)
	}
	conflict := request
	conflict.Card.Title = "different"
	conflict.PayloadSHA256, _ = conflict.Card.Digest()
	refused, err := c.UpdateInteraction(t.Context(), conflict)
	if err != nil || refused.SafeReason != "operation_conflict" || patches.Load() != 1 {
		t.Fatalf("conflicting operation=%+v %v", refused, err)
	}
	// Deliberate crash fixture: the real operation receipt committed, while
	// its presentation pointer still contains the earlier sealed claim.
	key := sha256.Sum256([]byte("lark\x00main\x00om-card"))
	directory := filepath.Join(root, "state", "interaction-updates", hex.EncodeToString(key[:]))
	opKey := sha256.Sum256([]byte(request.OperationID))
	operationRaw, err := os.ReadFile(filepath.Join(directory, "operations", hex.EncodeToString(opKey[:])+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var pending map[string]any
	if json.Unmarshal(operationRaw, &pending) != nil {
		t.Fatal("cannot read actual receipt")
	}
	pending["result"].(map[string]any)["delivery_state"] = uvim.InteractionUnknown
	presentationPath := filepath.Join(directory, "presentation.json")
	presentationRaw, err := os.ReadFile(presentationPath)
	if err != nil {
		t.Fatal(err)
	}
	var presentation map[string]any
	if json.Unmarshal(presentationRaw, &presentation) != nil {
		t.Fatal("cannot read actual presentation")
	}
	presentation["pending"], presentation["revision"] = pending, 1
	initialDigest, _ := interactionTestCard().Digest()
	presentation["payload_sha256"] = initialDigest
	presentationRaw, _ = json.Marshal(presentation)
	if err = os.WriteFile(presentationPath, presentationRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	repaired, err := c.UpdateInteraction(t.Context(), request)
	if err != nil || repaired != updated || patches.Load() != 1 {
		t.Fatalf("receipt-pointer recovery=%+v %v", repaired, err)
	}
	card.Revision = 3
	request.Card, request.ExpectedRevision, request.ReplacementRevision, request.OperationID = card, 2, 3, "operation-b"
	request.PayloadSHA256, _ = card.Digest()
	time.Sleep(510 * time.Millisecond)
	dropACK.Store(true)
	unknown, err := c.UpdateInteraction(t.Context(), request)
	if err != nil || unknown.DeliveryState != uvim.InteractionUnknown || patches.Load() != 2 {
		t.Fatalf("lost-ACK update=%+v patches=%d err=%v", unknown, patches.Load(), err)
	}
	again, err := c.UpdateInteraction(t.Context(), request)
	if err != nil || again != unknown || patches.Load() != 2 {
		t.Fatal("unknown update was retried")
	}
	request.OperationID = "operation-c"
	blocked, err := c.UpdateInteraction(t.Context(), request)
	if err != nil || blocked.SafeReason != "prior_update_unknown" || patches.Load() != 2 {
		t.Fatalf("unknown new-operation retry=%+v %v", blocked, err)
	}
}

func TestInteractionCallbackIsDurableBeforeACKAndKeepsDistinctClicks(t *testing.T) {
	anchor := time.Now().UTC()
	log, err := uvim.NewEventLog(filepath.Join(t.TempDir(), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	ackDone := make(chan error, 1)
	var api *httptest.Server
	api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "native-secret", "expire": 3600})
		case "/open-apis/im/v1/messages/om-card":
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{"message_id": "om-card", "chat_id": "oc-group", "msg_type": "interactive", "parent_id": "om-parent", "root_id": "om-root", "thread_id": "thread-native", "sender": map[string]string{"id": "app", "id_type": "app_id", "sender_type": "app"}}}}})
		case "/callback/ws/endpoint":
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": strings.Replace(api.URL, "http:", "ws:", 1) + "/ws?service_id=1"}})
		case "/ws":
			connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				ackDone <- err
				return
			}
			defer connection.Close()
			for i, id := range []string{"click-a", "click-a", "click-b"} {
				payload, _ := json.Marshal(map[string]any{"schema": "2.0", "header": map[string]string{"event_type": "card.action.trigger", "event_id": id, "app_id": "app", "create_time": strconv.FormatInt(anchor.UnixMicro(), 10)}, "event": map[string]any{"operator": map[string]string{"open_id": "ou-user"}, "token": "native-update-secret", "context": map[string]string{"open_chat_id": "oc-group", "open_message_id": "om-card"}, "action": map[string]any{"value": callbackValue(interactionTestCard(), interactionTestCard().Actions[0])}}})
				frame := &wsFrame{Method: frameMethodData, Headers: []frameHeader{{Key: frameHeaderMessageID, Value: fmt.Sprint(i)}, {Key: frameHeaderTypeKey, Value: "card"}}, Payload: payload}
				if err = connection.WriteMessage(websocket.BinaryMessage, frame.marshal()); err != nil {
					ackDone <- err
					return
				}
				connection.SetReadDeadline(time.Now().Add(3 * time.Second))
				_, raw, err := connection.ReadMessage()
				if err != nil {
					ackDone <- err
					return
				}
				ack, err := unmarshalFrame(raw)
				if err != nil || !strings.Contains(string(ack.Payload), `"code":200`) {
					ackDone <- errors.New("callback did not ACK accepted transport")
					return
				}
				events, readErr := log.ReadAfter(t.Context(), 0)
				want := 1
				if i == 2 {
					want = 2
				}
				if readErr != nil || len(events) != want {
					ackDone <- errors.New("callback ACK preceded durable append or collapsed clicks")
					return
				}
				for _, event := range events {
					encoded, _ := json.Marshal(event)
					if event.Referrer.ParentMessageID != "om-parent" || event.Referrer.RootMessageID != "om-root" || event.Referrer.ThreadID != "thread-native" {
						ackDone <- errors.New("callback lost its verified native thread")
						return
					}
					if event.Type != uvim.EventInteractionAction || event.Message.Text != "" || event.Interaction == nil || strings.Contains(string(encoded), "native-update-secret") {
						ackDone <- errors.New("callback lost typed scope or leaked vendor token")
						return
					}
				}
			}
			ackDone <- nil
		default:
			t.Errorf("unexpected callback request %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer api.Close()
	provider, err := New(Config{AppID: "app", AppSecret: "secret", BotOpenID: "bot", ConnectorID: "main", BaseURL: api.URL, CallbackBaseURL: api.URL, Now: func() time.Time { return anchor }})
	if err != nil {
		t.Fatal(err)
	}
	hub := server.NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: t.TempDir()})
	hub.SetAuthToken("connector-secret")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- provider.Run(ctx, hub) }()
	select {
	case err := <-ackDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("callback transport did not complete")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("callback provider did not stop")
	}
}
