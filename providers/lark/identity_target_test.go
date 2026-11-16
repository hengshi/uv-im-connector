package lark

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/client"
	"github.com/hengshi/uv-im-connector/server"
)

func TestInteractionInverseIdentityRequiresCurrentNativePrivateChat(t *testing.T) {
	var chatMode atomic.Value
	chatMode.Store("p2p")
	var chats atomic.Int32
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "native", "expire": 3600})
		case "/open-apis/im/v1/messages/om-card":
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{"message_id": "om-card", "chat_id": "oc-private", "msg_type": "interactive", "sender": map[string]string{"id": "app", "id_type": "app_id", "sender_type": "app"}}}}})
		case "/open-apis/im/v1/chats/oc-private":
			chats.Add(1)
			if chatMode.Load().(string) == "denied" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]string{"chat_mode": chatMode.Load().(string)}})
		default:
			t.Errorf("unexpected native identity call %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer native.Close()
	p, err := New(Config{AppID: "app", AppSecret: "secret", ConnectorID: "main", BaseURL: native.URL})
	if err != nil {
		t.Fatal(err)
	}
	log, err := uvim.NewEventLog(filepath.Join(t.TempDir(), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	hub := server.NewHub(uvim.NewProviderRegistry(p), log, nil)
	hub.SetAuthToken("private-api")
	if err = hub.EnableIdentityLinks(filepath.Join(t.TempDir(), "links.json")); err != nil {
		t.Fatal(err)
	}
	defer hub.CloseIdentityLinks()
	api := httptest.NewServer(hub.Handler())
	defer api.Close()
	c := client.New(api.URL)
	c.Token = "private-api"
	origin := uvim.Event{ID: "original-private", Type: uvim.EventMessageCreate, Provider: "lark", Connector: "main", Channel: uvim.Channel{ID: "oc-private", Type: uvim.ChannelDirect}, User: uvim.User{ID: "ou-requester", Kind: uvim.UserKindUser}, Message: uvim.Message{ID: "om-origin"}, Referrer: uvim.Referrer{Target: &uvim.OutboundTarget{ID: "oc-private", Kind: uvim.TargetConversation}}}
	if err = hub.Emit(t.Context(), origin); err != nil {
		t.Fatal(err)
	}
	linked, err := c.LinkIdentity(t.Context(), uvim.IdentityLinkRequest{Identity: uvim.UserIdentity{Type: "gitlab.userid", Scope: "gitlab.example", ID: "17"}, Event: uvim.IdentityEventRef{Provider: "lark", Connector: "main", EventID: origin.ID}, Proof: "verified-provider-note"})
	if err != nil || !linked.Found || chats.Load() != 0 {
		t.Fatalf("released direct identity changed: linked=%v chat_reads=%d err=%v", linked.Found, chats.Load(), err)
	}
	for _, mode := range []string{"p2p", "group", "denied"} {
		chatMode.Store(mode)
		id := "click-" + mode
		raw, _ := json.Marshal(map[string]any{"schema": "2.0", "header": map[string]string{"event_type": "card.action.trigger", "event_id": id, "app_id": "app", "create_time": strconv.FormatInt(time.Now().UnixMicro(), 10)}, "event": map[string]any{"operator": map[string]string{"open_id": "ou-requester"}, "context": map[string]string{"open_chat_id": "oc-private", "open_message_id": "om-card"}, "action": map[string]any{"value": callbackValue(interactionTestCard(), interactionTestCard().Actions[0])}}})
		event, ok, decodeErr := DecodePayload(raw, DecoderConfig{AppID: "app", Connector: "main"})
		if decodeErr != nil || !ok {
			t.Fatalf("callback decode: %v %v", ok, decodeErr)
		}
		if err = hub.Emit(t.Context(), event); err != nil {
			t.Fatal(err)
		}
		resolved, resolveErr := c.ResolveInboundIdentity(t.Context(), uvim.IdentityInboundResolveRequest{Provider: "lark", Connector: "main", EventID: id})
		if resolveErr != nil || resolved.NativeActor.UserID != "ou-requester" || len(resolved.Links) != 1 || resolved.EventDigest == "" {
			t.Fatalf("typed callback actor/link proof: %+v %v", resolved, resolveErr)
		}
		if (resolved.DirectTarget != nil) != (mode == "p2p") {
			t.Fatalf("%s inherited private target without native p2p proof: %+v", mode, resolved.DirectTarget)
		}
		if resolved.DirectTarget != nil && (resolved.DirectTarget.ID != "oc-private" || resolved.DirectTarget.Kind != uvim.TargetConversation) {
			t.Fatal("private target was reconstructed as a user address")
		}
	}
	if chats.Load() != 3 {
		t.Fatalf("private action proof was cached across scope changes: %d", chats.Load())
	}
}
