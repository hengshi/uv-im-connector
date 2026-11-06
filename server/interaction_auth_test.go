package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/lark"
)

func TestInteractionRequiresBearerAndHistoricalHandlesStayPrivateWithoutIt(t *testing.T) {
	log, err := uvim.NewEventLog(filepath.Join(t.TempDir(), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	actor := uvim.User{ID: "original-user", Kind: uvim.UserKindUser}
	action := uvim.Event{ID: "click", Type: uvim.EventInteractionAction, Provider: "lark", Connector: "main", Channel: uvim.Channel{ID: "chat"}, User: actor, Referrer: uvim.Referrer{MessageID: "card-message"}, Interaction: &uvim.InteractionActionEvent{SchemaVersion: uvim.InteractionSchemaV1, EventID: "click", Provider: "lark", Connector: "main", ConversationID: "chat", MessageID: "card-message", CardID: "card", Revision: 1, ActionID: "answer", Kind: uvim.InteractionAnswer, Token: "must-remain-private", Actor: actor, OccurredAt: time.Now().UTC(), BotAppID: "app", BotProofDigest: strings.Repeat("a", 64)}}
	if _, _, err = log.Append(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	if _, _, err = log.Append(t.Context(), uvim.Event{ID: "ordinary", Provider: "lark", Connector: "main", Message: uvim.Message{ID: "ordinary", Text: "visible"}}); err != nil {
		t.Fatal(err)
	}
	provider, err := lark.New(lark.Config{AppID: "app", AppSecret: "secret", ConnectorID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	hub := NewHub(uvim.NewProviderRegistry(provider), log, nil)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	if err = hub.Emit(t.Context(), action); err == nil {
		t.Fatal("unauthenticated interaction ingress accepted")
	}
	response, err := http.Get(server.URL + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || strings.Contains(string(raw), "must-remain-private") || strings.Contains(string(raw), "interaction.action") || !strings.Contains(string(raw), "visible") {
		t.Fatalf("private HTTP action exposure: %d %s", response.StatusCode, raw)
	}
	connection, _, err := websocket.DefaultDialer.Dial(strings.Replace(server.URL, "http:", "ws:", 1)+"/v1/events/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	connection.SetReadDeadline(time.Now().Add(time.Second))
	_, raw, err = connection.ReadMessage()
	if err != nil || strings.Contains(string(raw), "must-remain-private") || !strings.Contains(string(raw), "visible") {
		t.Fatalf("private WS action exposure: %s %v", raw, err)
	}
	card := uvim.InteractionCard{SchemaVersion: uvim.InteractionSchemaV1, CardID: "card", Revision: 1, Title: "Title"}
	payload, _ := json.Marshal(uvim.OutboundMessage{Provider: "lark", Connector: "main", ChannelID: "chat", Interaction: &card})
	response, err = http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated card send status=%d", response.StatusCode)
	}
}
