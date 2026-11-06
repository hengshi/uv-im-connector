package uvim

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func recoveryInteractionEvent() Event {
	actor := User{ID: "original-user", Kind: UserKindUser}
	return Event{ID: "click-before-ack", Type: EventInteractionAction, Provider: "lark", Connector: "main", Channel: Channel{ID: "chat"}, User: actor, Referrer: Referrer{MessageID: "card-message"}, Interaction: &InteractionActionEvent{SchemaVersion: InteractionSchemaV1, EventID: "click-before-ack", Provider: "lark", Connector: "main", ConversationID: "chat", MessageID: "card-message", CardID: "card", Revision: 1, ActionID: "answer", Kind: InteractionAnswer, Token: "private-handle", Actor: actor, OccurredAt: time.Now().UTC(), BotAppID: "app", BotProofDigest: strings.Repeat("a", 64)}}
}

func TestInteractionLogRecoversPreACKTornTailThenDeduplicatesDurableAction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	log, err := NewEventLog(path)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = log.Append(t.Context(), Event{ID: "ordinary", Provider: "lark", Connector: "main", Message: Message{ID: "ordinary"}})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	torn := recoveryInteractionEvent()
	raw, _ := json.Marshal(torn)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write(raw[:len(raw)/2]); err != nil {
		t.Fatal(err)
	}
	file.Close()
	visible, err := log.ReadAfter(t.Context(), 0)
	if err != nil || len(visible) != 1 {
		t.Fatalf("uncommitted tail became readable: %d %v", len(visible), err)
	}
	restarted, err := NewEventLog(path)
	if err != nil {
		t.Fatal(err)
	}
	recovered, _ := os.ReadFile(path)
	if string(recovered) != string(committed) {
		t.Fatal("recovery altered committed prefix")
	}
	appended, fresh, err := restarted.Append(t.Context(), torn)
	if err != nil || !fresh || appended.Sequence != 2 {
		t.Fatalf("retried pre-ACK action: %+v %v %v", appended, fresh, err)
	}
	// The append has synced but the transport has not sent ACK yet. A fresh
	// process must retain this action and accept vendor redelivery once.
	again, err := NewEventLog(path)
	if err != nil {
		t.Fatal(err)
	}
	_, fresh, err = again.Append(t.Context(), torn)
	if err != nil || fresh {
		t.Fatalf("durable-before-ACK duplicated: %v %v", fresh, err)
	}
	visible, err = again.ReadAfter(context.Background(), 0)
	if err != nil || len(visible) != 2 || visible[1].Interaction == nil {
		t.Fatalf("durable action disappeared: %d %v", len(visible), err)
	}
	if err = os.WriteFile(path, append(committed, []byte("invalid-complete-record\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = NewEventLog(path); err == nil {
		t.Fatal("complete corrupt record was silently repaired")
	}
	torn.Interaction.SchemaVersion = "interaction.future"
	raw, _ = json.Marshal(torn)
	if err = os.WriteFile(path, append(committed, append(raw, '\n')...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = NewEventLog(path); err == nil {
		t.Fatal("complete invalid interaction schema was accepted on replay")
	}
}
