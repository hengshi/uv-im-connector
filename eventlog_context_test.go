package uvim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEventLogConversationContextCursorAdvancesAndRevalidatesSelectedBodies(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	log, err := NewEventLog(path)
	if err != nil {
		t.Fatal(err)
	}
	for index, item := range []struct{ id, text string }{{"a", "body-one"}, {"b", "body-two"}} {
		if _, fresh, err := log.Append(context.Background(), Event{ID: "evt-" + item.id, Type: EventMessageCreate, Provider: "test", Connector: "main", Channel: Channel{ID: "group", Type: ChannelGroup}, User: User{ID: "sender", Kind: UserKindUser}, Message: Message{ID: "message-" + item.id, Text: item.text, CreatedAt: time.Date(2026, time.November, 6, 15, index, 0, 0, time.UTC)}}); err != nil || !fresh {
			t.Fatalf("append %d fresh=%t err=%v", index, fresh, err)
		}
	}
	initial, err := log.ConversationContextAdvance(context.Background(), ConversationContextCursor{})
	if err != nil || !initial.Reset || len(initial.Events) != 2 || len(initial.Cursor.Records) != 2 || initial.Cursor.EOFOffset <= 0 || initial.Cursor.FileIdentity == "" {
		t.Fatalf("initial=%+v err=%v", initial, err)
	}
	encoded, err := json.Marshal(initial.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "body-one") || strings.Contains(string(encoded), "body-two") {
		t.Fatalf("cursor copied EventLog bodies: %s", encoded)
	}
	stable, err := log.ConversationContextAdvance(context.Background(), initial.Cursor)
	if err != nil || stable.Reset || len(stable.Events) != 0 || stable.Cursor.EOFOffset != initial.Cursor.EOFOffset {
		t.Fatalf("stable=%+v err=%v", stable, err)
	}
	if _, fresh, err := log.Append(context.Background(), Event{ID: "evt-c", Type: EventMessageCreate, Provider: "test", Connector: "main", Channel: Channel{ID: "group", Type: ChannelGroup}, User: User{ID: "sender", Kind: UserKindUser}, Message: Message{ID: "message-c", Text: "body-three", CreatedAt: time.Date(2026, time.November, 6, 15, 2, 0, 0, time.UTC)}}); err != nil || !fresh {
		t.Fatalf("append third fresh=%t err=%v", fresh, err)
	}
	advanced, err := log.ConversationContextAdvance(context.Background(), stable.Cursor)
	if err != nil || advanced.Reset || len(advanced.Events) != 1 || advanced.Events[0].Message.Text != "body-three" || advanced.Cursor.EOFOffset <= stable.Cursor.EOFOffset {
		t.Fatalf("advanced=%+v err=%v", advanced, err)
	}
	selected, err := log.ConversationContextSelected(context.Background(), advanced.Cursor, []ConversationContextRecord{advanced.Cursor.Records[0], advanced.Cursor.Records[len(advanced.Cursor.Records)-1]})
	if err != nil || len(selected) != 2 || selected[advanced.Cursor.Records[0].Sequence].Message.Text != "body-one" || selected[advanced.Cursor.Records[len(advanced.Cursor.Records)-1].Sequence].Message.Text != "body-three" {
		t.Fatalf("selected=%+v err=%v", selected, err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(dir, "replacement.jsonl")
	if err := os.WriteFile(replacement, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := log.ConversationContextAdvance(context.Background(), advanced.Cursor)
	if err != nil || !rebuilt.Reset || len(rebuilt.Events) != 3 {
		t.Fatalf("inode replacement rebuilt=%+v err=%v", rebuilt, err)
	}
}

func TestEventLogConversationContextBoundsRawRebuildAndCatchup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	log, err := NewEventLog(path)
	if err != nil {
		t.Fatal(err)
	}
	writeBatch := func(first, count int) {
		t.Helper()
		file, openErr := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if openErr != nil {
			t.Fatal(openErr)
		}
		for offset := 0; offset < count; offset++ {
			sequence := first + offset
			raw, marshalErr := json.Marshal(Event{ID: fmt.Sprintf("evt-%d", sequence), Sequence: int64(sequence), Type: EventMessageCreate, Provider: "test", Connector: "main", Channel: Channel{ID: "group", Type: ChannelGroup}, Message: Message{ID: fmt.Sprintf("message-%d", sequence), Text: "small"}})
			if marshalErr != nil {
				_ = file.Close()
				t.Fatal(marshalErr)
			}
			if _, writeErr := file.Write(append(raw, '\n')); writeErr != nil {
				_ = file.Close()
				t.Fatal(writeErr)
			}
		}
		if closeErr := file.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	writeBatch(1, conversationWindowLogRecords+1)
	initial, err := log.ConversationContextAdvance(context.Background(), ConversationContextCursor{})
	if err != nil || !initial.Reset || len(initial.Events) != conversationWindowLogRecords || len(initial.Cursor.Records) != conversationWindowLogRecords || initial.Cursor.LowerBoundSequence != 2 {
		t.Fatalf("bounded initial=%+v err=%v", initial, err)
	}
	writeBatch(conversationWindowLogRecords+2, conversationWindowLogRecords+1)
	advanced, err := log.ConversationContextAdvance(context.Background(), initial.Cursor)
	if err != nil || !advanced.Reset || len(advanced.Events) != conversationWindowLogRecords || len(advanced.Cursor.Records) != conversationWindowLogRecords || advanced.Cursor.LowerBoundSequence != int64(conversationWindowLogRecords+3) {
		t.Fatalf("bounded catchup=%+v err=%v", advanced, err)
	}
}
