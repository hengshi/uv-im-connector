package uvim_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/dingtalk"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
	"github.com/hengshi/uv-im-connector/providers/memory"
	"github.com/hengshi/uv-im-connector/providers/slack"
	"github.com/hengshi/uv-im-connector/providers/telegram"
)

func TestUnsupportedCardCannotFallThroughToTextOrResourceSend(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer server.Close()
	slackProvider, err := slack.New(slack.Config{BaseURL: server.URL, Token: "test"})
	if err != nil {
		t.Fatal(err)
	}
	telegramProvider, err := telegram.New(telegram.Config{BaseURL: server.URL, Token: "test"})
	if err != nil {
		t.Fatal(err)
	}
	dingProvider, err := dingtalk.New(dingtalk.Config{BaseURL: server.URL, ClientID: "test", ClientSecret: "test"})
	if err != nil {
		t.Fatal(err)
	}
	generic, err := httpchannel.New(httpchannel.Config{ProviderID: "test", ConnectorID: "main", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	providers := []uvim.Provider{slackProvider, telegramProvider, dingProvider, generic, memory.New("memory")}
	card := &uvim.InteractionCard{SchemaVersion: uvim.InteractionSchemaV1, CardID: "card", Revision: 1, Title: "Title"}
	for _, provider := range providers {
		for _, resources := range [][]uvim.ResourceRef{nil, {{Kind: uvim.ElementFile, URL: server.URL + "/resource", Name: "file.txt"}}} {
			_, err := provider.Send(context.Background(), uvim.OutboundMessage{Provider: provider.ID(), Connector: provider.ConnectorID(), ChannelID: "chat", Text: "must not be sent", Interaction: card, Resources: resources})
			if err == nil {
				t.Fatalf("%s silently accepted a card", provider.ID())
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("unsupported card caused %d native calls", calls.Load())
	}
	if err := uvim.ValidateMessageUpdate(uvim.MessageUpdate{OutboundMessage: uvim.OutboundMessage{Provider: "lark", Connector: "main", Text: "text", Interaction: card}, MessageID: "message"}, uvim.Capabilities{}); err == nil {
		t.Fatal("text update accepted a card")
	}
}

func TestInteractionIdentityProfiles(t *testing.T) {
	card := uvim.InteractionCard{SchemaVersion: uvim.InteractionSchemaV1, CardID: "ip_card", Revision: 1, Title: "需要选择", Actions: []uvim.InteractionAction{{ID: "answer", Label: "继续", Kind: uvim.InteractionAnswer, Token: "opaque-handle"}}}
	digest, err := card.Digest()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		proof uvim.InteractionPublication
		valid bool
	}{
		{"native message", uvim.InteractionPublication{NativeMessageID: "native-message"}, true},
		{"card instance", uvim.InteractionPublication{NativeCardID: card.CardID, BotAppID: "bot"}, true},
		{"mixed namespaces", uvim.InteractionPublication{NativeMessageID: "native-message", NativeCardID: card.CardID, BotAppID: "bot"}, false},
		{"wrong card", uvim.InteractionPublication{NativeCardID: "other", BotAppID: "bot"}, false},
		{"unproved bot", uvim.InteractionPublication{NativeCardID: card.CardID}, false},
		{"absent native identity", uvim.InteractionPublication{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.proof.CardID, tc.proof.Revision, tc.proof.PayloadSHA256 = card.CardID, card.Revision, digest
			if tc.proof.ValidFor(card) != tc.valid {
				t.Fatalf("unexpected confirmation validity for %+v", tc.proof)
			}
		})
	}
	encoded, err := uvim.EncodeInteractionCallbackValue(card, card.Actions[0])
	if err != nil {
		t.Fatal(err)
	}
	value, err := uvim.DecodeInteractionCallbackValue(encoded)
	if err != nil || value.CardID != card.CardID || value.Revision != 1 || value.ActionID != "answer" || value.Kind != uvim.InteractionAnswer || value.Token != "opaque-handle" {
		t.Fatalf("callback value=%+v err=%v", value, err)
	}
	actor := uvim.User{ID: "person", Kind: uvim.UserKindUser}
	now := time.Now().UTC()
	for _, tc := range []struct {
		name   string
		mutate func(*uvim.Event)
		valid  bool
	}{
		{"verified card click", func(*uvim.Event) {}, true},
		{"legacy profile cannot imply card", func(e *uvim.Event) { e.Interaction.SchemaVersion = uvim.InteractionSchemaV1 }, false},
		{"callback id is not original message id", func(e *uvim.Event) { e.Interaction.MessageID = e.ID; e.Referrer.MessageID = e.ID }, false},
		{"wrong native card", func(e *uvim.Event) { e.Interaction.NativeCardID = "other" }, false},
		{"wrong bot", func(e *uvim.Event) { e.Login.ID = "other" }, false},
		{"missing audience", func(e *uvim.Event) { e.Referrer.Target = nil }, false},
		{"direct target cannot be a group", func(e *uvim.Event) { e.Referrer.Target.Kind = uvim.TargetGroup }, false},
		{"direct target must match native actor", func(e *uvim.Event) { e.Referrer.Target.ID = "other" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := uvim.Event{ID: "native-callback-1", Type: uvim.EventInteractionAction, Provider: "wecom", Connector: "main", Time: now, Login: uvim.Login{ID: "bot"}, Channel: uvim.Channel{ID: "person", Type: uvim.ChannelDirect}, User: actor,
				Referrer:    uvim.Referrer{ChannelID: "person", Target: &uvim.OutboundTarget{ID: "person", Kind: uvim.TargetUser}},
				Interaction: &uvim.InteractionActionEvent{SchemaVersion: uvim.InteractionCardInstanceVersionV1, EventID: "native-callback-1", Provider: "wecom", Connector: "main", ConversationID: "person", NativeCardID: card.CardID, CardID: card.CardID, Revision: 1, ActionID: "answer", Kind: uvim.InteractionAnswer, Token: "opaque-handle", Actor: actor, OccurredAt: now, BotAppID: "bot", BotProofDigest: "native-proof"}}
			tc.mutate(&event)
			if err := uvim.ValidateInteractionEvent(event); (err == nil) != tc.valid {
				t.Fatalf("callback validation err=%v, want valid=%v", err, tc.valid)
			}
		})
	}
}
