package wecom

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

func wecomInteractionTestCard() uvim.InteractionCard {
	return uvim.InteractionCard{
		SchemaVersion: uvim.InteractionSchemaV1,
		CardID:        "task-card_1@jarvis",
		Revision:      1,
		Title:         "需要确认部署范围",
		Summary:       "任务 #42 正在等待确认",
		Facts:         []uvim.CardFact{{Label: "环境", Value: "staging"}},
		Question:      &uvim.CardQuestion{ID: "scope", Prompt: "请选择部署范围", Help: "确认后将继续执行"},
		Actions: []uvim.InteractionAction{
			{ID: "staging", Label: "仅预发", Kind: uvim.InteractionAnswer, Token: strings.Repeat("a", 64)},
			{ID: "production", Label: "生产", Kind: uvim.InteractionAnswer, Token: strings.Repeat("b", 64)},
		},
	}
}

func TestWeComCapabilitiesAdvertiseOnlyFixedCardInstances(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	caps := provider.Capabilities()
	if !caps.InteractiveMessage || !caps.InteractiveActionCallback || caps.InteractionCardInstanceVersion != uvim.InteractionCardInstanceVersionV1 || caps.CardMaxActions != wecomTemplateCardMaxButtons || caps.InteractionFreeText || caps.InteractiveMessageUpdate {
		t.Fatalf("capabilities = %+v", caps)
	}
}

func TestWeComSendInteractionUsesNativeTemplateCardWithoutMessageLocator(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret", Now: func() time.Time { return time.Unix(100, 0).UTC() }})
	if err != nil {
		t.Fatal(err)
	}
	card := wecomInteractionTestCard()
	key, err := uvim.EncodeInteractionCallbackValue(card, card.Actions[0])
	if err != nil {
		t.Fatal(err)
	}
	conn := &sendTestConn{}
	conn.onWrite = func(raw []byte) {
		var sent frame
		if err := json.Unmarshal(raw, &sent); err != nil {
			t.Error(err)
			return
		}
		want := map[string]any{
			"chatid":  "group-1",
			"msgtype": "template_card",
			"template_card": map[string]any{
				"card_type":      wecomTemplateCardTypeButton,
				"task_id":        card.CardID,
				"main_title":     map[string]any{"title": card.Title},
				"sub_title_text": "任务 #42 正在等待确认\n环境: staging\n请选择部署范围\n确认后将继续执行",
				"button_list": []any{
					map[string]any{"text": "仅预发", "key": key},
					map[string]any{"text": "生产", "key": mustWeComInteractionKey(t, card, card.Actions[1])},
				},
			},
		}
		if sent.Cmd != cmdSend || !reflect.DeepEqual(sent.Body, want) {
			t.Errorf("frame = %+v, want %+v", sent, want)
		}
		code := 0
		provider.resolvePending(sent.Headers.ReqID, frame{Headers: sent.Headers, ErrCode: &code})
	}
	activateSendTestConn(provider, conn)
	result, err := provider.Send(context.Background(), uvim.OutboundMessage{
		Target:      &uvim.OutboundTarget{Kind: uvim.TargetGroup, ID: "group-1"},
		Interaction: &card,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "" || result.Interaction == nil || !result.Interaction.ValidFor(card) || result.Interaction.NativeMessageID != "" || result.Interaction.NativeCardID != card.CardID || result.Interaction.BotAppID != "bot" || len(result.Publications) != 0 {
		t.Fatalf("result = %+v", result)
	}
}

func TestWeComSendInteractionReplyUsesCallbackRequestID(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	card := wecomInteractionTestCard()
	conn := &sendTestConn{}
	conn.onWrite = func(raw []byte) {
		var sent frame
		if err := json.Unmarshal(raw, &sent); err != nil {
			t.Error(err)
			return
		}
		if sent.Cmd != cmdRespond || sent.Headers.ReqID != "incoming-request" || sent.Body["chatid"] != nil || sent.Body["msgtype"] != "template_card" {
			t.Errorf("reply frame = %+v", sent)
		}
		code := 0
		provider.resolvePending(sent.Headers.ReqID, frame{Headers: sent.Headers, ErrCode: &code})
	}
	activateSendTestConn(provider, conn)
	result, err := provider.Send(context.Background(), uvim.OutboundMessage{
		Target:      &uvim.OutboundTarget{Kind: uvim.TargetGroup, ID: "group-1"},
		Referrer:    uvim.Referrer{ReplyToken: "incoming-request", MessageID: "incoming-message", ChannelID: "group-1", Target: &uvim.OutboundTarget{Kind: uvim.TargetGroup, ID: "group-1"}},
		Interaction: &card,
	})
	if err != nil || result.MessageID != "" || result.Interaction == nil || result.Interaction.NativeCardID != card.CardID {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestWeComInteractionUnsupportedPresentationDoesNotWrite(t *testing.T) {
	for _, test := range []struct {
		name       string
		card       uvim.InteractionCard
		channelID  string
		targetKind string
		referrer   uvim.Referrer
	}{
		{name: "free-text", card: withWeComFreeTextCard()},
		{name: "more-than-six-actions", card: withWeComActions(7)},
		{name: "invalid-task-id", card: withWeComCardID("task id")},
		{name: "event-key-over-budget", card: withWeComOverBudgetKey()},
		{name: "disabled-action", card: withWeComDisabledAction()},
		{name: "non-native-conversation-target", card: wecomInteractionTestCard(), targetKind: uvim.TargetConversation},
		{name: "callback-conversation-mismatch", card: wecomInteractionTestCard(), channelID: "different-private-conversation"},
		{name: "missing-reply-target", card: wecomInteractionTestCard(), referrer: uvim.Referrer{ReplyToken: "incoming-request"}},
		{name: "mismatching-reply-target", card: wecomInteractionTestCard(), referrer: uvim.Referrer{ReplyToken: "incoming-request", Target: &uvim.OutboundTarget{Kind: uvim.TargetGroup, ID: "other-group"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider, err := New(Config{BotID: "bot", Secret: "secret"})
			if err != nil {
				t.Fatal(err)
			}
			writes := 0
			conn := &sendTestConn{onWrite: func(raw []byte) {
				writes++
				var sent frame
				if err := json.Unmarshal(raw, &sent); err != nil {
					t.Error(err)
					return
				}
				code := 0
				provider.resolvePending(sent.Headers.ReqID, frame{Headers: sent.Headers, ErrCode: &code})
			}}
			activateSendTestConn(provider, conn)
			_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: test.channelID, Referrer: test.referrer, Target: &uvim.OutboundTarget{Kind: uvim.FirstNonEmpty(test.targetKind, uvim.TargetUser), ID: "user-1"}, Interaction: &test.card})
			failure, ok := uvim.ProviderSendFailure(err)
			if !ok || writes != 0 || failure.Category != uvim.SendFailureInvalidRequest || failure.DeliveryState != uvim.DeliveryNotAttempted {
				t.Fatalf("writes=%d failure=%+v error=%v", writes, failure, err)
			}
		})
	}
}

func TestDecodeTemplateCardEventKeepsCurrentNativeScope(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret", ConnectorID: "main", Now: func() time.Time { return time.Unix(100, 0).UTC() }})
	if err != nil {
		t.Fatal(err)
	}
	card := wecomInteractionTestCard()
	key := mustWeComInteractionKey(t, card, card.Actions[0])
	for _, test := range []struct {
		name        string
		chatType    string
		chatID      string
		wantChannel string
		wantType    string
		wantTarget  uvim.OutboundTarget
	}{
		{"group", "group", "group-1", "group-1", uvim.ChannelGroup, uvim.OutboundTarget{Kind: uvim.TargetGroup, ID: "group-1"}},
		{"single", "single", "untrusted-chat-id", "user-1", uvim.ChannelDirect, uvim.OutboundTarget{Kind: uvim.TargetUser, ID: "user-1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			event, ok := provider.decodeTemplateCardEvent(frame{Cmd: cmdEventCallback, Headers: headers{ReqID: "request-1"}, Body: map[string]any{
				"msgid": "callback-1", "msgtype": "event", "aibotid": "bot", "chattype": test.chatType, "chatid": test.chatID,
				"from":  map[string]any{"userid": "user-1"},
				"event": map[string]any{"eventtype": "template_card_event", "task_id": card.CardID, "event_key": key},
			}})
			if !ok || event.ID != "callback-1" || event.Type != uvim.EventInteractionAction || event.Channel.ID != test.wantChannel || event.Channel.Type != test.wantType || event.Referrer.Target == nil || *event.Referrer.Target != test.wantTarget || event.Referrer.MessageID != "" || event.Referrer.ReplyToken != "" || event.Message.ID != "" || event.Message.Text != "" || len(event.Message.Elements) != 0 || len(event.Message.Resources) != 0 {
				t.Fatalf("event=%+v", event)
			}
			action := event.Interaction
			if action == nil || action.SchemaVersion != uvim.InteractionCardInstanceVersionV1 || action.EventID != event.ID || action.MessageID != "" || action.NativeCardID != card.CardID || action.CardID != card.CardID || action.Revision != card.Revision || action.ActionID != card.Actions[0].ID || action.Token != card.Actions[0].Token || action.Actor.ID != "user-1" || action.BotAppID != "bot" || action.BotProofDigest == "" || event.Login.ID != "bot" {
				t.Fatalf("action=%+v event=%+v", action, event)
			}
			if err := uvim.ValidateInteractionEvent(event); err != nil {
				t.Fatalf("ValidateInteractionEvent: %v", err)
			}
		})
	}
}

func TestDecodeTemplateCardEventRejectsUnprovenScopeAndCard(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	card := wecomInteractionTestCard()
	key := mustWeComInteractionKey(t, card, card.Actions[0])
	base := map[string]any{
		"msgid": "callback-1", "msgtype": "event", "aibotid": "bot", "chattype": "group", "chatid": "group-1",
		"from":  map[string]any{"userid": "user-1"},
		"event": map[string]any{"eventtype": "template_card_event", "task_id": card.CardID, "event_key": key},
	}
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"wrong-bot", func(body map[string]any) { body["aibotid"] = "other" }},
		{"missing-chat-type", func(body map[string]any) { delete(body, "chattype") }},
		{"group-without-chat-id", func(body map[string]any) { delete(body, "chatid") }},
		{"task-does-not-match-card", func(body map[string]any) { uvim.MapStringAny(body["event"])["task_id"] = "other-card" }},
		{"not-a-template-event", func(body map[string]any) { uvim.MapStringAny(body["event"])["eventtype"] = "feedback_event" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := cloneWeComMap(base)
			test.mutate(body)
			if event, ok := provider.decodeTemplateCardEvent(frame{Cmd: cmdEventCallback, Body: body}); ok {
				t.Fatalf("unexpected event=%+v", event)
			}
		})
	}
}

func TestWeComPrivateTargetUsesOnlyCurrentDirectProof(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret", ConnectorID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	card := wecomInteractionTestCard()
	key := mustWeComInteractionKey(t, card, card.Actions[0])
	direct, ok := provider.decodeTemplateCardEvent(frame{Cmd: cmdEventCallback, Body: map[string]any{
		"msgid": "direct-callback", "msgtype": "event", "aibotid": "bot", "chattype": "single",
		"from":  map[string]any{"userid": "user-1"},
		"event": map[string]any{"eventtype": "template_card_event", "task_id": card.CardID, "event_key": key},
	}})
	if !ok {
		t.Fatal("direct card callback was not decoded")
	}
	group, ok := provider.decodeTemplateCardEvent(frame{Cmd: cmdEventCallback, Body: map[string]any{
		"msgid": "group-callback", "msgtype": "event", "aibotid": "bot", "chattype": "group", "chatid": "group-1",
		"from":  map[string]any{"userid": "user-1"},
		"event": map[string]any{"eventtype": "template_card_event", "task_id": card.CardID, "event_key": key},
	}})
	if !ok {
		t.Fatal("group card callback was not decoded")
	}
	wrongBot := direct
	wrongBot.Login.ID = "other-bot"
	ordinaryDirect := uvim.Event{
		Type:      uvim.EventMessageCreate,
		Provider:  "wecom",
		Connector: "main",
		Channel:   uvim.Channel{ID: "user-1", Type: uvim.ChannelDirect},
		User:      uvim.User{ID: "user-1", Kind: uvim.UserKindUser},
		Referrer:  uvim.Referrer{Target: &uvim.OutboundTarget{Kind: uvim.TargetUser, ID: "user-1"}},
	}
	for _, test := range []struct {
		name   string
		event  uvim.Event
		want   uvim.OutboundTarget
		wantOK bool
	}{
		{"ordinary-direct-message", ordinaryDirect, uvim.OutboundTarget{Kind: uvim.TargetUser, ID: "user-1"}, true},
		{"current-direct-card-callback", direct, uvim.OutboundTarget{Kind: uvim.TargetUser, ID: "user-1"}, true},
		{"group-card-callback", group, uvim.OutboundTarget{}, false},
		{"card-callback-wrong-bot", wrongBot, uvim.OutboundTarget{}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			target, err := provider.PrivateTarget(context.Background(), test.event)
			if (err == nil) != test.wantOK || test.wantOK && target != test.want {
				t.Fatalf("target=%+v error=%v wantOK=%t want=%+v", target, err, test.wantOK, test.want)
			}
		})
	}
}

func mustWeComInteractionKey(t *testing.T, card uvim.InteractionCard, action uvim.InteractionAction) string {
	t.Helper()
	key, err := uvim.EncodeInteractionCallbackValue(card, action)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func withWeComFreeTextCard() uvim.InteractionCard {
	card := wecomInteractionTestCard()
	card.Question.AllowFreeText = true
	card.Question.FreeTextActionID = card.Actions[0].ID
	return card
}

func withWeComActions(count int) uvim.InteractionCard {
	card := wecomInteractionTestCard()
	card.Actions = make([]uvim.InteractionAction, count)
	for i := range card.Actions {
		card.Actions[i] = uvim.InteractionAction{ID: "choice-" + string(rune('a'+i)), Label: "Choice", Kind: uvim.InteractionAnswer, Token: strings.Repeat(string(rune('a'+i)), 64)}
	}
	return card
}

func withWeComCardID(id string) uvim.InteractionCard {
	card := wecomInteractionTestCard()
	card.CardID = id
	return card
}

func withWeComDisabledAction() uvim.InteractionCard {
	card := wecomInteractionTestCard()
	card.Actions[0].Disabled = true
	return card
}

func withWeComOverBudgetKey() uvim.InteractionCard {
	card := wecomInteractionTestCard()
	card.CardID = strings.Repeat("a", wecomTemplateCardTaskMax)
	card.Actions = []uvim.InteractionAction{{
		ID:    strings.Repeat("b", 256),
		Label: "Choice",
		Kind:  uvim.InteractionAnswer,
		Token: strings.Repeat("c", 512),
	}}
	return card
}

func cloneWeComMap(value map[string]any) map[string]any {
	raw, _ := json.Marshal(value)
	var cloned map[string]any
	_ = json.Unmarshal(raw, &cloned)
	return cloned
}
