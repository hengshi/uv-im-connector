package dingtalk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/client"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
	"github.com/hengshi/uv-im-connector/server"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/handler"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/payload"
)

func TestDecodeUsesSessionWebhookExpiry(t *testing.T) {
	expiresAt := time.Date(2026, 7, 16, 11, 30, 0, 0, time.UTC)
	event, ok, err := Decode([]byte(`{
  "msgId": "m1",
  "msgtype": "text",
  "senderStaffId": "u1",
  "conversationId": "g1",
  "conversationType": "2",
  "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?session=secret",
  "sessionWebhookExpiredTime": `+formatUnixMilli(expiresAt)+`,
  "text": {"content": "hello"}
}`), httpchannel.Config{BaseURL: "https://oapi.dingtalk.com", ConnectorID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("decode ok = false")
	}
	if event.Referrer.ExpiresAt == nil || !event.Referrer.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("reply expiry = %v", event.Referrer.ExpiresAt)
	}
}

func formatUnixMilli(value time.Time) string {
	return fmt.Sprintf("%d", value.UnixMilli())
}

func TestNewRejectsPartialStreamCredentials(t *testing.T) {
	for _, config := range []Config{
		{ClientID: "client-id"},
		{ClientSecret: "client-secret"},
	} {
		if _, err := New(config); err == nil {
			t.Fatal("New() accepted a partial DingTalk Stream credential pair")
		}
	}
}

func TestNewRejectsCardTemplateWithoutStreamCredentials(t *testing.T) {
	if _, err := New(Config{CardTemplateID: "published-template"}); err == nil {
		t.Fatal("New() accepted a card template without Stream credentials")
	}
	withoutTemplate, err := New(Config{ClientID: "app", ClientSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	withoutTemplateCaps := withoutTemplate.Capabilities()
	if withoutTemplateCaps.InteractiveMessage || withoutTemplateCaps.InteractiveActionCallback || len(withoutTemplateCaps.InteractionSchemaVersions) != 0 || withoutTemplateCaps.InteractionCardInstanceVersion != "" || withoutTemplateCaps.CardMaxActions != 0 || len(withoutTemplateCaps.InteractionActorIdentityTypes) != 0 {
		t.Fatalf("card-disabled capabilities = %+v", withoutTemplateCaps)
	}
	provider, err := New(Config{ClientID: "app", ClientSecret: "secret", CardTemplateID: "published-template"})
	if err != nil {
		t.Fatal(err)
	}
	caps := provider.Capabilities()
	if !caps.InteractiveMessage || !caps.InteractiveActionCallback || len(caps.InteractionSchemaVersions) != 1 || caps.InteractionSchemaVersions[0] != uvim.InteractionSchemaV1 || caps.InteractionCardInstanceVersion != uvim.InteractionCardInstanceVersionV1 || caps.CardMaxActions != uvim.InteractionMaxActions || caps.InteractionFreeText || len(caps.InteractionActorIdentityTypes) != 1 || caps.InteractionActorIdentityTypes[0] != "dingtalk.userid" {
		t.Fatalf("card capabilities = %+v", caps)
	}
}

func TestDecodeSenderIDFallbackDoesNotInventCardActorIdentity(t *testing.T) {
	provider, err := New(Config{ClientID: "app", ClientSecret: "secret", CardTemplateID: "published-template"})
	if err != nil {
		t.Fatal(err)
	}
	event, ok, err := Decode([]byte(`{
  "msgId": "m-fallback",
  "msgtype": "text",
  "senderId": "sender-id-only",
  "conversationId": "group-1",
  "conversationType": "2",
  "text": {"content": "continue"}
}`), httpchannel.Config{ConnectorID: "main"})
	if err != nil || !ok || event.User.ID != "sender-id-only" || event.Message.Text != "continue" {
		t.Fatalf("fallback event=%+v ok=%t err=%v", event, ok, err)
	}
	for _, identity := range event.User.Identities {
		if identity.Type == "dingtalk.userid" && identity.ID == event.User.ID && identity.Source == uvim.IdentitySourceCallback {
			t.Fatalf("senderId fallback invented a card-compatible staff identity: %+v", event.User.Identities)
		}
	}
	if len(provider.Capabilities().InteractionActorIdentityTypes) != 1 || provider.Capabilities().InteractionActorIdentityTypes[0] != "dingtalk.userid" {
		t.Fatalf("card actor capabilities = %+v", provider.Capabilities())
	}
}

func TestDecodeQuotedReplyPreservesOnlyNativeParentMaterial(t *testing.T) {
	event, ok, err := Decode([]byte(`{
  "msgId": "current-message",
  "msgtype": "text",
  "senderStaffId": "u1",
  "conversationId": "group-1",
  "conversationType": "2",
  "text": {"content": "continue this task"},
  "isReplyMsg": true,
  "originalMsgId": "parent-message",
  "repliedMsg": {
    "msgId": "parent-message",
    "msgType": "text",
    "content": {"text": "opaque||3||1||132"}
  }
}`), httpchannel.Config{ConnectorID: "main"})
	if err != nil || !ok {
		t.Fatalf("Decode() ok=%t err=%v", ok, err)
	}
	if event.Message.Text != "continue this task" || event.Referrer.ParentMessageID != "parent-message" {
		t.Fatalf("quoted event = %+v", event)
	}
	if len(event.Message.Resources) != 1 || event.Message.Resources[0].SourceMessageID != "parent-message" || event.Message.Resources[0].Error != uvim.QuotedMessageReferenceUnavailable || event.Message.Resources[0].InternalURL != "" {
		t.Fatalf("quoted resource = %+v", event.Message.Resources)
	}
	if len(event.Message.Elements) < 2 || event.Message.Elements[len(event.Message.Elements)-1].Type != uvim.ElementQuote || event.Message.Elements[len(event.Message.Elements)-1].ID != "parent-message" {
		t.Fatalf("quoted elements = %+v", event.Message.Elements)
	}
}

func TestDecodeNestedQuoteMessagePreservesCurrentInstruction(t *testing.T) {
	event, ok, err := Decode([]byte(`{
  "msgId": "current-message",
  "msgtype": "text",
  "senderStaffId": "u1",
  "conversationId": "group-1",
  "conversationType": "2",
  "text": {"content": "continue this task"},
  "quoteMessage": {
    "isReplyMsg": true,
    "repliedMsg": {"msgId": "parent-message", "msgType": "text"}
  }
}`), httpchannel.Config{ConnectorID: "main"})
	if err != nil || !ok || event.Message.Text != "continue this task" || event.Referrer.ParentMessageID != "parent-message" {
		t.Fatalf("nested quote event=%+v ok=%t err=%v", event, ok, err)
	}
	if len(event.Message.Resources) != 1 || event.Message.Resources[0].SourceMessageID != "parent-message" || event.Message.Resources[0].Error != uvim.QuotedMessageReferenceUnavailable || event.Message.Resources[0].InternalURL != "" {
		t.Fatalf("nested quote resource=%+v", event.Message.Resources)
	}
}

func TestDecodeMarksIncompleteQuotedReplyUnavailableInsteadOfDefaultRouting(t *testing.T) {
	for _, raw := range []string{
		`{"msgId":"current","msgtype":"text","senderStaffId":"u1","conversationId":"group-1","conversationType":"2","text":{"content":"continue"},"isReplyMsg":true}`,
		`{"msgId":"current","msgtype":"text","senderStaffId":"u1","conversationId":"group-1","conversationType":"2","text":{"content":"continue"},"isReplyMsg":true,"repliedMsg":{"msgId":"current","msgType":"text"}}`,
		// An incidental originalMsgId never supplies quote semantics or a parent.
		`{"msgId":"current","msgtype":"text","senderStaffId":"u1","conversationId":"group-1","conversationType":"2","text":{"content":"continue"},"isReplyMsg":true,"originalMsgId":"parent"}`,
		// A typed unavailable quote is still an event when the current message
		// has no text. The shared router owns the user-facing refusal.
		`{"msgId":"current","msgtype":"text","senderStaffId":"u1","conversationId":"group-1","conversationType":"2","isReplyMsg":true}`,
	} {
		event, ok, err := Decode([]byte(raw), httpchannel.Config{ConnectorID: "main"})
		if err != nil || !ok || event.ID != "current" || event.Referrer.ParentMessageID != "" {
			t.Fatalf("incomplete quote event=%+v ok=%t err=%v", event, ok, err)
		}
		if len(event.Message.Resources) != 1 || event.Message.Resources[0].Error != uvim.QuotedMessageReferenceUnavailable || event.Message.Resources[0].SourceMessageID != "" || event.Message.Resources[0].Kind != uvim.ElementFile || event.Message.Resources[0].Name != "quoted-message.txt" {
			t.Fatalf("incomplete quote resource=%+v", event.Message.Resources)
		}
		wantElements := 1
		if event.Message.Text != "" {
			wantElements++
		}
		if len(event.Message.Elements) != wantElements || event.Message.Elements[len(event.Message.Elements)-1].Type != uvim.ElementQuote || event.Message.Elements[len(event.Message.Elements)-1].ID != "" {
			t.Fatalf("incomplete quote elements=%+v", event.Message.Elements)
		}
	}
}

func TestDecodeDoesNotInferQuoteFromOriginalMessageID(t *testing.T) {
	event, ok, err := Decode([]byte(`{"msgId":"current","msgtype":"text","senderStaffId":"u1","conversationId":"group-1","conversationType":"2","text":{"content":"continue"},"originalMsgId":"not-quote-proof"}`), httpchannel.Config{ConnectorID: "main"})
	if err != nil || !ok || event.Message.Text != "continue" || event.Referrer.ParentMessageID != "" || len(event.Message.Resources) != 0 || len(event.Message.Elements) != 0 {
		t.Fatalf("ordinary original-message event=%+v ok=%t err=%v", event, ok, err)
	}
}

func TestSendWithoutRawTextBudgetReachesEitherTransport(t *testing.T) {
	for _, app := range []bool{false, true} {
		t.Run(fmt.Sprintf("app=%t", app), func(t *testing.T) {
			prefix := "Budget regression\n"
			text := prefix + strings.Repeat("x", 20*1024-len(prefix))
			sends, tokens := 0, 0
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodPost {
					t.Errorf("method = %s", req.Method)
				}
				if req.URL.Path == "/v1.0/oauth2/accessToken" && app {
					tokens++
					_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": "access-token"})
					return
				}
				sends++
				var markdown map[string]string
				if app {
					var body struct {
						RobotCode string   `json:"robotCode"`
						MsgKey    string   `json:"msgKey"`
						MsgParam  string   `json:"msgParam"`
						UserIDs   []string `json:"userIds"`
					}
					if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if req.URL.Path != "/v1.0/robot/oToMessages/batchSend" || req.Header.Get("x-acs-dingtalk-access-token") != "access-token" || body.RobotCode != "app" || body.MsgKey != "sampleMarkdown" || len(body.UserIDs) != 1 || body.UserIDs[0] != "recipient" {
						t.Errorf("native app request path=%s body=%+v", req.URL.Path, body)
					}
					if err := json.Unmarshal([]byte(body.MsgParam), &markdown); err != nil {
						t.Error(err)
					}
					_ = json.NewEncoder(w).Encode(map[string]string{"processQueryKey": "native-direct"})
				} else {
					var body struct {
						MsgType  string            `json:"msgtype"`
						Markdown map[string]string `json:"markdown"`
					}
					if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if req.URL.Path != "/robot/send" || req.URL.Query().Get("access_token") != "group-token" || body.MsgType != "markdown" {
						t.Errorf("native webhook request path=%s type=%s", req.URL.Path, body.MsgType)
					}
					markdown = body.Markdown
					_ = json.NewEncoder(w).Encode(map[string]int{"errcode": 0})
				}
				if markdown["title"] != "Budget regression" || markdown["text"] != text {
					t.Errorf("native markdown title=%q text bytes=%d, want complete %d bytes", markdown["title"], len(markdown["text"]), len(text))
				}
			}))
			defer api.Close()
			config := Config{Token: "group-token", BaseURL: api.URL}
			kind, wantTokens, wantID := uvim.TargetGroup, 0, ""
			if app {
				config.ClientID, config.ClientSecret = "app", "secret"
				kind, wantTokens, wantID = uvim.TargetUser, 1, "native-direct"
			}
			provider, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			result, err := provider.Send(t.Context(), uvim.OutboundMessage{Text: text, Target: &uvim.OutboundTarget{ID: "recipient", Kind: kind}})
			if err != nil || sends != 1 || tokens != wantTokens || result.Provider != "dingtalk" || result.MessageID != wantID {
				t.Fatalf("sends=%d tokens=%d result=%+v err=%v", sends, tokens, result, err)
			}
		})
	}
}

func dingtalkInteractionCard() uvim.InteractionCard {
	return uvim.InteractionCard{
		SchemaVersion: uvim.InteractionSchemaV1,
		CardID:        "card-instance-1",
		Revision:      1,
		Title:         "Choose a task",
		Summary:       "The task needs your choice.",
		Facts:         []uvim.CardFact{{Label: "Task", Value: "T-1"}},
		Question:      &uvim.CardQuestion{ID: "question-1", Prompt: "Which option?", Help: "Choose one action."},
		Actions: []uvim.InteractionAction{{
			ID: "answer", Label: "Confirm", Kind: uvim.InteractionAnswer, Token: "opaque-handle",
		}},
	}
}

func TestInteractionCardUsesPublishedTemplateOnlyForNativeGroup(t *testing.T) {
	var requests int
	cardResponse := map[string]any{
		"success": true,
		"result": map[string]any{
			"outTrackId": "card-instance-1",
			"deliverResults": []map[string]any{{
				"carrierId": "opaque-carrier-id",
				"spaceId":   "group-1",
				"spaceType": "IM_GROUP",
				"success":   true,
			}},
		},
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests++
		switch req.URL.Path {
		case "/v1.0/oauth2/accessToken":
			if req.Header.Get("x-acs-dingtalk-access-token") != "" {
				t.Error("token request carried an access token")
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": "access-token"})
		case "/v1.0/card/instances/createAndDeliver":
			if req.Header.Get("x-acs-dingtalk-access-token") != "access-token" {
				t.Errorf("card access token = %q", req.Header.Get("x-acs-dingtalk-access-token"))
			}
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Error(err)
				return
			}
			if body["cardTemplateId"] != "published-template" || body["outTrackId"] != "card-instance-1" || body["callbackType"] != "STREAM" || body["openSpaceId"] != "dtv1.card//IM_GROUP.group-1" || body["userIdType"] != float64(1) {
				t.Errorf("card request = %+v", body)
			}
			data := uvim.MapStringAny(body["cardData"])
			params := uvim.MapStringAny(data["cardParamMap"])
			if params["title"] != "Choose a task" || params["action_1_id"] != "answer" || uvim.StringValue(params["action_1_callback"]) == "" {
				t.Errorf("card params = %+v", params)
			}
			if delivery := uvim.MapStringAny(body["imGroupOpenDeliverModel"]); delivery["robotCode"] != "app" {
				t.Errorf("card delivery = %+v", delivery)
			}
			_ = json.NewEncoder(w).Encode(cardResponse)
		default:
			t.Errorf("unexpected request %s", req.URL.Path)
		}
	}))
	defer api.Close()

	provider, err := New(Config{BaseURL: api.URL, ConnectorID: "main", ClientID: "app", ClientSecret: "secret", CardTemplateID: "published-template"})
	if err != nil {
		t.Fatal(err)
	}
	card := dingtalkInteractionCard()
	result, err := provider.Send(t.Context(), uvim.OutboundMessage{
		Provider: "dingtalk", Connector: "main", ChannelID: "group-1", ChannelType: uvim.ChannelGroup,
		Target: &uvim.OutboundTarget{ID: "group-1", Kind: uvim.TargetGroup}, Interaction: &card,
	})
	if err != nil || result.MessageID != "" || result.Interaction == nil || result.Interaction.NativeCardID != card.CardID || result.Interaction.BotAppID != "app" {
		t.Fatalf("card result=%+v err=%v", result, err)
	}
	if requests != 2 {
		t.Fatalf("native card requests = %d", requests)
	}

	for _, testCase := range []struct {
		name     string
		response map[string]any
	}{
		{name: "missing top-level success", response: map[string]any{"result": map[string]any{"outTrackId": card.CardID, "deliverResults": []map[string]any{{"spaceId": "group-1", "spaceType": "IM_GROUP", "success": true}}}}},
		{name: "false top-level success", response: map[string]any{"success": false, "result": map[string]any{"outTrackId": card.CardID, "deliverResults": []map[string]any{{"spaceId": "group-1", "spaceType": "IM_GROUP", "success": true}}}}},
		{name: "wrong out track ID", response: map[string]any{"success": true, "result": map[string]any{"outTrackId": "other-card", "deliverResults": []map[string]any{{"spaceId": "group-1", "spaceType": "IM_GROUP", "success": true}}}}},
		{name: "wrong group scope", response: map[string]any{"success": true, "result": map[string]any{"outTrackId": card.CardID, "deliverResults": []map[string]any{{"spaceId": "other-group", "spaceType": "IM_GROUP", "success": true}}}}},
		{name: "missing delivery results", response: map[string]any{"success": true, "result": map[string]any{"outTrackId": card.CardID}}},
		{name: "multiple delivery results", response: map[string]any{"success": true, "result": map[string]any{"outTrackId": card.CardID, "deliverResults": []map[string]any{{"spaceId": "group-1", "spaceType": "IM_GROUP", "success": true}, {"spaceId": "other-group", "spaceType": "IM_GROUP", "success": true}}}}},
		{name: "wrong space type", response: map[string]any{"success": true, "result": map[string]any{"outTrackId": card.CardID, "deliverResults": []map[string]any{{"spaceId": "group-1", "spaceType": "IM_ROBOT", "success": true}}}}},
		{name: "failed group delivery", response: map[string]any{"success": true, "result": map[string]any{"outTrackId": card.CardID, "deliverResults": []map[string]any{{"spaceId": "group-1", "spaceType": "IM_GROUP", "success": false, "errorMsg": "delivery failed"}}}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cardResponse = testCase.response
			provider, err := New(Config{BaseURL: api.URL, ConnectorID: "main", ClientID: "app", ClientSecret: "secret", CardTemplateID: "published-template"})
			if err != nil {
				t.Fatal(err)
			}
			result, err := provider.Send(t.Context(), uvim.OutboundMessage{
				Provider: "dingtalk", Connector: "main", ChannelID: "group-1", ChannelType: uvim.ChannelGroup,
				Target: &uvim.OutboundTarget{ID: "group-1", Kind: uvim.TargetGroup}, Interaction: &card,
			})
			failure, ok := uvim.ProviderSendFailure(err)
			if !ok || failure.DeliveryState != uvim.DeliveryUnknown || failure.Retryable || result.Interaction != nil || result.MessageID != "" || len(result.Publications) != 0 {
				t.Fatalf("unconfirmed card result=%+v failure=%+v err=%v", result, failure, err)
			}
		})
	}

	beforeUnsupported := requests
	for _, unsupported := range []uvim.OutboundMessage{
		{Provider: "dingtalk", Connector: "main", ChannelID: "direct-conversation", ChannelType: uvim.ChannelDirect, Target: &uvim.OutboundTarget{ID: "u1", Kind: uvim.TargetUser}, Interaction: &card},
		{Provider: "dingtalk", Connector: "main", ChannelID: "group-1", ChannelType: uvim.ChannelGroup, Target: &uvim.OutboundTarget{ID: "group-1", Kind: uvim.TargetGroup}, Interaction: &uvim.InteractionCard{SchemaVersion: uvim.InteractionSchemaV1, CardID: "card-free", Revision: 1, Title: "Free", Question: &uvim.CardQuestion{ID: "q", Prompt: "Explain", AllowFreeText: true, FreeTextActionID: "answer"}, Actions: []uvim.InteractionAction{{ID: "answer", Label: "Answer", Kind: uvim.InteractionAnswer, Token: "opaque"}}}},
	} {
		_, err := provider.Send(t.Context(), unsupported)
		failure, ok := uvim.ProviderSendFailure(err)
		if !ok || failure.DeliveryState != uvim.DeliveryNotAttempted {
			t.Fatalf("unsupported card failure=%+v err=%v", failure, err)
		}
	}
	if requests != beforeUnsupported {
		t.Fatalf("unsupported card made native requests: %d", requests)
	}
}

func TestStreamCardCallbackUsesNativeCardAndGroupScope(t *testing.T) {
	provider, err := New(Config{ConnectorID: "main", ClientID: "app", ClientSecret: "secret", CardTemplateID: "published-template"})
	if err != nil {
		t.Fatal(err)
	}
	card := dingtalkInteractionCard()
	encoded, err := uvim.EncodeInteractionCallbackValue(card, card.Actions[0])
	if err != nil {
		t.Fatal(err)
	}
	content, err := json.Marshal(map[string]any{"cardPrivateData": map[string]any{
		"actionIds": []string{"answer"},
		"params":    map[string]string{dingtalkCardCallbackParam: encoded},
	}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{
		"content":    string(content),
		"corpId":     "corp-1",
		"outTrackId": card.CardID,
		"spaceId":    "group-1",
		"spaceType":  "IM_GROUP",
		"type":       "actionCallback",
		"userId":     "u1",
		"userIdType": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	fake := &fakeStreamClient{
		topic: payload.CardInstanceCallbackTopic,
		headers: payload.DataFrameHeader{
			payload.DataFrameHeaderKMessageId: "card-click-1",
			payload.DataFrameHeaderKTime:      "1777777777000",
			"appId":                           "native-app-header",
		},
		data:         string(data),
		afterMessage: cancel,
	}
	provider.newStreamClient = func(string, string) streamClient { return fake }
	var events []uvim.Event
	if err := provider.Run(ctx, uvim.EventSinkFunc(func(_ context.Context, event uvim.Event) error {
		events = append(events, event)
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %+v", events)
	}
	event := events[0]
	if event.ID != "card-click-1" || event.Message.ID != "" || event.Referrer.MessageID != "" || event.Channel.ID != "group-1" || event.Channel.Type != uvim.ChannelGroup || event.Referrer.Target == nil || event.Referrer.Target.Kind != uvim.TargetGroup || event.Referrer.Target.ID != "group-1" {
		t.Fatalf("card callback event scope = %+v", event)
	}
	if event.Login.ID != "app" || event.Interaction == nil || event.Interaction.SchemaVersion != uvim.InteractionCardInstanceVersionV1 || event.Interaction.NativeCardID != card.CardID || event.Interaction.CardID != card.CardID || event.Interaction.BotAppID != "app" || event.Interaction.Actor.ID != "u1" {
		t.Fatalf("card callback interaction = %+v", event)
	}
	if len(event.Interaction.Actor.Identities) != 1 || event.Interaction.Actor.Identities[0] != (uvim.UserIdentity{Type: "dingtalk.userid", ID: "u1", Scope: "corp-1", Source: uvim.IdentitySourceCallback}) {
		t.Fatalf("card callback actor identity = %+v", event.Interaction.Actor.Identities)
	}

	bad := &payload.DataFrame{Headers: payload.DataFrameHeader{
		payload.DataFrameHeaderKTopic:     payload.CardInstanceCallbackTopic,
		payload.DataFrameHeaderKMessageId: "card-click-2",
	}, Data: strings.Replace(string(data), "IM_GROUP", "IM_ROBOT", 1)}
	if _, ok, err := provider.decodeCardCallback(bad); err == nil || ok {
		t.Fatalf("direct scope callback accepted ok=%t err=%v", ok, err)
	}
}

func TestDecodeContentRichTextAndDownloadCodes(t *testing.T) {
	event, ok, err := Decode([]byte(`{
  "msgId": "m-rich",
  "msgtype": "richText",
  "createAt": 1794484800000,
  "conversationTitle": "Release review",
  "robotCode": "robot-1",
  "senderStaffId": "u1",
  "senderNick": "Ada",
  "conversationId": "g1",
  "conversationType": "2",
  "content": {
    "richText": [
      {"text": "hello"},
      {"type": "picture", "downloadCode": "pic-code", "fileName": "pic.png"},
      {"text": "world"},
      {"type": "file", "downloadCode": "file-code", "fileName": "report.pdf", "fileSize": 12}
    ]
  }
}`), httpchannel.Config{ConnectorID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("decode ok = false")
	}
	if event.Message.Text != "hello\nworld" {
		t.Fatalf("text = %q", event.Message.Text)
	}
	if event.Channel.Name != "Release review" || !event.Message.CreatedAt.Equal(time.UnixMilli(1794484800000)) {
		t.Fatalf("native conversation/time lost: channel=%+v created_at=%v", event.Channel, event.Message.CreatedAt)
	}
	if len(event.Message.Resources) != 2 {
		t.Fatalf("resources = %+v", event.Message.Resources)
	}
	if event.Message.Resources[0].Kind != uvim.ElementImage || event.Message.Resources[0].Key != "pic-code" {
		t.Fatalf("image resource = %+v", event.Message.Resources[0])
	}
	if event.Message.Resources[1].Kind != uvim.ElementFile || event.Message.Resources[1].Key != "file-code" || event.Message.Resources[1].SizeBytes != 12 {
		t.Fatalf("file resource = %+v", event.Message.Resources[1])
	}
	if event.Message.Resources[0].Private["robot_code"] != "robot-1" {
		t.Fatalf("private metadata = %+v", event.Message.Resources[0].Private)
	}
}

func TestDownloadExchangesDownloadCodeForTemporaryURL(t *testing.T) {
	var sawTokenRequest, sawDownloadRequest bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			sawTokenRequest = true
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["appKey"] != "client-id" || body["appSecret"] != "client-secret" {
				t.Fatalf("token body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": "access-token"})
		case "/v1.0/robot/messageFiles/download":
			sawDownloadRequest = true
			if got := r.Header.Get("x-acs-dingtalk-access-token"); got != "access-token" {
				t.Fatalf("download token = %q", got)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["robotCode"] != "robot-1" || body["downloadCode"] != "download-code" {
				t.Fatalf("download body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"downloadUrl": apiURL(r) + "/file"})
		case "/file":
			_, _ = io.WriteString(w, "file bytes")
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer api.Close()

	provider, err := New(Config{BaseURL: api.URL, ClientID: "client-id", ClientSecret: "client-secret"})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := provider.Download(context.Background(), uvim.ResourceDownloadRequest{
		Dir: t.TempDir(),
		Resource: uvim.ResourceRef{
			Provider: "dingtalk",
			Kind:     uvim.ElementImage,
			Name:     "pic.png",
			Key:      "download-code",
			URL:      api.URL + "/legacy-file",
			Private:  map[string]string{"robot_code": "robot-1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawTokenRequest || !sawDownloadRequest {
		t.Fatalf("token=%t download=%t", sawTokenRequest, sawDownloadRequest)
	}
	if ref.InternalURL == "" || ref.Key != "" {
		t.Fatalf("downloaded ref = %+v", ref)
	}
}

func apiURL(r *http.Request) string {
	return "http://" + r.Host
}

// The SDK is stubbed; the real DingTalk adapter, Hub and event log execute.
func TestStreamRunEmitsNormalizedEventWithSDKStub(t *testing.T) {
	for _, failStartup := range []bool{false, true} {
		t.Run(fmt.Sprintf("startup-failure=%t", failStartup), func(t *testing.T) {
			provider, err := New(Config{
				ConnectorID:  "main",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
			})
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			fake := &fakeStreamClient{
				data: `{
  "msgId": "m-stream",
  "msgtype": "text",
  "senderStaffId": "u1",
  "senderNick": "Actor",
  "conversationId": "c1",
  "conversationType": "1",
  "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?session=secret",
  "text": {"content": "/start JARVIS-IM-REAL-E2E-1"}
}`,
				afterMessage: cancel,
			}
			provider.newStreamClient = func(string, string) streamClient {
				if failStartup {
					failStartup = false
					return &fakeStreamClient{startErr: errors.New("temporary connection failure")}
				}
				return fake
			}
			log, err := uvim.NewEventLog(t.TempDir() + "/events.jsonl")
			if err != nil {
				t.Fatal(err)
			}
			hub := server.NewHub(uvim.NewProviderRegistry(provider), log, nil)
			hub.SetAuthToken("caller-token")
			if err := hub.RunProviders(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("runner did not recover and emit before cancellation: %v", err)
			}
			events, err := log.ReadAfter(context.Background(), 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != 1 {
				t.Fatalf("events = %d, want 1", len(events))
			}
			event := events[0]
			if event.Provider != "dingtalk" || event.Connector != "main" || event.ID != "m-stream" {
				t.Fatalf("unexpected event identity: %#v", event)
			}
			if event.User.ID != "u1" || event.Channel.ID != "c1" || event.Message.Text != "/start JARVIS-IM-REAL-E2E-1" {
				t.Fatalf("unexpected normalized event: %#v", event)
			}
			if event.Referrer.ReplyToken == "" {
				t.Fatal("stream event lost session webhook reply token")
			}
			if err := hub.EnableIdentityLinks(t.TempDir() + "/links.json"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = hub.CloseIdentityLinks() })
			api := httptest.NewServer(hub.Handler())
			defer api.Close()
			c := client.New(api.URL)
			c.Token = "caller-token"
			eventRef := uvim.IdentityEventRef{Provider: event.Provider, Connector: event.Connector, EventID: event.ID}
			linked, err := c.LinkIdentity(t.Context(), uvim.IdentityLinkRequest{Identity: uvim.UserIdentity{Type: "github.userid", Scope: "github.com", ID: "42"}, Event: eventRef, Proof: "github:comment-42"})
			if err != nil || linked.Link == nil {
				t.Fatalf("link native private actor: %+v %v", linked, err)
			}
			resolved, err := c.ResolveInboundIdentity(t.Context(), uvim.IdentityInboundResolveRequest{Provider: event.Provider, Connector: event.Connector, EventID: event.ID})
			if err != nil || resolved.DirectTarget == nil || resolved.DirectTarget.Kind != uvim.TargetUser || resolved.DirectTarget.ID != "u1" || resolved.NativeActor.UserID != "u1" || len(resolved.Links) != 1 {
				t.Fatalf("private actor/target proof: %+v %v", resolved, err)
			}
		})
	}
}

func TestStreamRunPreservesDownloadCodeRobotCode(t *testing.T) {
	provider, err := New(Config{
		ConnectorID:  "main",
		ClientID:     "client-id",
		ClientSecret: "client-secret",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeStreamClient{
		data: `{
  "msgId": "m-stream-picture",
  "msgtype": "picture",
  "robotCode": "robot-1",
  "senderStaffId": "u1",
  "conversationId": "c1",
  "conversationType": "2",
  "content": {"downloadCode": "download-code", "fileName": "image.png"}
}`,
		afterMessage: cancel,
	}
	provider.newStreamClient = func(string, string) streamClient { return fake }

	var events []uvim.Event
	err = provider.Run(ctx, uvim.EventSinkFunc(func(_ context.Context, event uvim.Event) error {
		events = append(events, event)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	resources := events[0].Message.Resources
	if len(resources) != 1 {
		t.Fatalf("resources = %+v", resources)
	}
	if resources[0].Kind != uvim.ElementImage || resources[0].Key != "download-code" || resources[0].Name != "image.png" {
		t.Fatalf("resource = %+v", resources[0])
	}
	if resources[0].Private["robot_code"] != "robot-1" {
		t.Fatalf("private metadata = %+v", resources[0].Private)
	}
}

func TestStreamRunDownloadsDownloadCodeThroughHub(t *testing.T) {
	var sawDownloadRequest bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["appKey"] != "client-id" || body["appSecret"] != "client-secret" {
				t.Fatalf("token body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": "access-token"})
		case "/v1.0/robot/messageFiles/download":
			sawDownloadRequest = true
			if got := r.Header.Get("x-acs-dingtalk-access-token"); got != "access-token" {
				t.Fatalf("download token = %q", got)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["robotCode"] != "robot-1" || body["downloadCode"] != "download-code" {
				t.Fatalf("download body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"downloadUrl": apiURL(r) + "/file"})
		case "/file":
			_, _ = io.WriteString(w, "hub file bytes")
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer api.Close()

	provider, err := New(Config{
		ConnectorID:  "main",
		BaseURL:      api.URL,
		ClientID:     "client-id",
		ClientSecret: "client-secret",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &fakeStreamClient{
		data: `{
  "msgId": "m-stream-picture",
  "msgtype": "picture",
  "robotCode": "robot-1",
  "senderStaffId": "u1",
  "conversationId": "c1",
  "conversationType": "2",
  "content": {"downloadCode": "download-code", "fileName": "image.png"}
}`,
		afterMessage: cancel,
	}
	provider.newStreamClient = func(string, string) streamClient { return fake }

	store := &uvim.ResourceStore{Dir: t.TempDir()}
	log, err := uvim.NewEventLog(t.TempDir() + "/events.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	hub := server.NewHub(uvim.NewProviderRegistry(provider), log, store)
	if err := provider.Run(ctx, hub); err != nil {
		t.Fatal(err)
	}
	events, err := log.ReadAfter(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if !sawDownloadRequest {
		t.Fatal("Hub did not trigger the DingTalk downloadCode exchange")
	}
	resources := events[0].Message.Resources
	if len(resources) != 1 {
		t.Fatalf("resources = %+v", resources)
	}
	ref := resources[0]
	if ref.Kind != uvim.ElementImage || ref.Name != "image.png" || ref.InternalURL == "" || ref.Error != "" {
		t.Fatalf("persisted resource = %+v", ref)
	}
	if ref.Key != "" || ref.URL != "" || ref.Private != nil {
		t.Fatalf("persisted resource was not sanitized: %+v", ref)
	}
	file, _, err := store.Open(ref.InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hub file bytes" {
		t.Fatalf("downloaded body = %q", data)
	}
}

type fakeStreamClient struct {
	startErr     error
	handlers     map[string]handler.IFrameHandler
	topic        string
	headers      payload.DataFrameHeader
	data         string
	afterMessage func()
}

func (f *fakeStreamClient) RegisterCallbackRouter(topic string, frameHandler handler.IFrameHandler) {
	if f.handlers == nil {
		f.handlers = map[string]handler.IFrameHandler{}
	}
	f.handlers[topic] = frameHandler
}

func (f *fakeStreamClient) Start(ctx context.Context) error {
	if f.startErr != nil {
		return f.startErr
	}
	topic := f.topic
	if topic == "" {
		topic = payload.BotMessageCallbackTopic
	}
	callback := f.handlers[topic]
	if callback == nil {
		return fmt.Errorf("callback handler was not registered for %s", topic)
	}
	headers := payload.DataFrameHeader{payload.DataFrameHeaderKTopic: topic}
	for key, value := range f.headers {
		headers[key] = value
	}
	_, err := callback(ctx, &payload.DataFrame{
		Headers: headers,
		Data:    f.data,
	})
	if err != nil {
		return err
	}
	if f.afterMessage != nil {
		f.afterMessage()
	}
	return nil
}

func (f *fakeStreamClient) Close() {}
