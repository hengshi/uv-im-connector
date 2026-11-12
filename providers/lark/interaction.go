package lark

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"html"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

// UpdateInteraction uses the card PATCH API, which is distinct from text PUT
// and returns only a status code (no message-id echo):
// https://open.feishu.cn/document/server-docs/im-v1/message-card/patch
func (p *Provider) UpdateInteraction(ctx context.Context, update uvim.InteractionMessageUpdate) (uvim.InteractionUpdateResult, error) {
	result := uvim.InteractionUpdateResult{Version: uvim.InteractionUpdateVersionV1, OperationID: update.OperationID, NativeMessageID: update.NativeMessageID, CardID: update.CardID, ReplacementRevision: update.ReplacementRevision, PayloadSHA256: update.PayloadSHA256, DeliveryState: uvim.InteractionNotAttempted}
	if err := update.Validate(); err != nil {
		return result, err
	}
	if update.Provider != p.ID() || update.Connector != p.ConnectorID() {
		return result, errors.New("interaction provider scope mismatch")
	}
	message, err := p.messageIdentity(ctx, update.NativeMessageID)
	if err != nil {
		return result, err
	}
	conversation := update.Referrer.ChannelID
	if conversation == "" && (update.Target.Kind == uvim.TargetConversation || update.Target.Kind == uvim.TargetGroup) {
		conversation = update.Target.ID
	}
	if message.ID != update.NativeMessageID || message.Type != "interactive" || message.Deleted || message.Sender.Type != "app" || message.Sender.Kind != "app_id" || message.Sender.ID != p.config.AppID || conversation == "" || message.ChatID != conversation || update.Referrer.ThreadID != "" && message.ThreadID != update.Referrer.ThreadID {
		return result, errors.New("interaction native message ownership unavailable")
	}
	card, err := renderInteractionCard(update.Card)
	if err != nil {
		return result, err
	}
	content, err := json.Marshal(card)
	if err != nil {
		return result, err
	}
	body, err := json.Marshal(map[string]string{"content": string(content)})
	if err != nil {
		return result, err
	}
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return result, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPatch, p.baseURL()+"/open-apis/im/v1/messages/"+url.PathEscape(update.NativeMessageID), bytes.NewReader(body))
	if err != nil {
		return result, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	result.DeliveryState = uvim.InteractionUnknown
	raw, err := p.doJSON(request, "lark interaction update")
	if err != nil {
		if failure, known := uvim.ProviderSendFailure(err); known {
			switch failure.DeliveryState {
			case uvim.DeliveryNotAttempted:
				result.DeliveryState = uvim.InteractionNotAttempted
			case uvim.DeliveryRejected:
				result.DeliveryState = uvim.InteractionFailedDefinite
			}
		}
		return result, err
	}
	var response struct {
		Code *int `json:"code"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Code == nil {
		return result, errors.New("interaction update receipt unavailable")
	}
	if *response.Code != 0 {
		result.DeliveryState = uvim.InteractionFailedDefinite
		return result, errors.New("interaction update rejected")
	}
	result.DeliveryState = uvim.InteractionDelivered
	return result, nil
}

type interactionCallbackValue = uvim.InteractionCallbackValue

func callbackValue(card uvim.InteractionCard, action uvim.InteractionAction) interactionCallbackValue {
	return interactionCallbackValue{SchemaVersion: uvim.InteractionSchemaV1, CardID: card.CardID, Revision: card.Revision, ActionID: action.ID, Kind: action.Kind, Token: action.Token}
}

// Render Card JSON 2.0 using neutral fields only. No vendor card JSON or
// callback URL can be supplied by the caller. See the Feishu Card JSON 2.0
// component and card.action.trigger contracts.
func renderInteractionCard(card uvim.InteractionCard) (map[string]any, error) {
	if err := card.Validate(); err != nil {
		return nil, err
	}
	plain := func(text string) map[string]any { return map[string]any{"tag": "plain_text", "content": text} }
	text := func(value string) map[string]any {
		escaped := strings.NewReplacer("\\", "\\\\", "*", "\\*", "_", "\\_", "`", "\\`", "[", "\\[", "]", "\\]", "!", "\\!", "#", "\\#", "|", "\\|").Replace(html.EscapeString(value))
		return map[string]any{"tag": "markdown", "content": escaped}
	}
	elements := []any{}
	if card.Summary != "" {
		elements = append(elements, text(card.Summary))
	}
	for _, fact := range card.Facts {
		elements = append(elements, text(fact.Label+"："+fact.Value))
	}
	if card.Question != nil {
		elements = append(elements, text(card.Question.Prompt))
		if card.Question.Help != "" {
			elements = append(elements, text(card.Question.Help))
		}
	}
	for _, action := range card.Actions {
		button := map[string]any{"tag": "button", "text": plain(action.Label), "type": "default", "disabled": action.Disabled,
			"behaviors": []any{map[string]any{"type": "callback", "value": callbackValue(card, action)}}}
		if card.Question != nil && card.Question.AllowFreeText && action.ID == card.Question.FreeTextActionID {
			button["name"], button["form_action_type"] = "answer_submit", "submit"
			elements = append(elements, map[string]any{"tag": "form", "name": "jarvis_answer", "elements": []any{
				map[string]any{"tag": "input", "name": "answer", "placeholder": plain("请输入补充信息")}, button}})
		} else {
			elements = append(elements, button)
		}
	}
	if card.DetailURL != "" {
		elements = append(elements, map[string]any{"tag": "button", "text": plain("查看详情"), "type": "default", "behaviors": []any{map[string]any{"type": "open_url", "default_url": card.DetailURL}}})
	}
	result := map[string]any{"schema": "2.0", "config": map[string]any{"update_multi": true}, "header": map[string]any{"title": plain(card.Title)}, "body": map[string]any{"elements": elements}}
	raw, err := json.Marshal(result)
	if err != nil || len(raw) > uvim.InteractionCardMaxBytes {
		return nil, errors.New("native interaction card budget exceeded")
	}
	return result, nil
}

type interactionCallbackContext struct {
	MessageID string `json:"open_message_id"`
	ChatID    string `json:"open_chat_id"`
}

// Decode only the structured callback emitted on the authenticated Lark
// WebSocket. The provider's configured AppID is required; payload identity is
// never its own credential. Vendor update tokens are deliberately discarded.
func decodeInteractionAction(env eventEnvelope, config DecoderConfig) (uvim.Event, bool, error) {
	if env.Schema != "2.0" || config.AppID == "" || env.Header.AppID != config.AppID || env.Header.EventID == "" {
		return uvim.Event{}, false, errors.New("invalid interaction callback application")
	}
	var body struct {
		Operator struct {
			OpenID    string `json:"open_id"`
			UserID    string `json:"user_id"`
			UnionID   string `json:"union_id"`
			TenantKey string `json:"tenant_key"`
		} `json:"operator"`
		Action struct {
			Value     json.RawMessage            `json:"value"`
			FormValue map[string]json.RawMessage `json:"form_value"`
		} `json:"action"`
		Context interactionCallbackContext `json:"context"`
	}
	if err := json.Unmarshal(env.Event, &body); err != nil {
		return uvim.Event{}, false, errors.New("invalid interaction callback body")
	}
	context := body.Context
	if env.Context.MessageID != "" || env.Context.ChatID != "" {
		if context != (interactionCallbackContext{}) && context != env.Context {
			return uvim.Event{}, false, errors.New("conflicting interaction callback context")
		}
		context = env.Context
	}
	if body.Operator.OpenID == "" || context.ChatID == "" || context.MessageID == "" || len(body.Action.Value) > uvim.InteractionCardMaxBytes {
		return uvim.Event{}, false, errors.New("interaction callback lacks native scope")
	}
	decoder := json.NewDecoder(strings.NewReader(string(body.Action.Value)))
	decoder.DisallowUnknownFields()
	var value interactionCallbackValue
	if err := decoder.Decode(&value); err != nil {
		return uvim.Event{}, false, errors.New("invalid interaction callback value")
	}
	stamp, err := strconv.ParseInt(env.Header.CreateTime, 10, 64)
	if err != nil || stamp <= 0 {
		return uvim.Event{}, false, errors.New("interaction callback time is unavailable")
	}
	occurred := time.UnixMicro(stamp).UTC()
	now := time.Now().UTC()
	if config.Now != nil {
		now = config.Now().UTC()
	}
	if occurred.Before(now.Add(-5*time.Minute)) || occurred.After(now.Add(time.Minute)) {
		return uvim.Event{}, false, errors.New("interaction callback is outside transport time window")
	}
	answer := ""
	if raw, present := body.Action.FormValue["answer"]; present {
		if json.Unmarshal(raw, &answer) != nil {
			return uvim.Event{}, false, errors.New("invalid interaction answer")
		}
	}
	user := uvim.User{ID: body.Operator.OpenID, Kind: uvim.UserKindUser, Identities: uvim.NormalizeUserIdentities(
		uvim.UserIdentity{Type: "lark.open_id", ID: body.Operator.OpenID, Scope: config.AppID, Source: uvim.IdentitySourceCallback},
		uvim.UserIdentity{Type: "lark.user_id", ID: body.Operator.UserID, Scope: body.Operator.TenantKey, Source: uvim.IdentitySourceCallback},
		uvim.UserIdentity{Type: "lark.union_id", ID: body.Operator.UnionID, Source: uvim.IdentitySourceCallback})}
	proof := sha256.Sum256([]byte(strings.Join([]string{"lark-authenticated-ws/v1", config.AppID, env.Header.EventID, env.Header.CreateTime, context.ChatID, context.MessageID}, "\x00")))
	action := &uvim.InteractionActionEvent{SchemaVersion: value.SchemaVersion, EventID: env.Header.EventID, Provider: "lark", Connector: config.Connector,
		ConversationID: context.ChatID, MessageID: context.MessageID, CardID: value.CardID, Revision: value.Revision, ActionID: value.ActionID, Kind: value.Kind, Token: value.Token,
		Actor: user, SubmittedText: answer, OccurredAt: occurred, BotAppID: config.AppID, BotProofDigest: hex.EncodeToString(proof[:])}
	event := uvim.Event{ID: env.Header.EventID, Type: uvim.EventInteractionAction, Provider: "lark", Connector: config.Connector, Time: occurred, Login: uvim.Login{Platform: "lark", Connector: config.Connector, ID: config.AppID},
		Channel: uvim.Channel{ID: context.ChatID}, User: user, Referrer: uvim.Referrer{MessageID: context.MessageID, ChannelID: context.ChatID, Target: &uvim.OutboundTarget{Kind: uvim.TargetConversation, ID: context.ChatID}}, Addressed: true, Interaction: action}
	if err := uvim.ValidateInteractionEvent(event); err != nil {
		return uvim.Event{}, false, err
	}
	return event, true, nil
}
