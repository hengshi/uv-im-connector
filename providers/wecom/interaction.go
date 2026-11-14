package wecom

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	uvim "github.com/hengshi/uv-im-connector"
)

const (
	wecomTemplateCardTypeButton = "button_interaction"
	wecomTemplateCardTypeText   = "text_notice"
	wecomTemplateCardMaxButtons = 6
	wecomTemplateCardKeyMax     = 1024
	wecomTemplateCardTaskMax    = 128
)

// sendInteraction uses the documented template_card wire format. Its ACK is
// deliberately not promoted to a native message receipt: WeCom returns no
// native outbound message ID for this WebSocket operation.
func (p *Provider) sendInteraction(ctx context.Context, msg uvim.OutboundMessage) (uvim.SendResult, error) {
	// A single callback identifies its conversation by the current userid;
	// group callbacks use chatid. A different historic channel cannot be
	// reconstructed from the card, so reject before sending and retain text.
	target := msg.ResolvedTarget()
	if target.ID == "" || (target.Kind != uvim.TargetUser && target.Kind != uvim.TargetGroup) ||
		(msg.ChannelType != "" && ((target.Kind == uvim.TargetUser && msg.ChannelType != uvim.ChannelDirect) || (target.Kind == uvim.TargetGroup && msg.ChannelType != uvim.ChannelGroup))) {
		return uvim.SendResult{}, wecomInteractionPreparationError(errors.New("wecom card requires a native direct or group target"))
	}
	if msg.Referrer.ReplyToken != "" && (msg.Referrer.Target == nil || (uvim.OutboundMessage{Target: msg.Referrer.Target}).ResolvedTarget() != target) {
		return uvim.SendResult{}, wecomInteractionPreparationError(errors.New("wecom card reply scope does not match native target"))
	}
	for _, channelID := range []string{msg.ChannelID, msg.Referrer.ChannelID} {
		if channelID != "" && channelID != target.ID {
			return uvim.SendResult{}, wecomInteractionPreparationError(errors.New("wecom card conversation cannot match callback target"))
		}
	}
	card, err := renderInteractionCard(*msg.Interaction)
	if err != nil {
		return uvim.SendResult{}, wecomInteractionPreparationError(err)
	}
	digest, err := msg.Interaction.Digest()
	if err != nil {
		return uvim.SendResult{}, wecomInteractionPreparationError(err)
	}
	conn, writeMu, err := p.waitActive(ctx)
	if err != nil {
		return uvim.SendResult{}, err
	}
	body := map[string]any{"msgtype": "template_card", "template_card": card}
	if msg.Referrer.ReplyToken != "" {
		reqID := msg.Referrer.ReplyToken
		if err := p.withReplyLock(ctx, reqID, func() error {
			return p.sendWithAck(ctx, conn, writeMu, reqID, frame{Cmd: cmdRespond, Headers: headers{ReqID: reqID}, Body: body})
		}); err != nil {
			return uvim.SendResult{}, err
		}
	} else {
		target := msg.ResolvedTarget()
		if target.ID == "" {
			return uvim.SendResult{}, wecomInteractionPreparationError(errors.New("target id is required"))
		}
		reqID := p.reqID(cmdSend)
		body["chatid"] = target.ID
		if err := p.sendWithAck(ctx, conn, writeMu, reqID, frame{Cmd: cmdSend, Headers: headers{ReqID: reqID}, Body: body}); err != nil {
			return uvim.SendResult{}, err
		}
	}
	return uvim.SendResult{
		Provider:  p.ID(),
		Connector: p.ConnectorID(),
		Time:      p.now().UTC(),
		Interaction: &uvim.InteractionPublication{
			CardID:        msg.Interaction.CardID,
			Revision:      msg.Interaction.Revision,
			PayloadSHA256: digest,
			NativeCardID:  msg.Interaction.CardID,
			BotAppID:      p.config.BotID,
		},
	}, nil
}

// renderInteractionCard accepts only the neutral card model. WeCom template
// cards have no disabled state or free-text input, so either would make a
// partly actionable question and is rejected before the socket is touched.
func renderInteractionCard(card uvim.InteractionCard) (map[string]any, error) {
	if err := card.Validate(); err != nil {
		return nil, err
	}
	if !validWeComTaskID(card.CardID) {
		return nil, errors.New("invalid wecom template card task id")
	}
	if card.Question != nil && card.Question.AllowFreeText {
		return nil, errors.New("wecom template cards do not support free-text interaction")
	}
	if len(card.Actions) > wecomTemplateCardMaxButtons {
		return nil, errors.New("wecom template card action limit exceeded")
	}

	content := make([]string, 0, 2+len(card.Facts))
	if card.Summary != "" {
		content = append(content, card.Summary)
	}
	for _, fact := range card.Facts {
		content = append(content, fact.Label+": "+fact.Value)
	}
	if card.Question != nil {
		content = append(content, card.Question.Prompt)
		if card.Question.Help != "" {
			content = append(content, card.Question.Help)
		}
	}

	// The official SDK documents sub_title_text and button text as display
	// recommendations, not transport maxima. Preserve the neutral content
	// rather than silently truncating it; task_id, button count, and key are
	// the documented hard limits enforced above.
	template := map[string]any{
		"card_type":      wecomTemplateCardTypeText,
		"task_id":        card.CardID,
		"main_title":     map[string]any{"title": card.Title},
		"sub_title_text": strings.Join(content, "\n"),
	}
	if card.DetailURL != "" {
		template["card_action"] = map[string]any{"type": 1, "url": card.DetailURL}
	}
	if len(card.Actions) == 0 {
		return template, nil
	}

	buttons := make([]any, 0, len(card.Actions))
	for _, action := range card.Actions {
		if action.Disabled {
			return nil, errors.New("wecom template cards do not support disabled actions")
		}
		key, err := uvim.EncodeInteractionCallbackValue(card, action)
		if err != nil {
			return nil, err
		}
		if len(key) > wecomTemplateCardKeyMax {
			return nil, errors.New("wecom template card action key limit exceeded")
		}
		buttons = append(buttons, map[string]any{"text": action.Label, "key": key})
	}
	template["card_type"] = wecomTemplateCardTypeButton
	template["button_list"] = buttons
	return template, nil
}

func validWeComTaskID(value string) bool {
	if len(value) == 0 || len(value) > wecomTemplateCardTaskMax {
		return false
	}
	for _, char := range []byte(value) {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-' || char == '@' {
			continue
		}
		return false
	}
	return true
}

func wecomInteractionPreparationError(err error) error {
	if err == nil {
		return nil
	}
	if failure, ok := uvim.ProviderSendFailure(err); ok {
		failure.DeliveryState = uvim.DeliveryNotAttempted
		return uvim.NewProviderSendFailure(failure, "wecom interaction presentation unsupported", err)
	}
	return uvim.NewProviderSendFailure(uvim.SendFailure{
		Category:      uvim.SendFailureInvalidRequest,
		DeliveryState: uvim.DeliveryNotAttempted,
	}, "wecom interaction presentation unsupported", err)
}

// decodeTemplateCardEvent accepts only an authenticated event callback that
// proves the configured bot, actor and current direct/group scope. A card
// callback is intentionally not a native chat message or quote anchor.
func (p *Provider) decodeTemplateCardEvent(in frame) (uvim.Event, bool) {
	body := in.Body
	eventBody := uvim.MapStringAny(body["event"])
	if uvim.StringValue(eventBody["eventtype"]) != "template_card_event" || uvim.StringValue(body["msgtype"]) != "event" || uvim.StringValue(body["aibotid"]) != p.config.BotID {
		return uvim.Event{}, false
	}
	callbackID := uvim.StringValue(body["msgid"])
	actorID := uvim.StringValue(uvim.MapStringAny(body["from"])["userid"])
	taskID := uvim.StringValue(eventBody["task_id"])
	callbackValue := uvim.StringValue(eventBody["event_key"])
	if callbackID == "" || actorID == "" || !validWeComTaskID(taskID) || callbackValue == "" || len(callbackValue) > wecomTemplateCardKeyMax {
		return uvim.Event{}, false
	}
	value, err := uvim.DecodeInteractionCallbackValue(callbackValue)
	if err != nil || value.CardID != taskID {
		return uvim.Event{}, false
	}

	channelType := uvim.ChannelDirect
	channelID := actorID
	target := uvim.OutboundTarget{Kind: uvim.TargetUser, ID: actorID}
	switch uvim.StringValue(body["chattype"]) {
	case "single":
		// A single callback's native target is the callback actor. A chatid,
		// if supplied by an unexpected payload, has no authority here.
	case "group":
		channelID = uvim.StringValue(body["chatid"])
		if channelID == "" {
			return uvim.Event{}, false
		}
		channelType = uvim.ChannelGroup
		target = uvim.OutboundTarget{Kind: uvim.TargetGroup, ID: channelID}
	default:
		// Event callbacks declare the scope explicitly; do not infer single.
		return uvim.Event{}, false
	}

	occurred := p.now().UTC()
	user := uvim.User{ID: actorID, Kind: uvim.UserKindUser, Identities: uvim.NormalizeUserIdentities(
		uvim.UserIdentity{Type: "wecom.callback_userid", ID: actorID, Source: uvim.IdentitySourceCallback},
	)}
	proof := sha256.Sum256([]byte(strings.Join([]string{
		"wecom-authenticated-ws/v1", p.config.BotID, callbackID, channelType, channelID, actorID, taskID, callbackValue,
	}, "\x00")))
	action := &uvim.InteractionActionEvent{
		SchemaVersion:  uvim.InteractionCardInstanceVersionV1,
		EventID:        callbackID,
		Provider:       p.ID(),
		Connector:      p.ConnectorID(),
		ConversationID: channelID,
		NativeCardID:   taskID,
		CardID:         value.CardID,
		Revision:       value.Revision,
		ActionID:       value.ActionID,
		Kind:           value.Kind,
		Token:          value.Token,
		Actor:          user,
		OccurredAt:     occurred,
		BotAppID:       p.config.BotID,
		BotProofDigest: hex.EncodeToString(proof[:]),
	}
	event := uvim.Event{
		ID:          callbackID,
		Type:        uvim.EventInteractionAction,
		Provider:    p.ID(),
		Connector:   p.ConnectorID(),
		Time:        occurred,
		Login:       uvim.Login{Platform: p.ID(), Connector: p.ConnectorID(), ID: p.config.BotID},
		Channel:     uvim.Channel{ID: channelID, Type: channelType},
		User:        user,
		Referrer:    uvim.Referrer{ChannelID: channelID, Target: &target},
		Addressed:   true,
		Interaction: action,
	}
	if err := uvim.ValidateInteractionEvent(event); err != nil {
		return uvim.Event{}, false
	}
	return event, true
}
