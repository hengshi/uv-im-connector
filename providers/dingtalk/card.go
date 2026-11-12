package dingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

const (
	dingtalkCardMaxActions    = uvim.InteractionMaxActions
	dingtalkCardCallbackParam = "jarvis_callback"
)

func (p *Provider) sendInteraction(ctx context.Context, msg uvim.OutboundMessage) (result uvim.SendResult, err error) {
	defer func() { err = uvim.NewProviderSendOperationError("dingtalk card send", err) }()
	if err := uvim.ValidateOutboundInteraction(msg, p.Capabilities()); err != nil {
		return result, dingtalkCardPreparationError(err)
	}
	if err := uvim.ValidateOutboundTarget(msg, p.Capabilities()); err != nil {
		return result, dingtalkCardPreparationError(fmt.Errorf("dingtalk card: %w", err))
	}
	if msg.Interaction == nil {
		return result, dingtalkCardPreparationError(fmt.Errorf("dingtalk card: interaction is required"))
	}
	spaceID, err := dingtalkCardGroupSpace(msg)
	if err != nil {
		return result, dingtalkCardPreparationError(err)
	}
	data, err := dingtalkCardData(*msg.Interaction)
	if err != nil {
		return result, dingtalkCardPreparationError(err)
	}
	accessToken, err := p.accessToken(ctx)
	if err != nil {
		return result, beforeSendError("dingtalk card access token", err)
	}
	body := map[string]any{
		"cardTemplateId": p.config.CardTemplateID,
		"outTrackId":     msg.Interaction.CardID,
		"callbackType":   "STREAM",
		"cardData":       map[string]any{"cardParamMap": data},
		"openSpaceId":    "dtv1.card//IM_GROUP." + spaceID,
		"userIdType":     1,
		"imGroupOpenSpaceModel": map[string]any{
			"supportForward": false,
		},
		"imGroupOpenDeliverModel": map[string]any{
			"robotCode": p.config.ClientID,
		},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return result, dingtalkCardPreparationError(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.apiBaseURL()+"/v1.0/card/instances/createAndDeliver", bytes.NewReader(raw))
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("x-acs-dingtalk-access-token", accessToken)
	response, err := doDingTalkJSON(req)
	if err != nil {
		return result, err
	}
	var receipt struct {
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
	}
	if err := json.Unmarshal(response, &receipt); err != nil {
		return result, err
	}
	// Accept either JSON representation of a zero code; keep no-code receipts
	// compatible with the endpoint's successful response shape.
	code := strings.Trim(strings.TrimSpace(string(receipt.Code)), `"`)
	if code != "" && code != "0" {
		return result, uvim.NewProviderResponseError(response, "dingtalk card: provider rejected card", fmt.Errorf("dingtalk card: code=%s message=%q", code, receipt.Message))
	}
	digest, err := msg.Interaction.Digest()
	if err != nil {
		return result, dingtalkCardPreparationError(err)
	}
	return uvim.SendResult{
		Provider:  p.ID(),
		Connector: p.ConnectorID(),
		Time:      time.Now().UTC(),
		Interaction: &uvim.InteractionPublication{
			CardID:        msg.Interaction.CardID,
			Revision:      msg.Interaction.Revision,
			PayloadSHA256: digest,
			NativeCardID:  msg.Interaction.CardID,
			BotAppID:      p.config.ClientID,
		},
	}, nil
}

func dingtalkCardGroupSpace(msg uvim.OutboundMessage) (string, error) {
	target := msg.ResolvedTarget()
	if target.Kind != uvim.TargetGroup || strings.TrimSpace(target.ID) == "" {
		return "", fmt.Errorf("dingtalk card: only a native group target is supported")
	}
	if msg.ChannelType != "" && msg.ChannelType != uvim.ChannelGroup {
		return "", fmt.Errorf("dingtalk card: channel type must be group")
	}
	for _, channelID := range []string{msg.ChannelID, msg.Referrer.ChannelID} {
		if channelID != "" && channelID != target.ID {
			return "", fmt.Errorf("dingtalk card: group channel and target must match")
		}
	}
	return target.ID, nil
}

func dingtalkCardData(card uvim.InteractionCard) (map[string]string, error) {
	if err := card.Validate(); err != nil {
		return nil, err
	}
	if card.Question != nil && card.Question.AllowFreeText {
		return nil, fmt.Errorf("dingtalk card: free-text questions are unsupported")
	}
	if len(card.Actions) > dingtalkCardMaxActions {
		return nil, fmt.Errorf("dingtalk card: too many actions")
	}
	facts := make([]string, 0, len(card.Facts))
	for _, fact := range card.Facts {
		facts = append(facts, fact.Label+": "+fact.Value)
	}
	data := map[string]string{
		"title":        card.Title,
		"summary":      card.Summary,
		"facts":        strings.Join(facts, "\n"),
		"detail_url":   card.DetailURL,
		"action_count": strconv.Itoa(len(card.Actions)),
	}
	if card.Question != nil {
		data["question"] = card.Question.Prompt
		data["question_help"] = card.Question.Help
	}
	for index, action := range card.Actions {
		callback, err := uvim.EncodeInteractionCallbackValue(card, action)
		if err != nil {
			return nil, err
		}
		prefix := "action_" + strconv.Itoa(index+1) + "_"
		data[prefix+"id"] = action.ID
		data[prefix+"label"] = action.Label
		data[prefix+"callback"] = callback
		data[prefix+"disabled"] = strconv.FormatBool(action.Disabled)
	}
	return data, nil
}

func dingtalkCardPreparationError(err error) error {
	return uvim.NewProviderSendFailure(uvim.SendFailure{
		Category:      uvim.SendFailureInvalidRequest,
		DeliveryState: uvim.DeliveryNotAttempted,
	}, err.Error(), err)
}
