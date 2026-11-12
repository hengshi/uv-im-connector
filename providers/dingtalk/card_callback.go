package dingtalk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/handler"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/payload"
)

func (p *Provider) cardCallbackHandler(sink uvim.EventSink) handler.IFrameHandler {
	return func(ctx context.Context, frame *payload.DataFrame) (*payload.DataFrameResponse, error) {
		event, ok, err := p.decodeCardCallback(frame)
		if err != nil {
			return nil, fmt.Errorf("dingtalk card callback: %w", err)
		}
		if !ok {
			return payload.NewSuccessDataFrameResponse(), nil
		}
		if err := sink.Emit(ctx, event); err != nil {
			return nil, fmt.Errorf("dingtalk card callback: emit: %w", err)
		}
		p.setState("event")
		return payload.NewSuccessDataFrameResponse(), nil
	}
}

// decodeCardCallback accepts only a card action carried by this Provider's
// authenticated Stream connection. It is intentionally not reused by webhook
// ingress: the configured Stream application, not callback JSON, proves bot
// ownership.
func (p *Provider) decodeCardCallback(frame *payload.DataFrame) (uvim.Event, bool, error) {
	if frame == nil || frame.GetTopic() != payload.CardInstanceCallbackTopic {
		return uvim.Event{}, false, errors.New("unexpected card callback topic")
	}
	if p.config.ClientID == "" || p.config.ClientSecret == "" || p.config.CardTemplateID == "" {
		return uvim.Event{}, false, errors.New("card callback is not configured")
	}
	eventID := strings.TrimSpace(frame.GetMessageId())
	if eventID == "" {
		return uvim.Event{}, false, errors.New("card callback lacks frame message ID")
	}
	var callback struct {
		Content    string `json:"content"`
		CorpID     string `json:"corpId"`
		OutTrackID string `json:"outTrackId"`
		SpaceID    string `json:"spaceId"`
		SpaceType  string `json:"spaceType"`
		Type       string `json:"type"`
		UserID     string `json:"userId"`
		UserIDType int    `json:"userIdType"`
	}
	if err := json.Unmarshal([]byte(frame.Data), &callback); err != nil {
		return uvim.Event{}, false, errors.New("invalid card callback payload")
	}
	if callback.Type != "actionCallback" || callback.SpaceType != "IM_GROUP" || strings.TrimSpace(callback.SpaceID) == "" || strings.TrimSpace(callback.UserID) == "" || callback.UserIDType != 1 || strings.TrimSpace(callback.OutTrackID) == "" || strings.TrimSpace(callback.Content) == "" {
		return uvim.Event{}, false, errors.New("card callback lacks native group scope")
	}
	var actionData struct {
		CardPrivateData struct {
			ActionIDs []string       `json:"actionIds"`
			Params    map[string]any `json:"params"`
		} `json:"cardPrivateData"`
	}
	if err := json.Unmarshal([]byte(callback.Content), &actionData); err != nil {
		return uvim.Event{}, false, errors.New("invalid card callback content")
	}
	encoded := strings.TrimSpace(uvim.StringValue(actionData.CardPrivateData.Params[dingtalkCardCallbackParam]))
	value, err := uvim.DecodeInteractionCallbackValue(encoded)
	if err != nil {
		return uvim.Event{}, false, errors.New("invalid card callback value")
	}
	if len(actionData.CardPrivateData.ActionIDs) != 1 || actionData.CardPrivateData.ActionIDs[0] != value.ActionID || value.CardID != callback.OutTrackID {
		return uvim.Event{}, false, errors.New("card callback does not match native card instance")
	}
	actor := uvim.User{
		ID:   callback.UserID,
		Kind: uvim.UserKindUser,
		Identities: uvim.NormalizeUserIdentities(
			uvim.UserIdentity{Type: "dingtalk.userid", ID: callback.UserID, Scope: callback.CorpID, Source: uvim.IdentitySourceCallback},
		),
	}
	payloadDigest := sha256.Sum256([]byte(frame.Data))
	proof := sha256.Sum256([]byte(strings.Join([]string{
		"dingtalk-authenticated-stream-card/v1",
		p.config.ClientID,
		eventID,
		frame.GetHeader("appId"),
		frame.GetHeader(payload.DataFrameHeaderKTime),
		hex.EncodeToString(payloadDigest[:]),
	}, "\x00")))
	now := time.Now().UTC()
	action := &uvim.InteractionActionEvent{
		SchemaVersion:  uvim.InteractionCardInstanceVersionV1,
		EventID:        eventID,
		Provider:       p.ID(),
		Connector:      p.ConnectorID(),
		ConversationID: callback.SpaceID,
		NativeCardID:   callback.OutTrackID,
		CardID:         value.CardID,
		Revision:       value.Revision,
		ActionID:       value.ActionID,
		Kind:           value.Kind,
		Token:          value.Token,
		Actor:          actor,
		OccurredAt:     now,
		BotAppID:       p.config.ClientID,
		BotProofDigest: hex.EncodeToString(proof[:]),
	}
	event := uvim.Event{
		ID:        eventID,
		Type:      uvim.EventInteractionAction,
		Provider:  p.ID(),
		Connector: p.ConnectorID(),
		Time:      now,
		Login:     uvim.Login{Platform: p.ID(), Connector: p.ConnectorID(), ID: p.config.ClientID},
		Channel:   uvim.Channel{ID: callback.SpaceID, Type: uvim.ChannelGroup},
		User:      actor,
		Referrer: uvim.Referrer{
			ChannelID: callback.SpaceID,
			Target:    &uvim.OutboundTarget{ID: callback.SpaceID, Kind: uvim.TargetGroup},
		},
		Addressed:   true,
		Interaction: action,
	}
	if err := uvim.ValidateInteractionEvent(event); err != nil {
		return uvim.Event{}, false, err
	}
	return event, true, nil
}
