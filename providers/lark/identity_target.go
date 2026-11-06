package lark

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"

	uvim "github.com/hengshi/uv-im-connector"
)

// PrivateTarget preserves the released direct-message proof. A card callback
// has no chat classification, so it needs current native p2p proof instead of
// inheriting a delivery target from an old identity link.
func (p *Provider) PrivateTarget(ctx context.Context, event uvim.Event) (uvim.OutboundTarget, error) {
	if event.Provider != p.ID() || event.Connector != p.ConnectorID() || event.User.ID == "" || (event.User.Kind != "" && event.User.Kind != uvim.UserKindUser) {
		return uvim.OutboundTarget{}, errors.New("personal native actor unavailable")
	}
	if event.Type == uvim.EventMessageCreate && event.Channel.Type == uvim.ChannelDirect && event.Referrer.Target != nil {
		return *event.Referrer.Target, nil
	}
	if event.Type != uvim.EventInteractionAction || uvim.ValidateInteractionEvent(event) != nil || event.Interaction.BotAppID != p.config.AppID || event.Login.ID != p.config.AppID {
		return uvim.OutboundTarget{}, errors.New("private native event unavailable")
	}
	lookupCtx, cancel := context.WithTimeout(ctx, uvim.ConversationWindowTimeout)
	defer cancel()
	message, err := p.messageIdentity(lookupCtx, event.Interaction.MessageID)
	if err != nil || message.Deleted || message.ID != event.Interaction.MessageID || message.ChatID != event.Channel.ID || message.Type != "interactive" || message.Sender.ID != p.config.AppID || message.Sender.Kind != "app_id" || message.Sender.Type != "app" || message.ThreadID != event.Referrer.ThreadID {
		return uvim.OutboundTarget{}, errors.New("native interaction scope unavailable")
	}
	token, err := p.tenantAccessToken(lookupCtx)
	if err != nil {
		return uvim.OutboundTarget{}, errors.New("private native lookup unavailable")
	}
	request, err := http.NewRequestWithContext(lookupCtx, http.MethodGet, p.baseURL()+"/open-apis/im/v1/chats/"+url.PathEscape(message.ChatID), nil)
	if err != nil {
		return uvim.OutboundTarget{}, errors.New("private native lookup unavailable")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	raw, err := p.doJSON(request, "lark private interaction target")
	if err != nil {
		return uvim.OutboundTarget{}, errors.New("private native lookup unavailable")
	}
	var response struct {
		Code *int `json:"code"`
		Data struct {
			Mode string `json:"chat_mode"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Code == nil || *response.Code != 0 || response.Data.Mode != "p2p" {
		return uvim.OutboundTarget{}, errors.New("private native conversation unavailable")
	}
	return uvim.OutboundTarget{ID: message.ChatID, Kind: uvim.TargetConversation}, nil
}
