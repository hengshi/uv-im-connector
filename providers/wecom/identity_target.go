package wecom

import (
	"context"
	"errors"

	uvim "github.com/hengshi/uv-im-connector"
)

// PrivateTarget returns the current callback's native private recipient. It
// deliberately does not consult an older identity link, presentation, or card:
// a WeCom card callback itself proves direct scope only when it explicitly
// carries chattype=single and the decoder has accepted the configured bot.
func (p *Provider) PrivateTarget(ctx context.Context, event uvim.Event) (uvim.OutboundTarget, error) {
	_ = ctx
	if event.Provider != p.ID() || event.Connector != p.ConnectorID() || event.User.ID == "" || (event.User.Kind != "" && event.User.Kind != uvim.UserKindUser) {
		return uvim.OutboundTarget{}, errors.New("wecom private native actor unavailable")
	}
	if event.Type == uvim.EventMessageCreate && event.Channel.Type == uvim.ChannelDirect && event.Referrer.Target != nil {
		// Preserve the established ordinary-message proof unchanged.
		return *event.Referrer.Target, nil
	}
	if event.Type != uvim.EventInteractionAction || event.Interaction == nil || event.Interaction.SchemaVersion != uvim.InteractionCardInstanceVersionV1 || uvim.ValidateInteractionEvent(event) != nil || event.Login.ID != p.config.BotID || event.Interaction.BotAppID != p.config.BotID || event.Channel.Type != uvim.ChannelDirect || event.Referrer.Target == nil {
		return uvim.OutboundTarget{}, errors.New("wecom private card callback unavailable")
	}
	target := *event.Referrer.Target
	if target.Kind != uvim.TargetUser || target.ID != event.User.ID {
		return uvim.OutboundTarget{}, errors.New("wecom private card callback target unavailable")
	}
	return target, nil
}

var _ uvim.PrivateTargetResolver = (*Provider)(nil)
