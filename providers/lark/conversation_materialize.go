package lark

import (
	"context"
	"strings"

	uvim "github.com/hengshi/uv-im-connector"
)

// MaterializeConversationResource re-proves both the native trigger and the
// exact source message before it uses the Hub-private Lark resource key. It is
// intentionally not a general resource lookup API.
func (p *Provider) MaterializeConversationResource(ctx context.Context, request uvim.ConversationResourceMaterializeRequest) (uvim.ResourceRef, error) {
	if !validConversationWindowEvent(p, request.Trigger) || strings.TrimSpace(request.Source) != nativeWindowSource || strings.TrimSpace(request.SourceMessage) == "" {
		return uvim.ResourceRef{}, uvim.ErrConversationResourceUnavailable
	}
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return uvim.ResourceRef{}, err
	}
	trigger, code, err := p.nativeWindowMessage(ctx, token, request.Trigger.Message.ID)
	if err != nil || code != 0 || !validNativeSearchTrigger(request.Trigger, trigger) || !sameNativeSearchScope(nativeSearchScope(p, request.Trigger, trigger), request.Scope) {
		return uvim.ResourceRef{}, uvim.ErrConversationResourceUnavailable
	}
	source, code, err := p.nativeWindowMessage(ctx, token, request.SourceMessage)
	if err != nil || code != 0 || source.ID != request.SourceMessage {
		return uvim.ResourceRef{}, uvim.ErrConversationResourceUnavailable
	}
	accepted, _ := nativeWindowItems(request.Trigger, trigger, []quotedMessageItem{source})
	if len(accepted) != 1 || accepted[0].MessageID != request.SourceMessage || !containsNativeMaterial(accepted[0].Resources, request.Resource, request.SourceOrdinal) {
		return uvim.ResourceRef{}, uvim.ErrConversationResourceUnavailable
	}
	ref := request.Resource
	ref.Provider, ref.Connector = p.ID(), p.ConnectorID()
	ref.SourceMessageID = request.SourceMessage
	ref.Metadata = map[string]string{"message_id": request.SourceMessage}
	return p.Download(ctx, uvim.ResourceDownloadRequest{Resource: ref, Event: request.Trigger, Dir: request.Dir})
}

func containsNativeMaterial(resources []uvim.ContextResource, wanted uvim.ResourceRef, ordinal int) bool {
	for _, resource := range resources {
		candidate := resource.Resource
		if resource.SourceOrdinal == ordinal && candidate.Kind == wanted.Kind && candidate.Key != "" && candidate.Key == wanted.Key {
			return true
		}
	}
	return false
}

var _ uvim.ConversationResourceMaterializer = (*Provider)(nil)
