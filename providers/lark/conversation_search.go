package lark

import (
	"context"
	"errors"
	"strings"

	uvim "github.com/hengshi/uv-im-connector"
)

// ReadConversationSearchPage returns exactly one provider page. The Hub owns
// user filters, cursor lifetime, and the raw continuation; this reader owns
// native trigger proof and page/container construction only.
func (p *Provider) ReadConversationSearchPage(ctx context.Context, request uvim.ConversationSearchPageRequest) uvim.ConversationSearchPage {
	if !validConversationWindowEvent(p, request.Trigger) || request.Limit < 1 || request.Limit > nativeWindowPageSize {
		return nativeSearchUnavailable(request.Trigger, 0, nil)
	}
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return nativeSearchUnavailable(request.Trigger, 0, err)
	}
	trigger, code, err := p.nativeWindowMessage(ctx, token, request.Trigger.Message.ID)
	if err != nil || code != 0 {
		return nativeSearchUnavailable(request.Trigger, code, err)
	}
	if !validNativeSearchTrigger(request.Trigger, trigger) {
		return nativeSearchUnavailable(request.Trigger, 0, nil)
	}
	scope := nativeSearchScope(p, request.Trigger, trigger)
	if request.ExpectedScope != nil && !sameNativeSearchScope(scope, *request.ExpectedScope) {
		return nativeSearchUnavailable(request.Trigger, 0, nil)
	}
	items, hasMore, nextPageToken, code, err := p.nativeSearchList(ctx, token, trigger, request.Limit, request.PageToken)
	if err != nil || code != 0 {
		return nativeSearchUnavailable(request.Trigger, code, err)
	}
	candidates, boundaryOmitted := nativeSearchCandidates(request.Trigger, trigger, items)
	return uvim.ConversationSearchPage{
		Status:          "available",
		Scope:           scope,
		Candidates:      candidates,
		CandidateCount:  len(items),
		BoundaryOmitted: boundaryOmitted,
		HasMore:         hasMore,
		NextPageToken:   nextPageToken,
	}
}

func validNativeSearchTrigger(event uvim.Event, item quotedMessageItem) bool {
	nativeTime := parseMillis(item.CreateTime)
	return item.ID == event.Message.ID && item.ChatID == event.Channel.ID && !item.Deleted && !nativeTime.IsZero() && nativeTime.Equal(event.Message.CreatedAt) && (event.Referrer.ThreadID == "" || item.ThreadID == event.Referrer.ThreadID)
}

func nativeSearchScope(p *Provider, event uvim.Event, trigger quotedMessageItem) uvim.ConversationSearchScope {
	return uvim.ConversationSearchScope{
		Provider:         p.ID(),
		Connector:        p.ConnectorID(),
		ConversationID:   trigger.ChatID,
		ThreadID:         trigger.ThreadID,
		TriggerMessageID: event.Message.ID,
		AnchorTime:       event.Message.CreatedAt.UTC(),
	}
}

func sameNativeSearchScope(left, right uvim.ConversationSearchScope) bool {
	return strings.TrimSpace(left.Provider) == strings.TrimSpace(right.Provider) &&
		strings.TrimSpace(left.Connector) == strings.TrimSpace(right.Connector) &&
		strings.TrimSpace(left.ConversationID) == strings.TrimSpace(right.ConversationID) &&
		strings.TrimSpace(left.ThreadID) == strings.TrimSpace(right.ThreadID) &&
		strings.TrimSpace(left.TriggerMessageID) == strings.TrimSpace(right.TriggerMessageID) &&
		left.AnchorTime.Equal(right.AnchorTime)
}

func (p *Provider) nativeSearchList(ctx context.Context, token string, trigger quotedMessageItem, limit int, pageToken string) ([]quotedMessageItem, bool, string, int, error) {
	page, code, err := p.nativeConversationList(ctx, token, trigger, limit, pageToken)
	if err != nil || code != 0 {
		return nil, false, "", code, err
	}
	if (page.HasMore && strings.TrimSpace(page.PageToken) == "") || (!page.HasMore && strings.TrimSpace(page.PageToken) != "") {
		return nil, false, "", 0, errors.New("invalid native search continuation")
	}
	return page.Items, page.HasMore, page.PageToken, 0, nil
}

func nativeSearchCandidates(event uvim.Event, trigger quotedMessageItem, items []quotedMessageItem) ([]uvim.ConversationSearchCandidate, int) {
	accepted, boundaryOmitted := nativeWindowItems(event, trigger, items)
	original := make(map[string]quotedMessageItem, len(items))
	for _, item := range items {
		original[item.ID] = item
	}
	candidates := make([]uvim.ConversationSearchCandidate, 0, len(accepted))
	for _, message := range accepted {
		item, ok := original[message.MessageID]
		if !ok {
			continue
		}
		candidates = append(candidates, uvim.ConversationSearchCandidate{Message: message, Readable: nativeSearchReadable(item)})
	}
	return candidates, boundaryOmitted
}

func nativeSearchReadable(item quotedMessageItem) bool {
	if item.Type == "post" {
		return strings.TrimSpace(renderQuotedPost(item.Body.Content).text) != ""
	}
	return strings.TrimSpace(messageBody(item.Type, item.Body.Content)) != ""
}

func nativeSearchUnavailable(event uvim.Event, code int, err error) uvim.ConversationSearchPage {
	window := nativeWindowUnavailable(newConversationWindowResult(event), code, err)
	return uvim.ConversationSearchPage{Status: window.Status, Reason: window.Reason}
}

var _ uvim.ConversationSearchReader = (*Provider)(nil)
