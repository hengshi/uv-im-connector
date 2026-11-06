package lark

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	uvim "github.com/hengshi/uv-im-connector"
)

const (
	nativeWindowPageSize = 10
	nativeWindowSource   = "native"
)

// ReadConversationWindow returns only a bounded, native-scoped description of
// earlier messages. It deliberately has no fallback: a failed topic lookup
// must not broaden into the containing chat.
func (p *Provider) ReadConversationWindow(ctx context.Context, event uvim.Event) uvim.ConversationWindowResult {
	result := newConversationWindowResult(event)
	if !validConversationWindowEvent(p, event) {
		return nativeWindowUnavailable(result, 0, nil)
	}

	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return nativeWindowUnavailable(result, 0, err)
	}
	trigger, code, err := p.nativeWindowMessage(ctx, token, event.Message.ID)
	if err != nil || code != 0 {
		return nativeWindowUnavailable(result, code, err)
	}
	if !validNativeWindowTrigger(event, trigger) {
		return nativeWindowUnavailable(result, 0, nil)
	}

	result.ConversationID = trigger.ChatID
	result.ThreadID = trigger.ThreadID
	items, hasMore, code, err := p.nativeWindowList(ctx, token, trigger)
	if err != nil || code != 0 {
		return nativeWindowUnavailable(result, code, err)
	}

	result.CandidateCount = len(items)
	result.HasMore = hasMore
	result.Items, result.BoundaryOmitted = nativeWindowItems(event, trigger, items)
	if result.BoundaryOmitted > 0 && (result.HasMore || result.CandidateCount == nativeWindowPageSize) {
		result.Status = "scan_limited"
		result.Reason = "window_scan_limited"
		return result
	}
	if len(result.Items) > 0 {
		result.Status = "available"
		return result
	}
	result.Status = "none"
	return result
}

func newConversationWindowResult(event uvim.Event) uvim.ConversationWindowResult {
	return uvim.ConversationWindowResult{
		Version:          uvim.ConversationWindowVersionV1,
		Source:           nativeWindowSource,
		Status:           "unavailable",
		ConversationID:   strings.TrimSpace(event.Channel.ID),
		ThreadID:         strings.TrimSpace(event.Referrer.ThreadID),
		TriggerMessageID: strings.TrimSpace(event.Message.ID),
		AnchorTime:       event.Message.CreatedAt.UTC(),
	}
}

func validConversationWindowEvent(p *Provider, event uvim.Event) bool {
	return p != nil &&
		strings.TrimSpace(event.Provider) == p.ID() &&
		strings.TrimSpace(event.Connector) == p.ConnectorID() &&
		strings.TrimSpace(event.Login.ID) != "" &&
		strings.TrimSpace(event.Login.ID) == strings.TrimSpace(p.config.AppID) &&
		event.Channel.Type == uvim.ChannelGroup &&
		strings.TrimSpace(event.Channel.ID) != "" &&
		strings.TrimSpace(event.Message.ID) != "" &&
		!event.Message.CreatedAt.IsZero() &&
		strings.TrimSpace(event.Referrer.MessageID) == strings.TrimSpace(event.Message.ID) &&
		strings.TrimSpace(event.Referrer.ChannelID) == strings.TrimSpace(event.Channel.ID)
}

func validNativeWindowTrigger(event uvim.Event, item quotedMessageItem) bool {
	nativeTime := parseMillis(item.CreateTime)
	if item.ID != event.Message.ID || item.ChatID != event.Channel.ID || item.Deleted || nativeTime.IsZero() || !nativeTime.Equal(event.Message.CreatedAt) {
		return false
	}
	return event.Referrer.ThreadID == "" || event.Referrer.ThreadID == item.ThreadID
}

func (p *Provider) nativeWindowMessage(ctx context.Context, token, id string) (quotedMessageItem, int, error) {
	raw, err := p.nativeWindowGet(ctx, token, "/open-apis/im/v1/messages/"+url.PathEscape(id))
	if err != nil {
		return quotedMessageItem{}, 0, err
	}
	var response struct {
		Code int `json:"code"`
		Data struct {
			Items []quotedMessageItem `json:"items"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return quotedMessageItem{}, 0, errors.New("invalid native message response")
	}
	if response.Code != 0 {
		return quotedMessageItem{}, response.Code, nil
	}
	matches := make([]quotedMessageItem, 0, 1)
	for _, item := range response.Data.Items {
		if item.ID == id {
			matches = append(matches, item)
		}
	}
	if len(matches) != 1 {
		return quotedMessageItem{}, 0, errors.New("native message identity mismatch")
	}
	return matches[0], 0, nil
}

func (p *Provider) nativeWindowList(ctx context.Context, token string, trigger quotedMessageItem) ([]quotedMessageItem, bool, int, error) {
	query := url.Values{}
	query.Set("page_size", strconv.Itoa(nativeWindowPageSize))
	query.Set("sort_type", "ByCreateTimeDesc")
	if trigger.ThreadID != "" {
		query.Set("container_id_type", "thread")
		query.Set("container_id", trigger.ThreadID)
	} else {
		query.Set("container_id_type", "chat")
		query.Set("container_id", trigger.ChatID)
		triggerMillis := parseMillis(trigger.CreateTime).UnixMilli()
		query.Set("end_time", strconv.FormatInt((triggerMillis+999)/1000, 10))
	}
	raw, err := p.nativeWindowGet(ctx, token, "/open-apis/im/v1/messages?"+query.Encode())
	if err != nil {
		return nil, false, 0, err
	}
	var response struct {
		Code int `json:"code"`
		Data struct {
			Items   []quotedMessageItem `json:"items"`
			HasMore bool                `json:"has_more"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return nil, false, 0, errors.New("invalid native message list response")
	}
	if response.Code != 0 {
		return nil, false, response.Code, nil
	}
	if len(response.Data.Items) > nativeWindowPageSize {
		return nil, false, 0, errors.New("native message page exceeds requested limit")
	}
	return response.Data.Items, response.Data.HasMore, 0, nil
}

func (p *Provider) nativeWindowGet(ctx context.Context, token, path string) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL()+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return p.doJSON(req, "lark native conversation window")
}

func nativeWindowItems(event uvim.Event, trigger quotedMessageItem, items []quotedMessageItem) ([]uvim.ContextMessage, int) {
	counts := make(map[string]int, len(items))
	for _, item := range items {
		counts[item.ID]++
	}
	accepted := make([]uvim.ContextMessage, 0, len(items))
	boundaryOmitted := 0
	parentID := strings.TrimSpace(event.Referrer.ParentMessageID)
	for _, item := range items {
		createdAt := parseMillis(item.CreateTime)
		if !createdAt.IsZero() && !createdAt.Before(event.Message.CreatedAt) {
			boundaryOmitted++
			continue
		}
		if item.ID == "" || counts[item.ID] != 1 || item.ID == event.Message.ID || item.ID == parentID || item.Deleted || item.ChatID != trigger.ChatID || createdAt.IsZero() {
			continue
		}
		if trigger.ThreadID != "" {
			if item.ThreadID != trigger.ThreadID {
				continue
			}
		} else if item.ThreadID != "" {
			continue
		}
		if item.Sender.Type == "app" && item.Type == "interactive" {
			continue
		}
		text, resources := nativeWindowContent(item)
		accepted = append(accepted, uvim.ContextMessage{
			MessageID:    item.ID,
			SenderID:     item.Sender.ID,
			SenderIDType: item.Sender.Kind,
			SenderType:   item.Sender.Type,
			CreatedAt:    createdAt,
			Text:         text,
			Resources:    resources,
		})
	}
	sort.SliceStable(accepted, func(i, j int) bool {
		return accepted[i].CreatedAt.Before(accepted[j].CreatedAt)
	})
	return accepted, boundaryOmitted
}

func nativeWindowContent(item quotedMessageItem) (string, []uvim.ContextResource) {
	var text string
	var resources []uvim.ResourceRef
	if item.Type == "post" {
		post := renderQuotedPost(item.Body.Content)
		text, resources = post.text, post.resources
		if text == "" {
			text = "[Post content unavailable]"
		}
	} else {
		text = messageBody(item.Type, item.Body.Content)
		resources = messageResources(item.Type, item.Body.Content)
		if text == "" {
			text = "[Message content unavailable]"
		}
	}
	descriptors := make([]uvim.ContextResource, 0, len(resources))
	for _, resource := range resources {
		kind := strings.TrimSpace(resource.Kind)
		if kind == "" {
			continue
		}
		descriptors = append(descriptors, uvim.ContextResource{Kind: kind, Name: resource.Name})
	}
	return text, descriptors
}

func nativeWindowUnavailable(result uvim.ConversationWindowResult, code int, err error) uvim.ConversationWindowResult {
	result.Status = "unavailable"
	result.Reason = "native_unavailable"
	if errors.Is(err, context.DeadlineExceeded) {
		result.Reason = "window_timeout"
		return result
	}
	if failure, ok := uvim.ProviderSendFailure(err); ok {
		switch failure.Category {
		case uvim.SendFailurePermission:
			result.Status, result.Reason = "permission_denied", "native_permission_denied"
			return result
		case uvim.SendFailureRateLimited:
			result.Status, result.Reason = "rate_limited", "native_rate_limited"
			return result
		}
	}
	switch code {
	case 99991002, 230001, 99991672:
		result.Status, result.Reason = "permission_denied", "native_permission_denied"
	case 230020:
		result.Status, result.Reason = "rate_limited", "native_rate_limited"
	}
	return result
}

var _ uvim.ConversationWindowReader = (*Provider)(nil)
