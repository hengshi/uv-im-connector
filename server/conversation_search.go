package server

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

const (
	conversationSearchMaxPages       = 5
	conversationSearchMaxLimit       = 10
	conversationSearchCursorTTL      = 15 * time.Minute
	conversationSearchCursorCapacity = 256
	conversationSearchMetadataMax    = 64 << 10
)

type conversationSearchFilter struct {
	From      *time.Time
	To        time.Time
	SenderIDs []string
	Keywords  []string
}

type conversationSearchCursor struct {
	IssuedAt    time.Time
	Provider    string
	Connector   string
	EventID     string
	MessageID   string
	EventDigest string
	Audience    string
	Filter      conversationSearchFilter
	Limit       int
	Scope       uvim.ConversationSearchScope
	PageToken   string
	TokenHashes []string
}

func (h *Hub) handleConversationSearch(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if !h.conversationWindowAuthOK(req) {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var input uvim.ConversationSearchRequest
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_conversation_search_request")
		return
	}
	trimConversationSearchRequest(&input)
	if !validConversationSearchRequestIdentity(input) {
		writeError(w, http.StatusBadRequest, "invalid_conversation_search_request")
		return
	}

	provider := h.registry.Get(input.Provider, input.Connector)
	if provider == nil || provider.Capabilities().ConversationSearchVersion != uvim.ConversationSearchVersionV1 {
		writeJSON(w, http.StatusOK, unavailableConversationSearch("unsupported"))
		return
	}
	reader, ok := provider.(uvim.ConversationSearchReader)
	if !ok {
		writeJSON(w, http.StatusOK, unavailableConversationSearch("unsupported"))
		return
	}

	ctx, cancel := context.WithTimeout(req.Context(), uvim.ConversationSearchTimeout)
	defer cancel()
	cursor, cursorReason := h.conversationSearchCursor(input)
	if cursorReason != "" {
		writeJSON(w, http.StatusOK, unavailableConversationSearch(cursorReason))
		return
	}
	trigger, digest, reason := h.conversationSearchTrigger(ctx, input)
	if reason != "" {
		writeJSON(w, http.StatusOK, unavailableConversationSearch(reason))
		return
	}

	var filter conversationSearchFilter
	limit := input.Limit
	pageToken := ""
	var expectedScope *uvim.ConversationSearchScope
	var tokenHashes []string
	if cursor != nil {
		if !sameConversationSearchCursorIdentity(*cursor, input) {
			writeJSON(w, http.StatusOK, unavailableConversationSearch("invalid_cursor"))
			return
		}
		if input.Filter != nil {
			provided, err := canonicalConversationSearchFilter(input.Filter, trigger.Message.CreatedAt)
			if err != nil || !sameConversationSearchFilter(provided, cursor.Filter) {
				writeJSON(w, http.StatusOK, unavailableConversationSearch("invalid_cursor"))
				return
			}
		}
		filter = cloneConversationSearchFilter(cursor.Filter)
		if limit == 0 {
			limit = cursor.Limit
		}
		if limit != cursor.Limit {
			writeJSON(w, http.StatusOK, unavailableConversationSearch("invalid_cursor"))
			return
		}
		pageToken = cursor.PageToken
		scope := cursor.Scope
		expectedScope = &scope
		tokenHashes = append([]string(nil), cursor.TokenHashes...)
	} else {
		if limit == 0 {
			limit = conversationSearchMaxLimit
		}
		if limit < 1 || limit > conversationSearchMaxLimit {
			writeError(w, http.StatusBadRequest, "invalid_conversation_search_request")
			return
		}
		var err error
		filter, err = canonicalConversationSearchFilter(input.Filter, trigger.Message.CreatedAt)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_conversation_search_request")
			return
		}
	}
	if limit < 1 || limit > conversationSearchMaxLimit {
		writeJSON(w, http.StatusOK, unavailableConversationSearch("invalid_cursor"))
		return
	}

	result := newConversationSearchResult(input, trigger, digest, filter)
	for pageCount := 0; pageCount < conversationSearchMaxPages; pageCount++ {
		page := reader.ReadConversationSearchPage(ctx, uvim.ConversationSearchPageRequest{
			Trigger: trigger, Limit: limit, PageToken: pageToken, ExpectedScope: expectedScope,
		})
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			writeJSON(w, http.StatusOK, unavailableConversationSearch("search_timeout"))
			return
		}
		if page.Status != "available" || !validConversationSearchPage(page, trigger, expectedScope, limit) {
			writeJSON(w, http.StatusOK, unavailableConversationSearchPage(page))
			return
		}
		if expectedScope == nil {
			scope := page.Scope
			expectedScope = &scope
			result.Provider, result.Connector = scope.Provider, scope.Connector
			result.ConversationID, result.ThreadID = scope.ConversationID, scope.ThreadID
		}
		result.PageCount++
		result.CandidateCount += page.CandidateCount
		result.BoundaryOmitted += page.BoundaryOmitted
		for _, candidate := range page.Candidates {
			observeConversationSearchCandidate(&result, candidate.Message.CreatedAt)
		}
		matches := filterConversationSearchCandidates(page.Candidates, filter)
		result.MatchCount += len(matches)
		if len(matches) > 0 {
			for _, candidate := range matches {
				result.Items = append(result.Items, candidate.Message)
			}
			result.ReturnedCount = len(result.Items)
			if !page.HasMore {
				result.Status = "available"
				writeJSON(w, http.StatusOK, h.finalizeConversationSearch(ctx, input, trigger, digest, result))
				return
			}
			status, nextCursor := h.newConversationSearchCursor(input, filter, limit, *expectedScope, page.NextPageToken, tokenHashes)
			if status == "native_unavailable" {
				writeJSON(w, http.StatusOK, unavailableConversationSearch(status))
				return
			}
			result.Status = "available"
			result.HasMore = true
			if status == "cursor_capacity_limited" {
				result.Status, result.Reason = "scan_limited", status
			} else {
				result.NextCursor = nextCursor
			}
			writeJSON(w, http.StatusOK, h.finalizeConversationSearch(ctx, input, trigger, digest, result))
			return
		}
		if !page.HasMore {
			result.Status = "none"
			writeJSON(w, http.StatusOK, h.finalizeConversationSearch(ctx, input, trigger, digest, result))
			return
		}
		if pageCount+1 == conversationSearchMaxPages {
			status, nextCursor := h.newConversationSearchCursor(input, filter, limit, *expectedScope, page.NextPageToken, tokenHashes)
			if status == "native_unavailable" {
				writeJSON(w, http.StatusOK, unavailableConversationSearch(status))
				return
			}
			result.Status, result.Reason, result.HasMore = "scan_limited", "search_scan_limited", true
			if status == "cursor_capacity_limited" {
				result.Reason = status
			} else {
				result.NextCursor = nextCursor
			}
			writeJSON(w, http.StatusOK, h.finalizeConversationSearch(ctx, input, trigger, digest, result))
			return
		}
		pageToken = page.NextPageToken
		expectedScope = &page.Scope
		if tokenHashSeen(tokenHashes, pageToken) {
			writeJSON(w, http.StatusOK, unavailableConversationSearch("native_unavailable"))
			return
		}
		if len(tokenHashes)+1 >= conversationSearchCursorCapacity {
			result.Status, result.Reason, result.HasMore = "scan_limited", "cursor_capacity_limited", true
			writeJSON(w, http.StatusOK, h.finalizeConversationSearch(ctx, input, trigger, digest, result))
			return
		}
		tokenHashes = append(tokenHashes, conversationSearchTokenHash(pageToken))
	}
	writeJSON(w, http.StatusOK, unavailableConversationSearch("native_unavailable"))
}

func trimConversationSearchRequest(input *uvim.ConversationSearchRequest) {
	input.Provider = strings.TrimSpace(input.Provider)
	input.Connector = strings.TrimSpace(input.Connector)
	input.TriggerEventID = strings.TrimSpace(input.TriggerEventID)
	input.TriggerMessageID = strings.TrimSpace(input.TriggerMessageID)
	input.ExpectedEventDigest = strings.TrimSpace(input.ExpectedEventDigest)
	input.AudienceDigest = strings.TrimSpace(input.AudienceDigest)
	input.Cursor = strings.TrimSpace(input.Cursor)
}

func validConversationSearchRequestIdentity(input uvim.ConversationSearchRequest) bool {
	return input.Provider != "" && input.Connector != "" && input.TriggerEventID != "" && input.TriggerMessageID != "" && input.ExpectedEventDigest != "" && input.AudienceDigest != ""
}

func unavailableConversationSearch(reason string) uvim.ConversationSearchResult {
	return uvim.ConversationSearchResult{Version: uvim.ConversationSearchVersionV1, Status: "unavailable", Reason: reason}
}

func unavailableConversationSearchPage(page uvim.ConversationSearchPage) uvim.ConversationSearchResult {
	if page.Status == "permission_denied" || page.Status == "rate_limited" {
		return uvim.ConversationSearchResult{Version: uvim.ConversationSearchVersionV1, Status: page.Status, Reason: page.Reason}
	}
	return unavailableConversationSearch(searchPageReason(page))
}

func (h *Hub) conversationSearchTrigger(ctx context.Context, input uvim.ConversationSearchRequest) (uvim.Event, string, string) {
	trigger, err := h.eventLog.ConversationWindowTrigger(ctx, uvim.ConversationWindowRequest{
		Provider: input.Provider, Connector: input.Connector, TriggerEventID: input.TriggerEventID, TriggerMessageID: input.TriggerMessageID,
	})
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return uvim.Event{}, "", "search_timeout"
	}
	if err != nil || trigger.Message.CreatedAt.IsZero() {
		return uvim.Event{}, "", "trigger_unavailable"
	}
	digest, err := uvim.ConversationEventDigest(trigger)
	if err != nil || digest != input.ExpectedEventDigest {
		return uvim.Event{}, "", "trigger_unavailable"
	}
	return trigger, digest, ""
}

func newConversationSearchResult(input uvim.ConversationSearchRequest, trigger uvim.Event, digest string, filter conversationSearchFilter) uvim.ConversationSearchResult {
	to := filter.To
	result := uvim.ConversationSearchResult{
		Version: uvim.ConversationSearchVersionV1, Source: "native", Status: "unavailable", EventDigest: digest,
		Provider: input.Provider, Connector: input.Connector, ConversationID: trigger.Channel.ID,
		ThreadID: trigger.Referrer.ThreadID, TriggerMessageID: trigger.Message.ID, AnchorTime: trigger.Message.CreatedAt.UTC(),
		EffectiveFrom: cloneTime(filter.From), EffectiveTo: &to,
	}
	return result
}

func (h *Hub) finalizeConversationSearch(ctx context.Context, input uvim.ConversationSearchRequest, trigger uvim.Event, digest string, result uvim.ConversationSearchResult) uvim.ConversationSearchResult {
	_, latest, reason := h.conversationSearchTrigger(ctx, input)
	if reason != "" || latest != digest || !sameConversationSearchResultTrigger(result, trigger) {
		return unavailableConversationSearch("trigger_unavailable")
	}
	return result
}

func sameConversationSearchResultTrigger(result uvim.ConversationSearchResult, trigger uvim.Event) bool {
	return result.EventDigest != "" && result.TriggerMessageID == trigger.Message.ID && result.AnchorTime.Equal(trigger.Message.CreatedAt)
}

func searchPageReason(page uvim.ConversationSearchPage) string {
	switch page.Status {
	case "permission_denied", "rate_limited":
		return page.Reason
	case "unavailable":
		if page.Reason != "" {
			return page.Reason
		}
	}
	return "native_unavailable"
}

func validConversationSearchPage(page uvim.ConversationSearchPage, trigger uvim.Event, expected *uvim.ConversationSearchScope, limit int) bool {
	if page.CandidateCount < len(page.Candidates) || page.CandidateCount > limit || page.BoundaryOmitted < 0 || page.BoundaryOmitted > page.CandidateCount || (page.HasMore && strings.TrimSpace(page.NextPageToken) == "") || (!page.HasMore && strings.TrimSpace(page.NextPageToken) != "") || page.Scope.AnchorTime.IsZero() || page.Scope.Provider != trigger.Provider || page.Scope.Connector != trigger.Connector || page.Scope.ConversationID != trigger.Channel.ID || page.Scope.TriggerMessageID != trigger.Message.ID || !page.Scope.AnchorTime.Equal(trigger.Message.CreatedAt) || (trigger.Referrer.ThreadID != "" && page.Scope.ThreadID != trigger.Referrer.ThreadID) {
		return false
	}
	if expected != nil && !sameConversationSearchScope(page.Scope, *expected) {
		return false
	}
	seen := make(map[string]struct{}, len(page.Candidates))
	for _, candidate := range page.Candidates {
		item := candidate.Message
		if strings.TrimSpace(item.MessageID) == "" || item.CreatedAt.IsZero() || !item.CreatedAt.Before(trigger.Message.CreatedAt) {
			return false
		}
		if _, duplicate := seen[item.MessageID]; duplicate {
			return false
		}
		seen[item.MessageID] = struct{}{}
	}
	return true
}

func sameConversationSearchScope(left, right uvim.ConversationSearchScope) bool {
	return left.Provider == right.Provider && left.Connector == right.Connector && left.ConversationID == right.ConversationID && left.ThreadID == right.ThreadID && left.TriggerMessageID == right.TriggerMessageID && left.AnchorTime.Equal(right.AnchorTime)
}

func canonicalConversationSearchFilter(input *uvim.ConversationSearchFilter, anchor time.Time) (conversationSearchFilter, error) {
	anchor = anchor.UTC()
	filter := conversationSearchFilter{To: anchor}
	if input == nil {
		return filter, nil
	}
	if input.From != nil {
		from := input.From.UTC()
		filter.From = &from
	}
	if input.To != nil && input.To.Before(filter.To) {
		filter.To = input.To.UTC()
	}
	if filter.From != nil && !filter.From.Before(filter.To) {
		return conversationSearchFilter{}, errors.New("empty effective range")
	}
	filter.SenderIDs = canonicalSearchStrings(input.SenderIDs, false)
	keywords, err := canonicalSearchKeywords(input.Keywords)
	if err != nil {
		return conversationSearchFilter{}, err
	}
	filter.Keywords = keywords
	return filter, nil
}

func canonicalSearchStrings(values []string, lower bool) []string {
	unique := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if lower {
			value = strings.ToLower(value)
		}
		if value != "" {
			unique[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(unique))
	for value := range unique {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func canonicalSearchKeywords(values []string) ([]string, error) {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return nil, errors.New("empty keyword")
		}
	}
	return canonicalSearchStrings(values, true), nil
}

func cloneConversationSearchFilter(input conversationSearchFilter) conversationSearchFilter {
	return conversationSearchFilter{From: cloneTime(input.From), To: input.To, SenderIDs: append([]string(nil), input.SenderIDs...), Keywords: append([]string(nil), input.Keywords...)}
}

func sameConversationSearchFilter(left, right conversationSearchFilter) bool {
	return sameOptionalTime(left.From, right.From) && left.To.Equal(right.To) && slicesEqual(left.SenderIDs, right.SenderIDs) && slicesEqual(left.Keywords, right.Keywords)
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}

func sameOptionalTime(left, right *time.Time) bool {
	return left == nil && right == nil || left != nil && right != nil && left.Equal(*right)
}

func slicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func filterConversationSearchCandidates(candidates []uvim.ConversationSearchCandidate, filter conversationSearchFilter) []uvim.ConversationSearchCandidate {
	matches := make([]uvim.ConversationSearchCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		item := candidate.Message
		if filter.From != nil && item.CreatedAt.Before(*filter.From) || !item.CreatedAt.Before(filter.To) {
			continue
		}
		if len(filter.SenderIDs) > 0 && !containsSearchString(filter.SenderIDs, item.SenderID) {
			continue
		}
		if len(filter.Keywords) > 0 {
			if !candidate.Readable {
				continue
			}
			text := strings.ToLower(item.Text)
			matched := true
			for _, keyword := range filter.Keywords {
				if !strings.Contains(text, keyword) {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
		}
		matches = append(matches, candidate)
	}
	return matches
}

func containsSearchString(values []string, wanted string) bool {
	index := sort.SearchStrings(values, wanted)
	return index < len(values) && values[index] == wanted
}

func observeConversationSearchCandidate(result *uvim.ConversationSearchResult, observed time.Time) {
	observed = observed.UTC()
	if result.ScannedOldest == nil || observed.Before(*result.ScannedOldest) {
		result.ScannedOldest = &observed
	}
	if result.ScannedNewest == nil || observed.After(*result.ScannedNewest) {
		result.ScannedNewest = &observed
	}
}

func (h *Hub) conversationSearchCursor(input uvim.ConversationSearchRequest) (*conversationSearchCursor, string) {
	if input.Cursor == "" {
		return nil, ""
	}
	h.searchMu.Lock()
	defer h.searchMu.Unlock()
	h.purgeConversationSearchCursorsLocked(time.Now().UTC())
	cursor, ok := h.searchCursors[input.Cursor]
	if !ok {
		return nil, "cursor_expired"
	}
	copy := cloneConversationSearchCursor(cursor)
	return &copy, ""
}

func (h *Hub) newConversationSearchCursor(input uvim.ConversationSearchRequest, filter conversationSearchFilter, limit int, scope uvim.ConversationSearchScope, pageToken string, inheritedHashes []string) (string, string) {
	if strings.TrimSpace(pageToken) == "" || tokenHashSeen(inheritedHashes, pageToken) {
		return "native_unavailable", ""
	}
	tokenHashes := append(append([]string(nil), inheritedHashes...), conversationSearchTokenHash(pageToken))
	// A returned continuation would be the next page beyond this response. Do
	// not issue a cursor that reaches the declared chain bound while it still
	// advertises more native material.
	if len(tokenHashes) >= conversationSearchCursorCapacity {
		return "cursor_capacity_limited", ""
	}
	cursor := conversationSearchCursor{
		IssuedAt: time.Now().UTC(), Provider: input.Provider, Connector: input.Connector, EventID: input.TriggerEventID, MessageID: input.TriggerMessageID,
		EventDigest: input.ExpectedEventDigest, Audience: input.AudienceDigest, Filter: cloneConversationSearchFilter(filter), Limit: limit,
		Scope: scope, PageToken: pageToken, TokenHashes: tokenHashes,
	}
	if conversationSearchCursorMetadataBytes(cursor) > conversationSearchMetadataMax {
		return "cursor_capacity_limited", ""
	}
	var random [32]byte
	if _, err := io.ReadFull(crand.Reader, random[:]); err != nil {
		return "native_unavailable", ""
	}
	id := hex.EncodeToString(random[:])
	h.searchMu.Lock()
	defer h.searchMu.Unlock()
	h.purgeConversationSearchCursorsLocked(cursor.IssuedAt)
	for h.conversationSearchCursorCountLocked(cursor.Connector) >= conversationSearchCursorCapacity {
		h.evictOldestConversationSearchCursorLocked(cursor.Connector)
	}
	h.searchCursors[id] = cursor
	return "", id
}

func conversationSearchCursorMetadataBytes(cursor conversationSearchCursor) int {
	// Cursor fields are private (and therefore intentionally unexported), so
	// json.Marshal(cursor) would misleadingly return "{}". Count retained data
	// directly; the fixed allowance covers struct and map bookkeeping.
	size := 256 + len(cursor.Provider) + len(cursor.Connector) + len(cursor.EventID) + len(cursor.MessageID) + len(cursor.EventDigest) + len(cursor.Audience) + len(cursor.PageToken)
	size += len(cursor.Scope.Provider) + len(cursor.Scope.Connector) + len(cursor.Scope.ConversationID) + len(cursor.Scope.ThreadID) + len(cursor.Scope.TriggerMessageID)
	for _, value := range cursor.Filter.SenderIDs {
		size += len(value)
	}
	for _, value := range cursor.Filter.Keywords {
		size += len(value)
	}
	for _, hash := range cursor.TokenHashes {
		size += len(hash)
	}
	return size
}

func cloneConversationSearchCursor(input conversationSearchCursor) conversationSearchCursor {
	input.Filter = cloneConversationSearchFilter(input.Filter)
	input.TokenHashes = append([]string(nil), input.TokenHashes...)
	return input
}

func (h *Hub) purgeConversationSearchCursorsLocked(now time.Time) {
	for id, cursor := range h.searchCursors {
		if now.Sub(cursor.IssuedAt) >= conversationSearchCursorTTL {
			delete(h.searchCursors, id)
		}
	}
}

func (h *Hub) conversationSearchCursorCountLocked(connector string) int {
	count := 0
	for _, cursor := range h.searchCursors {
		if cursor.Connector == connector {
			count++
		}
	}
	return count
}

func (h *Hub) evictOldestConversationSearchCursorLocked(connector string) {
	var oldestID string
	var oldest time.Time
	for id, cursor := range h.searchCursors {
		if cursor.Connector != connector {
			continue
		}
		if oldestID == "" || cursor.IssuedAt.Before(oldest) {
			oldestID, oldest = id, cursor.IssuedAt
		}
	}
	if oldestID != "" {
		delete(h.searchCursors, oldestID)
	}
}

func sameConversationSearchCursorIdentity(cursor conversationSearchCursor, input uvim.ConversationSearchRequest) bool {
	return cursor.Provider == input.Provider && cursor.Connector == input.Connector && cursor.EventID == input.TriggerEventID && cursor.MessageID == input.TriggerMessageID && cursor.EventDigest == input.ExpectedEventDigest && cursor.Audience == input.AudienceDigest
}

func conversationSearchTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func tokenHashSeen(hashes []string, token string) bool {
	wanted := conversationSearchTokenHash(token)
	for _, hash := range hashes {
		if hash == wanted {
			return true
		}
	}
	return false
}
