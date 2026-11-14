package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
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
	HasTo     bool
	SenderIDs []string
	Keywords  []string
}

type conversationSearchCursor struct {
	IssuedAt    time.Time                    `json:"issued_at"`
	Provider    string                       `json:"provider"`
	Connector   string                       `json:"connector"`
	Source      string                       `json:"source,omitempty"`
	EventID     string                       `json:"trigger_event_id"`
	MessageID   string                       `json:"trigger_message_id"`
	EventDigest string                       `json:"event_digest"`
	Audience    string                       `json:"audience_digest"`
	Filter      conversationSearchFilter     `json:"filter"`
	Limit       int                          `json:"limit"`
	Scope       uvim.ConversationSearchScope `json:"scope"`
	PageToken   string                       `json:"-"`
	TokenHashes []string                     `json:"token_hashes,omitempty"`
	// SourceGeneration, SourceFileIdentity, and IndexLowerBound bind every
	// continuation to the exact retained EventLog projection.  The file proof
	// prevents a replacement that preserves the same trigger digest from
	// resuming an old native provider token.
	SourceGeneration       string    `json:"source_generation,omitempty"`
	SourceFileIdentity     string    `json:"source_file_identity,omitempty"`
	IndexLowerBound        int64     `json:"index_lower_bound_sequence,omitempty"`
	ReceivedBeforeTime     time.Time `json:"received_before_time,omitempty"`
	ReceivedBeforeSequence int64     `json:"received_before_sequence,omitempty"`
}

// conversationSearchCursorRecord is stored only in the Hub-private material
// journal. PageTokenCipher is AES-GCM ciphertext; a provider continuation is
// never serialized in a response, EventLog entry, or plaintext journal field.
// The source generation/lower bound make a received cursor invalid as soon as
// its retained projection rolls over.
type conversationSearchCursorRecord struct {
	conversationSearchCursor
	Version         int       `json:"version"`
	ID              string    `json:"id"`
	ExpiresAt       time.Time `json:"expires_at"`
	PageTokenCipher string    `json:"page_token_cipher,omitempty"`
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
	if !validConversationSearchRequestIdentity(input) || !validConversationSearchSource(input.Source) {
		writeError(w, http.StatusBadRequest, "invalid_conversation_search_request")
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
	if input.Source == "received" {
		h.handleReceivedConversationSearch(w, ctx, input, cursor, trigger, digest)
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
	if cursor != nil {
		generation, identity, lowerBound, proofOK := h.conversationSearchCursorProof(ctx)
		if !proofOK || cursor.SourceGeneration != generation || cursor.SourceFileIdentity != identity || cursor.IndexLowerBound != lowerBound {
			// Do this before ReadConversationSearchPage: a persisted native token
			// may not cross an EventLog replacement/capacity boundary.
			writeJSON(w, http.StatusOK, unavailableConversationSearch("cursor_expired"))
			return
		}
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
			provided, err := canonicalConversationSearchFilter(input.Filter, conversationSearchAnchor(trigger))
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
		filter, err = canonicalConversationSearchFilter(input.Filter, conversationSearchAnchor(trigger))
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
	input.Source = strings.TrimSpace(input.Source)
	input.Cursor = strings.TrimSpace(input.Cursor)
}

func validConversationSearchRequestIdentity(input uvim.ConversationSearchRequest) bool {
	return input.Provider != "" && input.Connector != "" && input.TriggerEventID != "" && input.TriggerMessageID != "" && input.ExpectedEventDigest != "" && input.AudienceDigest != ""
}

func validConversationSearchSource(source string) bool {
	return source == "" || source == "native" || source == "received"
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
	if err != nil {
		return uvim.Event{}, "", "trigger_unavailable"
	}
	if _, ok := uvim.EventSourceTime(trigger); !ok {
		return uvim.Event{}, "", "trigger_unavailable"
	}
	digest, err := uvim.ConversationEventDigest(trigger)
	if err != nil || digest != input.ExpectedEventDigest {
		return uvim.Event{}, "", "trigger_unavailable"
	}
	return trigger, digest, ""
}

func newConversationSearchResult(input uvim.ConversationSearchRequest, trigger uvim.Event, digest string, filter conversationSearchFilter) uvim.ConversationSearchResult {
	var to *time.Time
	if filter.HasTo {
		value := filter.To
		to = &value
	}
	result := uvim.ConversationSearchResult{
		Version: uvim.ConversationSearchVersionV1, Source: "native", Status: "unavailable", EventDigest: digest,
		Provider: input.Provider, Connector: input.Connector, ConversationID: trigger.Channel.ID,
		ThreadID: trigger.Referrer.ThreadID, TriggerMessageID: trigger.Message.ID, AnchorTime: conversationSearchAnchor(trigger), AnchorTimeSource: uvim.EventSourceTimeSource(trigger),
		EffectiveFrom: cloneTime(filter.From), EffectiveTo: to,
	}
	return result
}

func (h *Hub) finalizeConversationSearch(ctx context.Context, input uvim.ConversationSearchRequest, trigger uvim.Event, digest string, result uvim.ConversationSearchResult) uvim.ConversationSearchResult {
	_, latest, reason := h.conversationSearchTrigger(ctx, input)
	if reason != "" || latest != digest || !sameConversationSearchResultTrigger(result, trigger) {
		return unavailableConversationSearch("trigger_unavailable")
	}
	if len(result.Items) > 0 && h.materialResources != nil {
		if err := h.materialResources.present(h.authToken, input, trigger, &result); err != nil {
			markConversationResourcesUnavailable(&result)
		}
	}
	return result
}

func markConversationResourcesUnavailable(result *uvim.ConversationSearchResult) {
	if result == nil {
		return
	}
	for itemIndex := range result.Items {
		for resourceIndex := range result.Items[itemIndex].Resources {
			clearConversationResourcePresentation(&result.Items[itemIndex].Resources[resourceIndex], "unavailable")
		}
	}
}

// handleReceivedConversationSearch is deliberately separate from native search:
// it reads only the Hub's retained EventLog projection and never asks a provider
// for history. The default search source remains native for compatibility.
func (h *Hub) handleReceivedConversationSearch(w http.ResponseWriter, ctx context.Context, input uvim.ConversationSearchRequest, cursor *conversationSearchCursor, trigger uvim.Event, digest string) {
	provider := h.registry.Get(input.Provider, input.Connector)
	if provider == nil || h.receivedContext == nil {
		writeJSON(w, http.StatusOK, unavailableConversationSearch("unsupported"))
		return
	}
	var filter conversationSearchFilter
	limit := input.Limit
	if cursor != nil {
		if cursor.Source != "received" || !sameConversationSearchCursorIdentity(*cursor, input) {
			writeJSON(w, http.StatusOK, unavailableConversationSearch("invalid_cursor"))
			return
		}
		if input.Filter != nil {
			provided, err := canonicalReceivedConversationSearchFilter(input.Filter)
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
	} else {
		if limit == 0 {
			limit = conversationSearchMaxLimit
		}
		var err error
		filter, err = canonicalReceivedConversationSearchFilter(input.Filter)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_conversation_search_request")
			return
		}
	}
	if limit < 1 || limit > conversationSearchMaxLimit {
		writeError(w, http.StatusBadRequest, "invalid_conversation_search_request")
		return
	}
	before := time.Time{}
	beforeSequence := int64(0)
	if cursor != nil {
		before, beforeSequence = cursor.ReceivedBeforeTime, cursor.ReceivedBeforeSequence
	}
	query, err := h.receivedContext.query(ctx, h.authToken, trigger, filter, limit, before, beforeSequence)
	if err != nil {
		writeJSON(w, http.StatusOK, unavailableConversationSearch("index_rebuild_limited"))
		return
	}
	if cursor != nil && (cursor.SourceGeneration != query.Generation || cursor.IndexLowerBound != query.LowerBound) {
		// A persisted continuation cannot outlive its exact received projection:
		// generation rollover or capacity eviction invalidates it rather than
		// letting its time/sequence position select a different history view.
		writeJSON(w, http.StatusOK, unavailableConversationSearch("cursor_expired"))
		return
	}
	result := newConversationSearchResult(input, trigger, digest, filter)
	result.Source = "received"
	result.EventLogGeneration, result.TriggerEntryDigest, result.IndexLowerBoundSequence = query.Generation, query.TriggerDigest, query.LowerBound
	result.PageCount, result.CandidateCount, result.MatchCount = query.PageCount, query.CandidateCount, query.MatchCount
	result.ScannedOldest, result.ScannedNewest = query.ScannedOldest, query.ScannedNewest
	result.Items, result.ReturnedCount, result.HasMore = query.Items, len(query.Items), query.HasMore
	if query.Reason == "received_unavailable" || query.Reason == "index_rebuild_limited" || query.Reason == "index_capacity_limited" {
		result.Status, result.Reason = "unavailable", query.Reason
		writeJSON(w, http.StatusOK, result)
		return
	}
	if !h.receivedMaterializeAvailable(provider) {
		markConversationResourcesUnrecoverable(&result)
	}
	if result.HasMore {
		status, next := h.newReceivedConversationSearchCursor(input, filter, limit, uvim.ConversationSearchScope{Provider: input.Provider, Connector: input.Connector, ConversationID: trigger.Channel.ID, ThreadID: trigger.Referrer.ThreadID, TriggerMessageID: trigger.Message.ID, AnchorTime: conversationSearchAnchor(trigger)}, query.Generation, query.LowerBound, query.NextBeforeTime, query.NextBeforeSequence)
		if status == "cursor_capacity_limited" {
			result.Status, result.Reason = "scan_limited", status
		} else if status != "" {
			result.Status, result.Reason = "scan_limited", "index_capacity_limited"
		} else {
			result.Status, result.NextCursor = "available", next
		}
	} else if len(result.Items) == 0 {
		result.Status = "none"
	} else {
		result.Status = "available"
	}
	if query.Reason == "search_scan_limited" {
		result.Status, result.Reason = "scan_limited", query.Reason
	}
	// A retained lower bound means this response may contain safe, strongly
	// proved rows but cannot make a complete received-history claim. Preserve
	// those rows/cursor facts while reporting the original unavailable capacity
	// state rather than disguising it as a successful partial search.
	if query.LowerBound > 1 {
		result.Status, result.Reason = "unavailable", "index_capacity_limited"
	}
	writeJSON(w, http.StatusOK, h.finalizeConversationSearch(ctx, input, trigger, digest, result))
}

func markConversationResourcesUnrecoverable(result *uvim.ConversationSearchResult) {
	if result == nil {
		return
	}
	for itemIndex := range result.Items {
		for resourceIndex := range result.Items[itemIndex].Resources {
			clearConversationResourcePresentation(&result.Items[itemIndex].Resources[resourceIndex], "unrecoverable")
		}
	}
}

// clearConversationResourcePresentation removes Hub-private materialization
// state before a descriptor crosses a result boundary. An empty availability
// preserves the source-specific state for callers that only need redaction.
func clearConversationResourcePresentation(resource *uvim.ContextResource, availability string) {
	resource.MaterializeHandle, resource.SourceEpoch = "", ""
	if availability != "" {
		resource.Availability = availability
	}
	resource.Resource = uvim.ResourceRef{}
}

func sameConversationSearchResultTrigger(result uvim.ConversationSearchResult, trigger uvim.Event) bool {
	return result.EventDigest != "" && result.TriggerMessageID == trigger.Message.ID && result.AnchorTime.Equal(conversationSearchAnchor(trigger))
}

func conversationSearchAnchor(trigger uvim.Event) time.Time {
	anchor, _ := uvim.EventSourceTime(trigger)
	return anchor
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
	anchor := conversationSearchAnchor(trigger)
	if anchor.IsZero() || page.CandidateCount < len(page.Candidates) || page.CandidateCount > limit || page.BoundaryOmitted < 0 || page.BoundaryOmitted > page.CandidateCount || (page.HasMore && strings.TrimSpace(page.NextPageToken) == "") || (!page.HasMore && strings.TrimSpace(page.NextPageToken) != "") || page.Scope.AnchorTime.IsZero() || page.Scope.Provider != trigger.Provider || page.Scope.Connector != trigger.Connector || page.Scope.ConversationID != trigger.Channel.ID || page.Scope.TriggerMessageID != trigger.Message.ID || !page.Scope.AnchorTime.Equal(anchor) || (trigger.Referrer.ThreadID != "" && page.Scope.ThreadID != trigger.Referrer.ThreadID) {
		return false
	}
	if expected != nil && !sameConversationSearchScope(page.Scope, *expected) {
		return false
	}
	seen := make(map[string]struct{}, len(page.Candidates))
	for _, candidate := range page.Candidates {
		item := candidate.Message
		if strings.TrimSpace(item.MessageID) == "" || item.CreatedAt.IsZero() || !item.CreatedAt.Before(anchor) {
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
	filter := conversationSearchFilter{To: anchor, HasTo: true}
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

// canonicalReceivedConversationSearchFilter intentionally has no implicit
// trigger-time upper bound. Received material is ordered by its durable
// EventLog sequence; provider CreatedAt is only display order unless the
// caller explicitly supplied a time predicate.
func canonicalReceivedConversationSearchFilter(input *uvim.ConversationSearchFilter) (conversationSearchFilter, error) {
	filter := conversationSearchFilter{}
	if input == nil {
		return filter, nil
	}
	if input.From != nil {
		from := input.From.UTC()
		filter.From = &from
	}
	if input.To != nil {
		filter.To, filter.HasTo = input.To.UTC(), true
	}
	if filter.From != nil && filter.HasTo && !filter.From.Before(filter.To) {
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
	return conversationSearchFilter{From: cloneTime(input.From), To: input.To, HasTo: input.HasTo, SenderIDs: append([]string(nil), input.SenderIDs...), Keywords: append([]string(nil), input.Keywords...)}
}

func sameConversationSearchFilter(left, right conversationSearchFilter) bool {
	return sameOptionalTime(left.From, right.From) && left.HasTo == right.HasTo && (!left.HasTo || left.To.Equal(right.To)) && slices.Equal(left.SenderIDs, right.SenderIDs) && slices.Equal(left.Keywords, right.Keywords)
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

func filterConversationSearchCandidates(candidates []uvim.ConversationSearchCandidate, filter conversationSearchFilter) []uvim.ConversationSearchCandidate {
	matches := make([]uvim.ConversationSearchCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		item := candidate.Message
		if filter.From != nil && item.CreatedAt.Before(*filter.From) || filter.HasTo && !item.CreatedAt.Before(filter.To) {
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
	if h == nil || h.materialResources == nil {
		return nil, "cursor_expired"
	}
	cursor, ok, err := h.materialResources.loadSearchCursor(h.authToken, input.Cursor)
	if err != nil || !ok {
		// Unknown, expired, unreadable, or journal-invalid cursor state has one
		// non-oracular result. Do not expose whether an opaque ID once existed.
		return nil, "cursor_expired"
	}
	return &cursor, ""
}

// conversationSearchCursorProof is deliberately EventLog-owned evidence, not
// a provider continuation property.  Native and received searches share the
// same retained trigger authority, so a native token cannot survive a log
// replacement merely because the replacement repeats the trigger bytes.
func (h *Hub) conversationSearchCursorProof(ctx context.Context) (string, string, int64, bool) {
	if h == nil || h.eventLog == nil {
		return "", "", 0, false
	}
	delta, err := h.eventLog.ConversationContextAdvance(ctx, uvim.ConversationContextCursor{})
	if err != nil || delta.Cursor.Version != 1 || delta.Cursor.Generation == "" || delta.Cursor.FileIdentity == "" || delta.Cursor.LowerBoundSequence <= 0 || len(delta.Cursor.Records) == 0 {
		return "", "", 0, false
	}
	return delta.Cursor.Generation, delta.Cursor.FileIdentity, delta.Cursor.LowerBoundSequence, true
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
	generation, identity, lowerBound, proofOK := h.conversationSearchCursorProof(context.Background())
	if !proofOK {
		return "native_unavailable", ""
	}
	cursor := conversationSearchCursor{
		IssuedAt: time.Now().UTC(), Provider: input.Provider, Connector: input.Connector, EventID: input.TriggerEventID, MessageID: input.TriggerMessageID,
		Source: input.Source, EventDigest: input.ExpectedEventDigest, Audience: input.AudienceDigest, Filter: cloneConversationSearchFilter(filter), Limit: limit,
		Scope: scope, PageToken: pageToken, TokenHashes: tokenHashes, SourceGeneration: generation, SourceFileIdentity: identity, IndexLowerBound: lowerBound,
	}
	if conversationSearchCursorMetadataBytes(cursor) > conversationSearchMetadataMax {
		return "cursor_capacity_limited", ""
	}
	if h == nil || h.materialResources == nil {
		return "native_unavailable", ""
	}
	id, err := h.materialResources.storeSearchCursor(h.authToken, cursor)
	if err != nil {
		return "native_unavailable", ""
	}
	return "", id
}

func (h *Hub) newReceivedConversationSearchCursor(input uvim.ConversationSearchRequest, filter conversationSearchFilter, limit int, scope uvim.ConversationSearchScope, generation string, lowerBound int64, before time.Time, sequence int64) (string, string) {
	if strings.TrimSpace(generation) == "" || lowerBound < 0 || before.IsZero() || sequence <= 0 {
		return "index_capacity_limited", ""
	}
	cursor := conversationSearchCursor{
		IssuedAt: time.Now().UTC(), Provider: input.Provider, Connector: input.Connector, Source: "received", EventID: input.TriggerEventID, MessageID: input.TriggerMessageID,
		EventDigest: input.ExpectedEventDigest, Audience: input.AudienceDigest, Filter: cloneConversationSearchFilter(filter), Limit: limit,
		Scope: scope, SourceGeneration: generation, IndexLowerBound: lowerBound, ReceivedBeforeTime: before.UTC(), ReceivedBeforeSequence: sequence,
	}
	if conversationSearchCursorMetadataBytes(cursor) > conversationSearchMetadataMax {
		return "cursor_capacity_limited", ""
	}
	if h == nil || h.materialResources == nil {
		return "index_capacity_limited", ""
	}
	id, err := h.materialResources.storeSearchCursor(h.authToken, cursor)
	if err != nil {
		return "index_capacity_limited", ""
	}
	return "", id
}

func conversationSearchCursorMetadataBytes(cursor conversationSearchCursor) int {
	// Count the pre-encryption private record directly; the fixed allowance
	// covers journal, cipher, and map bookkeeping without serializing a native
	// continuation into diagnostics just to measure it.
	size := 320 + len(cursor.Provider) + len(cursor.Connector) + len(cursor.Source) + len(cursor.EventID) + len(cursor.MessageID) + len(cursor.EventDigest) + len(cursor.Audience) + len(cursor.PageToken)
	size += len(cursor.SourceGeneration) + len(cursor.SourceFileIdentity)
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

// storeSearchCursor persists an opaque continuation in the existing private
// journal. A cursor is deliberately independent from a materialization record:
// it cannot become a provider reference, a ResourceRef, or Task/Run authority.
func (r *conversationMaterialRegistry) storeSearchCursor(authToken string, cursor conversationSearchCursor) (string, error) {
	if r == nil || strings.TrimSpace(authToken) == "" || !validConversationSearchCursor(cursor) {
		return "", errors.New("conversation search cursor unavailable")
	}
	id, err := conversationMaterialRandomID()
	if err != nil {
		return "", err
	}
	record := conversationSearchCursorRecord{
		conversationSearchCursor: cloneConversationSearchCursor(cursor),
		Version:                  1, ID: id, ExpiresAt: cursor.IssuedAt.UTC().Add(conversationSearchCursorTTL),
	}
	record.IssuedAt = cursor.IssuedAt.UTC()
	record.ReceivedBeforeTime = cursor.ReceivedBeforeTime.UTC()
	record.PageToken = ""
	if cursor.Source != "received" {
		record.PageTokenCipher, err = conversationMaterialEncryptCursorToken(authToken, cursor.PageToken)
		if err != nil {
			return "", err
		}
	}
	if !validConversationSearchCursorRecord(record, id) || conversationSearchCursorMetadataBytes(record.conversationSearchCursor)+len(record.PageTokenCipher) > conversationSearchMetadataMax {
		return "", errors.New("conversation search cursor metadata exceeds bound")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(authToken); err != nil {
		return "", err
	}
	r.purgeLocked(record.IssuedAt)
	for r.searchCursorCountLocked(record.Connector) >= conversationSearchCursorCapacity {
		r.evictOldestSearchCursorLocked(record.Connector)
	}
	r.state.Cursors[id] = record
	if err := r.persistLocked(); err != nil {
		return "", err
	}
	return id, nil
}

func (r *conversationMaterialRegistry) loadSearchCursor(authToken, id string) (conversationSearchCursor, bool, error) {
	if r == nil || strings.TrimSpace(authToken) == "" || strings.TrimSpace(id) == "" {
		return conversationSearchCursor{}, false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(authToken); err != nil {
		return conversationSearchCursor{}, false, err
	}
	changed := r.purgeLocked(time.Now().UTC())
	record, ok := r.state.Cursors[id]
	if !ok {
		if changed {
			if err := r.persistLocked(); err != nil {
				return conversationSearchCursor{}, false, err
			}
		}
		return conversationSearchCursor{}, false, nil
	}
	cursor, err := conversationSearchCursorFromRecord(authToken, record)
	if err != nil {
		// An unreadable private continuation is not recoverable from any public
		// response field. Remove it and return the same opaque expiry outcome.
		delete(r.state.Cursors, id)
		if persistErr := r.persistLocked(); persistErr != nil {
			return conversationSearchCursor{}, false, persistErr
		}
		return conversationSearchCursor{}, false, nil
	}
	if changed {
		if err := r.persistLocked(); err != nil {
			return conversationSearchCursor{}, false, err
		}
	}
	return cursor, true, nil
}

func validConversationSearchCursor(cursor conversationSearchCursor) bool {
	if !validConversationSearchCursorCore(cursor) {
		return false
	}
	if cursor.Source == "received" {
		return cursor.PageToken == "" && cursor.SourceGeneration != "" && cursor.IndexLowerBound >= 0 && !cursor.ReceivedBeforeTime.IsZero() && cursor.ReceivedBeforeSequence > 0
	}
	return cursor.PageToken != "" && cursor.SourceGeneration != "" && cursor.SourceFileIdentity != "" && cursor.IndexLowerBound > 0
}

func validConversationSearchCursorRecord(record conversationSearchCursorRecord, id string) bool {
	if record.Version != 1 || record.ID != id || record.IssuedAt.IsZero() || !record.ExpiresAt.Equal(record.IssuedAt.Add(conversationSearchCursorTTL)) {
		return false
	}
	cursor := record.conversationSearchCursor
	if record.Source == "received" {
		return record.PageTokenCipher == "" && validConversationSearchCursor(cursor)
	}
	return record.PageTokenCipher != "" && validConversationSearchCursorCore(cursor) && cursor.Source != "received" && len(cursor.TokenHashes) > 0
}

func validConversationSearchCursorCore(cursor conversationSearchCursor) bool {
	return !cursor.IssuedAt.IsZero() && cursor.Provider != "" && cursor.Connector != "" && cursor.EventID != "" && cursor.MessageID != "" && cursor.EventDigest != "" && cursor.Audience != "" && cursor.Limit >= 1 && cursor.Limit <= conversationSearchMaxLimit && validConversationSearchSource(cursor.Source) && validConversationSearchScope(cursor.Scope, cursor) && len(cursor.TokenHashes) < conversationSearchCursorCapacity
}

func validConversationSearchScope(scope uvim.ConversationSearchScope, cursor conversationSearchCursor) bool {
	return scope.Provider == cursor.Provider && scope.Connector == cursor.Connector && scope.TriggerMessageID == cursor.MessageID && scope.ConversationID != "" && !scope.AnchorTime.IsZero()
}

func conversationSearchCursorFromRecord(authToken string, record conversationSearchCursorRecord) (conversationSearchCursor, error) {
	if !validConversationSearchCursorRecord(record, record.ID) {
		return conversationSearchCursor{}, errors.New("invalid conversation search cursor")
	}
	cursor := cloneConversationSearchCursor(record.conversationSearchCursor)
	if record.Source == "received" {
		return cursor, nil
	}
	pageToken, err := conversationMaterialDecryptCursorToken(authToken, record.PageTokenCipher)
	if err != nil || strings.TrimSpace(pageToken) == "" || !tokenHashSeen(record.TokenHashes, pageToken) {
		return conversationSearchCursor{}, errors.New("unrecoverable conversation search cursor")
	}
	cursor.PageToken = pageToken
	return cursor, nil
}

func (r *conversationMaterialRegistry) searchCursorCountLocked(connector string) int {
	count := 0
	for _, cursor := range r.state.Cursors {
		if cursor.Connector == connector {
			count++
		}
	}
	return count
}

func (r *conversationMaterialRegistry) evictOldestSearchCursorLocked(connector string) {
	var oldestID string
	var oldest time.Time
	for id, cursor := range r.state.Cursors {
		if cursor.Connector != connector {
			continue
		}
		if oldestID == "" || cursor.IssuedAt.Before(oldest) || cursor.IssuedAt.Equal(oldest) && id < oldestID {
			oldestID, oldest = id, cursor.IssuedAt
		}
	}
	if oldestID != "" {
		delete(r.state.Cursors, oldestID)
	}
}

func sameConversationSearchCursorIdentity(cursor conversationSearchCursor, input uvim.ConversationSearchRequest) bool {
	return cursor.Provider == input.Provider && cursor.Connector == input.Connector && cursor.Source == input.Source && cursor.EventID == input.TriggerEventID && cursor.MessageID == input.TriggerMessageID && cursor.EventDigest == input.ExpectedEventDigest && cursor.Audience == input.AudienceDigest
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
