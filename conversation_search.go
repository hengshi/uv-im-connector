package uvim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

const (
	ConversationSearchVersionV1 = "v1"
	ConversationContextQueryVersionV1 = "v1"
	ConversationSearchTimeout   = 5 * time.Second
)

// ConversationEventDigest identifies the persisted, sanitized trigger that a
// caller is permitted to use as a native-history anchor. It is deliberately
// neither an event-log receipt nor a provider payload digest.
func ConversationEventDigest(event Event) (string, error) {
	raw, err := json.Marshal(event.Sanitized())
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// ConversationSearchFilter contains only local narrowing predicates. A nil
// filter on a continuation means "reuse the cursor's filter"; it is distinct
// from an explicitly supplied empty object at the JSON boundary.
type ConversationSearchFilter struct {
	From      *time.Time `json:"from,omitempty"`
	To        *time.Time `json:"to,omitempty"`
	SenderIDs []string   `json:"sender_ids,omitempty"`
	Keywords  []string   `json:"keywords,omitempty"`
}

// ConversationSearchRequest identifies a retained native trigger and carries
// only caller-produced proof/binding values. It intentionally has no native
// conversation, topic, provider-page-token, or path override.
type ConversationSearchRequest struct {
	Provider            string                    `json:"provider"`
	Connector           string                    `json:"connector"`
	TriggerEventID      string                    `json:"trigger_event_id"`
	TriggerMessageID    string                    `json:"trigger_message_id"`
	ExpectedEventDigest string                    `json:"expected_event_digest"`
	AudienceDigest      string                    `json:"audience_digest"`
	// Source defaults to native for compatibility. received is an explicit
	// EventLog-only read and never asks the provider for history.
	Source              string                    `json:"source,omitempty"`
	Filter              *ConversationSearchFilter `json:"filter,omitempty"`
	Limit               int                       `json:"limit,omitempty"`
	Cursor              string                    `json:"cursor,omitempty"`
}

// ConversationSearchResult contains reference material only. NextCursor is a
// Hub cursor, never a provider page token.
type ConversationSearchResult struct {
	Version          string           `json:"version"`
	Source           string           `json:"source,omitempty"`
	Status           string           `json:"status"`
	Reason           string           `json:"reason,omitempty"`
	EventDigest      string           `json:"event_digest,omitempty"`
	// EventLogGeneration and TriggerEntryDigest are populated only when a
	// result contains received EventLog material. They are durable source proof,
	// not an authorization grant or a provider history cursor.
	EventLogGeneration      string           `json:"event_log_generation,omitempty"`
	TriggerEntryDigest      string           `json:"trigger_entry_digest,omitempty"`
	IndexLowerBoundSequence int64            `json:"index_lower_bound_sequence,omitempty"`
	Provider         string           `json:"provider,omitempty"`
	Connector        string           `json:"connector,omitempty"`
	ConversationID   string           `json:"conversation_id,omitempty"`
	ThreadID         string           `json:"thread_id,omitempty"`
	TriggerMessageID string           `json:"trigger_message_id,omitempty"`
	AnchorTime       time.Time        `json:"anchor_time,omitempty"`
	// AnchorTimeSource is message_created_at or event_time when an immutable
	// persisted source timestamp was available for the trigger.
	AnchorTimeSource string           `json:"anchor_time_source,omitempty"`
	EffectiveFrom    *time.Time       `json:"effective_from,omitempty"`
	EffectiveTo      *time.Time       `json:"effective_to,omitempty"`
	ScannedOldest    *time.Time       `json:"scanned_oldest,omitempty"`
	ScannedNewest    *time.Time       `json:"scanned_newest,omitempty"`
	PageCount        int              `json:"page_count,omitempty"`
	CandidateCount   int              `json:"candidate_count,omitempty"`
	MatchCount       int              `json:"match_count,omitempty"`
	ReturnedCount    int              `json:"returned_count,omitempty"`
	BoundaryOmitted  int              `json:"boundary_omitted,omitempty"`
	Items            []ContextMessage `json:"items,omitempty"`
	HasMore          bool             `json:"has_more,omitempty"`
	NextCursor       string           `json:"next_cursor,omitempty"`
}

// ConversationSearchScope is verified by a native reader before pagination.
// It deliberately excludes provider credentials and continuation tokens.
type ConversationSearchScope struct {
	Provider         string    `json:"provider"`
	Connector        string    `json:"connector"`
	ConversationID   string    `json:"conversation_id"`
	ThreadID         string    `json:"thread_id,omitempty"`
	TriggerMessageID string    `json:"trigger_message_id"`
	AnchorTime       time.Time `json:"anchor_time"`
}

// ConversationSearchCandidate is internal reader-to-Hub material. Readable
// prevents placeholder text from satisfying a keyword predicate.
type ConversationSearchCandidate struct {
	Message  ContextMessage
	Readable bool
}

// ConversationSearchPageRequest is intentionally not an HTTP payload. The
// Hub supplies its retained trigger and keeps provider continuations private.
type ConversationSearchPageRequest struct {
	Trigger       Event
	Limit         int
	PageToken     string
	ExpectedScope *ConversationSearchScope
}

// ConversationSearchPage is one verified native page. NextPageToken must not
// leave the Hub process or be copied into a public result.
type ConversationSearchPage struct {
	Status          string
	Reason          string
	Scope           ConversationSearchScope
	Candidates      []ConversationSearchCandidate
	CandidateCount  int
	BoundaryOmitted int
	HasMore         bool
	NextPageToken   string
}

// ConversationSearchReader is optional because only a provider with a native,
// authenticated history API may expose this bounded read.
type ConversationSearchReader interface {
	ReadConversationSearchPage(context.Context, ConversationSearchPageRequest) ConversationSearchPage
}
