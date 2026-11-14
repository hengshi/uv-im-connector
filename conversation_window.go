package uvim

import (
	"context"
	"time"
)

const (
	ConversationWindowVersionV1 = "v1"
	ConversationWindowTimeout   = 5 * time.Second
)

// ConversationWindowRequest identifies a previously received native message.
// The connector resolves its conversation and time boundary from the retained
// event instead of accepting caller-selected history scope.
type ConversationWindowRequest struct {
	Provider         string `json:"provider"`
	Connector        string `json:"connector"`
	TriggerEventID   string `json:"trigger_event_id"`
	TriggerMessageID string `json:"trigger_message_id"`
}

// ConversationWindowResult is a bounded description of native or retained
// received conversation material. Its items are reference material only.
type ConversationWindowResult struct {
	Version string `json:"version"`
	// Source is native for an adapter history read or received for the Hub's
	// bounded EventLog projection.
	Source string `json:"source,omitempty"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
	// Received-only durable source proof. These are not authorization grants
	// and remain empty for adapter-native reads.
	EventLogGeneration      string    `json:"event_log_generation,omitempty"`
	TriggerEntryDigest      string    `json:"trigger_entry_digest,omitempty"`
	IndexLowerBoundSequence int64     `json:"index_lower_bound_sequence,omitempty"`
	ConversationID          string    `json:"conversation_id,omitempty"`
	ThreadID                string    `json:"thread_id,omitempty"`
	TriggerMessageID        string    `json:"trigger_message_id,omitempty"`
	AnchorTime              time.Time `json:"anchor_time,omitempty"`
	// AnchorTimeSource is message_created_at or event_time when the trigger
	// needed the immutable Event.Time fallback.
	AnchorTimeSource string           `json:"anchor_time_source,omitempty"`
	CandidateCount   int              `json:"candidate_count,omitempty"`
	BoundaryOmitted  int              `json:"boundary_omitted,omitempty"`
	HasMore          bool             `json:"has_more,omitempty"`
	Items            []ContextMessage `json:"items,omitempty"`
}

type ContextMessage struct {
	MessageID string `json:"message_id"`
	// Sequence is EventLog ingress order for received context only. Native
	// adapters leave it zero; callers must never use a native message ID as a
	// time/order surrogate.
	Sequence     int64             `json:"sequence,omitempty"`
	SenderID     string            `json:"sender_id,omitempty"`
	SenderIDType string            `json:"sender_id_type,omitempty"`
	SenderType   string            `json:"sender_type,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	Text         string            `json:"text,omitempty"`
	Resources    []ContextResource `json:"resources,omitempty"`
}

// ContextResource intentionally exposes only a readable descriptor. It is not
// a downloadable ResourceRef and cannot carry provider handles or URLs. Resource
// is an in-process adapter-to-Hub value used only while the Hub journals a
// materialize handle; it is never serialized.
type ContextResource struct {
	MaterializeHandle string      `json:"materialize_handle,omitempty"`
	Source            string      `json:"source,omitempty"`
	SourceEpoch       string      `json:"source_epoch,omitempty"`
	SourceMessageID   string      `json:"source_message_id,omitempty"`
	SourceGeneration  string      `json:"source_generation,omitempty"`
	SourceEntryDigest string      `json:"source_entry_digest,omitempty"`
	SourceSequence    int64       `json:"source_sequence,omitempty"`
	SourceOrdinal     int         `json:"source_ordinal,omitempty"`
	Kind              string      `json:"kind"`
	Name              string      `json:"name,omitempty"`
	MIME              string      `json:"mime,omitempty"`
	SizeBytes         int64       `json:"size_bytes,omitempty"`
	SHA256            string      `json:"sha256,omitempty"`
	Availability      string      `json:"availability,omitempty"`
	Resource          ResourceRef `json:"-"`
}

// ConversationWindowReader is optional because only adapters with a real,
// authenticated native conversation-history API can provide this material.
type ConversationWindowReader interface {
	ReadConversationWindow(context.Context, Event) ConversationWindowResult
}
