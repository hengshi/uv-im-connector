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

// ConversationWindowResult is a bounded, provider-normalized description of
// native conversation material. Its items are reference material only.
type ConversationWindowResult struct {
	Version string `json:"version"`
	// Source identifies the kind of material, currently the literal "native".
	Source           string           `json:"source,omitempty"`
	Status           string           `json:"status"`
	Reason           string           `json:"reason,omitempty"`
	ConversationID   string           `json:"conversation_id,omitempty"`
	ThreadID         string           `json:"thread_id,omitempty"`
	TriggerMessageID string           `json:"trigger_message_id,omitempty"`
	AnchorTime       time.Time        `json:"anchor_time,omitempty"`
	CandidateCount   int              `json:"candidate_count,omitempty"`
	BoundaryOmitted  int              `json:"boundary_omitted,omitempty"`
	HasMore          bool             `json:"has_more,omitempty"`
	Items            []ContextMessage `json:"items,omitempty"`
}

type ContextMessage struct {
	MessageID    string            `json:"message_id"`
	SenderID     string            `json:"sender_id,omitempty"`
	SenderIDType string            `json:"sender_id_type,omitempty"`
	SenderType   string            `json:"sender_type,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	Text         string            `json:"text,omitempty"`
	Resources    []ContextResource `json:"resources,omitempty"`
}

// ContextResource intentionally exposes only a readable descriptor. It is not
// a downloadable ResourceRef and cannot carry provider handles or URLs.
type ContextResource struct {
	Kind string `json:"kind"`
	Name string `json:"name,omitempty"`
}

// ConversationWindowReader is optional because only adapters with a real,
// authenticated native conversation-history API can provide this material.
type ConversationWindowReader interface {
	ReadConversationWindow(context.Context, Event) ConversationWindowResult
}
