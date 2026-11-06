package uvim

import (
	"context"
	"errors"
)

const ConversationMaterializeVersionV1 = "v1"

// ErrConversationResourceUnavailable is intentionally coarse: it lets the
// Hub distinguish a withdrawn/stale selected resource from an ambiguous
// provider attempt without exposing provider response details.
var ErrConversationResourceUnavailable = errors.New("conversation resource unavailable")

// ConversationMaterializeRequest is a Hub-private request. The identity fields
// must be copied from the retained search trigger; callers never choose a
// conversation, thread, source message, provider URL, or provider key.
type ConversationMaterializeRequest struct {
	Provider            string `json:"provider"`
	Connector           string `json:"connector"`
	TriggerEventID      string `json:"trigger_event_id"`
	TriggerMessageID    string `json:"trigger_message_id"`
	ExpectedEventDigest string `json:"expected_event_digest"`
	AudienceDigest      string `json:"audience_digest"`
	SourceEpoch         string `json:"source_epoch"`
	MaterializeHandle   string `json:"materialize_handle"`
}

// ConversationMaterializeResult is deliberately small. Only a ready result
// can contain an internal ResourceRef; normalized states never expose a
// provider key, URL, or private error text.
type ConversationMaterializeResult struct {
	Version   string      `json:"version"`
	State     string      `json:"state"`
	Reason    string      `json:"reason,omitempty"`
	Resource  ResourceRef `json:"resource,omitempty"`
}

// ConversationResourceMaterializeRequest is adapter-internal. The Hub derives
// every field from its retained trigger and durable opaque handle record.
type ConversationResourceMaterializeRequest struct {
	Trigger       Event
	Scope         ConversationSearchScope
	Source        string
	SourceMessage string
	SourceOrdinal int
	Resource      ResourceRef
	Dir           string
}

// ConversationResourceMaterializer is optional. Implementations must verify
// that the exact source resource still belongs to the verified trigger scope
// before downloading it.
type ConversationResourceMaterializer interface {
	MaterializeConversationResource(context.Context, ConversationResourceMaterializeRequest) (ResourceRef, error)
}
