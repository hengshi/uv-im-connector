package uvim

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	EventMessageCreate  = "message.create"
	EventMessageUpdate  = "message.update"
	EventMessageDelete  = "message.delete"
	EventReactionAdd    = "reaction.add"
	EventReactionRemove = "reaction.remove"
	EventProviderHealth = "provider.health"
)

const (
	ChannelDirect = "direct"
	ChannelGroup  = "group"
	ChannelThread = "thread"
	ChannelRoom   = "room"
)

const (
	TargetUser         = "user"
	TargetGroup        = "group"
	TargetChannel      = "channel"
	TargetConversation = "conversation"
)

const (
	ElementText  = "text"
	ElementAt    = "at"
	ElementQuote = "quote"
	ElementImage = "image"
	ElementAudio = "audio"
	ElementVideo = "video"
	ElementFile  = "file"
)

type Event struct {
	Sequence  int64     `json:"sequence,omitempty"`
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Provider  string    `json:"provider"`
	Connector string    `json:"connector,omitempty"`
	Time      time.Time `json:"time"`
	Login     Login     `json:"login,omitempty"`
	Channel   Channel   `json:"channel,omitempty"`
	User      User      `json:"user,omitempty"`
	Message   Message   `json:"message,omitempty"`
	Referrer  Referrer  `json:"referrer,omitempty"`
	Addressed bool      `json:"addressed,omitempty"`
}

func (e Event) DedupeKey() string {
	eventType := strings.TrimSpace(e.Type)
	if eventType == "" {
		eventType = "event"
	}
	for _, value := range []string{e.Message.ID, e.Referrer.MessageID, e.ID} {
		if value = strings.TrimSpace(value); value != "" {
			return e.Provider + ":" + e.Connector + ":" + eventType + ":" + e.Channel.ID + ":" + value
		}
	}
	return ""
}

func (e Event) Sanitized() Event {
	e.Message = e.Message.Sanitized()
	return e
}

type Login struct {
	ID        string `json:"id,omitempty"`
	Platform  string `json:"platform,omitempty"`
	Connector string `json:"connector,omitempty"`
	Name      string `json:"name,omitempty"`
}

type Channel struct {
	ID   string `json:"id,omitempty"`
	Type string `json:"type,omitempty"`
	Name string `json:"name,omitempty"`
}

type User struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
}

type Message struct {
	ID        string        `json:"id,omitempty"`
	Type      string        `json:"type,omitempty"`
	Text      string        `json:"text,omitempty"`
	Elements  []Element     `json:"elements,omitempty"`
	Resources []ResourceRef `json:"resources,omitempty"`
	CreatedAt time.Time     `json:"created_at,omitempty"`
}

func (m Message) Sanitized() Message {
	if len(m.Resources) == 0 {
		m.Elements = SanitizeElements(m.Elements)
		return m
	}
	m.Resources = SanitizeResources(m.Resources)
	m.Elements = SanitizeElements(m.Elements)
	return m
}

type Element struct {
	Type      string         `json:"type"`
	Text      string         `json:"text,omitempty"`
	ID        string         `json:"id,omitempty"`
	Name      string         `json:"name,omitempty"`
	URL       string         `json:"url,omitempty"`
	MIME      string         `json:"mime,omitempty"`
	SizeBytes int64          `json:"size_bytes,omitempty"`
	Children  []Element      `json:"children,omitempty"`
	Resource  *ResourceRef   `json:"resource,omitempty"`
	Attrs     map[string]any `json:"attrs,omitempty"`
}

func Text(text string) Element {
	return Element{Type: ElementText, Text: text}
}

func File(ref ResourceRef) Element {
	return Element{Type: ref.Kind, Resource: &ref, URL: ref.InternalURL, Name: ref.Name, MIME: ref.MIME, SizeBytes: ref.SizeBytes}
}

func (e Element) Sanitized() Element {
	if e.Resource != nil {
		ref := e.Resource.Sanitized()
		e.Resource = &ref
		e.URL = ref.InternalURL
	} else if IsResourceElement(e.Type) && !strings.HasPrefix(strings.TrimSpace(e.URL), "internal://") {
		e.URL = ""
	}
	e.Attrs = nil
	e.Children = SanitizeElements(e.Children)
	return e
}

func SanitizeElements(elements []Element) []Element {
	if len(elements) == 0 {
		return nil
	}
	out := make([]Element, len(elements))
	for i := range elements {
		out[i] = elements[i].Sanitized()
	}
	return out
}

func IsResourceElement(elementType string) bool {
	switch elementType {
	case ElementImage, ElementAudio, ElementVideo, ElementFile:
		return true
	default:
		return false
	}
}

type Referrer struct {
	MessageID       string          `json:"message_id,omitempty"`
	ParentMessageID string          `json:"parent_message_id,omitempty"`
	RootMessageID   string          `json:"root_message_id,omitempty"`
	ChannelID       string          `json:"channel_id,omitempty"`
	ThreadID        string          `json:"thread_id,omitempty"`
	ReplyToken      string          `json:"reply_token,omitempty"`
	Target          *OutboundTarget `json:"target,omitempty"`
}

type ResourceRef struct {
	ID          string            `json:"id,omitempty"`
	Provider    string            `json:"provider,omitempty"`
	Connector   string            `json:"connector,omitempty"`
	Kind        string            `json:"kind,omitempty"`
	Name        string            `json:"name,omitempty"`
	Key         string            `json:"key,omitempty"`
	URL         string            `json:"url,omitempty"`
	InternalURL string            `json:"internal_url,omitempty"`
	MIME        string            `json:"mime,omitempty"`
	SizeBytes   int64             `json:"size_bytes,omitempty"`
	SHA256      string            `json:"sha256,omitempty"`
	Error       string            `json:"error,omitempty"`
	Secret      string            `json:"-"`
	Private     map[string]string `json:"-"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func (r ResourceRef) Sanitized() ResourceRef {
	r.Key = ""
	r.URL = ""
	r.Secret = ""
	r.Private = nil
	r.Metadata = nil
	return r
}

func SanitizeResources(resources []ResourceRef) []ResourceRef {
	if len(resources) == 0 {
		return nil
	}
	out := make([]ResourceRef, len(resources))
	for i := range resources {
		out[i] = resources[i].Sanitized()
	}
	return out
}

type Capabilities struct {
	Inbound          bool     `json:"inbound"`
	Outbound         bool     `json:"outbound"`
	DirectMessage    bool     `json:"direct_message,omitempty"`
	GroupMessage     bool     `json:"group_message,omitempty"`
	ThreadReply      bool     `json:"thread_reply,omitempty"`
	ReplyMessage     bool     `json:"reply_message,omitempty"`
	ProactiveDirect  bool     `json:"proactive_direct,omitempty"`
	ProactiveGroup   bool     `json:"proactive_group,omitempty"`
	TargetKinds      []string `json:"target_kinds,omitempty"`
	EditMessage      bool     `json:"edit_message,omitempty"`
	DeleteMessage    bool     `json:"delete_message,omitempty"`
	UploadResource   bool     `json:"upload_resource,omitempty"`
	DownloadResource bool     `json:"download_resource,omitempty"`
	ResourceKinds    []string `json:"resource_kinds,omitempty"`
	ChannelTypes     []string `json:"channel_types,omitempty"`
}

type OutboundTarget struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

type OutboundMessage struct {
	ID          string            `json:"id,omitempty"`
	Provider    string            `json:"provider"`
	Connector   string            `json:"connector,omitempty"`
	Target      *OutboundTarget   `json:"target,omitempty"`
	ChannelID   string            `json:"channel_id,omitempty"`
	ChannelType string            `json:"channel_type,omitempty"`
	Text        string            `json:"text,omitempty"`
	Elements    []Element         `json:"elements,omitempty"`
	Resources   []ResourceRef     `json:"resources,omitempty"`
	Referrer    Referrer          `json:"referrer,omitempty"`
	Final       bool              `json:"final,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// ResolvedTarget prefers an explicit send target, then the adapter-provided
// reply target. Legacy channel_id remains a provider-native conversation ID;
// channel_type supplies only its semantic target kind.
func (m OutboundMessage) ResolvedTarget() OutboundTarget {
	if m.Target != nil {
		return OutboundTarget{
			ID:   strings.TrimSpace(m.Target.ID),
			Kind: strings.ToLower(strings.TrimSpace(m.Target.Kind)),
		}
	}
	if m.Referrer.Target != nil {
		return OutboundTarget{
			ID:   strings.TrimSpace(m.Referrer.Target.ID),
			Kind: strings.ToLower(strings.TrimSpace(m.Referrer.Target.Kind)),
		}
	}
	target := OutboundTarget{ID: strings.TrimSpace(FirstNonEmpty(m.ChannelID, m.Referrer.ChannelID))}
	switch strings.ToLower(strings.TrimSpace(m.ChannelType)) {
	case ChannelDirect:
		target.Kind = TargetUser
	case ChannelGroup:
		target.Kind = TargetGroup
	case ChannelThread:
		target.Kind = TargetChannel
	default:
		target.Kind = TargetConversation
	}
	return target
}

func ValidateOutboundTarget(m OutboundMessage, capabilities Capabilities) error {
	hasReplyHandle := strings.TrimSpace(m.Referrer.MessageID) != "" || strings.TrimSpace(m.Referrer.ReplyToken) != ""
	if m.Target == nil && m.Referrer.Target == nil {
		channelType := strings.ToLower(strings.TrimSpace(m.ChannelType))
		switch channelType {
		case "", ChannelDirect, ChannelGroup, ChannelThread, ChannelRoom:
		default:
			return fmt.Errorf("invalid legacy channel_type %q", channelType)
		}
		hasChannel := strings.TrimSpace(FirstNonEmpty(m.ChannelID, m.Referrer.ChannelID)) != ""
		if !hasChannel && !hasReplyHandle {
			return fmt.Errorf("legacy channel_id or reply referrer is required")
		}
		if hasReplyHandle {
			if !capabilities.ReplyMessage {
				return fmt.Errorf("reply messages are not supported")
			}
			return nil
		}
	}
	target := m.ResolvedTarget()
	if target.ID == "" {
		return fmt.Errorf("target id is required")
	}
	if target.Kind == "" {
		return fmt.Errorf("target kind is required")
	}
	valid := target.Kind == TargetUser || target.Kind == TargetGroup || target.Kind == TargetChannel || target.Kind == TargetConversation
	if !valid {
		return fmt.Errorf("invalid target kind %q", target.Kind)
	}
	for _, kind := range capabilities.TargetKinds {
		if kind == target.Kind {
			if hasReplyHandle {
				if !capabilities.ReplyMessage {
					return fmt.Errorf("reply messages are not supported")
				}
				return nil
			}
			if target.Kind == TargetUser {
				if !capabilities.ProactiveDirect {
					return fmt.Errorf("proactive direct messages are not supported")
				}
				return nil
			}
			if !capabilities.ProactiveGroup {
				return fmt.Errorf("proactive group messages are not supported")
			}
			return nil
		}
	}
	return fmt.Errorf("target kind %q is not supported", target.Kind)
}

type SendResult struct {
	Provider  string    `json:"provider"`
	Connector string    `json:"connector,omitempty"`
	MessageID string    `json:"message_id,omitempty"`
	Time      time.Time `json:"time"`
}

type providerSendError struct {
	detail string
	err    error
}

func (e *providerSendError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return e.detail
}

func (e *providerSendError) Unwrap() error { return e.err }

// NewProviderSendError marks a bounded provider failure reason as safe to
// return to an authenticated API caller while preserving the internal error.
func NewProviderSendError(detail string, err error) error {
	return &providerSendError{detail: TrimOutboundText(detail, 1024), err: err}
}

func ProviderSendErrorDetail(err error) string {
	var sendErr *providerSendError
	if errors.As(err, &sendErr) {
		return sendErr.detail
	}
	return ""
}

type Health struct {
	Provider     string       `json:"provider"`
	Connector    string       `json:"connector,omitempty"`
	State        string       `json:"state"`
	Reason       string       `json:"reason,omitempty"`
	CheckedAt    time.Time    `json:"checked_at"`
	Capabilities Capabilities `json:"capabilities,omitempty"`
}
