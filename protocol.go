package uvim

import (
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
	MessageID  string `json:"message_id,omitempty"`
	ChannelID  string `json:"channel_id,omitempty"`
	ThreadID   string `json:"thread_id,omitempty"`
	ReplyToken string `json:"reply_token,omitempty"`
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
	EditMessage      bool     `json:"edit_message,omitempty"`
	DeleteMessage    bool     `json:"delete_message,omitempty"`
	UploadResource   bool     `json:"upload_resource,omitempty"`
	DownloadResource bool     `json:"download_resource,omitempty"`
	ResourceKinds    []string `json:"resource_kinds,omitempty"`
	ChannelTypes     []string `json:"channel_types,omitempty"`
}

type OutboundMessage struct {
	ID          string            `json:"id,omitempty"`
	Provider    string            `json:"provider"`
	Connector   string            `json:"connector,omitempty"`
	ChannelID   string            `json:"channel_id,omitempty"`
	ChannelType string            `json:"channel_type,omitempty"`
	Text        string            `json:"text,omitempty"`
	Elements    []Element         `json:"elements,omitempty"`
	Resources   []ResourceRef     `json:"resources,omitempty"`
	Referrer    Referrer          `json:"referrer,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type SendResult struct {
	Provider  string    `json:"provider"`
	Connector string    `json:"connector,omitempty"`
	MessageID string    `json:"message_id,omitempty"`
	Time      time.Time `json:"time"`
}

type Health struct {
	Provider     string       `json:"provider"`
	Connector    string       `json:"connector,omitempty"`
	State        string       `json:"state"`
	Reason       string       `json:"reason,omitempty"`
	CheckedAt    time.Time    `json:"checked_at"`
	Capabilities Capabilities `json:"capabilities,omitempty"`
}
