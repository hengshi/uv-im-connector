package uvim

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
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
	if e.Channel.Type == ChannelDirect && strings.TrimSpace(e.Channel.Name) == "" {
		e.Channel.Name = FirstNonEmpty(e.User.DisplayName, e.User.Name)
	}
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
	ExpiresAt       *time.Time      `json:"expires_at,omitempty"`
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
	ReplyMaxUses     int      `json:"reply_max_uses,omitempty"`
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

// ValidateOutboundResources verifies that every outbound resource is covered by
// the exact provider capability advertised to callers. Provider adapters with
// custom send paths must call this too; otherwise they can accidentally bypass
// the generic HTTP channel validation.
func ValidateOutboundResources(m OutboundMessage, capabilities Capabilities) error {
	if len(m.Resources) == 0 {
		return nil
	}
	if !capabilities.UploadResource {
		return fmt.Errorf("resources are not supported")
	}
	supported := make(map[string]struct{}, len(capabilities.ResourceKinds))
	for _, kind := range capabilities.ResourceKinds {
		kind = strings.ToLower(strings.TrimSpace(kind))
		if kind != "" {
			supported[kind] = struct{}{}
		}
	}
	for _, ref := range m.Resources {
		kind := strings.ToLower(strings.TrimSpace(ref.Kind))
		if _, ok := supported[kind]; !ok {
			return fmt.Errorf("resource kind %q is not supported", ref.Kind)
		}
	}
	return nil
}

type SendResult struct {
	Provider  string    `json:"provider"`
	Connector string    `json:"connector,omitempty"`
	MessageID string    `json:"message_id,omitempty"`
	Time      time.Time `json:"time"`
}

const (
	SendFailureUnknown             = "unknown"
	SendFailureInvalidRequest      = "invalid-request"
	SendFailureAuthentication      = "authentication"
	SendFailurePermission          = "permission"
	SendFailureTargetUnavailable   = "target-unavailable"
	SendFailureRateLimited         = "rate-limited"
	SendFailureProviderUnavailable = "provider-unavailable"
	SendFailureTimeout             = "timeout"
	SendFailureTransport           = "transport"
	SendFailureProviderRejected    = "provider-rejected"
	SendFailurePayloadTooLarge     = "payload-too-large"

	DeliveryNotAttempted = "not-attempted"
	DeliveryRejected     = "rejected"
	DeliveryUnknown      = "unknown"
)

// SendFailure is the provider-neutral, credential-safe decision input returned
// to callers when an outbound message cannot be delivered. Detail remains a
// human-readable compatibility field; callers should drive retry and lifecycle
// policy from this structure instead of parsing provider text.
type SendFailure struct {
	Category          string `json:"category"`
	Retryable         bool   `json:"retryable"`
	DeliveryState     string `json:"delivery_state"`
	HTTPStatus        int    `json:"http_status,omitempty"`
	ProviderCode      string `json:"provider_code,omitempty"`
	RetryAfterSeconds int    `json:"retry_after_seconds,omitempty"`
	RequestID         string `json:"request_id,omitempty"`
}

// Sanitized returns a bounded failure suitable for persistence and policy.
func (failure SendFailure) Sanitized() SendFailure {
	return normalizeSendFailure(failure)
}

type providerSendError struct {
	detail  string
	failure SendFailure
	err     error
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
	failure, ok := ProviderSendFailure(err)
	if !ok {
		failure = classifyProviderSendFailure(err)
	}
	return NewProviderSendFailure(failure, detail, err)
}

// NewProviderSendFailure attaches provider-neutral delivery facts to a safe
// public detail while preserving the original internal error.
func NewProviderSendFailure(failure SendFailure, detail string, err error) error {
	return &providerSendError{
		detail:  TrimOutboundText(detail, 1024),
		failure: normalizeSendFailure(failure),
		err:     err,
	}
}

// NewProviderResponseError marks a syntactically successful provider response
// whose business result rejected the message. Only bounded machine-like codes
// are extracted from raw; provider messages and response bodies are not copied.
func NewProviderResponseError(raw []byte, _ string, err error) error {
	return NewProviderResponseErrorWithCode(raw, "", err)
}

// NewProviderResponseErrorWithCode marks a provider's typed response code as
// safe decision metadata. Adapters must pass a field they already parsed from
// their provider response; arbitrary raw response strings remain untrusted.
func NewProviderResponseErrorWithCode(raw []byte, providerCode string, err error) error {
	failure := providerResponseFailure(raw)
	if providerCode = safeProviderMachineValue(providerCode); providerCode != "" {
		failure.ProviderCode = providerCode
	}
	logDetail := "provider rejected request"
	if failure.ProviderCode != "" {
		logDetail += ": code " + failure.ProviderCode
	}
	return NewProviderSendLogError(logDetail, NewProviderSendFailure(failure, "", err))
}

// NewProviderHTTPError marks a non-2xx provider response. HTTP status, retry
// metadata, request ID, and a bounded provider code are safe decision facts;
// the raw response body remains private.
func NewProviderHTTPError(status int, header http.Header, raw []byte, detail string, err error) error {
	return NewProviderSendFailure(ProviderHTTPFailure(status, header, raw), detail, err)
}

func ProviderSendErrorDetail(err error) string {
	var sendErr *providerSendError
	if errors.As(err, &sendErr) {
		return sendErr.detail
	}
	return ""
}

// ProviderSendFailure returns normalized provider-neutral delivery facts.
func ProviderSendFailure(err error) (SendFailure, bool) {
	var sendErr *providerSendError
	if !errors.As(err, &sendErr) {
		return SendFailure{}, false
	}
	return normalizeSendFailure(sendErr.failure), true
}

// ProviderHTTPFailure classifies a provider HTTP response without retaining or
// exposing its body. A non-2xx response is a rejection only where replay is
// known to be safe; ambiguous timeout/server outcomes remain delivery unknown.
func ProviderHTTPFailure(status int, header http.Header, raw []byte) SendFailure {
	return providerHTTPFailureAt(status, header, raw, time.Now())
}

func providerHTTPFailureAt(status int, header http.Header, raw []byte, now time.Time) SendFailure {
	failure := providerResponseFailure(raw)
	failure.HTTPStatus = status
	switch {
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
		failure.Category = SendFailureInvalidRequest
		failure.DeliveryState = DeliveryRejected
	case status == http.StatusUnauthorized:
		failure.Category = SendFailureAuthentication
		failure.DeliveryState = DeliveryRejected
	case status == http.StatusForbidden:
		failure.Category = SendFailurePermission
		failure.DeliveryState = DeliveryRejected
	case status == http.StatusNotFound || status == http.StatusGone:
		failure.Category = SendFailureTargetUnavailable
		failure.DeliveryState = DeliveryRejected
	case status == http.StatusRequestEntityTooLarge:
		failure.Category = SendFailurePayloadTooLarge
		failure.DeliveryState = DeliveryRejected
	case status == http.StatusTooManyRequests || status == http.StatusTooEarly:
		failure.Category = SendFailureRateLimited
		failure.Retryable = true
		failure.DeliveryState = DeliveryRejected
	case status == http.StatusRequestTimeout || status == http.StatusGatewayTimeout:
		failure.Category = SendFailureTimeout
		failure.Retryable = true
		failure.DeliveryState = DeliveryUnknown
	case status >= 500 && status <= 599:
		failure.Category = SendFailureProviderUnavailable
		failure.Retryable = true
		failure.DeliveryState = DeliveryUnknown
	case status >= 400:
		failure.Category = SendFailureProviderRejected
		failure.DeliveryState = DeliveryRejected
	}
	if header != nil {
		failure.RetryAfterSeconds = retryAfterSeconds(header.Get("Retry-After"), now)
		for _, name := range []string{"X-Request-Id", "X-Request-ID", "X-Lark-Request-Id", "X-Slack-Req-Id"} {
			if value := safeProviderMachineValue(header.Get(name)); value != "" {
				failure.RequestID = value
				break
			}
		}
	}
	return normalizeSendFailure(failure)
}

func retryAfterSeconds(value string, now time.Time) int {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil {
		return int(min(seconds, 3600))
	} else if errors.Is(err, strconv.ErrRange) {
		return 3600
	}
	retryAt, err := http.ParseTime(value)
	if err != nil || !retryAt.After(now) {
		return 0
	}
	delay := retryAt.Sub(now)
	if delay >= time.Hour {
		return 3600
	}
	return int((delay + time.Second - 1) / time.Second)
}

func providerResponseFailure(raw []byte) SendFailure {
	failure := SendFailure{Category: SendFailureProviderRejected, DeliveryState: DeliveryRejected}
	var payload any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if decoder.Decode(&payload) == nil {
		failure.ProviderCode = providerFailureCode(payload)
	}
	return failure
}

func providerFailureCode(payload any) string {
	object, ok := payload.(map[string]any)
	if !ok {
		return ""
	}
	for _, key := range []string{"code", "error_code", "errcode", "retcode"} {
		if value := safeProviderMachineValue(fmt.Sprint(object[key])); value != "" && value != "<nil>" {
			return value
		}
	}
	if nested, ok := object["error"].(map[string]any); ok {
		if value := safeProviderMachineValue(fmt.Sprint(nested["code"])); value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}

func safeProviderMachineValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return ""
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune("._:-/", r) {
			continue
		}
		return ""
	}
	return value
}

func normalizeSendFailure(failure SendFailure) SendFailure {
	switch failure.Category {
	case SendFailureInvalidRequest, SendFailureAuthentication, SendFailurePermission,
		SendFailureTargetUnavailable, SendFailureRateLimited, SendFailureProviderUnavailable,
		SendFailureTimeout, SendFailureTransport, SendFailureProviderRejected, SendFailurePayloadTooLarge:
	default:
		failure.Category = SendFailureUnknown
	}
	switch failure.DeliveryState {
	case DeliveryNotAttempted, DeliveryRejected, DeliveryUnknown:
	default:
		failure.DeliveryState = DeliveryUnknown
	}
	failure.ProviderCode = safeProviderMachineValue(failure.ProviderCode)
	failure.RequestID = safeProviderMachineValue(failure.RequestID)
	if failure.HTTPStatus < 100 || failure.HTTPStatus > 599 {
		failure.HTTPStatus = 0
	}
	if failure.RetryAfterSeconds < 0 {
		failure.RetryAfterSeconds = 0
	} else if failure.RetryAfterSeconds > 3600 {
		failure.RetryAfterSeconds = 3600
	}
	return failure
}

func classifyProviderSendFailure(err error) SendFailure {
	failure := SendFailure{Category: SendFailureUnknown, DeliveryState: DeliveryUnknown}
	if err == nil {
		return failure
	}
	if errors.Is(err, context.DeadlineExceeded) {
		failure.Category = SendFailureTimeout
		failure.Retryable = true
		return failure
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		failure.Category = SendFailureTransport
		failure.Retryable = true
		failure.DeliveryState = DeliveryNotAttempted
		return failure
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		failure.Category = SendFailureProviderUnavailable
		failure.Retryable = true
		failure.DeliveryState = DeliveryNotAttempted
		return failure
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		failure.Category = SendFailureTransport
		failure.Retryable = true
	}
	return failure
}

type providerSendLogError struct {
	detail string
	err    error
}

func (e *providerSendLogError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return e.detail
}

func (e *providerSendLogError) Unwrap() error { return e.err }

// NewProviderSendLogError marks a bounded provider diagnostic as safe for
// private service logs only while preserving the internal error.
func NewProviderSendLogError(detail string, err error) error {
	return &providerSendLogError{detail: providerSendErrorLogText(detail), err: err}
}

// NewProviderSendOperationError adds a credential-safe provider stage without
// exposing an arbitrary underlying error. Existing public/private markers are
// preserved, while known structured causes retain their safe classification.
func NewProviderSendOperationError(operation string, err error) error {
	if err == nil {
		return err
	}
	if _, ok := ProviderSendFailure(err); ok {
		return err
	}
	wrapped := err
	var logErr *providerSendLogError
	if !errors.As(err, &logErr) || logErr.detail == "" {
		detail := strings.TrimSpace(operation)
		if cause, known := providerSendErrorLogDetail(err, 0); known {
			if detail != "" {
				detail += ": " + cause
			} else {
				detail = cause
			}
		}
		if detail == "" {
			detail = "provider operation failed"
		}
		wrapped = NewProviderSendLogError(detail, err)
	}
	return NewProviderSendFailure(classifyProviderSendFailure(err), ProviderSendErrorDetail(err), wrapped)
}

// ProviderSendErrorLogDetail returns a credential-safe diagnostic for private
// service logs. Adapter-marked details are already safe; known transport errors
// retain bounded structured causes without copying arbitrary error strings.
func ProviderSendErrorLogDetail(err error) string {
	detail, ok := providerSendErrorLogDetail(err, 0)
	if !ok {
		return "unmarked provider error"
	}
	return providerSendErrorLogText(detail)
}

func providerSendErrorLogDetail(err error, depth int) (string, bool) {
	if err == nil {
		return "", false
	}
	if depth >= 8 {
		return "provider error chain truncated", true
	}
	var logErr *providerSendLogError
	if errors.As(err, &logErr) && logErr.detail != "" {
		return logErr.detail, true
	}
	if detail := ProviderSendErrorDetail(err); detail != "" {
		return detail, true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		endpoint := "<redacted-url>"
		if parsed, parseErr := url.Parse(urlErr.URL); parseErr == nil && parsed.Scheme != "" && parsed.Host != "" {
			endpoint = parsed.Scheme + "://" + parsed.Host
		}
		cause, known := providerSendErrorLogDetail(urlErr.Err, depth+1)
		if !known {
			cause = "transport failure"
		}
		return fmt.Sprintf("%s %q: %s", providerHTTPMethod(urlErr.Op), endpoint, cause), true
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		cause, known := providerSendErrorLogDetail(netErr.Err, depth+1)
		if !known {
			cause = "network failure"
		}
		return providerNetOperation(netErr.Op, netErr.Net) + ": " + cause, true
	}
	var syscallErr *os.SyscallError
	if errors.As(err, &syscallErr) {
		cause, known := providerSendErrorLogDetail(syscallErr.Err, depth+1)
		if !known {
			cause = "system call failed"
		}
		return providerSystemCall(syscallErr.Syscall) + ": " + cause, true
	}
	if errors.Is(err, context.Canceled) {
		return "provider request canceled", true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "provider request timed out", true
	}
	if detail := providerErrnoLogDetail(err); detail != "" {
		return detail, true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		switch {
		case dnsErr.IsTimeout:
			return "dns lookup timed out", true
		case dnsErr.IsNotFound:
			return "dns name not found", true
		case dnsErr.IsTemporary:
			return "temporary dns failure", true
		default:
			return "dns lookup failed", true
		}
	}
	var certificateErr *tls.CertificateVerificationError
	if errors.As(err, &certificateErr) {
		return "tls certificate verification failed", true
	}
	var unknownAuthority x509.UnknownAuthorityError
	if errors.As(err, &unknownAuthority) {
		return "tls certificate signed by unknown authority", true
	}
	var hostnameErr x509.HostnameError
	if errors.As(err, &hostnameErr) {
		return "tls certificate hostname mismatch", true
	}
	var invalidCertificate x509.CertificateInvalidError
	if errors.As(err, &invalidCertificate) {
		return "tls certificate is invalid", true
	}
	var recordHeaderErr tls.RecordHeaderError
	if errors.As(err, &recordHeaderErr) {
		return "invalid tls record", true
	}
	var protocolErr *http.ProtocolError
	if errors.As(err, &protocolErr) {
		return "invalid HTTP response", true
	}
	var textProtocolErr *textproto.Error
	if errors.As(err, &textProtocolErr) {
		return fmt.Sprintf("remote protocol error: code %d", textProtocolErr.Code), true
	}
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return "invalid JSON response", true
	}
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return "invalid JSON response type", true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return "truncated provider response", true
	}
	if errors.Is(err, io.EOF) {
		return "empty provider response", true
	}
	var genericNetErr net.Error
	if errors.As(err, &genericNetErr) {
		if genericNetErr.Timeout() {
			return "network operation timed out", true
		}
		return "network operation failed", true
	}
	return "", false
}

func providerHTTPMethod(method string) string {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodGet:
		return "Get"
	case http.MethodHead:
		return "Head"
	case http.MethodPost:
		return "Post"
	case http.MethodPut:
		return "Put"
	case http.MethodPatch:
		return "Patch"
	case http.MethodDelete:
		return "Delete"
	case http.MethodConnect:
		return "Connect"
	case http.MethodOptions:
		return "Options"
	case http.MethodTrace:
		return "Trace"
	default:
		return "request"
	}
}

func providerNetOperation(operation, network string) string {
	switch strings.ToLower(strings.TrimSpace(operation)) {
	case "accept", "dial", "listen", "lookup", "read", "readfrom", "resolve", "write", "writeto":
		operation = strings.ToLower(strings.TrimSpace(operation))
	default:
		operation = "network"
	}
	switch strings.ToLower(strings.TrimSpace(network)) {
	case "ip", "ip4", "ip6", "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6", "unix", "unixgram", "unixpacket":
		return operation + " " + strings.ToLower(strings.TrimSpace(network))
	default:
		return operation
	}
}

func providerSystemCall(name string) string {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "accept", "connect", "read", "recvfrom", "sendto", "write":
		return strings.ToLower(strings.TrimSpace(name))
	default:
		return "system call"
	}
}

func providerErrnoLogDetail(err error) string {
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection reset"
	case errors.Is(err, syscall.ECONNABORTED):
		return "connection aborted"
	case errors.Is(err, syscall.EPIPE):
		return "broken pipe"
	case errors.Is(err, syscall.ETIMEDOUT):
		return "connection timed out"
	case errors.Is(err, syscall.ENETUNREACH):
		return "network unreachable"
	case errors.Is(err, syscall.EHOSTUNREACH):
		return "host unreachable"
	default:
		return ""
	}
}

func providerSendErrorLogText(detail string) string {
	return TrimOutboundText(strings.Join(strings.Fields(detail), " "), 1024)
}

type Health struct {
	Provider     string       `json:"provider"`
	Connector    string       `json:"connector,omitempty"`
	State        string       `json:"state"`
	Reason       string       `json:"reason,omitempty"`
	CheckedAt    time.Time    `json:"checked_at"`
	Capabilities Capabilities `json:"capabilities,omitempty"`
}
