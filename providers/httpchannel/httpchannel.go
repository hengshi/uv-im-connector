package httpchannel

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

type DecodeFunc func([]byte, Config) (uvim.Event, bool, error)
type DecodeEventsFunc func([]byte, Config) ([]uvim.Event, error)
type SendFunc func(uvim.OutboundMessage, Config) (Request, error)
type PrepareSendFunc func(context.Context, uvim.OutboundMessage, Config) (uvim.OutboundMessage, error)
type ParseSendResponseFunc func([]byte) (string, error)

type Request struct {
	Method string
	Path   string
	Body   any
	Form   url.Values
	Header http.Header
	NoAuth bool
}

type Config struct {
	ProviderID        string
	ConnectorID       string
	BaseURL           string
	Token             string
	WebhookSecret     string
	ResourceStore     *uvim.ResourceStore
	HTTPClient        *http.Client
	Now               func() time.Time
	Logger            *slog.Logger
	Capabilities      uvim.Capabilities
	Decode            DecodeFunc
	DecodeEvents      DecodeEventsFunc
	PrepareSend       PrepareSendFunc
	Send              SendFunc
	ParseSendResponse ParseSendResponseFunc
}

type Provider struct {
	config Config
	now    func() time.Time
}

func New(config Config) (*Provider, error) {
	if strings.TrimSpace(config.ProviderID) == "" {
		return nil, fmt.Errorf("http channel provider: provider id is required")
	}
	if config.ConnectorID == "" {
		config.ConnectorID = config.ProviderID
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 15 * time.Second}
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Provider{config: config, now: config.Now}, nil
}

func (p *Provider) ID() string          { return p.config.ProviderID }
func (p *Provider) ConnectorID() string { return p.config.ConnectorID }
func (p *Provider) Capabilities() uvim.Capabilities {
	caps := p.config.Capabilities
	if !caps.Inbound && (p.config.Decode != nil || p.config.DecodeEvents != nil) {
		caps.Inbound = true
	}
	if !caps.Outbound && p.config.Send != nil {
		caps.Outbound = true
	}
	if !caps.DownloadResource {
		caps.DownloadResource = true
	}
	if len(caps.ResourceKinds) == 0 {
		caps.ResourceKinds = []string{uvim.ElementImage, uvim.ElementAudio, uvim.ElementVideo, uvim.ElementFile}
	}
	if len(caps.ChannelTypes) == 0 {
		caps.ChannelTypes = []string{uvim.ChannelDirect, uvim.ChannelGroup, uvim.ChannelThread}
	}
	return caps
}

func (p *Provider) Run(ctx context.Context, _ uvim.EventSink) error {
	<-ctx.Done()
	return ctx.Err()
}

func (p *Provider) Send(ctx context.Context, msg uvim.OutboundMessage) (uvim.SendResult, error) {
	if p.config.Send == nil {
		return uvim.SendResult{}, fmt.Errorf("%s send: outbound is not supported", p.ID())
	}
	if err := uvim.ValidateOutboundTarget(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("%s send: %w", p.ID(), err)
	}
	if len(msg.Resources) > 0 || hasNonTextElements(msg.Elements) {
		return uvim.SendResult{}, fmt.Errorf("%s send: resources and rich elements are not supported", p.ID())
	}
	if strings.TrimSpace(msg.Text) == "" && len(msg.Elements) > 0 {
		msg.Text = textFromElements(msg.Elements)
	}
	if strings.TrimSpace(msg.Text) == "" {
		return uvim.SendResult{}, fmt.Errorf("%s send: text is required", p.ID())
	}
	if p.config.PrepareSend != nil {
		prepared, err := p.config.PrepareSend(ctx, msg, p.config)
		if err != nil {
			return uvim.SendResult{}, err
		}
		msg = prepared
	}
	outReq, err := p.config.Send(msg, p.config)
	if err != nil {
		return uvim.SendResult{}, err
	}
	method := outReq.Method
	if method == "" {
		method = http.MethodPost
	}
	body, contentType, err := encodeBody(outReq)
	if err != nil {
		return uvim.SendResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(p.config.BaseURL, "/")+outReq.Path, body)
	if err != nil {
		return uvim.SendResult{}, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for key, values := range outReq.Header {
		req.Header.Del(key)
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	if p.config.Token != "" && !outReq.NoAuth && req.Header.Get("Authorization") == "" {
		req.Header.Set("Authorization", authorizationValue(p.config.Token))
	}
	resp, err := p.config.HTTPClient.Do(req)
	if err != nil {
		return uvim.SendResult{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return uvim.SendResult{}, fmt.Errorf("%s send: read response: %w", p.ID(), err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		sendErr := sendHTTPError(p.ID(), resp.StatusCode, raw)
		detail := fmt.Sprintf("%s send: http %d", p.ID(), resp.StatusCode)
		return uvim.SendResult{}, uvim.NewProviderSendError(detail, sendErr)
	}
	messageID := msg.ID
	if p.config.ParseSendResponse != nil {
		messageID, err = p.config.ParseSendResponse(raw)
		if err != nil {
			sendErr := fmt.Errorf("%s send: %w", p.ID(), err)
			if detail := uvim.ProviderSendErrorDetail(err); detail != "" {
				return uvim.SendResult{}, uvim.NewProviderSendError(detail, sendErr)
			}
			return uvim.SendResult{}, sendErr
		}
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: messageID, Time: p.now().UTC()}, nil
}

func (p *Provider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	ref := req.Resource
	if strings.TrimSpace(ref.URL) == "" {
		return ref, fmt.Errorf("%s download: resource url is required", p.ID())
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, ref.URL, nil)
	if err != nil {
		return ref, err
	}
	if token := strings.TrimSpace(ref.Secret); token != "" {
		httpReq.Header.Set("Authorization", authorizationValue(token))
	}
	store := p.store(req.Dir)
	return store.SaveHTTP(ctx, httpReq, ref)
}

func (p *Provider) Health(context.Context) uvim.Health {
	return uvim.Health{Provider: p.ID(), Connector: p.ConnectorID(), State: "ok", CheckedAt: p.now().UTC(), Capabilities: p.Capabilities()}
}

func (p *Provider) ServeWebhook(w http.ResponseWriter, req *http.Request, sink uvim.EventSink) {
	if req.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "error": "method_not_allowed"})
		return
	}
	if !secretOK(req, p.config.WebhookSecret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"ok": false, "error": "unauthorized"})
		return
	}
	raw, err := io.ReadAll(io.LimitReader(req.Body, uvim.DefaultResourceMaxBytes+1))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "read_failed"})
		return
	}
	if int64(len(raw)) > uvim.DefaultResourceMaxBytes {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"ok": false, "error": "payload_too_large"})
		return
	}
	if p.config.Decode == nil && p.config.DecodeEvents == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "error": "decode_not_supported"})
		return
	}
	events, err := p.decodeEvents(raw)
	if err != nil {
		p.config.Logger.Warn("webhook decode failed", "provider", p.ID(), "err", err.Error())
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "decode_failed"})
		return
	}
	for _, event := range events {
		if event.Provider == "" {
			event.Provider = p.ID()
		}
		if event.Connector == "" {
			event.Connector = p.ConnectorID()
		}
		if event.Time.IsZero() {
			event.Time = p.now().UTC()
		}
		if err := sink.Emit(req.Context(), event); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "error": "emit_failed"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (p *Provider) decodeEvents(raw []byte) ([]uvim.Event, error) {
	if p.config.DecodeEvents != nil {
		return p.config.DecodeEvents(raw, p.config)
	}
	event, ok, err := p.config.Decode(raw, p.config)
	if err != nil || !ok {
		return nil, err
	}
	return []uvim.Event{event}, nil
}

func (p *Provider) store(dir string) *uvim.ResourceStore {
	if p.config.ResourceStore != nil && dir == "" {
		return p.config.ResourceStore
	}
	store := &uvim.ResourceStore{Dir: dir, HTTPClient: p.config.HTTPClient}
	if store.Dir == "" && p.config.ResourceStore != nil {
		store.Dir = p.config.ResourceStore.Dir
		store.MaxBytes = p.config.ResourceStore.MaxBytes
	}
	return store
}

func encodeBody(req Request) (io.Reader, string, error) {
	if req.Form != nil {
		return strings.NewReader(req.Form.Encode()), "application/x-www-form-urlencoded", nil
	}
	if req.Body == nil {
		return http.NoBody, "", nil
	}
	raw, err := json.Marshal(req.Body)
	if err != nil {
		return nil, "", err
	}
	return bytes.NewReader(raw), "application/json; charset=utf-8", nil
}

func authorizationValue(token string) string {
	token = strings.TrimSpace(token)
	lower := strings.ToLower(token)
	for _, prefix := range []string{"bearer ", "bot ", "basic "} {
		if strings.HasPrefix(lower, prefix) {
			return token
		}
	}
	return "Bearer " + token
}

func secretOK(req *http.Request, secret string) bool {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return false
	}
	for _, value := range []string{
		req.Header.Get("X-UV-Webhook-Secret"),
		req.Header.Get("X-Webhook-Secret"),
		req.URL.Query().Get("secret"),
	} {
		if strings.TrimSpace(value) == secret {
			return true
		}
	}
	return false
}

func BotAuthorization(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(token), "bot ") {
		return token
	}
	return "Bot " + token
}

func Authorization(token string) string {
	return authorizationValue(token)
}

func sendHTTPError(provider string, status int, raw []byte) error {
	detail := strings.TrimSpace(string(raw))
	if len(detail) > 512 {
		detail = detail[:512]
	}
	if detail == "" {
		return fmt.Errorf("%s send: http %d", provider, status)
	}
	return fmt.Errorf("%s send: http %d: %s", provider, status, detail)
}

func hasNonTextElements(elements []uvim.Element) bool {
	for _, element := range elements {
		if element.Type != "" && element.Type != uvim.ElementText {
			return true
		}
		if element.Resource != nil {
			return true
		}
		if hasNonTextElements(element.Children) {
			return true
		}
	}
	return false
}

func textFromElements(elements []uvim.Element) string {
	parts := make([]string, 0, len(elements))
	for _, element := range elements {
		if element.Type == uvim.ElementText && strings.TrimSpace(element.Text) != "" {
			parts = append(parts, element.Text)
		}
		if childText := textFromElements(element.Children); childText != "" {
			parts = append(parts, childText)
		}
	}
	return strings.Join(parts, "\n")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
