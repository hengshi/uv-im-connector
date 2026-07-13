package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
)

const (
	RegionFeishu       = "feishu"
	RegionLark         = "lark"
	defaultFeishuURL   = "https://open.feishu.cn"
	defaultLarkURL     = "https://open.larksuite.com"
	defaultHTTPTimeout = 15 * time.Second
)

type Config struct {
	ConnectorID     string
	AppID           string
	AppSecret       string
	Region          string
	BotOpenID       string
	BotUnionID      string
	BaseURL         string
	CallbackBaseURL string
	HTTPClient      *http.Client
	Dialer          WSDialer
	ResourceStore   *uvim.ResourceStore
	PingInterval    time.Duration
	ReadDeadline    time.Duration
	WriteTimeout    time.Duration
	ChunkTTL        time.Duration
	Now             func() time.Time
	Logger          *slog.Logger
}

type Provider struct {
	config Config
	now    func() time.Time

	tokenMu  sync.Mutex
	token    string
	tokenExp time.Time

	stateMu sync.Mutex
	state   string
}

type WSDialer interface {
	DialContext(ctx context.Context, urlStr string, requestHeader http.Header) (WSConn, *http.Response, error)
}

type WSConn interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteMessage(messageType int, data []byte) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	Close() error
}

type GorillaDialer struct {
	Dialer *websocket.Dialer
}

func (g GorillaDialer) DialContext(ctx context.Context, urlStr string, requestHeader http.Header) (WSConn, *http.Response, error) {
	dialer := g.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	return dialer.DialContext(ctx, urlStr, requestHeader)
}

func New(config Config) (*Provider, error) {
	if strings.TrimSpace(config.AppID) == "" || strings.TrimSpace(config.AppSecret) == "" {
		return nil, fmt.Errorf("lark provider: app_id and app_secret are required")
	}
	if config.ConnectorID == "" {
		config.ConnectorID = "lark"
	}
	if config.Region == "" {
		config.Region = RegionFeishu
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: defaultHTTPTimeout}
	}
	if config.Dialer == nil {
		config.Dialer = GorillaDialer{Dialer: &websocket.Dialer{HandshakeTimeout: 15 * time.Second}}
	}
	if config.PingInterval <= 0 {
		config.PingInterval = 2 * time.Minute
	}
	if config.ReadDeadline <= 0 {
		config.ReadDeadline = 6 * time.Minute
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.ChunkTTL <= 0 {
		config.ChunkTTL = 5 * time.Second
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Provider{config: config, now: config.Now, state: "configured"}, nil
}

func (p *Provider) ID() string          { return "lark" }
func (p *Provider) ConnectorID() string { return p.config.ConnectorID }
func (p *Provider) Capabilities() uvim.Capabilities {
	return uvim.Capabilities{
		Inbound:          true,
		Outbound:         true,
		DirectMessage:    true,
		GroupMessage:     true,
		ThreadReply:      true,
		ReplyMessage:     true,
		ProactiveDirect:  true,
		ProactiveGroup:   true,
		TargetKinds:      []string{uvim.TargetUser, uvim.TargetGroup, uvim.TargetConversation},
		UploadResource:   false,
		DownloadResource: true,
		ResourceKinds:    []string{uvim.ElementImage, uvim.ElementAudio, uvim.ElementVideo, uvim.ElementFile},
		ChannelTypes:     []string{uvim.ChannelDirect, uvim.ChannelGroup},
	}
}

func (p *Provider) Health(context.Context) uvim.Health {
	p.stateMu.Lock()
	state := p.state
	p.stateMu.Unlock()
	return uvim.Health{Provider: p.ID(), Connector: p.ConnectorID(), State: state, CheckedAt: time.Now().UTC(), Capabilities: p.Capabilities()}
}

func (p *Provider) Run(ctx context.Context, sink uvim.EventSink) error {
	endpoint, err := p.endpoint(ctx)
	if err != nil {
		p.setState("error")
		return err
	}
	conn, _, err := p.config.Dialer.DialContext(ctx, endpoint.URL, endpoint.Headers)
	if err != nil {
		p.setState("error")
		return fmt.Errorf("dial ws: %w", err)
	}
	defer conn.Close()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var writeMu sync.Mutex
	pingInterval := endpoint.PingInterval
	if pingInterval <= 0 {
		pingInterval = p.config.PingInterval
	}
	pingDone := make(chan struct{})
	go p.pingLoop(runCtx, conn, &writeMu, endpoint.ServiceID, pingInterval, pingDone)
	defer func() {
		cancel()
		<-pingDone
	}()
	assembler := newChunkAssembler(p.config.ChunkTTL, p.config.Now)
	p.setState("connected")

	for {
		if err := conn.SetReadDeadline(p.now().Add(p.config.ReadDeadline)); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			p.setState("error")
			return fmt.Errorf("set read deadline: %w", err)
		}
		msgType, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return nil
			}
			p.setState("error")
			return fmt.Errorf("read message: %w", err)
		}
		if msgType != websocket.BinaryMessage {
			continue
		}
		inbound, err := unmarshalFrame(raw)
		if err != nil {
			p.config.Logger.Warn("lark frame decode failed", "err", err.Error(), "raw_len", len(raw))
			continue
		}
		if inbound.Method == frameMethodControl {
			if inbound.headerValue(frameHeaderTypeKey) == frameHeaderTypePing {
				if err := p.writeFrame(&writeMu, conn, newPongFrame(endpoint.ServiceID)); err != nil {
					return fmt.Errorf("write pong: %w", err)
				}
			}
			continue
		}
		sum, seq, messageID := parseChunkHeaders(inbound)
		payload := inbound.Payload
		if sum > 1 {
			assembled, complete := assembler.admit(messageID, sum, seq, inbound.Payload)
			if !complete {
				continue
			}
			payload = assembled
		}
		event, ok, decodeErr := DecodePayload(payload, DecoderConfig{
			AppID:      p.config.AppID,
			BotOpenID:  p.config.BotOpenID,
			BotUnionID: p.config.BotUnionID,
			Connector:  p.ConnectorID(),
		})
		if decodeErr != nil {
			p.config.Logger.Warn("lark payload decode failed", "err", decodeErr.Error(), "payload_len", len(payload))
			if err := p.writeFrame(&writeMu, conn, newAckFrame(inbound, true)); err != nil {
				return fmt.Errorf("write ack: %w", err)
			}
			continue
		}
		if err := p.writeFrame(&writeMu, conn, newAckFrame(inbound, true)); err != nil {
			return fmt.Errorf("write ack: %w", err)
		}
		if !ok {
			continue
		}
		p.setState("event")
		if err := sink.Emit(ctx, event); err != nil {
			return fmt.Errorf("emit event: %w", err)
		}
	}
}

func (p *Provider) Send(ctx context.Context, msg uvim.OutboundMessage) (uvim.SendResult, error) {
	if err := uvim.ValidateOutboundTarget(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("lark send: %w", err)
	}
	if len(msg.Resources) > 0 || hasNonTextElements(msg.Elements) {
		return uvim.SendResult{}, fmt.Errorf("lark send: resources and rich elements are not supported")
	}
	text := uvim.TrimOutboundText(msg.Text, 20000)
	if text == "" && len(msg.Elements) > 0 {
		text = uvim.TrimOutboundText(textFromElements(msg.Elements), 20000)
	}
	if text == "" {
		return uvim.SendResult{}, fmt.Errorf("lark send: text is required")
	}
	contentRaw, _ := json.Marshal(map[string]string{"text": text})
	body := map[string]any{"msg_type": "text", "content": string(contentRaw)}
	endpoint := ""
	base := p.baseURL()
	if strings.TrimSpace(msg.Referrer.MessageID) != "" {
		endpoint = base + "/open-apis/im/v1/messages/" + url.PathEscape(msg.Referrer.MessageID) + "/reply"
	} else {
		target := msg.ResolvedTarget()
		if target.ID == "" {
			return uvim.SendResult{}, fmt.Errorf("lark send: message_id or target id is required")
		}
		receiveIDType := "chat_id"
		typedTarget := msg.Target != nil || msg.Referrer.Target != nil
		if typedTarget && target.Kind == uvim.TargetUser && !strings.HasPrefix(target.ID, "ou_") {
			sendErr := fmt.Errorf("lark send: user target id must be an Open ID")
			return uvim.SendResult{}, uvim.NewProviderSendError(sendErr.Error(), sendErr)
		}
		if (typedTarget && target.Kind == uvim.TargetUser) || strings.HasPrefix(target.ID, "ou_") {
			receiveIDType = "open_id"
		}
		endpoint = base + "/open-apis/im/v1/messages?receive_id_type=" + receiveIDType
		body["receive_id"] = target.ID
	}
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return uvim.SendResult{}, err
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return uvim.SendResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return uvim.SendResult{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	respRaw, err := p.doJSON(req)
	if err != nil {
		return uvim.SendResult{}, err
	}
	var decoded struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			MessageID string `json:"message_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respRaw, &decoded); err != nil {
		return uvim.SendResult{}, fmt.Errorf("decode lark send response: %w", err)
	}
	if decoded.Code != 0 {
		sendErr := fmt.Errorf("lark send: code=%d msg=%q", decoded.Code, decoded.Msg)
		return uvim.SendResult{}, uvim.NewProviderSendError(sendErr.Error(), sendErr)
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: decoded.Data.MessageID, Time: time.Now().UTC()}, nil
}

func (p *Provider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	ref := req.Resource
	if strings.TrimSpace(ref.Key) == "" {
		return ref, fmt.Errorf("lark download: resource key is required")
	}
	messageID := uvim.FirstNonEmpty(req.Event.Message.ID, req.Message.ID, ref.Metadata["message_id"])
	if strings.TrimSpace(messageID) == "" {
		return ref, fmt.Errorf("lark download: message id is required")
	}
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return ref, err
	}
	endpoint := p.baseURL() + "/open-apis/im/v1/messages/" + url.PathEscape(messageID) + "/resources/" + url.PathEscape(ref.Key)
	reqURL, err := url.Parse(endpoint)
	if err != nil {
		return ref, err
	}
	query := reqURL.Query()
	query.Set("type", resourceType(ref.Kind))
	reqURL.RawQuery = query.Encode()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return ref, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	return p.store(req.Dir).SaveHTTP(ctx, httpReq, ref)
}

type endpoint struct {
	URL          string
	Headers      http.Header
	ServiceID    int32
	PingInterval time.Duration
}

func (p *Provider) endpoint(ctx context.Context) (endpoint, error) {
	body := map[string]string{"AppID": p.config.AppID, "AppSecret": p.config.AppSecret}
	raw, err := json.Marshal(body)
	if err != nil {
		return endpoint{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.callbackBaseURL()+"/callback/ws/endpoint", bytes.NewReader(raw))
	if err != nil {
		return endpoint{}, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("locale", "zh")
	respRaw, err := p.doJSON(req)
	if err != nil {
		return endpoint{}, err
	}
	var decoded struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			URL          string `json:"URL"`
			ClientConfig struct {
				PingInterval int `json:"PingInterval"`
			} `json:"ClientConfig"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respRaw, &decoded); err != nil {
		return endpoint{}, fmt.Errorf("decode endpoint response: %w", err)
	}
	if decoded.Code != 0 || decoded.Data.URL == "" {
		return endpoint{}, fmt.Errorf("lark ws endpoint: code=%d msg=%q", decoded.Code, decoded.Msg)
	}
	serviceID, err := parseServiceID(decoded.Data.URL)
	if err != nil {
		return endpoint{}, err
	}
	return endpoint{URL: decoded.Data.URL, Headers: http.Header{}, ServiceID: serviceID, PingInterval: time.Duration(decoded.Data.ClientConfig.PingInterval) * time.Second}, nil
}

func (p *Provider) tenantAccessToken(ctx context.Context) (string, error) {
	p.tokenMu.Lock()
	if p.token != "" && p.now().Before(p.tokenExp.Add(-5*time.Minute)) {
		token := p.token
		p.tokenMu.Unlock()
		return token, nil
	}
	p.tokenMu.Unlock()
	body := map[string]string{"app_id": p.config.AppID, "app_secret": p.config.AppSecret}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL()+"/open-apis/auth/v3/tenant_access_token/internal", bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	respRaw, err := p.doJSON(req)
	if err != nil {
		return "", err
	}
	var decoded struct {
		Code              int    `json:"code"`
		Msg               string `json:"msg"`
		TenantAccessToken string `json:"tenant_access_token"`
		Expire            int64  `json:"expire"`
	}
	if err := json.Unmarshal(respRaw, &decoded); err != nil {
		return "", fmt.Errorf("decode tenant_access_token response: %w", err)
	}
	if decoded.Code != 0 || decoded.TenantAccessToken == "" {
		return "", fmt.Errorf("lark tenant_access_token: code=%d msg=%q", decoded.Code, decoded.Msg)
	}
	p.tokenMu.Lock()
	p.token = decoded.TenantAccessToken
	p.tokenExp = p.now().Add(time.Duration(decoded.Expire) * time.Second)
	p.tokenMu.Unlock()
	return decoded.TenantAccessToken, nil
}

func (p *Provider) doJSON(req *http.Request) (json.RawMessage, error) {
	resp, err := p.config.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	return raw, nil
}

func (p *Provider) writeFrame(mu *sync.Mutex, conn WSConn, frame *wsFrame) error {
	raw := frame.marshal()
	mu.Lock()
	defer mu.Unlock()
	if err := conn.SetWriteDeadline(p.now().Add(p.config.WriteTimeout)); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.BinaryMessage, raw)
}

func (p *Provider) pingLoop(ctx context.Context, conn WSConn, writeMu *sync.Mutex, serviceID int32, interval time.Duration, done chan<- struct{}) {
	defer close(done)
	if interval <= 0 {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.writeFrame(writeMu, conn, newPingFrame(serviceID)); err != nil {
				p.config.Logger.Warn("lark ping failed", "err", err.Error())
				_ = conn.Close()
				return
			}
		}
	}
}

func (p *Provider) baseURL() string {
	if base := strings.TrimRight(strings.TrimSpace(p.config.BaseURL), "/"); base != "" {
		return base
	}
	if strings.EqualFold(strings.TrimSpace(p.config.Region), RegionLark) {
		return defaultLarkURL
	}
	return defaultFeishuURL
}

func (p *Provider) callbackBaseURL() string {
	if base := strings.TrimRight(strings.TrimSpace(p.config.CallbackBaseURL), "/"); base != "" {
		return base
	}
	return p.baseURL()
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

func (p *Provider) setState(state string) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	p.state = state
}

func parseServiceID(rawURL string) (int32, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return 0, err
	}
	value := u.Query().Get("service_id")
	if value == "" {
		return 0, errors.New("missing service_id query parameter")
	}
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid service_id %q: %w", value, err)
	}
	return int32(n), nil
}

func resourceType(kind string) string {
	if strings.EqualFold(strings.TrimSpace(kind), uvim.ElementImage) {
		return "image"
	}
	return "file"
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

func truncate(value string, max int) string {
	if max <= 0 || len(value) <= max {
		return value
	}
	return value[:max] + "...(truncated)"
}
