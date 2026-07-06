package wecom

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
)

const (
	defaultWSURL     = "wss://openws.work.weixin.qq.com"
	cmdSubscribe     = "aibot_subscribe"
	cmdHeartbeat     = "ping"
	cmdCallback      = "aibot_msg_callback"
	cmdEventCallback = "aibot_event_callback"
	cmdRespond       = "aibot_respond_msg"
	cmdSend          = "aibot_send_msg"
)

type Config struct {
	ConnectorID       string
	BotID             string
	Secret            string
	WSURL             string
	Dialer            WSDialer
	HTTPClient        *http.Client
	ResourceStore     *uvim.ResourceStore
	HeartbeatInterval time.Duration
	ReadDeadline      time.Duration
	WriteTimeout      time.Duration
	AckTimeout        time.Duration
	Now               func() time.Time
	Logger            *slog.Logger
}

type Provider struct {
	config Config
	now    func() time.Time

	mu           sync.Mutex
	pending      map[string]chan frame
	replyLocks   map[string]*replyLock
	replyLocksMu sync.Mutex

	activeMu    sync.Mutex
	activeConn  WSConn
	activeWrite *sync.Mutex
	state       string
}

type replyLock struct {
	ch   chan struct{}
	refs int
}

type frame struct {
	Cmd     string         `json:"cmd,omitempty"`
	Headers headers        `json:"headers,omitempty"`
	Body    map[string]any `json:"body,omitempty"`
	ErrCode *int           `json:"errcode,omitempty"`
	ErrMsg  string         `json:"errmsg,omitempty"`
}

type headers struct {
	ReqID string `json:"req_id,omitempty"`
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
	if strings.TrimSpace(config.BotID) == "" || strings.TrimSpace(config.Secret) == "" {
		return nil, fmt.Errorf("wecom provider: bot_id and secret are required")
	}
	if config.ConnectorID == "" {
		config.ConnectorID = "wecom"
	}
	if config.WSURL == "" {
		config.WSURL = defaultWSURL
	}
	if config.Dialer == nil {
		config.Dialer = GorillaDialer{Dialer: &websocket.Dialer{HandshakeTimeout: 15 * time.Second}}
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 15 * time.Second}
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 30 * time.Second
	}
	if config.ReadDeadline <= 0 {
		config.ReadDeadline = 2 * time.Minute
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.AckTimeout <= 0 {
		config.AckTimeout = 8 * time.Second
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Provider{config: config, now: config.Now, pending: map[string]chan frame{}, replyLocks: map[string]*replyLock{}, state: "configured"}, nil
}

func (p *Provider) ID() string          { return "wecom" }
func (p *Provider) ConnectorID() string { return p.config.ConnectorID }
func (p *Provider) Capabilities() uvim.Capabilities {
	return uvim.Capabilities{
		Inbound:          true,
		Outbound:         true,
		DirectMessage:    true,
		GroupMessage:     true,
		ThreadReply:      true,
		UploadResource:   false,
		DownloadResource: true,
		ResourceKinds:    []string{uvim.ElementImage, uvim.ElementVideo, uvim.ElementFile},
		ChannelTypes:     []string{uvim.ChannelDirect, uvim.ChannelGroup},
	}
}

func (p *Provider) Health(context.Context) uvim.Health {
	p.activeMu.Lock()
	state := p.state
	p.activeMu.Unlock()
	return uvim.Health{Provider: p.ID(), Connector: p.ConnectorID(), State: state, CheckedAt: time.Now().UTC(), Capabilities: p.Capabilities()}
}

func (p *Provider) Run(ctx context.Context, sink uvim.EventSink) error {
	conn, _, err := p.config.Dialer.DialContext(ctx, p.config.WSURL, http.Header{})
	if err != nil {
		p.setState("error")
		return fmt.Errorf("dial ws: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer conn.Close()
	var writeMu sync.Mutex

	authReqID := p.reqID(cmdSubscribe)
	if err := p.writeFrame(conn, &writeMu, frame{
		Cmd:     cmdSubscribe,
		Headers: headers{ReqID: authReqID},
		Body: map[string]any{
			"bot_id": p.config.BotID,
			"secret": p.config.Secret,
		},
	}); err != nil {
		p.setState("error")
		return fmt.Errorf("send auth: %w", err)
	}
	if err := p.waitAuth(ctx, conn, authReqID); err != nil {
		p.setState("error")
		return err
	}
	p.setActive(conn, &writeMu)
	defer p.clearActive(conn)
	p.setState("connected")

	heartbeatDone := make(chan struct{})
	go p.heartbeat(runCtx, conn, &writeMu, heartbeatDone)
	defer func() {
		cancel()
		<-heartbeatDone
		p.failAllPending("connection closed")
	}()

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
		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue
		}
		var inbound frame
		if err := json.Unmarshal(raw, &inbound); err != nil {
			p.config.Logger.Warn("wecom frame decode failed", "err", err.Error(), "raw_len", len(raw))
			continue
		}
		if inbound.Cmd == "" {
			p.resolvePending(inbound.Headers.ReqID, inbound)
			continue
		}
		if inbound.Cmd == cmdEventCallback {
			if eventType := uvim.StringValue(uvim.MapStringAny(inbound.Body["event"])["eventtype"]); eventType == "disconnected_event" {
				p.setState("disconnected")
				return fmt.Errorf("wecom disconnected_event: another connector is active")
			}
			continue
		}
		if inbound.Cmd != cmdCallback {
			continue
		}
		event, ok := p.decodeMessage(inbound)
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
	if len(msg.Resources) > 0 || hasNonTextElements(msg.Elements) {
		return uvim.SendResult{}, fmt.Errorf("wecom send: resources and rich elements are not supported")
	}
	text := uvim.TrimOutboundText(msg.Text, 20000)
	if text == "" && len(msg.Elements) > 0 {
		text = uvim.TrimOutboundText(textFromElements(msg.Elements), 20000)
	}
	if text == "" {
		return uvim.SendResult{}, fmt.Errorf("wecom send: text is required")
	}
	conn, writeMu, err := p.waitActive(ctx)
	if err != nil {
		return uvim.SendResult{}, err
	}
	reqID := uvim.FirstNonEmpty(msg.Referrer.ReplyToken, p.reqID(cmdSend))
	out := frame{Headers: headers{ReqID: reqID}}
	if msg.Referrer.ReplyToken != "" {
		streamID := "uv-" + uvim.SafeSegment(uvim.FirstNonEmpty(msg.Referrer.MessageID, msg.ID, reqID))
		out.Cmd = cmdRespond
		out.Body = map[string]any{
			"msgtype": "stream",
			"stream": map[string]any{
				"id":      streamID,
				"content": text,
				"finish":  msg.Final,
			},
		}
		if err := p.withReplyLock(ctx, reqID, func() error {
			return p.sendWithAck(ctx, conn, writeMu, reqID, out)
		}); err != nil {
			return uvim.SendResult{}, err
		}
		return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: streamID, Time: time.Now().UTC()}, nil
	}
	recipient := strings.TrimSpace(msg.ChannelID)
	if recipient == "" {
		return uvim.SendResult{}, fmt.Errorf("wecom send: channel_id is required")
	}
	out.Cmd = cmdSend
	out.Body = map[string]any{
		"chatid":  recipient,
		"msgtype": "text",
		"text": map[string]any{
			"content": text,
		},
	}
	if err := p.sendWithAck(ctx, conn, writeMu, reqID, out); err != nil {
		return uvim.SendResult{}, err
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: reqID, Time: time.Now().UTC()}, nil
}

func (p *Provider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	ref := req.Resource
	if strings.TrimSpace(ref.URL) == "" {
		return ref, fmt.Errorf("wecom download: resource url is required")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, ref.URL, nil)
	if err != nil {
		return ref, err
	}
	store := p.store(req.Dir)
	if ref.Secret == "" {
		return store.SaveHTTP(ctx, httpReq, ref)
	}
	resp, err := p.config.HTTPClient.Do(httpReq)
	if err != nil {
		return ref, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ref, fmt.Errorf("http %d", resp.StatusCode)
	}
	maxBytes := p.maxResourceBytes()
	var buf bytes.Buffer
	n, err := buf.ReadFrom(io.LimitReader(resp.Body, maxBytes+33))
	if err != nil {
		return ref, err
	}
	if n > maxBytes+32 {
		return ref, fmt.Errorf("resource exceeds max size %d bytes", maxBytes)
	}
	plain, err := DecryptAttachment(buf.Bytes(), ref.Secret)
	if err != nil {
		return ref, err
	}
	if int64(len(plain)) > maxBytes {
		return ref, fmt.Errorf("resource exceeds max size %d bytes", maxBytes)
	}
	return store.Save(ctx, bytes.NewReader(plain), ref)
}

func (p *Provider) decodeMessage(in frame) (uvim.Event, bool) {
	body := in.Body
	if len(body) == 0 {
		return uvim.Event{}, false
	}
	msgType := uvim.StringValue(body["msgtype"])
	text := p.messageText(body, msgType)
	resources := p.messageResources(body, msgType)
	if strings.TrimSpace(text) == "" && len(resources) == 0 {
		return uvim.Event{}, false
	}
	chatType := uvim.StringValue(body["chattype"])
	channelType := uvim.ChannelDirect
	if strings.EqualFold(chatType, "group") {
		channelType = uvim.ChannelGroup
	}
	from := uvim.MapStringAny(body["from"])
	userID := uvim.StringValue(from["userid"])
	channelID := uvim.FirstNonEmpty(uvim.StringValue(body["chatid"]), userID)
	messageID := uvim.FirstNonEmpty(uvim.StringValue(body["msgid"]), in.Headers.ReqID)
	return uvim.Event{
		ID:        uvim.FirstNonEmpty(uvim.StringValue(body["msgid"]), in.Headers.ReqID),
		Type:      uvim.EventMessageCreate,
		Provider:  p.ID(),
		Connector: p.ConnectorID(),
		Time:      time.Now().UTC(),
		Login:     uvim.Login{Platform: p.ID(), Connector: p.ConnectorID(), ID: p.config.BotID},
		Channel:   uvim.Channel{ID: channelID, Type: channelType},
		User:      uvim.User{ID: userID},
		Message: uvim.Message{
			ID:        messageID,
			Type:      msgType,
			Text:      text,
			Elements:  elementsFromTextAndResources(text, resources),
			Resources: resources,
		},
		Referrer:  uvim.Referrer{MessageID: messageID, ChannelID: channelID, ReplyToken: in.Headers.ReqID},
		Addressed: channelType != uvim.ChannelGroup,
	}, true
}

func (p *Provider) messageText(body map[string]any, msgType string) string {
	switch msgType {
	case "text":
		return uvim.StringValue(uvim.MapStringAny(body["text"])["content"])
	case "voice":
		return uvim.StringValue(uvim.MapStringAny(body["voice"])["content"])
	case "mixed":
		items, _ := uvim.MapStringAny(body["mixed"])["msg_item"].([]any)
		var parts []string
		for _, itemValue := range items {
			item := uvim.MapStringAny(itemValue)
			switch uvim.StringValue(item["msgtype"]) {
			case "text":
				if content := uvim.StringValue(uvim.MapStringAny(item["text"])["content"]); content != "" {
					parts = append(parts, content)
				}
			case "image":
				parts = append(parts, "[Image]")
			case "file":
				parts = append(parts, "[File]")
			case "video":
				parts = append(parts, "[Video]")
			}
		}
		return strings.Join(parts, "\n")
	case "image":
		return "[Image]"
	case "file":
		return "[File]"
	case "video":
		return "[Video]"
	default:
		return ""
	}
}

func (p *Provider) messageResources(body map[string]any, msgType string) []uvim.ResourceRef {
	switch msgType {
	case "mixed":
		items, _ := uvim.MapStringAny(body["mixed"])["msg_item"].([]any)
		var refs []uvim.ResourceRef
		for _, itemValue := range items {
			item := uvim.MapStringAny(itemValue)
			if ref, ok := resourceFromBody(item, uvim.StringValue(item["msgtype"]), p.ID(), p.ConnectorID()); ok {
				refs = append(refs, ref)
			}
		}
		return refs
	case "image", "file", "video":
		if ref, ok := resourceFromBody(body, msgType, p.ID(), p.ConnectorID()); ok {
			return []uvim.ResourceRef{ref}
		}
	}
	return nil
}

func resourceFromBody(body map[string]any, msgType, provider, connector string) (uvim.ResourceRef, bool) {
	payload := uvim.MapStringAny(body[msgType])
	if len(payload) == 0 {
		return uvim.ResourceRef{}, false
	}
	ref := uvim.ResourceRef{
		Provider:  provider,
		Connector: connector,
		Kind:      msgType,
		Name:      uvim.FirstNonEmpty(uvim.StringValue(payload["file_name"]), uvim.StringValue(payload["filename"]), uvim.StringValue(payload["name"])),
		Key:       uvim.FirstNonEmpty(uvim.StringValue(payload["media_id"]), uvim.StringValue(payload["file_id"]), uvim.StringValue(payload["fileid"])),
		URL:       uvim.FirstNonEmpty(uvim.StringValue(payload["url"]), uvim.StringValue(payload["download_url"])),
		Secret:    uvim.StringValue(payload["aeskey"]),
	}
	return ref, ref.URL != ""
}

func elementsFromTextAndResources(text string, refs []uvim.ResourceRef) []uvim.Element {
	var out []uvim.Element
	if strings.TrimSpace(text) != "" {
		out = append(out, uvim.Text(text))
	}
	for _, ref := range refs {
		out = append(out, uvim.File(ref.Sanitized()))
	}
	return out
}

func (p *Provider) waitAuth(ctx context.Context, conn WSConn, reqID string) error {
	deadline := p.now().Add(p.config.AckTimeout)
	for {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return err
		}
		msgType, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("auth read: %w", err)
		}
		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue
		}
		var ack frame
		if err := json.Unmarshal(raw, &ack); err != nil {
			continue
		}
		if ack.Headers.ReqID != reqID {
			continue
		}
		if ack.ErrCode != nil && *ack.ErrCode != 0 {
			return fmt.Errorf("wecom auth failed: errcode=%d errmsg=%q", *ack.ErrCode, ack.ErrMsg)
		}
		return nil
	}
}

func (p *Provider) heartbeat(ctx context.Context, conn WSConn, writeMu *sync.Mutex, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(p.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reqID := p.reqID(cmdHeartbeat)
			hctx, cancel := context.WithTimeout(ctx, p.config.AckTimeout)
			err := p.sendWithAck(hctx, conn, writeMu, reqID, frame{Cmd: cmdHeartbeat, Headers: headers{ReqID: reqID}})
			cancel()
			if err != nil {
				p.config.Logger.Warn("wecom heartbeat failed", "err", err.Error())
				_ = conn.Close()
				return
			}
		}
	}
}

func (p *Provider) sendWithAck(ctx context.Context, conn WSConn, writeMu *sync.Mutex, reqID string, out frame) error {
	ctx, cancel := context.WithTimeout(ctx, p.config.AckTimeout)
	defer cancel()
	ch := p.registerPending(reqID)
	defer p.unregisterPending(reqID)
	if err := p.writeFrame(conn, writeMu, out); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ack := <-ch:
		if ack.ErrCode != nil && *ack.ErrCode != 0 {
			return fmt.Errorf("wecom ack error: errcode=%d errmsg=%q", *ack.ErrCode, ack.ErrMsg)
		}
		return nil
	}
}

func (p *Provider) writeFrame(conn WSConn, writeMu *sync.Mutex, out frame) error {
	raw, err := json.Marshal(out)
	if err != nil {
		return err
	}
	writeMu.Lock()
	defer writeMu.Unlock()
	if err := conn.SetWriteDeadline(p.now().Add(p.config.WriteTimeout)); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, raw)
}

func (p *Provider) waitActive(ctx context.Context) (WSConn, *sync.Mutex, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if conn, writeMu := p.active(); conn != nil && writeMu != nil {
		return conn, writeMu, nil
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("wecom active connection unavailable: %w", ctx.Err())
		case <-ticker.C:
			if conn, writeMu := p.active(); conn != nil && writeMu != nil {
				return conn, writeMu, nil
			}
		}
	}
}

func (p *Provider) active() (WSConn, *sync.Mutex) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	return p.activeConn, p.activeWrite
}

func (p *Provider) setActive(conn WSConn, writeMu *sync.Mutex) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	p.activeConn = conn
	p.activeWrite = writeMu
}

func (p *Provider) clearActive(conn WSConn) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	if p.activeConn == conn {
		p.activeConn = nil
		p.activeWrite = nil
	}
	p.state = "stopped"
}

func (p *Provider) setState(state string) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	p.state = state
}

func (p *Provider) registerPending(reqID string) chan frame {
	ch := make(chan frame, 1)
	p.mu.Lock()
	p.pending[reqID] = ch
	p.mu.Unlock()
	return ch
}

func (p *Provider) unregisterPending(reqID string) {
	p.mu.Lock()
	delete(p.pending, reqID)
	p.mu.Unlock()
}

func (p *Provider) resolvePending(reqID string, in frame) {
	if reqID == "" {
		return
	}
	p.mu.Lock()
	ch := p.pending[reqID]
	p.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- in:
	default:
	}
}

func (p *Provider) failAllPending(reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	code := -1
	for reqID, ch := range p.pending {
		select {
		case ch <- frame{Headers: headers{ReqID: reqID}, ErrCode: &code, ErrMsg: reason}:
		default:
		}
		delete(p.pending, reqID)
	}
}

func (p *Provider) withReplyLock(ctx context.Context, reqID string, fn func() error) error {
	lock, err := p.acquireReplyLock(ctx, reqID)
	if err != nil {
		return err
	}
	defer p.releaseReplyLock(reqID, lock)
	return fn()
}

func (p *Provider) acquireReplyLock(ctx context.Context, reqID string) (*replyLock, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if reqID == "" {
		reqID = "default"
	}
	p.replyLocksMu.Lock()
	lock := p.replyLocks[reqID]
	if lock == nil {
		lock = &replyLock{ch: make(chan struct{}, 1)}
		lock.ch <- struct{}{}
		p.replyLocks[reqID] = lock
	}
	lock.refs++
	p.replyLocksMu.Unlock()
	select {
	case <-lock.ch:
		return lock, nil
	case <-ctx.Done():
		p.releaseReplyLockRef(reqID, lock)
		return nil, ctx.Err()
	}
}

func (p *Provider) releaseReplyLock(reqID string, lock *replyLock) {
	if reqID == "" {
		reqID = "default"
	}
	if lock == nil {
		return
	}
	select {
	case lock.ch <- struct{}{}:
	default:
	}
	p.releaseReplyLockRef(reqID, lock)
}

func (p *Provider) releaseReplyLockRef(reqID string, lock *replyLock) {
	if reqID == "" {
		reqID = "default"
	}
	if lock == nil {
		return
	}
	p.replyLocksMu.Lock()
	lock.refs--
	if lock.refs == 0 {
		delete(p.replyLocks, reqID)
	}
	p.replyLocksMu.Unlock()
}

func (p *Provider) reqID(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, p.now().UnixNano())
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

func (p *Provider) maxResourceBytes() int64 {
	if p.config.ResourceStore != nil && p.config.ResourceStore.MaxBytes > 0 {
		return p.config.ResourceStore.MaxBytes
	}
	return uvim.DefaultResourceMaxBytes
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
