package wecom

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
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
	cmdUploadInit    = "aibot_upload_media_init"
	cmdUploadChunk   = "aibot_upload_media_chunk"
	cmdUploadFinish  = "aibot_upload_media_finish"
	mediaVoice       = "voice"
	uploadChunkSize  = 512 * 1024
	uploadMaxChunks  = 100
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
		ReplyMessage:     true,
		ProactiveDirect:  true,
		ProactiveGroup:   true,
		TargetKinds:      []string{uvim.TargetUser, uvim.TargetGroup, uvim.TargetConversation},
		UploadResource:   p.config.ResourceStore != nil,
		DownloadResource: true,
		ResourceKinds:    []string{uvim.ElementImage, uvim.ElementAudio, uvim.ElementVideo, uvim.ElementFile},
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
	if err := uvim.ValidateOutboundTarget(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("wecom send: %w", err)
	}
	if err := uvim.ValidateOutboundResources(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("wecom send: %w", err)
	}
	if len(msg.Resources) > 1 {
		return uvim.SendResult{}, fmt.Errorf("wecom send: one resource per message is supported")
	}
	if hasNonTextElements(msg.Elements) {
		return uvim.SendResult{}, fmt.Errorf("wecom send: rich elements are not supported")
	}
	text := uvim.TrimOutboundText(msg.Text, 20000)
	if text == "" && len(msg.Elements) > 0 {
		text = uvim.TrimOutboundText(textFromElements(msg.Elements), 20000)
	}
	if text != "" && len(msg.Resources) > 0 {
		return uvim.SendResult{}, fmt.Errorf("wecom send: text and resources must be sent separately")
	}
	if text == "" && len(msg.Resources) == 0 {
		return uvim.SendResult{}, fmt.Errorf("wecom send: text or resource is required")
	}
	conn, writeMu, err := p.waitActive(ctx)
	if err != nil {
		return uvim.SendResult{}, err
	}
	if len(msg.Resources) == 1 {
		media, err := p.uploadResource(ctx, conn, writeMu, msg.Resources[0])
		if err != nil {
			return uvim.SendResult{}, err
		}
		return p.sendMedia(ctx, conn, writeMu, msg, media)
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
	target := msg.ResolvedTarget()
	recipient := target.ID
	if recipient == "" {
		return uvim.SendResult{}, fmt.Errorf("wecom send: target id is required")
	}
	out.Cmd = cmdSend
	out.Body = map[string]any{
		"chatid":  recipient,
		"msgtype": "markdown",
		"markdown": map[string]any{
			"content": text,
		},
	}
	if err := p.sendWithAck(ctx, conn, writeMu, reqID, out); err != nil {
		return uvim.SendResult{}, err
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: reqID, Time: time.Now().UTC()}, nil
}

type uploadedMedia struct {
	kind    string
	mediaID string
}

func (p *Provider) uploadResource(ctx context.Context, conn WSConn, writeMu *sync.Mutex, ref uvim.ResourceRef) (uploadedMedia, error) {
	if p.config.ResourceStore == nil {
		return uploadedMedia{}, fmt.Errorf("wecom upload: resource store is not configured")
	}
	if !strings.HasPrefix(strings.TrimSpace(ref.InternalURL), "internal://") {
		return uploadedMedia{}, fmt.Errorf("wecom upload: internal resource is required")
	}
	file, _, err := p.config.ResourceStore.Open(ref.InternalURL)
	if err != nil {
		return uploadedMedia{}, uvim.NewProviderSendError("wecom resource is unavailable", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, int64(uploadChunkSize*uploadMaxChunks)+1))
	closeErr := file.Close()
	if readErr != nil {
		return uploadedMedia{}, uvim.NewProviderSendError("wecom resource read failed", readErr)
	}
	if closeErr != nil {
		return uploadedMedia{}, uvim.NewProviderSendError("wecom resource close failed", closeErr)
	}
	if len(data) == 0 {
		return uploadedMedia{}, fmt.Errorf("wecom upload: empty resources are not supported")
	}
	totalChunks, err := wecomUploadChunkCount(len(data))
	if err != nil {
		return uploadedMedia{}, err
	}
	kind := wecomMediaKind(ref.Kind)
	filename := uvim.ResourceUploadName(0, ref, ref.MIME)
	// WeCom requires MD5 as a transport checksum; it is not used for security.
	digest := md5.Sum(data) // #nosec G401 -- required by the WeCom upload protocol
	initReqID := p.reqID(cmdUploadInit)
	initAck, err := p.requestWithAck(ctx, conn, writeMu, initReqID, frame{
		Cmd:     cmdUploadInit,
		Headers: headers{ReqID: initReqID},
		Body: map[string]any{
			"type":         kind,
			"filename":     filename,
			"total_size":   len(data),
			"total_chunks": totalChunks,
			"md5":          fmt.Sprintf("%x", digest),
		},
	})
	if err != nil {
		return uploadedMedia{}, err
	}
	uploadID := uvim.StringValue(initAck.Body["upload_id"])
	if uploadID == "" {
		return uploadedMedia{}, fmt.Errorf("wecom upload: init response missing upload_id")
	}
	for index := 0; index < totalChunks; index++ {
		start := index * uploadChunkSize
		end := min(start+uploadChunkSize, len(data))
		chunkReqID := p.reqID(cmdUploadChunk)
		if _, err := p.requestWithAck(ctx, conn, writeMu, chunkReqID, frame{
			Cmd:     cmdUploadChunk,
			Headers: headers{ReqID: chunkReqID},
			Body: map[string]any{
				"upload_id":   uploadID,
				"chunk_index": index + 1,
				"base64_data": base64.StdEncoding.EncodeToString(data[start:end]),
			},
		}); err != nil {
			return uploadedMedia{}, err
		}
	}
	finishReqID := p.reqID(cmdUploadFinish)
	finishAck, err := p.requestWithAck(ctx, conn, writeMu, finishReqID, frame{
		Cmd:     cmdUploadFinish,
		Headers: headers{ReqID: finishReqID},
		Body:    map[string]any{"upload_id": uploadID},
	})
	if err != nil {
		return uploadedMedia{}, err
	}
	mediaID := uvim.StringValue(finishAck.Body["media_id"])
	if mediaID == "" {
		return uploadedMedia{}, fmt.Errorf("wecom upload: finish response missing media_id")
	}
	return uploadedMedia{kind: kind, mediaID: mediaID}, nil
}

func wecomUploadChunkCount(size int) (int, error) {
	if size <= 0 {
		return 0, fmt.Errorf("wecom upload: empty resources are not supported")
	}
	chunks := (size + uploadChunkSize - 1) / uploadChunkSize
	if chunks > uploadMaxChunks {
		return 0, fmt.Errorf("wecom upload: resource exceeds %d bytes", uploadChunkSize*uploadMaxChunks)
	}
	return chunks, nil
}

func (p *Provider) sendMedia(ctx context.Context, conn WSConn, writeMu *sync.Mutex, msg uvim.OutboundMessage, media uploadedMedia) (uvim.SendResult, error) {
	content := map[string]any{"media_id": media.mediaID}
	body := map[string]any{"msgtype": media.kind, media.kind: content}
	if msg.Referrer.ReplyToken != "" {
		reqID := msg.Referrer.ReplyToken
		err := p.withReplyLock(ctx, reqID, func() error {
			return p.sendWithAck(ctx, conn, writeMu, reqID, frame{Cmd: cmdRespond, Headers: headers{ReqID: reqID}, Body: body})
		})
		if err != nil {
			return uvim.SendResult{}, err
		}
		return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: reqID, Time: time.Now().UTC()}, nil
	}
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return uvim.SendResult{}, fmt.Errorf("wecom send: target id is required")
	}
	reqID := p.reqID(cmdSend)
	body["chatid"] = target.ID
	if err := p.sendWithAck(ctx, conn, writeMu, reqID, frame{Cmd: cmdSend, Headers: headers{ReqID: reqID}, Body: body}); err != nil {
		return uvim.SendResult{}, err
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: reqID, Time: time.Now().UTC()}, nil
}

func wecomMediaKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case uvim.ElementImage:
		return uvim.ElementImage
	case uvim.ElementVideo:
		return uvim.ElementVideo
	case uvim.ElementAudio:
		return mediaVoice
	default:
		return uvim.ElementFile
	}
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
	targetKind := uvim.TargetUser
	if channelType == uvim.ChannelGroup {
		targetKind = uvim.TargetGroup
	}
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
		Referrer:  uvim.Referrer{MessageID: messageID, ChannelID: channelID, ReplyToken: in.Headers.ReqID, Target: &uvim.OutboundTarget{ID: channelID, Kind: targetKind}},
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
	_, err := p.requestWithAck(ctx, conn, writeMu, reqID, out)
	return err
}

func (p *Provider) requestWithAck(ctx context.Context, conn WSConn, writeMu *sync.Mutex, reqID string, out frame) (frame, error) {
	ctx, cancel := context.WithTimeout(ctx, p.config.AckTimeout)
	defer cancel()
	ch := p.registerPending(reqID)
	defer p.unregisterPending(reqID)
	if err := p.writeFrame(conn, writeMu, out); err != nil {
		return frame{}, err
	}
	select {
	case <-ctx.Done():
		return frame{}, ctx.Err()
	case ack := <-ch:
		if ack.ErrCode != nil && *ack.ErrCode != 0 {
			sendErr := fmt.Errorf("wecom ack error: errcode=%d errmsg=%q", *ack.ErrCode, ack.ErrMsg)
			return frame{}, uvim.NewProviderSendError(sendErr.Error(), sendErr)
		}
		return ack, nil
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
	return uvim.NewID(prefix)
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
