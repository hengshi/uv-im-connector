package matrix

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
)

type Config struct {
	ConnectorID   string
	BaseURL       string
	Token         string
	WebhookSecret string
	ResourceStore *uvim.ResourceStore
	HTTPClient    *http.Client
}

type Provider struct {
	base   *httpchannel.Provider
	config Config
}

func New(config Config) (*Provider, error) {
	base, err := httpchannel.New(httpchannel.Config{
		ProviderID:        "matrix",
		ConnectorID:       firstNonEmpty(config.ConnectorID, "matrix"),
		BaseURL:           config.BaseURL,
		Token:             config.Token,
		WebhookSecret:     config.WebhookSecret,
		ResourceStore:     config.ResourceStore,
		HTTPClient:        config.HTTPClient,
		Decode:            Decode,
		Send:              Send,
		ParseSendResponse: ParseSendResponse,
		Capabilities: uvim.Capabilities{
			Inbound:         true,
			Outbound:        true,
			DirectMessage:   true,
			GroupMessage:    true,
			ReplyMessage:    true,
			ProactiveDirect: true,
			ProactiveGroup:  true,
			TargetKinds:     []string{uvim.TargetConversation},
			ChannelTypes:    []string{uvim.ChannelRoom},
			UploadResource:  config.ResourceStore != nil,
		},
	})
	if err != nil {
		return nil, err
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Provider{base: base, config: config}, nil
}

func (p *Provider) ID() string          { return p.base.ID() }
func (p *Provider) ConnectorID() string { return p.base.ConnectorID() }
func (p *Provider) Capabilities() uvim.Capabilities {
	return p.base.Capabilities()
}
func (p *Provider) Run(ctx context.Context, sink uvim.EventSink) error { return p.base.Run(ctx, sink) }
func (p *Provider) Health(ctx context.Context) uvim.Health             { return p.base.Health(ctx) }
func (p *Provider) ServeWebhook(w http.ResponseWriter, req *http.Request, sink uvim.EventSink) {
	p.base.ServeWebhook(w, req, sink)
}
func (p *Provider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	return p.base.Download(ctx, req)
}

func (p *Provider) Send(ctx context.Context, msg uvim.OutboundMessage) (result uvim.SendResult, err error) {
	defer func() {
		err = uvim.NewProviderSendOperationError("matrix send", err)
	}()
	if len(msg.Resources) == 0 {
		return p.base.Send(ctx, msg)
	}
	if err := uvim.ValidateOutboundTarget(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("matrix send: %w", err)
	}
	if err := uvim.ValidateOutboundResources(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("matrix send: %w", err)
	}
	if len(msg.Resources) != 1 {
		return uvim.SendResult{}, fmt.Errorf("matrix send: one resource per message is supported")
	}
	if strings.TrimSpace(msg.Text) != "" || len(msg.Elements) > 0 {
		return uvim.SendResult{}, fmt.Errorf("matrix send: text, elements, and resources must be sent separately")
	}
	return p.sendResource(ctx, msg, msg.Resources[0])
}

func (p *Provider) sendResource(ctx context.Context, msg uvim.OutboundMessage, ref uvim.ResourceRef) (result uvim.SendResult, err error) {
	defer func() {
		err = uvim.NewProviderSendOperationError("matrix upload", err)
	}()
	if p.config.ResourceStore == nil || !strings.HasPrefix(strings.TrimSpace(ref.InternalURL), "internal://") {
		return uvim.SendResult{}, fmt.Errorf("matrix upload: internal resource is required")
	}
	file, _, err := p.config.ResourceStore.Open(ref.InternalURL)
	if err != nil {
		return uvim.SendResult{}, uvim.NewProviderSendError("matrix resource is unavailable", err)
	}
	data, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil {
		return uvim.SendResult{}, uvim.NewProviderSendError("matrix resource read failed", readErr)
	}
	if closeErr != nil {
		return uvim.SendResult{}, uvim.NewProviderSendError("matrix resource close failed", closeErr)
	}
	if len(data) == 0 {
		return uvim.SendResult{}, fmt.Errorf("matrix upload: empty resources are not supported")
	}
	name := uvim.ResourceUploadName(0, ref, ref.MIME)
	uploadURL := strings.TrimRight(p.config.BaseURL, "/") + "/_matrix/media/v3/upload?filename=" + url.QueryEscape(name)
	uploadReq, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, bytes.NewReader(data))
	if err != nil {
		return uvim.SendResult{}, err
	}
	p.authorize(uploadReq)
	if strings.TrimSpace(ref.MIME) != "" {
		uploadReq.Header.Set("Content-Type", ref.MIME)
	}
	uploadRaw, err := p.do(uploadReq, "matrix upload")
	if err != nil {
		return uvim.SendResult{}, err
	}
	var upload struct {
		ContentURI string `json:"content_uri"`
	}
	if err := json.Unmarshal(uploadRaw, &upload); err != nil {
		return uvim.SendResult{}, fmt.Errorf("matrix upload: decode response: %w", err)
	}
	if strings.TrimSpace(upload.ContentURI) == "" {
		missingErr := fmt.Errorf("matrix upload: content_uri missing")
		return uvim.SendResult{}, uvim.NewProviderSendLogError("matrix upload: content URI missing", missingErr)
	}
	target := msg.ResolvedTarget()
	txnID := url.PathEscape(uvim.FirstNonEmpty(msg.ID, uvim.NewID("txn")))
	endpoint := strings.TrimRight(p.config.BaseURL, "/") + "/_matrix/client/v3/rooms/" + url.PathEscape(target.ID) + "/send/m.room.message/" + txnID
	body := map[string]any{
		"msgtype": matrixMessageType(ref.Kind),
		"body":    name,
		"url":     upload.ContentURI,
		"info":    map[string]any{"mimetype": ref.MIME, "size": len(data)},
	}
	if msg.Referrer.MessageID != "" {
		body["m.relates_to"] = map[string]any{"m.in_reply_to": map[string]string{"event_id": msg.Referrer.MessageID}}
	}
	raw, _ := json.Marshal(body)
	sendReq, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(raw))
	if err != nil {
		return uvim.SendResult{}, err
	}
	p.authorize(sendReq)
	sendReq.Header.Set("Content-Type", "application/json; charset=utf-8")
	responseRaw, err := p.do(sendReq, "matrix send")
	if err != nil {
		return uvim.SendResult{}, err
	}
	messageID, err := ParseSendResponse(responseRaw)
	if err != nil {
		return uvim.SendResult{}, err
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: messageID, Time: time.Now().UTC()}, nil
}

func (p *Provider) authorize(req *http.Request) {
	if token := strings.TrimSpace(p.config.Token); token != "" {
		req.Header.Set("Authorization", httpchannel.Authorization(token))
	}
}

func (p *Provider) do(req *http.Request, operation string) ([]byte, error) {
	resp, err := p.config.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	publicOperation := operation
	if operation == "matrix upload" {
		publicOperation = "matrix send"
	}
	return httpchannel.ReadSendResponseWithPublicOperation(resp, operation, publicOperation)
}

func matrixMessageType(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case uvim.ElementImage:
		return "m.image"
	case uvim.ElementAudio:
		return "m.audio"
	case uvim.ElementVideo:
		return "m.video"
	default:
		return "m.file"
	}
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var event struct {
		EventID string `json:"event_id"`
		RoomID  string `json:"room_id"`
		Sender  string `json:"sender"`
		Type    string `json:"type"`
		Content struct {
			Body    string `json:"body"`
			MsgType string `json:"msgtype"`
			URL     string `json:"url"`
			Info    struct {
				MIME string `json:"mimetype"`
				Size int64  `json:"size"`
			} `json:"info"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return uvim.Event{}, false, err
	}
	if event.EventID == "" || event.Type != "m.room.message" {
		return uvim.Event{}, false, nil
	}
	refs := matrixResources(event.Content.URL, event.Content.Body, event.Content.MsgType, event.Content.Info.MIME, event.Content.Info.Size, config)
	return uvim.Event{
		ID:        event.EventID,
		Type:      uvim.EventMessageCreate,
		Provider:  "matrix",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: event.RoomID, Type: uvim.ChannelRoom},
		User:      uvim.User{ID: event.Sender},
		Message:   uvim.Message{ID: event.EventID, Text: event.Content.Body, Type: event.Content.MsgType, Resources: refs},
		Referrer:  uvim.Referrer{MessageID: event.EventID, ChannelID: event.RoomID, Target: &uvim.OutboundTarget{ID: event.RoomID, Kind: uvim.TargetConversation}},
		Addressed: true,
	}, true, nil
}

func Send(msg uvim.OutboundMessage, _ httpchannel.Config) (httpchannel.Request, error) {
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return httpchannel.Request{}, fmt.Errorf("matrix send: target room id is required")
	}
	txnID := url.PathEscape(uvim.FirstNonEmpty(msg.ID, uvim.NewID("txn")))
	roomID := url.PathEscape(target.ID)
	body := map[string]any{"msgtype": "m.text", "body": msg.Text}
	if msg.Referrer.MessageID != "" {
		body["m.relates_to"] = map[string]any{
			"m.in_reply_to": map[string]string{"event_id": msg.Referrer.MessageID},
		}
	}
	return httpchannel.Request{
		Method: "PUT",
		Path:   "/_matrix/client/v3/rooms/" + roomID + "/send/m.room.message/" + txnID,
		Body:   body,
	}, nil
}

func ParseSendResponse(raw []byte) (string, error) {
	var response struct {
		EventID string `json:"event_id"`
		ErrCode string `json:"errcode"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if response.EventID == "" {
		businessErr := fmt.Errorf("event id missing: errcode=%q error=%q", response.ErrCode, response.Error)
		return "", uvim.NewProviderResponseError(raw, businessErr.Error(), businessErr)
	}
	return response.EventID, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func matrixResources(rawURL, name, msgType, mime string, size int64, config httpchannel.Config) []uvim.ResourceRef {
	if rawURL == "" || msgType == "m.text" {
		return nil
	}
	downloadURL := rawURL
	if strings.HasPrefix(rawURL, "mxc://") {
		downloadURL = matrixDownloadURL(config.BaseURL, rawURL)
	}
	if downloadURL == "" {
		return nil
	}
	kind := uvim.ResourceKindFromMIME(mime, uvim.ElementFile)
	switch strings.ToLower(strings.TrimSpace(msgType)) {
	case "m.image":
		kind = uvim.ElementImage
	case "m.audio":
		kind = uvim.ElementAudio
	case "m.video":
		kind = uvim.ElementVideo
	}
	return []uvim.ResourceRef{{
		Provider:  "matrix",
		Connector: config.ConnectorID,
		Kind:      kind,
		Name:      name,
		URL:       downloadURL,
		MIME:      mime,
		SizeBytes: size,
		Secret:    config.Token,
	}}
}

func matrixDownloadURL(baseURL, mxc string) string {
	baseURL = strings.TrimRight(baseURL, "/")
	if baseURL == "" || !strings.HasPrefix(mxc, "mxc://") {
		return ""
	}
	parts := strings.SplitN(strings.TrimPrefix(mxc, "mxc://"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return ""
	}
	return baseURL + "/_matrix/media/v3/download/" + url.PathEscape(parts[0]) + "/" + url.PathEscape(parts[1])
}
