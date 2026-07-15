package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
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
	baseURL := config.BaseURL
	if baseURL == "" {
		baseURL = "https://slack.com"
	}
	base, err := httpchannel.New(httpchannel.Config{
		ProviderID:        "slack",
		ConnectorID:       firstNonEmpty(config.ConnectorID, "slack"),
		BaseURL:           baseURL,
		Token:             config.Token,
		WebhookSecret:     config.WebhookSecret,
		ResourceStore:     config.ResourceStore,
		HTTPClient:        config.HTTPClient,
		Decode:            Decode,
		Send:              Send,
		ParseSendResponse: ParseSendResponse,
		Capabilities: uvim.Capabilities{
			Inbound:          true,
			Outbound:         true,
			DirectMessage:    true,
			GroupMessage:     true,
			ThreadReply:      true,
			ReplyMessage:     true,
			ProactiveDirect:  true,
			ProactiveGroup:   true,
			TargetKinds:      []string{uvim.TargetUser, uvim.TargetChannel, uvim.TargetConversation},
			UploadResource:   config.ResourceStore != nil,
			DownloadResource: true,
			ResourceKinds:    []string{uvim.ElementImage, uvim.ElementAudio, uvim.ElementVideo, uvim.ElementFile},
			ChannelTypes:     []string{uvim.ChannelDirect, uvim.ChannelGroup, uvim.ChannelThread},
		},
	})
	if err != nil {
		return nil, err
	}
	config.BaseURL = baseURL
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

func (p *Provider) Send(ctx context.Context, msg uvim.OutboundMessage) (uvim.SendResult, error) {
	if len(msg.Resources) == 0 {
		return p.base.Send(ctx, msg)
	}
	if err := uvim.ValidateOutboundTarget(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("slack send: %w", err)
	}
	if err := uvim.ValidateOutboundResources(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("slack send: %w", err)
	}
	if len(msg.Resources) != 1 {
		return uvim.SendResult{}, fmt.Errorf("slack send: one resource per message is supported")
	}
	if len(msg.Elements) > 0 {
		return uvim.SendResult{}, fmt.Errorf("slack send: rich elements and resources must be sent separately")
	}
	return p.sendResource(ctx, msg, msg.Resources[0])
}

func (p *Provider) sendResource(ctx context.Context, msg uvim.OutboundMessage, ref uvim.ResourceRef) (uvim.SendResult, error) {
	if p.config.ResourceStore == nil || !strings.HasPrefix(strings.TrimSpace(ref.InternalURL), "internal://") {
		return uvim.SendResult{}, fmt.Errorf("slack upload: internal resource is required")
	}
	file, _, err := p.config.ResourceStore.Open(ref.InternalURL)
	if err != nil {
		return uvim.SendResult{}, uvim.NewProviderSendError("slack resource is unavailable", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, uvim.DefaultResourceMaxBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return uvim.SendResult{}, uvim.NewProviderSendError("slack resource read failed", readErr)
	}
	if closeErr != nil {
		return uvim.SendResult{}, uvim.NewProviderSendError("slack resource close failed", closeErr)
	}
	if len(data) == 0 {
		return uvim.SendResult{}, fmt.Errorf("slack upload: empty resources are not supported")
	}
	if int64(len(data)) > uvim.DefaultResourceMaxBytes {
		return uvim.SendResult{}, fmt.Errorf("slack upload: resource exceeds %d bytes", uvim.DefaultResourceMaxBytes)
	}
	name := uvim.ResourceUploadName(0, ref, ref.MIME)
	channelID, err := p.resourceChannelID(ctx, msg.ResolvedTarget())
	if err != nil {
		return uvim.SendResult{}, err
	}
	form := url.Values{"filename": {name}, "length": {strconv.Itoa(len(data))}}
	initRaw, err := p.request(ctx, "/api/files.getUploadURLExternal", strings.NewReader(form.Encode()), "application/x-www-form-urlencoded")
	if err != nil {
		return uvim.SendResult{}, err
	}
	var initResponse struct {
		OK        bool   `json:"ok"`
		UploadURL string `json:"upload_url"`
		FileID    string `json:"file_id"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(initRaw, &initResponse); err != nil {
		return uvim.SendResult{}, fmt.Errorf("slack upload: decode init response: %w", err)
	}
	if !initResponse.OK || initResponse.UploadURL == "" || initResponse.FileID == "" {
		businessErr := fmt.Errorf("slack upload init: error=%q", initResponse.Error)
		return uvim.SendResult{}, uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	uploadReq, err := http.NewRequestWithContext(ctx, http.MethodPost, initResponse.UploadURL, bytes.NewReader(data))
	if err != nil {
		return uvim.SendResult{}, err
	}
	if strings.TrimSpace(ref.MIME) != "" {
		uploadReq.Header.Set("Content-Type", ref.MIME)
	} else {
		uploadReq.Header.Set("Content-Type", "application/octet-stream")
	}
	uploadResp, err := p.config.HTTPClient.Do(uploadReq)
	if err != nil {
		return uvim.SendResult{}, err
	}
	_, copyErr := io.Copy(io.Discard, io.LimitReader(uploadResp.Body, 1<<20))
	closeErr = uploadResp.Body.Close()
	if copyErr != nil {
		return uvim.SendResult{}, copyErr
	}
	if closeErr != nil {
		return uvim.SendResult{}, closeErr
	}
	if uploadResp.StatusCode < 200 || uploadResp.StatusCode >= 300 {
		return uvim.SendResult{}, uvim.NewProviderSendError(fmt.Sprintf("slack upload: http %d", uploadResp.StatusCode), fmt.Errorf("slack upload: http %d", uploadResp.StatusCode))
	}
	complete := map[string]any{
		"files":      []map[string]string{{"id": initResponse.FileID, "title": name}},
		"channel_id": channelID,
	}
	if strings.TrimSpace(msg.Text) != "" {
		complete["initial_comment"] = msg.Text
	}
	if threadID := uvim.FirstNonEmpty(msg.Referrer.ThreadID, msg.Referrer.MessageID); threadID != "" {
		complete["thread_ts"] = threadID
	}
	completeRaw, _ := json.Marshal(complete)
	responseRaw, err := p.request(ctx, "/api/files.completeUploadExternal", bytes.NewReader(completeRaw), "application/json; charset=utf-8")
	if err != nil {
		return uvim.SendResult{}, err
	}
	var completeResponse struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
		Files []struct {
			ID string `json:"id"`
		} `json:"files"`
	}
	if err := json.Unmarshal(responseRaw, &completeResponse); err != nil {
		return uvim.SendResult{}, fmt.Errorf("slack upload: decode completion response: %w", err)
	}
	if !completeResponse.OK {
		businessErr := fmt.Errorf("slack upload completion: error=%q", completeResponse.Error)
		return uvim.SendResult{}, uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	messageID := initResponse.FileID
	if len(completeResponse.Files) > 0 && completeResponse.Files[0].ID != "" {
		messageID = completeResponse.Files[0].ID
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: messageID, Time: time.Now().UTC()}, nil
}

func (p *Provider) resourceChannelID(ctx context.Context, target uvim.OutboundTarget) (string, error) {
	if target.Kind != uvim.TargetUser || strings.HasPrefix(strings.ToUpper(target.ID), "D") {
		return target.ID, nil
	}
	raw, _ := json.Marshal(map[string]string{"users": target.ID})
	responseRaw, err := p.request(ctx, "/api/conversations.open", bytes.NewReader(raw), "application/json; charset=utf-8")
	if err != nil {
		return "", err
	}
	var response struct {
		OK      bool   `json:"ok"`
		Error   string `json:"error"`
		Channel struct {
			ID string `json:"id"`
		} `json:"channel"`
	}
	if err := json.Unmarshal(responseRaw, &response); err != nil {
		return "", fmt.Errorf("slack upload: decode conversation response: %w", err)
	}
	if !response.OK || response.Channel.ID == "" {
		businessErr := fmt.Errorf("slack conversation open: error=%q", response.Error)
		return "", uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	return response.Channel.ID, nil
}

func (p *Provider) request(ctx context.Context, path string, body io.Reader, contentType string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(p.config.BaseURL, "/")+path, body)
	if err != nil {
		return nil, err
	}
	if token := strings.TrimSpace(p.config.Token); token != "" {
		req.Header.Set("Authorization", httpchannel.Authorization(token))
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := p.config.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, uvim.NewProviderSendError(fmt.Sprintf("slack send: http %d", resp.StatusCode), fmt.Errorf("slack send: http %d", resp.StatusCode))
	}
	return raw, nil
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var env struct {
		Type      string `json:"type"`
		Challenge string `json:"challenge"`
		Event     struct {
			Type        string `json:"type"`
			User        string `json:"user"`
			Channel     string `json:"channel"`
			ChannelType string `json:"channel_type"`
			Text        string `json:"text"`
			Timestamp   string `json:"ts"`
			ThreadTS    string `json:"thread_ts"`
			Files       []struct {
				ID       string `json:"id"`
				Name     string `json:"name"`
				URL      string `json:"url_private_download"`
				MIME     string `json:"mimetype"`
				Size     int64  `json:"size"`
				Filetype string `json:"filetype"`
			} `json:"files"`
		} `json:"event"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return uvim.Event{}, false, err
	}
	if env.Type != "" && env.Type != "event_callback" {
		return uvim.Event{}, false, nil
	}
	if env.Event.Type != "" && env.Event.Type != "message" {
		return uvim.Event{}, false, nil
	}
	refs := make([]uvim.ResourceRef, 0, len(env.Event.Files))
	for _, file := range env.Event.Files {
		kind := uvim.ElementFile
		if strings.HasPrefix(file.MIME, "image/") {
			kind = uvim.ElementImage
		}
		if file.URL != "" {
			refs = append(refs, uvim.ResourceRef{Provider: "slack", Connector: config.ConnectorID, Kind: kind, Name: file.Name, URL: file.URL, MIME: file.MIME, SizeBytes: file.Size, Secret: config.Token})
		}
	}
	id := firstNonEmpty(env.Event.Timestamp, uvim.NewID("slack"))
	channelType := uvim.ChannelGroup
	if env.Event.ChannelType == "im" || strings.HasPrefix(env.Event.Channel, "D") {
		channelType = uvim.ChannelDirect
	}
	return uvim.Event{
		ID:        id,
		Type:      uvim.EventMessageCreate,
		Provider:  "slack",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: env.Event.Channel, Type: channelType},
		User:      uvim.User{ID: env.Event.User},
		Message:   uvim.Message{ID: id, Text: env.Event.Text, Type: "message", Resources: refs},
		Referrer:  uvim.Referrer{MessageID: id, ChannelID: env.Event.Channel, ThreadID: env.Event.ThreadTS, Target: &uvim.OutboundTarget{ID: env.Event.Channel, Kind: uvim.TargetChannel}},
		Addressed: true,
	}, true, nil
}

func Send(msg uvim.OutboundMessage, _ httpchannel.Config) (httpchannel.Request, error) {
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return httpchannel.Request{}, fmt.Errorf("slack send: target id is required")
	}
	body := map[string]any{"channel": target.ID, "text": msg.Text}
	if msg.Referrer.ThreadID != "" {
		body["thread_ts"] = msg.Referrer.ThreadID
	} else if msg.Referrer.MessageID != "" {
		body["thread_ts"] = msg.Referrer.MessageID
	}
	return httpchannel.Request{Path: "/api/chat.postMessage", Body: body}, nil
}

func ParseSendResponse(raw []byte) (string, error) {
	var response struct {
		OK      bool   `json:"ok"`
		Error   string `json:"error"`
		Warning string `json:"warning"`
		TS      string `json:"ts"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if !response.OK {
		businessErr := fmt.Errorf("error=%q warning=%q", response.Error, response.Warning)
		return "", uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	return response.TS, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
