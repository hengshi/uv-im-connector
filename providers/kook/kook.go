package kook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"

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

const (
	kookResourceURLKey  = "uv_kook_resource_url"
	kookResourceKindKey = "uv_kook_resource_kind"
	kookResourceNameKey = "uv_kook_resource_name"
)

func New(config Config) (*httpchannel.Provider, error) {
	baseURL := config.BaseURL
	if baseURL == "" {
		baseURL = "https://www.kookapp.cn"
	}
	return httpchannel.New(httpchannel.Config{
		ProviderID:        "kook",
		ConnectorID:       firstNonEmpty(config.ConnectorID, "kook"),
		BaseURL:           baseURL,
		Token:             config.Token,
		WebhookSecret:     config.WebhookSecret,
		ResourceStore:     config.ResourceStore,
		HTTPClient:        config.HTTPClient,
		Decode:            Decode,
		PrepareSend:       prepareSend,
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
			TargetKinds:     []string{uvim.TargetUser, uvim.TargetChannel},
			UploadResource:  config.ResourceStore != nil,
			ResourceKinds:   []string{uvim.ElementImage, uvim.ElementAudio, uvim.ElementVideo, uvim.ElementFile},
			ChannelTypes:    []string{uvim.ChannelDirect, uvim.ChannelGroup},
		},
	})
}

func prepareSend(ctx context.Context, msg uvim.OutboundMessage, config httpchannel.Config) (uvim.OutboundMessage, error) {
	if len(msg.Resources) == 0 {
		return msg, nil
	}
	if len(msg.Resources) != 1 {
		return msg, fmt.Errorf("kook send: one resource per message is supported")
	}
	if strings.TrimSpace(msg.Text) != "" || len(msg.Elements) > 0 {
		return msg, fmt.Errorf("kook send: text, elements, and resources must be sent separately")
	}
	ref := msg.Resources[0]
	if config.ResourceStore == nil || !strings.HasPrefix(strings.TrimSpace(ref.InternalURL), "internal://") {
		return msg, fmt.Errorf("kook upload: internal resource is required")
	}
	file, _, err := config.ResourceStore.Open(ref.InternalURL)
	if err != nil {
		return msg, uvim.NewProviderSendError("kook resource is unavailable", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, uvim.DefaultResourceMaxBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return msg, uvim.NewProviderSendError("kook resource read failed", readErr)
	}
	if closeErr != nil {
		return msg, uvim.NewProviderSendError("kook resource close failed", closeErr)
	}
	if len(data) == 0 {
		return msg, fmt.Errorf("kook upload: empty resources are not supported")
	}
	if int64(len(data)) > uvim.DefaultResourceMaxBytes {
		return msg, fmt.Errorf("kook upload: resource exceeds %d bytes", uvim.DefaultResourceMaxBytes)
	}
	name := uvim.ResourceUploadName(0, ref, ref.MIME)
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, name))
	if strings.TrimSpace(ref.MIME) != "" {
		header.Set("Content-Type", ref.MIME)
	}
	part, err := writer.CreatePart(header)
	if err != nil {
		return msg, err
	}
	if _, err := part.Write(data); err != nil {
		return msg, err
	}
	if err := writer.Close(); err != nil {
		return msg, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(config.BaseURL, "/")+"/api/v3/asset/create", &body)
	if err != nil {
		return msg, err
	}
	if auth := httpchannel.BotAuthorization(config.Token); auth != "" {
		req.Header.Set("Authorization", auth)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	client := config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return msg, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return msg, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return msg, uvim.NewProviderSendError(fmt.Sprintf("kook upload: http %d", resp.StatusCode), fmt.Errorf("kook upload: http %d", resp.StatusCode))
	}
	var decoded struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			URL string `json:"url"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return msg, fmt.Errorf("kook upload: decode response: %w", err)
	}
	if decoded.Code != 0 {
		businessErr := fmt.Errorf("kook upload: code=%d message=%q", decoded.Code, decoded.Message)
		return msg, uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	if decoded.Data.URL == "" {
		return msg, fmt.Errorf("kook upload: response missing url")
	}
	if msg.Metadata == nil {
		msg.Metadata = map[string]string{}
	}
	msg.Metadata[kookResourceURLKey] = decoded.Data.URL
	msg.Metadata[kookResourceKindKey] = ref.Kind
	msg.Metadata[kookResourceNameKey] = name
	msg.Resources = nil
	msg.Text = decoded.Data.URL
	return msg, nil
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var env struct {
		S int `json:"s"`
		D struct {
			MsgID       string `json:"msg_id"`
			ChannelType string `json:"channel_type"`
			TargetID    string `json:"target_id"`
			AuthorID    string `json:"author_id"`
			Content     string `json:"content"`
			Type        int    `json:"type"`
		} `json:"d"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return uvim.Event{}, false, err
	}
	if env.D.MsgID == "" {
		return uvim.Event{}, false, nil
	}
	refs := kookResources(env.D.Type, env.D.Content, config)
	channelType := uvim.ChannelGroup
	channelID := env.D.TargetID
	target := uvim.OutboundTarget{ID: env.D.TargetID, Kind: uvim.TargetChannel}
	if strings.EqualFold(env.D.ChannelType, "PERSON") || strings.EqualFold(env.D.ChannelType, "direct") {
		channelType = uvim.ChannelDirect
		channelID = env.D.AuthorID
		target = uvim.OutboundTarget{ID: env.D.AuthorID, Kind: uvim.TargetUser}
	}
	return uvim.Event{
		ID:        env.D.MsgID,
		Type:      uvim.EventMessageCreate,
		Provider:  "kook",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: channelID, Type: channelType},
		User:      uvim.User{ID: env.D.AuthorID},
		Message:   uvim.Message{ID: env.D.MsgID, Text: env.D.Content, Type: "message", Resources: refs},
		Referrer:  uvim.Referrer{MessageID: env.D.MsgID, ChannelID: channelID, Target: &target},
		Addressed: true,
	}, true, nil
}

func Send(msg uvim.OutboundMessage, config httpchannel.Config) (httpchannel.Request, error) {
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return httpchannel.Request{}, fmt.Errorf("kook send: target id is required")
	}
	header := map[string][]string{}
	if auth := httpchannel.BotAuthorization(config.Token); auth != "" {
		header["Authorization"] = []string{auth}
	}
	path := "/api/v3/message/create"
	if target.Kind == uvim.TargetUser {
		path = "/api/v3/direct-message/create"
	}
	content := msg.Text
	messageType := 1
	if resourceURL := msg.Metadata[kookResourceURLKey]; resourceURL != "" {
		kind := strings.ToLower(strings.TrimSpace(msg.Metadata[kookResourceKindKey]))
		if kind == uvim.ElementImage {
			messageType = 2
			content = resourceURL
		} else {
			moduleType := "file"
			if kind == uvim.ElementAudio {
				moduleType = "audio"
			} else if kind == uvim.ElementVideo {
				moduleType = "video"
			}
			card := []map[string]any{{
				"type":  "card",
				"theme": "none",
				"modules": []map[string]string{{
					"type":  moduleType,
					"src":   resourceURL,
					"title": msg.Metadata[kookResourceNameKey],
				}},
			}}
			raw, err := json.Marshal(card)
			if err != nil {
				return httpchannel.Request{}, err
			}
			messageType = 10
			content = string(raw)
		}
	}
	body := map[string]any{"target_id": target.ID, "content": content, "type": messageType}
	if msg.Referrer.MessageID != "" {
		body["quote"] = msg.Referrer.MessageID
		body["reply_msg_id"] = msg.Referrer.MessageID
	}
	return httpchannel.Request{Path: path, Body: body, Header: header}, nil
}

func ParseSendResponse(raw []byte) (string, error) {
	var response struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			MessageID string `json:"msg_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if response.Code != 0 {
		businessErr := fmt.Errorf("code=%d message=%q", response.Code, response.Message)
		return "", uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	return response.Data.MessageID, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func kookResources(messageType int, content string, config httpchannel.Config) []uvim.ResourceRef {
	if !strings.HasPrefix(content, "http://") && !strings.HasPrefix(content, "https://") {
		return nil
	}
	kind := uvim.ElementFile
	switch messageType {
	case 2:
		kind = uvim.ElementImage
	case 3:
		kind = uvim.ElementVideo
	case 8, 9:
		kind = uvim.ElementAudio
	}
	return []uvim.ResourceRef{{
		Provider:  "kook",
		Connector: config.ConnectorID,
		Kind:      kind,
		URL:       content,
		Secret:    config.Token,
	}}
}
