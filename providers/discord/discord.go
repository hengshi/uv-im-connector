package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

const maxAttachmentBytes = 10 * 1024 * 1024

func New(config Config) (*httpchannel.Provider, error) {
	baseURL := config.BaseURL
	if baseURL == "" {
		baseURL = "https://discord.com"
	}
	return httpchannel.New(httpchannel.Config{
		ProviderID:        "discord",
		ConnectorID:       firstNonEmpty(config.ConnectorID, "discord"),
		BaseURL:           baseURL,
		Token:             config.Token,
		WebhookSecret:     config.WebhookSecret,
		ResourceStore:     config.ResourceStore,
		HTTPClient:        config.HTTPClient,
		Decode:            Decode,
		PrepareSend:       PrepareSend,
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
			DownloadResource: true,
			UploadResource:   config.ResourceStore != nil,
			ResourceKinds:    []string{uvim.ElementImage, uvim.ElementVideo, uvim.ElementAudio, uvim.ElementFile},
			ChannelTypes:     []string{uvim.ChannelDirect, uvim.ChannelGroup, uvim.ChannelThread},
		},
	})
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var msg struct {
		ID        string `json:"id"`
		ChannelID string `json:"channel_id"`
		GuildID   string `json:"guild_id"`
		Content   string `json:"content"`
		Author    struct {
			ID       string `json:"id"`
			Username string `json:"username"`
		} `json:"author"`
		Attachments []struct {
			ID          string `json:"id"`
			Filename    string `json:"filename"`
			URL         string `json:"url"`
			ContentType string `json:"content_type"`
			Size        int64  `json:"size"`
		} `json:"attachments"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return uvim.Event{}, false, err
	}
	if msg.ID == "" || msg.ChannelID == "" {
		return uvim.Event{}, false, nil
	}
	refs := make([]uvim.ResourceRef, 0, len(msg.Attachments))
	for _, attachment := range msg.Attachments {
		kind := uvim.ElementFile
		switch {
		case strings.HasPrefix(attachment.ContentType, "image/"):
			kind = uvim.ElementImage
		case strings.HasPrefix(attachment.ContentType, "video/"):
			kind = uvim.ElementVideo
		case strings.HasPrefix(attachment.ContentType, "audio/"):
			kind = uvim.ElementAudio
		}
		if attachment.URL != "" {
			refs = append(refs, uvim.ResourceRef{Provider: "discord", Connector: config.ConnectorID, Kind: kind, Name: attachment.Filename, URL: attachment.URL, MIME: attachment.ContentType, SizeBytes: attachment.Size})
		}
	}
	channelType := uvim.ChannelGroup
	if msg.GuildID == "" {
		channelType = uvim.ChannelDirect
	}
	return uvim.Event{
		ID:        msg.ID,
		Type:      uvim.EventMessageCreate,
		Provider:  "discord",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: msg.ChannelID, Type: channelType},
		User:      uvim.User{ID: msg.Author.ID, Name: msg.Author.Username},
		Message:   uvim.Message{ID: msg.ID, Text: msg.Content, Type: "message", Resources: refs},
		Referrer:  uvim.Referrer{MessageID: msg.ID, ChannelID: msg.ChannelID, Target: &uvim.OutboundTarget{ID: msg.ChannelID, Kind: uvim.TargetChannel}},
		Addressed: true,
	}, true, nil
}

func Send(msg uvim.OutboundMessage, config httpchannel.Config) (httpchannel.Request, error) {
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return httpchannel.Request{}, fmt.Errorf("discord send: target id is required")
	}
	header := map[string][]string{}
	if auth := httpchannel.BotAuthorization(config.Token); auth != "" {
		header["Authorization"] = []string{auth}
	}
	body := map[string]any{}
	if strings.TrimSpace(msg.Text) != "" {
		body["content"] = msg.Text
	}
	if msg.Referrer.MessageID != "" {
		body["message_reference"] = map[string]string{"message_id": msg.Referrer.MessageID}
	}
	if len(msg.Resources) > 0 {
		if len(msg.Resources) != 1 {
			return httpchannel.Request{}, fmt.Errorf("discord send: one resource per message is supported")
		}
		ref := msg.Resources[0]
		if config.ResourceStore == nil || !strings.HasPrefix(strings.TrimSpace(ref.InternalURL), "internal://") {
			return httpchannel.Request{}, fmt.Errorf("discord upload: internal resource is required")
		}
		file, _, err := config.ResourceStore.Open(ref.InternalURL)
		if err != nil {
			return httpchannel.Request{}, uvim.NewProviderSendError("discord resource is unavailable", err)
		}
		data, readErr := io.ReadAll(io.LimitReader(file, maxAttachmentBytes+1))
		closeErr := file.Close()
		if readErr != nil {
			return httpchannel.Request{}, uvim.NewProviderSendError("discord resource read failed", readErr)
		}
		if closeErr != nil {
			return httpchannel.Request{}, uvim.NewProviderSendError("discord resource close failed", closeErr)
		}
		if len(data) == 0 {
			return httpchannel.Request{}, fmt.Errorf("discord upload: empty resources are not supported")
		}
		if len(data) > maxAttachmentBytes {
			return httpchannel.Request{}, fmt.Errorf("discord upload: resource exceeds %d bytes", maxAttachmentBytes)
		}
		name := uvim.ResourceUploadName(0, ref, ref.MIME)
		body["attachments"] = []map[string]any{{"id": 0, "filename": name}}
		payload, err := json.Marshal(body)
		if err != nil {
			return httpchannel.Request{}, err
		}
		return httpchannel.Request{
			Path:   "/api/v10/channels/" + target.ID + "/messages",
			Header: header,
			Multipart: &httpchannel.MultipartBody{
				Fields: map[string]string{"payload_json": string(payload)},
				Files:  []httpchannel.MultipartFile{{Field: "files[0]", Name: name, MIME: ref.MIME, Data: data}},
			},
		}, nil
	}
	return httpchannel.Request{Path: "/api/v10/channels/" + target.ID + "/messages", Body: body, Header: header}, nil
}

func PrepareSend(ctx context.Context, msg uvim.OutboundMessage, config httpchannel.Config) (uvim.OutboundMessage, error) {
	target := msg.ResolvedTarget()
	if msg.Target == nil || target.Kind != uvim.TargetUser {
		return msg, nil
	}
	raw, _ := json.Marshal(map[string]string{"recipient_id": target.ID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(config.BaseURL, "/")+"/api/v10/users/@me/channels", bytes.NewReader(raw))
	if err != nil {
		return msg, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	if auth := httpchannel.BotAuthorization(config.Token); auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := config.HTTPClient.Do(req)
	if err != nil {
		return msg, err
	}
	defer resp.Body.Close()
	responseRaw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return msg, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return msg, fmt.Errorf("discord create dm: http %d: %s", resp.StatusCode, strings.TrimSpace(string(responseRaw)))
	}
	var channel struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(responseRaw, &channel); err != nil {
		return msg, fmt.Errorf("discord create dm: decode response: %w", err)
	}
	if channel.ID == "" {
		return msg, fmt.Errorf("discord create dm: channel id missing")
	}
	msg.Target = &uvim.OutboundTarget{ID: channel.ID, Kind: uvim.TargetChannel}
	return msg, nil
}

func ParseSendResponse(raw []byte) (string, error) {
	var response struct {
		ID      string `json:"id"`
		Message string `json:"message"`
		Code    any    `json:"code"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if response.ID == "" {
		businessErr := fmt.Errorf("message id missing: code=%v message=%q", response.Code, response.Message)
		return "", uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	return response.ID, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
