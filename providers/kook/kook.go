package kook

import (
	"encoding/json"
	"fmt"
	"strings"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
)

type Config struct {
	ConnectorID   string
	BaseURL       string
	Token         string
	WebhookSecret string
}

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
			TargetKinds:     []string{uvim.TargetUser, uvim.TargetChannel},
			ChannelTypes:    []string{uvim.ChannelDirect, uvim.ChannelGroup},
		},
	})
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
	body := map[string]any{"target_id": target.ID, "content": msg.Text, "type": 1}
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
