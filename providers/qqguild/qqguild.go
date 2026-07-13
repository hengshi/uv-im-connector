package qqguild

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
		baseURL = "https://api.sgroup.qq.com"
	}
	return httpchannel.New(httpchannel.Config{
		ProviderID:        "qqguild",
		ConnectorID:       firstNonEmpty(config.ConnectorID, "qqguild"),
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
			TargetKinds:     []string{uvim.TargetUser, uvim.TargetGroup, uvim.TargetChannel},
			ChannelTypes:    []string{uvim.ChannelDirect, uvim.ChannelGroup},
		},
	})
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var msg struct {
		ID          string `json:"id"`
		ChannelID   string `json:"channel_id"`
		GuildID     string `json:"guild_id"`
		GroupOpenID string `json:"group_openid"`
		GroupID     string `json:"group_id"`
		Content     string `json:"content"`
		Author      struct {
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
	channelID := firstNonEmpty(msg.ChannelID, msg.GroupOpenID, msg.GroupID, msg.Author.ID)
	if msg.ID == "" || channelID == "" {
		return uvim.Event{}, false, nil
	}
	channelType := uvim.ChannelGroup
	targetKind := uvim.TargetChannel
	if msg.ChannelID == "" && msg.GroupOpenID == "" && msg.GroupID == "" {
		channelType = uvim.ChannelDirect
		targetKind = uvim.TargetUser
	} else if msg.ChannelID == "" {
		targetKind = uvim.TargetGroup
	}
	refs := make([]uvim.ResourceRef, 0, len(msg.Attachments))
	for _, attachment := range msg.Attachments {
		if attachment.URL == "" {
			continue
		}
		refs = append(refs, uvim.ResourceRef{
			Provider:  "qqguild",
			Connector: config.ConnectorID,
			Kind:      uvim.ResourceKindFromMIME(attachment.ContentType, uvim.ElementFile),
			Name:      attachment.Filename,
			URL:       attachment.URL,
			MIME:      attachment.ContentType,
			SizeBytes: attachment.Size,
			Secret:    config.Token,
		})
	}
	return uvim.Event{ID: msg.ID, Type: uvim.EventMessageCreate, Provider: "qqguild", Connector: config.ConnectorID, Channel: uvim.Channel{ID: channelID, Type: channelType}, User: uvim.User{ID: msg.Author.ID, Name: msg.Author.Username}, Message: uvim.Message{ID: msg.ID, Text: msg.Content, Type: "message", Resources: refs}, Referrer: uvim.Referrer{MessageID: msg.ID, ChannelID: channelID, Target: &uvim.OutboundTarget{ID: channelID, Kind: targetKind}}, Addressed: true}, true, nil
}

func Send(msg uvim.OutboundMessage, config httpchannel.Config) (httpchannel.Request, error) {
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return httpchannel.Request{}, fmt.Errorf("qqguild send: target channel id is required")
	}
	header := map[string][]string{}
	if auth := httpchannel.BotAuthorization(config.Token); auth != "" {
		header["Authorization"] = []string{auth}
	}
	body := map[string]any{"content": msg.Text}
	if msg.Referrer.MessageID != "" {
		body["msg_id"] = msg.Referrer.MessageID
	}
	path := "/channels/" + target.ID + "/messages"
	if target.Kind == uvim.TargetUser {
		path = "/v2/users/" + target.ID + "/messages"
		body["msg_type"] = 0
	} else if target.Kind == uvim.TargetGroup {
		path = "/v2/groups/" + target.ID + "/messages"
		body["msg_type"] = 0
	}
	return httpchannel.Request{Path: path, Body: body, Header: header}, nil
}

func ParseSendResponse(raw []byte) (string, error) {
	var response struct {
		ID      string `json:"id"`
		Code    any    `json:"code"`
		Message string `json:"message"`
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
