package qqguild

import (
	"encoding/json"
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
		ProviderID:    "qqguild",
		ConnectorID:   firstNonEmpty(config.ConnectorID, "qqguild"),
		BaseURL:       baseURL,
		Token:         config.Token,
		WebhookSecret: config.WebhookSecret,
		Decode:        Decode,
		Send:          Send,
		Capabilities: uvim.Capabilities{
			Inbound:      true,
			Outbound:     true,
			GroupMessage: true,
			ChannelTypes: []string{uvim.ChannelGroup},
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
	return uvim.Event{ID: msg.ID, Type: uvim.EventMessageCreate, Provider: "qqguild", Connector: config.ConnectorID, Channel: uvim.Channel{ID: msg.ChannelID, Type: uvim.ChannelGroup}, User: uvim.User{ID: msg.Author.ID, Name: msg.Author.Username}, Message: uvim.Message{ID: msg.ID, Text: msg.Content, Type: "message", Resources: refs}, Referrer: uvim.Referrer{MessageID: msg.ID, ChannelID: msg.ChannelID}, Addressed: true}, true, nil
}

func Send(msg uvim.OutboundMessage, config httpchannel.Config) (httpchannel.Request, error) {
	header := map[string][]string{}
	if auth := httpchannel.BotAuthorization(config.Token); auth != "" {
		header["Authorization"] = []string{auth}
	}
	return httpchannel.Request{Path: "/channels/" + msg.ChannelID + "/messages", Body: map[string]any{"content": msg.Text}, Header: header}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
