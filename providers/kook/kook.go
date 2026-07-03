package kook

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
		baseURL = "https://www.kookapp.cn"
	}
	return httpchannel.New(httpchannel.Config{
		ProviderID:    "kook",
		ConnectorID:   firstNonEmpty(config.ConnectorID, "kook"),
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
	return uvim.Event{
		ID:        env.D.MsgID,
		Type:      uvim.EventMessageCreate,
		Provider:  "kook",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: env.D.TargetID, Type: uvim.ChannelGroup},
		User:      uvim.User{ID: env.D.AuthorID},
		Message:   uvim.Message{ID: env.D.MsgID, Text: env.D.Content, Type: "message", Resources: refs},
		Referrer:  uvim.Referrer{MessageID: env.D.MsgID, ChannelID: env.D.TargetID},
		Addressed: true,
	}, true, nil
}

func Send(msg uvim.OutboundMessage, config httpchannel.Config) (httpchannel.Request, error) {
	header := map[string][]string{}
	if auth := httpchannel.BotAuthorization(config.Token); auth != "" {
		header["Authorization"] = []string{auth}
	}
	return httpchannel.Request{Path: "/api/v3/message/create", Body: map[string]any{"target_id": msg.ChannelID, "content": msg.Text, "type": 1}, Header: header}, nil
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
