package slack

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
		baseURL = "https://slack.com"
	}
	return httpchannel.New(httpchannel.Config{
		ProviderID:        "slack",
		ConnectorID:       firstNonEmpty(config.ConnectorID, "slack"),
		BaseURL:           baseURL,
		Token:             config.Token,
		WebhookSecret:     config.WebhookSecret,
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
			DownloadResource: true,
			ResourceKinds:    []string{uvim.ElementImage, uvim.ElementFile},
			ChannelTypes:     []string{uvim.ChannelDirect, uvim.ChannelGroup, uvim.ChannelThread},
		},
	})
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
