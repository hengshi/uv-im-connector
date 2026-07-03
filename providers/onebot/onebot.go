package onebot

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
	return httpchannel.New(httpchannel.Config{
		ProviderID:    "onebot",
		ConnectorID:   firstNonEmpty(config.ConnectorID, "onebot"),
		BaseURL:       config.BaseURL,
		Token:         config.Token,
		WebhookSecret: config.WebhookSecret,
		Decode:        Decode,
		Send:          Send,
		Capabilities: uvim.Capabilities{
			Inbound:       true,
			Outbound:      true,
			DirectMessage: true,
			GroupMessage:  true,
			ChannelTypes:  []string{uvim.ChannelDirect, uvim.ChannelGroup},
		},
	})
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var msg struct {
		PostType    string          `json:"post_type"`
		MessageType string          `json:"message_type"`
		MessageID   int64           `json:"message_id"`
		UserID      int64           `json:"user_id"`
		GroupID     int64           `json:"group_id"`
		Message     json.RawMessage `json:"message"`
		RawMessage  string          `json:"raw_message"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return uvim.Event{}, false, err
	}
	if msg.PostType != "message" || msg.MessageID == 0 {
		return uvim.Event{}, false, nil
	}
	channelType := uvim.ChannelDirect
	channelID := fmt.Sprint(msg.UserID)
	if msg.GroupID != 0 {
		channelType = uvim.ChannelGroup
		channelID = fmt.Sprint(msg.GroupID)
	}
	messageID := fmt.Sprint(msg.MessageID)
	refs := onebotResources(msg.Message, config)
	return uvim.Event{
		ID:        messageID,
		Type:      uvim.EventMessageCreate,
		Provider:  "onebot",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: channelID, Type: channelType},
		User:      uvim.User{ID: fmt.Sprint(msg.UserID)},
		Message:   uvim.Message{ID: messageID, Text: msg.RawMessage, Type: msg.MessageType, Resources: refs},
		Referrer:  uvim.Referrer{MessageID: messageID, ChannelID: channelID},
		Addressed: true,
	}, true, nil
}

func Send(msg uvim.OutboundMessage, _ httpchannel.Config) (httpchannel.Request, error) {
	body := map[string]any{"message": msg.Text}
	if msg.ChannelType == uvim.ChannelGroup {
		body["group_id"] = msg.ChannelID
	} else {
		body["user_id"] = msg.ChannelID
	}
	return httpchannel.Request{Path: "/send_msg", Body: body}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func onebotResources(raw json.RawMessage, config httpchannel.Config) []uvim.ResourceRef {
	var segments []struct {
		Type string         `json:"type"`
		Data map[string]any `json:"data"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &segments) != nil {
		return nil
	}
	var refs []uvim.ResourceRef
	for _, segment := range segments {
		kind := onebotKind(segment.Type)
		if kind == "" {
			continue
		}
		rawURL := firstNonEmpty(uvim.StringValue(segment.Data["url"]), uvim.StringValue(segment.Data["file"]))
		if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
			continue
		}
		refs = append(refs, uvim.ResourceRef{
			Provider:  "onebot",
			Connector: config.ConnectorID,
			Kind:      kind,
			Name:      firstNonEmpty(uvim.StringValue(segment.Data["name"]), uvim.StringValue(segment.Data["file"])),
			URL:       rawURL,
		})
	}
	return refs
}

func onebotKind(segmentType string) string {
	switch strings.ToLower(strings.TrimSpace(segmentType)) {
	case "image":
		return uvim.ElementImage
	case "record", "audio", "voice":
		return uvim.ElementAudio
	case "video":
		return uvim.ElementVideo
	case "file":
		return uvim.ElementFile
	default:
		return ""
	}
}
