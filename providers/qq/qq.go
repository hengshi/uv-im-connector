package qq

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
		ProviderID:        "qq",
		ConnectorID:       firstNonEmpty(config.ConnectorID, "qq"),
		BaseURL:           config.BaseURL,
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
			TargetKinds:     []string{uvim.TargetUser, uvim.TargetGroup},
			ChannelTypes:    []string{uvim.ChannelDirect, uvim.ChannelGroup},
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
		Sender      struct {
			Nickname string `json:"nickname"`
			Card     string `json:"card"`
		} `json:"sender"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return uvim.Event{}, false, err
	}
	if msg.PostType != "message" || msg.MessageID == 0 {
		return uvim.Event{}, false, nil
	}
	channelType := uvim.ChannelDirect
	channelID := fmt.Sprint(msg.UserID)
	targetKind := uvim.TargetUser
	if msg.GroupID != 0 {
		channelType = uvim.ChannelGroup
		channelID = fmt.Sprint(msg.GroupID)
		targetKind = uvim.TargetGroup
	}
	messageID := fmt.Sprint(msg.MessageID)
	refs := qqResources(msg.Message, config)
	return uvim.Event{
		ID:        messageID,
		Type:      uvim.EventMessageCreate,
		Provider:  "qq",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: channelID, Type: channelType},
		User:      uvim.User{ID: fmt.Sprint(msg.UserID), Name: firstNonEmpty(msg.Sender.Card, msg.Sender.Nickname)},
		Message:   uvim.Message{ID: messageID, Text: msg.RawMessage, Type: msg.MessageType, Resources: refs},
		Referrer:  uvim.Referrer{MessageID: messageID, ChannelID: channelID, Target: &uvim.OutboundTarget{ID: channelID, Kind: targetKind}},
		Addressed: true,
	}, true, nil
}

func Send(msg uvim.OutboundMessage, _ httpchannel.Config) (httpchannel.Request, error) {
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return httpchannel.Request{}, fmt.Errorf("qq send: target id is required")
	}
	body := map[string]any{"message": msg.Text}
	if msg.Referrer.MessageID != "" {
		body["message"] = []map[string]any{
			{"type": "reply", "data": map[string]string{"id": msg.Referrer.MessageID}},
			{"type": "text", "data": map[string]string{"text": msg.Text}},
		}
	}
	if target.Kind == uvim.TargetGroup {
		body["group_id"] = target.ID
	} else {
		body["user_id"] = target.ID
	}
	return httpchannel.Request{Path: "/send_msg", Body: body}, nil
}

func ParseSendResponse(raw []byte) (string, error) {
	var response struct {
		Status  string `json:"status"`
		RetCode int    `json:"retcode"`
		Message string `json:"message"`
		Wording string `json:"wording"`
		Data    struct {
			MessageID any `json:"message_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if response.Status != "ok" || response.RetCode != 0 {
		businessErr := fmt.Errorf("status=%q retcode=%d message=%q wording=%q", response.Status, response.RetCode, response.Message, response.Wording)
		return "", uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	if response.Data.MessageID == nil {
		return "", nil
	}
	return fmt.Sprint(response.Data.MessageID), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func qqResources(raw json.RawMessage, config httpchannel.Config) []uvim.ResourceRef {
	var segments []struct {
		Type string         `json:"type"`
		Data map[string]any `json:"data"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &segments) != nil {
		return nil
	}
	var refs []uvim.ResourceRef
	for _, segment := range segments {
		kind := qqKind(segment.Type)
		if kind == "" {
			continue
		}
		rawURL := firstNonEmpty(uvim.StringValue(segment.Data["url"]), uvim.StringValue(segment.Data["file"]))
		if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
			continue
		}
		refs = append(refs, uvim.ResourceRef{
			Provider:  "qq",
			Connector: config.ConnectorID,
			Kind:      kind,
			Name:      firstNonEmpty(uvim.StringValue(segment.Data["name"]), uvim.StringValue(segment.Data["file"])),
			URL:       rawURL,
		})
	}
	return refs
}

func qqKind(segmentType string) string {
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
