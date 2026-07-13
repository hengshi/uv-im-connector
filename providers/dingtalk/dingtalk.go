package dingtalk

import (
	"encoding/json"
	"fmt"
	"net/url"
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
		baseURL = "https://oapi.dingtalk.com"
	}
	return httpchannel.New(httpchannel.Config{
		ProviderID:        "dingtalk",
		ConnectorID:       firstNonEmpty(config.ConnectorID, "dingtalk"),
		BaseURL:           baseURL,
		Token:             config.Token,
		WebhookSecret:     config.WebhookSecret,
		Decode:            Decode,
		Send:              Send,
		ParseSendResponse: ParseSendResponse,
		Capabilities: uvim.Capabilities{
			Inbound:        true,
			Outbound:       true,
			DirectMessage:  true,
			GroupMessage:   true,
			ReplyMessage:   true,
			ProactiveGroup: true,
			TargetKinds:    []string{uvim.TargetUser, uvim.TargetGroup},
			ChannelTypes:   []string{uvim.ChannelDirect, uvim.ChannelGroup},
		},
	})
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var msg struct {
		MsgID            string `json:"msgId"`
		MsgType          string `json:"msgtype"`
		SenderStaffID    string `json:"senderStaffId"`
		SenderNick       string `json:"senderNick"`
		ConversationID   string `json:"conversationId"`
		ConversationType string `json:"conversationType"`
		SessionWebhook   string `json:"sessionWebhook"`
		Text             struct {
			Content string `json:"content"`
		} `json:"text"`
		Image map[string]any `json:"image"`
		File  map[string]any `json:"file"`
		Video map[string]any `json:"video"`
		Voice map[string]any `json:"voice"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return uvim.Event{}, false, err
	}
	if msg.MsgID == "" {
		return uvim.Event{}, false, nil
	}
	channelType := uvim.ChannelGroup
	target := uvim.OutboundTarget{ID: msg.ConversationID, Kind: uvim.TargetGroup}
	if msg.ConversationType == "1" {
		channelType = uvim.ChannelDirect
		target = uvim.OutboundTarget{ID: msg.SenderStaffID, Kind: uvim.TargetUser}
	}
	refs := dingtalkResources(config, msg.MsgType, msg.Image, msg.File, msg.Video, msg.Voice)
	return uvim.Event{
		ID:        msg.MsgID,
		Type:      uvim.EventMessageCreate,
		Provider:  "dingtalk",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: msg.ConversationID, Type: channelType},
		User:      uvim.User{ID: msg.SenderStaffID, Name: msg.SenderNick},
		Message:   uvim.Message{ID: msg.MsgID, Text: strings.TrimSpace(msg.Text.Content), Type: uvim.FirstNonEmpty(msg.MsgType, "text"), Resources: refs},
		Referrer:  uvim.Referrer{MessageID: msg.MsgID, ChannelID: msg.ConversationID, ReplyToken: msg.SessionWebhook, Target: &target},
		Addressed: true,
	}, true, nil
}

func Send(msg uvim.OutboundMessage, config httpchannel.Config) (httpchannel.Request, error) {
	body := map[string]any{"msgtype": "text", "text": map[string]any{"content": msg.Text}}
	if msg.Referrer.ReplyToken != "" {
		path, err := sameOriginPath(msg.Referrer.ReplyToken, config.BaseURL)
		if err != nil {
			return httpchannel.Request{}, fmt.Errorf("dingtalk send: invalid session webhook: %w", err)
		}
		return httpchannel.Request{Path: path, Body: body, NoAuth: true}, nil
	}
	target := msg.ResolvedTarget()
	if target.Kind == uvim.TargetUser {
		return httpchannel.Request{}, fmt.Errorf("dingtalk send: direct proactive messages require app robot server API credentials")
	}
	token := url.QueryEscape(config.Token)
	return httpchannel.Request{Path: "/robot/send?access_token=" + token, Body: body, NoAuth: true}, nil
}

func sameOriginPath(rawURL, baseURL string) (string, error) {
	target, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	if target.Scheme == "" || target.Host == "" || !strings.EqualFold(target.Scheme, base.Scheme) || !strings.EqualFold(target.Host, base.Host) {
		return "", fmt.Errorf("webhook origin does not match provider base URL")
	}
	path := target.EscapedPath()
	if path == "" {
		path = "/"
	}
	if target.RawQuery != "" {
		path += "?" + target.RawQuery
	}
	return path, nil
}

func ParseSendResponse(raw []byte) (string, error) {
	var response struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if response.ErrCode != 0 {
		businessErr := fmt.Errorf("errcode=%d errmsg=%q", response.ErrCode, response.ErrMsg)
		return "", uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	return "", nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func dingtalkResources(config httpchannel.Config, msgType string, payloads ...map[string]any) []uvim.ResourceRef {
	var refs []uvim.ResourceRef
	for _, payload := range payloads {
		if len(payload) == 0 {
			continue
		}
		rawURL := firstNonEmpty(
			uvim.StringValue(payload["url"]),
			uvim.StringValue(payload["downloadUrl"]),
			uvim.StringValue(payload["download_url"]),
			uvim.StringValue(payload["fileUrl"]),
			uvim.StringValue(payload["file_url"]),
			uvim.StringValue(payload["picUrl"]),
			uvim.StringValue(payload["pic_url"]),
		)
		if rawURL == "" {
			continue
		}
		mime := uvim.StringValue(payload["mime"])
		if mime == "" {
			mime = uvim.StringValue(payload["mimeType"])
		}
		refs = append(refs, uvim.ResourceRef{
			Provider:  "dingtalk",
			Connector: config.ConnectorID,
			Kind:      kindFromMessageType(msgType, mime),
			Name:      firstNonEmpty(uvim.StringValue(payload["fileName"]), uvim.StringValue(payload["file_name"]), uvim.StringValue(payload["name"])),
			URL:       rawURL,
			MIME:      mime,
			SizeBytes: sizeFromPayload(payload),
		})
	}
	return refs
}

func kindFromMessageType(messageType, mime string) string {
	switch strings.ToLower(strings.TrimSpace(messageType)) {
	case "image", "picture":
		return uvim.ElementImage
	case "voice", "audio":
		return uvim.ElementAudio
	case "video":
		return uvim.ElementVideo
	case "file":
		return uvim.ElementFile
	default:
		return uvim.ResourceKindFromMIME(mime, uvim.ElementFile)
	}
}

func sizeFromPayload(payload map[string]any) int64 {
	for _, key := range []string{"size", "fileSize", "file_size"} {
		switch value := payload[key].(type) {
		case float64:
			return int64(value)
		case int64:
			return value
		case int:
			return int64(value)
		}
	}
	return 0
}
