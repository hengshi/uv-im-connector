package wechatofficial

import (
	"encoding/json"
	"encoding/xml"
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
		baseURL = "https://api.weixin.qq.com"
	}
	return httpchannel.New(httpchannel.Config{
		ProviderID:        "wechat-official",
		ConnectorID:       firstNonEmpty(config.ConnectorID, "wechat-official"),
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
			ReplyMessage:    true,
			ProactiveDirect: true,
			TargetKinds:     []string{uvim.TargetUser},
			ChannelTypes:    []string{uvim.ChannelDirect},
		},
	})
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var msg struct {
		XMLName      xml.Name `xml:"xml"`
		ToUserName   string   `xml:"ToUserName"`
		FromUserName string   `xml:"FromUserName"`
		CreateTime   string   `xml:"CreateTime"`
		MsgType      string   `xml:"MsgType"`
		Content      string   `xml:"Content"`
		MsgID        string   `xml:"MsgId"`
		MediaID      string   `xml:"MediaId"`
		PicURL       string   `xml:"PicUrl"`
		Format       string   `xml:"Format"`
		ThumbMediaID string   `xml:"ThumbMediaId"`
	}
	if err := xml.Unmarshal(raw, &msg); err != nil {
		return uvim.Event{}, false, err
	}
	if msg.MsgID == "" {
		return uvim.Event{}, false, nil
	}
	refs := wechatResources(msg.MsgType, msg.MediaID, msg.PicURL, msg.Format, config)
	return uvim.Event{ID: msg.MsgID, Type: uvim.EventMessageCreate, Provider: "wechat-official", Connector: config.ConnectorID, Channel: uvim.Channel{ID: msg.FromUserName, Type: uvim.ChannelDirect}, User: uvim.User{ID: msg.FromUserName}, Message: uvim.Message{ID: msg.MsgID, Text: msg.Content, Type: msg.MsgType, Resources: refs}, Referrer: uvim.Referrer{MessageID: msg.MsgID, ChannelID: msg.FromUserName, Target: &uvim.OutboundTarget{ID: msg.FromUserName, Kind: uvim.TargetUser}}, Addressed: true}, true, nil
}

func Send(msg uvim.OutboundMessage, config httpchannel.Config) (httpchannel.Request, error) {
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return httpchannel.Request{}, fmt.Errorf("wechat-official send: target user id is required")
	}
	body := map[string]any{"touser": target.ID, "msgtype": "text", "text": map[string]string{"content": msg.Text}}
	return httpchannel.Request{Path: "/cgi-bin/message/custom/send?access_token=" + url.QueryEscape(config.Token), Body: body, NoAuth: true}, nil
}

func ParseSendResponse(raw []byte) (string, error) {
	var response struct {
		ErrCode   int    `json:"errcode"`
		ErrMsg    string `json:"errmsg"`
		MessageID any    `json:"msgid"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if response.ErrCode != 0 {
		businessErr := fmt.Errorf("errcode=%d errmsg=%q", response.ErrCode, response.ErrMsg)
		return "", uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	if response.MessageID == nil {
		return "", nil
	}
	return fmt.Sprint(response.MessageID), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func wechatResources(msgType, mediaID, picURL, format string, config httpchannel.Config) []uvim.ResourceRef {
	kind := ""
	switch strings.ToLower(strings.TrimSpace(msgType)) {
	case "image":
		kind = uvim.ElementImage
	case "voice":
		kind = uvim.ElementAudio
	case "video", "shortvideo":
		kind = uvim.ElementVideo
	default:
		return nil
	}
	rawURL := picURL
	if rawURL == "" && mediaID != "" && config.BaseURL != "" && config.Token != "" {
		rawURL = strings.TrimRight(config.BaseURL, "/") + "/cgi-bin/media/get?access_token=" + url.QueryEscape(config.Token) + "&media_id=" + url.QueryEscape(mediaID)
	}
	if rawURL == "" {
		return nil
	}
	return []uvim.ResourceRef{{
		Provider:  "wechat-official",
		Connector: config.ConnectorID,
		Kind:      kind,
		Key:       mediaID,
		URL:       rawURL,
		MIME:      mimeFromFormat(format),
	}}
}

func mimeFromFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "amr":
		return "audio/amr"
	case "speex":
		return "audio/speex"
	case "mp3":
		return "audio/mpeg"
	default:
		return ""
	}
}
