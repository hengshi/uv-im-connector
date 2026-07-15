package wechatofficial

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
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
	ResourceStore *uvim.ResourceStore
	HTTPClient    *http.Client
}

const (
	wechatMediaIDKey   = "uv_wechat_media_id"
	wechatMediaTypeKey = "uv_wechat_media_type"
	maxWechatImage     = 10 * 1024 * 1024
	maxWechatVoice     = 2 * 1024 * 1024
	maxWechatVideo     = 10 * 1024 * 1024
)

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
		ResourceStore:     config.ResourceStore,
		HTTPClient:        config.HTTPClient,
		Decode:            Decode,
		PrepareSend:       prepareSend,
		Send:              Send,
		ParseSendResponse: ParseSendResponse,
		Capabilities: uvim.Capabilities{
			Inbound:         true,
			Outbound:        true,
			DirectMessage:   true,
			ReplyMessage:    true,
			ProactiveDirect: true,
			TargetKinds:     []string{uvim.TargetUser},
			UploadResource:  config.ResourceStore != nil,
			ResourceKinds:   []string{uvim.ElementImage, uvim.ElementAudio, uvim.ElementVideo},
			ChannelTypes:    []string{uvim.ChannelDirect},
		},
	})
}

func prepareSend(ctx context.Context, msg uvim.OutboundMessage, config httpchannel.Config) (uvim.OutboundMessage, error) {
	if len(msg.Resources) == 0 {
		return msg, nil
	}
	if len(msg.Resources) != 1 {
		return msg, fmt.Errorf("wechat-official send: one resource per message is supported")
	}
	if strings.TrimSpace(msg.Text) != "" || len(msg.Elements) > 0 {
		return msg, fmt.Errorf("wechat-official send: text, elements, and resources must be sent separately")
	}
	ref := msg.Resources[0]
	mediaType, limit, err := wechatMediaRoute(ref.Kind)
	if err != nil {
		return msg, err
	}
	if config.ResourceStore == nil || !strings.HasPrefix(strings.TrimSpace(ref.InternalURL), "internal://") {
		return msg, fmt.Errorf("wechat-official upload: internal resource is required")
	}
	file, _, err := config.ResourceStore.Open(ref.InternalURL)
	if err != nil {
		return msg, uvim.NewProviderSendError("wechat-official resource is unavailable", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	closeErr := file.Close()
	if readErr != nil {
		return msg, uvim.NewProviderSendError("wechat-official resource read failed", readErr)
	}
	if closeErr != nil {
		return msg, uvim.NewProviderSendError("wechat-official resource close failed", closeErr)
	}
	if len(data) == 0 {
		return msg, fmt.Errorf("wechat-official upload: empty resources are not supported")
	}
	if len(data) > limit {
		return msg, fmt.Errorf("wechat-official upload: %s resource exceeds %d bytes", mediaType, limit)
	}
	name := uvim.ResourceUploadName(0, ref, ref.MIME)
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="media"; filename=%q`, name))
	if strings.TrimSpace(ref.MIME) != "" {
		header.Set("Content-Type", ref.MIME)
	}
	part, err := writer.CreatePart(header)
	if err != nil {
		return msg, err
	}
	if _, err := part.Write(data); err != nil {
		return msg, err
	}
	if err := writer.Close(); err != nil {
		return msg, err
	}
	endpoint := strings.TrimRight(config.BaseURL, "/") + "/cgi-bin/media/upload?access_token=" + url.QueryEscape(config.Token) + "&type=" + url.QueryEscape(mediaType)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return msg, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	client := config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return msg, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return msg, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return msg, uvim.NewProviderSendError(fmt.Sprintf("wechat-official upload: http %d", resp.StatusCode), fmt.Errorf("wechat-official upload: http %d", resp.StatusCode))
	}
	var decoded struct {
		MediaID string `json:"media_id"`
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return msg, fmt.Errorf("wechat-official upload: decode response: %w", err)
	}
	if decoded.ErrCode != 0 {
		businessErr := fmt.Errorf("wechat-official upload: errcode=%d errmsg=%q", decoded.ErrCode, decoded.ErrMsg)
		return msg, uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	if decoded.MediaID == "" {
		return msg, fmt.Errorf("wechat-official upload: response missing media_id")
	}
	if msg.Metadata == nil {
		msg.Metadata = map[string]string{}
	}
	msg.Metadata[wechatMediaIDKey] = decoded.MediaID
	msg.Metadata[wechatMediaTypeKey] = mediaType
	msg.Resources = nil
	return msg, nil
}

func wechatMediaRoute(kind string) (string, int, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case uvim.ElementImage:
		return "image", maxWechatImage, nil
	case uvim.ElementAudio:
		return "voice", maxWechatVoice, nil
	case uvim.ElementVideo:
		return "video", maxWechatVideo, nil
	default:
		return "", 0, fmt.Errorf("wechat-official upload: resource kind %q is not supported", kind)
	}
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
	if mediaID := msg.Metadata[wechatMediaIDKey]; mediaID != "" {
		mediaType := msg.Metadata[wechatMediaTypeKey]
		body = map[string]any{"touser": target.ID, "msgtype": mediaType, mediaType: map[string]string{"media_id": mediaID}}
	}
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
