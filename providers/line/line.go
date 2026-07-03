package line

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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

type Provider struct {
	base   *httpchannel.Provider
	config Config
}

func New(config Config) (*Provider, error) {
	baseURL := config.BaseURL
	if baseURL == "" {
		baseURL = "https://api.line.me"
	}
	base, err := httpchannel.New(httpchannel.Config{
		ProviderID:    "line",
		ConnectorID:   firstNonEmpty(config.ConnectorID, "line"),
		BaseURL:       baseURL,
		Token:         config.Token,
		WebhookSecret: config.WebhookSecret,
		Decode:        Decode,
		DecodeEvents:  DecodeEvents,
		Send:          Send,
		Capabilities: uvim.Capabilities{
			Inbound:       true,
			Outbound:      true,
			DirectMessage: true,
			GroupMessage:  true,
			ChannelTypes:  []string{uvim.ChannelDirect, uvim.ChannelGroup},
		},
	})
	if err != nil {
		return nil, err
	}
	config.BaseURL = baseURL
	return &Provider{base: base, config: config}, nil
}

func (p *Provider) ID() string          { return p.base.ID() }
func (p *Provider) ConnectorID() string { return p.base.ConnectorID() }
func (p *Provider) Capabilities() uvim.Capabilities {
	caps := p.base.Capabilities()
	caps.DownloadResource = true
	caps.ResourceKinds = []string{uvim.ElementImage, uvim.ElementAudio, uvim.ElementVideo, uvim.ElementFile}
	return caps
}
func (p *Provider) Run(ctx context.Context, sink uvim.EventSink) error { return p.base.Run(ctx, sink) }
func (p *Provider) Send(ctx context.Context, msg uvim.OutboundMessage) (uvim.SendResult, error) {
	return p.base.Send(ctx, msg)
}
func (p *Provider) Health(ctx context.Context) uvim.Health { return p.base.Health(ctx) }
func (p *Provider) ServeWebhook(w http.ResponseWriter, req *http.Request, sink uvim.EventSink) {
	p.base.ServeWebhook(w, req, sink)
}
func (p *Provider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	ref := req.Resource
	if ref.URL == "" {
		if ref.Key == "" {
			return ref, fmt.Errorf("line download: message id is required")
		}
		ref.URL = strings.TrimRight(p.config.BaseURL, "/") + "/v2/bot/message/" + url.PathEscape(ref.Key) + "/content"
		ref.Secret = p.config.Token
		req.Resource = ref
	}
	return p.base.Download(ctx, req)
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	events, err := DecodeEvents(raw, config)
	if err != nil || len(events) == 0 {
		return uvim.Event{}, false, err
	}
	return events[0], true, nil
}

func DecodeEvents(raw []byte, config httpchannel.Config) ([]uvim.Event, error) {
	var env struct {
		Events []lineWebhookEvent `json:"events"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, err
	}
	events := make([]uvim.Event, 0, len(env.Events))
	for _, item := range env.Events {
		if item.Message.ID == "" {
			continue
		}
		events = append(events, eventFromWebhookItem(item, config))
	}
	return events, nil
}

func eventFromWebhookItem(item lineWebhookEvent, config httpchannel.Config) uvim.Event {
	channelID := firstNonEmpty(item.Source.GroupID, item.Source.RoomID, item.Source.UserID)
	channelType := uvim.ChannelDirect
	if item.Source.GroupID != "" || item.Source.RoomID != "" {
		channelType = uvim.ChannelGroup
	}
	var refs []uvim.ResourceRef
	if item.Message.Type != "" && item.Message.Type != "text" && item.Message.ID != "" {
		refs = append(refs, uvim.ResourceRef{
			Provider:  "line",
			Connector: config.ConnectorID,
			Kind:      lineKind(item.Message.Type),
			Name:      item.Message.FileName,
			Key:       item.Message.ID,
			SizeBytes: item.Message.FileSize,
		})
	}
	return uvim.Event{
		ID:        item.Message.ID,
		Type:      uvim.EventMessageCreate,
		Provider:  "line",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: channelID, Type: channelType},
		User:      uvim.User{ID: item.Source.UserID},
		Message:   uvim.Message{ID: item.Message.ID, Text: item.Message.Text, Type: item.Message.Type, Resources: refs},
		Referrer:  uvim.Referrer{MessageID: item.Message.ID, ChannelID: channelID, ReplyToken: item.ReplyToken},
		Addressed: true,
	}
}

type lineWebhookEvent struct {
	ReplyToken string `json:"replyToken"`
	Source     struct {
		Type    string `json:"type"`
		UserID  string `json:"userId"`
		GroupID string `json:"groupId"`
		RoomID  string `json:"roomId"`
	} `json:"source"`
	Message struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Text     string `json:"text"`
		FileName string `json:"fileName"`
		FileSize int64  `json:"fileSize"`
	} `json:"message"`
}

func Send(msg uvim.OutboundMessage, _ httpchannel.Config) (httpchannel.Request, error) {
	return httpchannel.Request{Path: "/v2/bot/message/push", Body: map[string]any{"to": msg.ChannelID, "messages": []map[string]string{{"type": "text", "text": msg.Text}}}}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func lineKind(messageType string) string {
	switch strings.ToLower(strings.TrimSpace(messageType)) {
	case "image":
		return uvim.ElementImage
	case "audio":
		return uvim.ElementAudio
	case "video":
		return uvim.ElementVideo
	default:
		return uvim.ElementFile
	}
}
