package whatsapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
)

type Config struct {
	ConnectorID   string
	BaseURL       string
	Token         string
	PhoneNumberID string
	WebhookSecret string
}

type Provider struct {
	base   *httpchannel.Provider
	config Config
}

func New(config Config) (*Provider, error) {
	baseURL := config.BaseURL
	if baseURL == "" {
		baseURL = "https://graph.facebook.com"
	}
	base, err := httpchannel.New(httpchannel.Config{
		ProviderID:    "whatsapp",
		ConnectorID:   firstNonEmpty(config.ConnectorID, "whatsapp"),
		BaseURL:       baseURL,
		Token:         config.Token,
		WebhookSecret: config.WebhookSecret,
		Decode:        Decode,
		DecodeEvents:  DecodeEvents,
		Send: func(msg uvim.OutboundMessage, cfg httpchannel.Config) (httpchannel.Request, error) {
			return Send(msg, cfg, config.PhoneNumberID)
		},
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
			return ref, fmt.Errorf("whatsapp download: media id is required")
		}
		mediaURL, mime, size, err := p.mediaURL(ctx, ref.Key)
		if err != nil {
			return ref, err
		}
		ref.URL = mediaURL
		ref.Secret = p.config.Token
		ref.MIME = uvim.FirstNonEmpty(ref.MIME, mime)
		if ref.SizeBytes == 0 {
			ref.SizeBytes = size
		}
		req.Resource = ref
	}
	return p.base.Download(ctx, req)
}

func (p *Provider) mediaURL(ctx context.Context, mediaID string) (string, string, int64, error) {
	endpoint := strings.TrimRight(p.config.BaseURL, "/") + "/" + mediaID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", "", 0, err
	}
	if p.config.Token != "" {
		req.Header.Set("Authorization", httpchannel.Authorization(p.config.Token))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", 0, fmt.Errorf("whatsapp media: http %d", resp.StatusCode)
	}
	var decoded struct {
		URL      string `json:"url"`
		MIME     string `json:"mime_type"`
		FileSize int64  `json:"file_size"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", "", 0, err
	}
	if decoded.URL == "" {
		return "", "", 0, fmt.Errorf("whatsapp media: url missing")
	}
	return decoded.URL, decoded.MIME, decoded.FileSize, nil
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
		Entry []struct {
			Changes []struct {
				Value struct {
					Messages []whatsappWebhookMessage `json:"messages"`
				} `json:"value"`
			} `json:"changes"`
		} `json:"entry"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, err
	}
	var events []uvim.Event
	for _, entry := range env.Entry {
		for _, change := range entry.Changes {
			for _, msg := range change.Value.Messages {
				if msg.ID == "" {
					continue
				}
				events = append(events, eventFromWebhookMessage(msg, config))
			}
		}
	}
	return events, nil
}

func eventFromWebhookMessage(msg whatsappWebhookMessage, config httpchannel.Config) uvim.Event {
	refs := whatsappResources(msg.Image, msg.Audio, msg.Video, msg.Document, config)
	channelID := msg.From
	channelType := uvim.ChannelDirect
	if msg.Context.GroupID != "" {
		channelID = msg.Context.GroupID
		channelType = uvim.ChannelGroup
	}
	targetKind := uvim.TargetUser
	if channelType == uvim.ChannelGroup {
		targetKind = uvim.TargetGroup
	}
	return uvim.Event{
		ID:        msg.ID,
		Type:      uvim.EventMessageCreate,
		Provider:  "whatsapp",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: channelID, Type: channelType, Name: msg.Context.GroupSubject},
		User:      uvim.User{ID: msg.From},
		Message:   uvim.Message{ID: msg.ID, Text: msg.Text.Body, Type: msg.Type, Resources: refs},
		Referrer:  uvim.Referrer{MessageID: msg.ID, ParentMessageID: msg.Context.ID, ChannelID: channelID, Target: &uvim.OutboundTarget{ID: channelID, Kind: targetKind}},
		Addressed: true,
	}
}

type whatsappWebhookMessage struct {
	ID   string `json:"id"`
	From string `json:"from"`
	Type string `json:"type"`
	Text struct {
		Body string `json:"body"`
	} `json:"text"`
	Context struct {
		ID           string `json:"id"`
		GroupID      string `json:"group_id"`
		GroupSubject string `json:"group_subject"`
	} `json:"context"`
	Image    *whatsappMedia `json:"image"`
	Audio    *whatsappMedia `json:"audio"`
	Video    *whatsappMedia `json:"video"`
	Document *whatsappMedia `json:"document"`
}

type whatsappMedia struct {
	ID       string `json:"id"`
	MIME     string `json:"mime_type"`
	Caption  string `json:"caption"`
	Filename string `json:"filename"`
}

func Send(msg uvim.OutboundMessage, _ httpchannel.Config, phoneNumberID string) (httpchannel.Request, error) {
	phoneNumberID = strings.TrimSpace(phoneNumberID)
	if phoneNumberID == "" {
		return httpchannel.Request{}, fmt.Errorf("whatsapp send: phone_number_id is required")
	}
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return httpchannel.Request{}, fmt.Errorf("whatsapp send: target user id is required")
	}
	recipientType := "individual"
	if target.Kind == uvim.TargetGroup {
		recipientType = "group"
	}
	body := map[string]any{
		"messaging_product": "whatsapp",
		"recipient_type":    recipientType,
		"to":                target.ID,
		"type":              "text",
		"text":              map[string]string{"body": msg.Text},
	}
	if msg.Referrer.MessageID != "" {
		body["context"] = map[string]string{"message_id": msg.Referrer.MessageID}
	}
	return httpchannel.Request{Path: "/" + phoneNumberID + "/messages", Body: body}, nil
}

func ParseSendResponse(raw []byte) (string, error) {
	var response struct {
		Messages []struct {
			ID string `json:"id"`
		} `json:"messages"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if response.Error != nil {
		businessErr := fmt.Errorf("code=%d message=%q", response.Error.Code, response.Error.Message)
		return "", uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	if len(response.Messages) == 0 || response.Messages[0].ID == "" {
		businessErr := fmt.Errorf("message id missing")
		return "", uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	return response.Messages[0].ID, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func whatsappResources(image, audio, video, document *whatsappMedia, config httpchannel.Config) []uvim.ResourceRef {
	var refs []uvim.ResourceRef
	add := func(kind string, media *whatsappMedia) {
		if media == nil || media.ID == "" {
			return
		}
		refs = append(refs, uvim.ResourceRef{
			Provider:  "whatsapp",
			Connector: config.ConnectorID,
			Kind:      kind,
			Name:      media.Filename,
			Key:       media.ID,
			MIME:      media.MIME,
		})
	}
	add(uvim.ElementImage, image)
	add(uvim.ElementAudio, audio)
	add(uvim.ElementVideo, video)
	add(uvim.ElementFile, document)
	return refs
}
