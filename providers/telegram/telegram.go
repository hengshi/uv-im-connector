package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
)

type Config struct {
	ConnectorID   string
	BaseURL       string
	Token         string
	WebhookSecret string
	HTTPClient    *http.Client
	ResourceStore *uvim.ResourceStore
}

type Provider struct {
	base   *httpchannel.Provider
	config Config
}

func New(config Config) (*Provider, error) {
	baseURL := config.BaseURL
	if baseURL == "" {
		baseURL = "https://api.telegram.org"
	}
	base, err := httpchannel.New(httpchannel.Config{
		ProviderID:    "telegram",
		ConnectorID:   firstNonEmpty(config.ConnectorID, "telegram"),
		BaseURL:       baseURL,
		Token:         config.Token,
		WebhookSecret: config.WebhookSecret,
		HTTPClient:    config.HTTPClient,
		ResourceStore: config.ResourceStore,
		Decode:        Decode,
		Send:          Send,
		Capabilities: uvim.Capabilities{
			Inbound:          true,
			Outbound:         true,
			DirectMessage:    true,
			GroupMessage:     true,
			DownloadResource: true,
			ResourceKinds:    []string{uvim.ElementImage, uvim.ElementAudio, uvim.ElementVideo, uvim.ElementFile},
			ChannelTypes:     []string{uvim.ChannelDirect, uvim.ChannelGroup},
		},
	})
	if err != nil {
		return nil, err
	}
	config.BaseURL = baseURL
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Provider{base: base, config: config}, nil
}

func (p *Provider) ID() string          { return p.base.ID() }
func (p *Provider) ConnectorID() string { return p.base.ConnectorID() }
func (p *Provider) Capabilities() uvim.Capabilities {
	return p.base.Capabilities()
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
	if ref.URL != "" {
		return p.base.Download(ctx, req)
	}
	if ref.Key == "" {
		return ref, fmt.Errorf("telegram download: file id is required")
	}
	filePath, err := p.filePath(ctx, ref.Key)
	if err != nil {
		return ref, err
	}
	ref.URL = strings.TrimRight(p.config.BaseURL, "/") + "/file/bot" + p.config.Token + "/" + filePath
	req.Resource = ref
	return p.base.Download(ctx, req)
}

func (p *Provider) filePath(ctx context.Context, fileID string) (string, error) {
	endpoint := strings.TrimRight(p.config.BaseURL, "/") + "/bot" + p.config.Token + "/getFile?file_id=" + url.QueryEscape(fileID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	resp, err := p.config.HTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("telegram getFile: http %d", resp.StatusCode)
	}
	var decoded struct {
		OK     bool `json:"ok"`
		Result struct {
			FilePath string `json:"file_path"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", err
	}
	if !decoded.OK || decoded.Result.FilePath == "" {
		return "", fmt.Errorf("telegram getFile: file path missing")
	}
	return decoded.Result.FilePath, nil
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var update struct {
		UpdateID int64 `json:"update_id"`
		Message  *struct {
			MessageID int64  `json:"message_id"`
			Text      string `json:"text"`
			Caption   string `json:"caption"`
			Chat      struct {
				ID    int64  `json:"id"`
				Type  string `json:"type"`
				Title string `json:"title"`
			} `json:"chat"`
			From struct {
				ID        int64  `json:"id"`
				Username  string `json:"username"`
				FirstName string `json:"first_name"`
				LastName  string `json:"last_name"`
			} `json:"from"`
			Document *telegramFile `json:"document"`
			Audio    *telegramFile `json:"audio"`
			Video    *telegramFile `json:"video"`
			Photo    []struct {
				FileID string `json:"file_id"`
				Size   int64  `json:"file_size"`
			} `json:"photo"`
		} `json:"message"`
	}
	if err := json.Unmarshal(raw, &update); err != nil {
		return uvim.Event{}, false, err
	}
	if update.Message == nil {
		return uvim.Event{}, false, nil
	}
	msg := update.Message
	messageID := fmt.Sprint(msg.MessageID)
	chatID := fmt.Sprint(msg.Chat.ID)
	channelType := uvim.ChannelDirect
	if msg.Chat.Type == "group" || msg.Chat.Type == "supergroup" || msg.Chat.Type == "channel" {
		channelType = uvim.ChannelGroup
	}
	refs := telegramResources(msg.Document, msg.Audio, msg.Video, msg.Photo, config)
	text := firstNonEmpty(msg.Text, msg.Caption)
	return uvim.Event{
		ID:        fmt.Sprint(update.UpdateID),
		Type:      uvim.EventMessageCreate,
		Provider:  "telegram",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: chatID, Type: channelType, Name: msg.Chat.Title},
		User:      uvim.User{ID: fmt.Sprint(msg.From.ID), Name: strings.TrimSpace(msg.From.FirstName + " " + msg.From.LastName), DisplayName: msg.From.Username},
		Message:   uvim.Message{ID: messageID, Text: text, Type: "message", Resources: refs},
		Referrer:  uvim.Referrer{MessageID: messageID, ChannelID: chatID},
		Addressed: true,
	}, true, nil
}

type telegramFile struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MIME     string `json:"mime_type"`
	Size     int64  `json:"file_size"`
}

func telegramResources(document, audio, video *telegramFile, photos []struct {
	FileID string `json:"file_id"`
	Size   int64  `json:"file_size"`
}, config httpchannel.Config) []uvim.ResourceRef {
	var refs []uvim.ResourceRef
	if document != nil && document.FileID != "" {
		refs = append(refs, uvim.ResourceRef{Provider: "telegram", Connector: config.ConnectorID, Kind: uvim.ElementFile, Name: document.FileName, Key: document.FileID, MIME: document.MIME, SizeBytes: document.Size})
	}
	if audio != nil && audio.FileID != "" {
		refs = append(refs, uvim.ResourceRef{Provider: "telegram", Connector: config.ConnectorID, Kind: uvim.ElementAudio, Name: audio.FileName, Key: audio.FileID, MIME: audio.MIME, SizeBytes: audio.Size})
	}
	if video != nil && video.FileID != "" {
		refs = append(refs, uvim.ResourceRef{Provider: "telegram", Connector: config.ConnectorID, Kind: uvim.ElementVideo, Name: video.FileName, Key: video.FileID, MIME: video.MIME, SizeBytes: video.Size})
	}
	if len(photos) > 0 {
		photo := photos[len(photos)-1]
		if photo.FileID != "" {
			refs = append(refs, uvim.ResourceRef{Provider: "telegram", Connector: config.ConnectorID, Kind: uvim.ElementImage, Key: photo.FileID, SizeBytes: photo.Size})
		}
	}
	return refs
}

func Send(msg uvim.OutboundMessage, config httpchannel.Config) (httpchannel.Request, error) {
	return httpchannel.Request{Path: "/bot" + config.Token + "/sendMessage", Body: map[string]any{"chat_id": msg.ChannelID, "text": msg.Text}, NoAuth: true}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
