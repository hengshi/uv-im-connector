package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
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
		ProviderID:        "telegram",
		ConnectorID:       firstNonEmpty(config.ConnectorID, "telegram"),
		BaseURL:           baseURL,
		Token:             config.Token,
		WebhookSecret:     config.WebhookSecret,
		HTTPClient:        config.HTTPClient,
		ResourceStore:     config.ResourceStore,
		Decode:            Decode,
		Send:              Send,
		ParseSendResponse: ParseSendResponse,
		Capabilities: uvim.Capabilities{
			Inbound:          true,
			Outbound:         true,
			DirectMessage:    true,
			GroupMessage:     true,
			ReplyMessage:     true,
			ProactiveDirect:  true,
			ProactiveGroup:   true,
			TargetKinds:      []string{uvim.TargetUser, uvim.TargetGroup, uvim.TargetConversation},
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
		config.HTTPClient = &http.Client{}
	}
	return &Provider{base: base, config: config}, nil
}

func (p *Provider) ID() string          { return p.base.ID() }
func (p *Provider) ConnectorID() string { return p.base.ConnectorID() }
func (p *Provider) Capabilities() uvim.Capabilities {
	caps := p.base.Capabilities()
	caps.UploadResource = p.config.ResourceStore != nil
	return caps
}
func (p *Provider) Run(ctx context.Context, sink uvim.EventSink) error { return p.base.Run(ctx, sink) }
func (p *Provider) Send(ctx context.Context, msg uvim.OutboundMessage) (result uvim.SendResult, err error) {
	defer func() {
		err = uvim.NewProviderSendOperationError("telegram send", err)
	}()
	if len(msg.Resources) == 0 {
		return p.base.Send(ctx, msg)
	}
	if err := uvim.ValidateOutboundTarget(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("telegram send: %w", err)
	}
	if err := uvim.ValidateOutboundResources(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("telegram send: %w", err)
	}
	if len(msg.Resources) > 1 || strings.TrimSpace(msg.Text) != "" || len(msg.Elements) > 0 {
		return uvim.SendResourceSequence(ctx, msg, p.Send)
	}
	return p.sendResource(ctx, msg, msg.Resources[0])

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

func (p *Provider) sendResource(ctx context.Context, msg uvim.OutboundMessage, ref uvim.ResourceRef) (result uvim.SendResult, err error) {
	defer func() {
		err = uvim.NewProviderSendOperationError("telegram upload", err)
	}()
	if p.config.ResourceStore == nil {
		return uvim.SendResult{}, fmt.Errorf("telegram upload: resource store is not configured")
	}
	if !strings.HasPrefix(strings.TrimSpace(ref.InternalURL), "internal://") {
		return uvim.SendResult{}, fmt.Errorf("telegram upload: internal resource is required")
	}
	file, _, err := p.config.ResourceStore.Open(ref.InternalURL)
	if err != nil {
		return uvim.SendResult{}, uvim.NewProviderSendError("telegram resource is unavailable", err)
	}
	data, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil {
		return uvim.SendResult{}, uvim.NewProviderSendError("telegram resource read failed", readErr)
	}
	if closeErr != nil {
		return uvim.SendResult{}, uvim.NewProviderSendError("telegram resource close failed", closeErr)
	}
	method, field := telegramMediaRoute(ref)
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return uvim.SendResult{}, fmt.Errorf("telegram send: target chat id is required")
	}
	name := uvim.ResourceUploadName(0, ref, ref.MIME)
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("chat_id", target.ID); err != nil {
		return uvim.SendResult{}, err
	}
	if replyTo, err := strconv.ParseInt(msg.Referrer.MessageID, 10, 64); err == nil && replyTo != 0 {
		raw, _ := json.Marshal(map[string]int64{"message_id": replyTo})
		if err := writer.WriteField("reply_parameters", string(raw)); err != nil {
			return uvim.SendResult{}, err
		}
	}
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, field, name))
	if strings.TrimSpace(ref.MIME) != "" {
		header.Set("Content-Type", ref.MIME)
	}
	part, err := writer.CreatePart(header)
	if err != nil {
		return uvim.SendResult{}, err
	}
	if _, err := part.Write(data); err != nil {
		return uvim.SendResult{}, err
	}
	if err := writer.Close(); err != nil {
		return uvim.SendResult{}, err
	}
	endpoint := strings.TrimRight(p.config.BaseURL, "/") + "/bot" + p.config.Token + "/" + method
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return uvim.SendResult{}, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := p.config.HTTPClient.Do(req)
	if err != nil {
		return uvim.SendResult{}, err
	}
	defer resp.Body.Close()
	respRaw, err := httpchannel.ReadSendResponse(resp, "telegram send")
	if err != nil {
		return uvim.SendResult{}, err
	}
	messageID, err := ParseSendResponse(respRaw)
	if err != nil {
		return uvim.SendResult{}, err
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: messageID, Time: time.Now().UTC()}, nil
}

func telegramMediaRoute(ref uvim.ResourceRef) (method, field string) {
	mimeType := strings.ToLower(strings.TrimSpace(ref.MIME))
	switch strings.ToLower(strings.TrimSpace(ref.Kind)) {
	case uvim.ElementImage:
		if mimeType == "image/jpeg" || mimeType == "image/png" || mimeType == "image/webp" {
			return "sendPhoto", "photo"
		}
	case uvim.ElementAudio:
		if mimeType == "audio/mpeg" || mimeType == "audio/mp4" || mimeType == "audio/x-m4a" {
			return "sendAudio", "audio"
		}
	case uvim.ElementVideo:
		if mimeType == "video/mp4" {
			return "sendVideo", "video"
		}
	}
	return "sendDocument", "document"
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var update struct {
		UpdateID          int64            `json:"update_id"`
		Message           *telegramMessage `json:"message"`
		EditedMessage     *telegramMessage `json:"edited_message"`
		ChannelPost       *telegramMessage `json:"channel_post"`
		EditedChannelPost *telegramMessage `json:"edited_channel_post"`
	}
	if err := json.Unmarshal(raw, &update); err != nil {
		return uvim.Event{}, false, err
	}
	msg := update.Message
	eventType := uvim.EventMessageCreate
	if msg == nil && update.EditedMessage != nil {
		msg = update.EditedMessage
		eventType = uvim.EventMessageUpdate
	}
	if msg == nil && update.ChannelPost != nil {
		msg = update.ChannelPost
	}
	if msg == nil && update.EditedChannelPost != nil {
		msg = update.EditedChannelPost
		eventType = uvim.EventMessageUpdate
	}
	if msg == nil {
		return uvim.Event{}, false, nil
	}
	messageID := fmt.Sprint(msg.MessageID)
	chatID := fmt.Sprint(msg.Chat.ID)
	channelType := uvim.ChannelDirect
	targetKind := uvim.TargetUser
	if msg.Chat.Type == "group" || msg.Chat.Type == "supergroup" || msg.Chat.Type == "channel" {
		channelType = uvim.ChannelGroup
		targetKind = uvim.TargetGroup
	}
	refs := telegramResources(msg.Document, msg.Audio, msg.Video, msg.Voice, msg.VideoNote, msg.Sticker, msg.Photo, config)
	text := firstNonEmpty(msg.Text, msg.Caption, telegramMessageText(msg))
	parentID := ""
	if msg.ReplyToMessage != nil && msg.ReplyToMessage.MessageID != 0 {
		parentID = fmt.Sprint(msg.ReplyToMessage.MessageID)
	}
	threadID := ""
	if msg.MessageThreadID != 0 {
		threadID = fmt.Sprint(msg.MessageThreadID)
	}
	userID := ""
	if msg.From.ID != 0 {
		userID = fmt.Sprint(msg.From.ID)
	}
	return uvim.Event{
		ID:        fmt.Sprint(update.UpdateID),
		Type:      eventType,
		Provider:  "telegram",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: chatID, Type: channelType, Name: msg.Chat.Title},
		User:      uvim.User{ID: userID, Name: strings.TrimSpace(msg.From.FirstName + " " + msg.From.LastName), DisplayName: msg.From.Username},
		Message:   uvim.Message{ID: messageID, Text: text, Type: "message", Resources: refs},
		Referrer:  uvim.Referrer{MessageID: messageID, ParentMessageID: parentID, ChannelID: chatID, ThreadID: threadID, Target: &uvim.OutboundTarget{ID: chatID, Kind: targetKind}},
		Addressed: true,
	}, true, nil
}

type telegramMessage struct {
	MessageID       int64  `json:"message_id"`
	MessageThreadID int64  `json:"message_thread_id"`
	Text            string `json:"text"`
	Caption         string `json:"caption"`
	Chat            struct {
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
	ReplyToMessage *struct {
		MessageID int64 `json:"message_id"`
	} `json:"reply_to_message"`
	Document  *telegramFile `json:"document"`
	Audio     *telegramFile `json:"audio"`
	Video     *telegramFile `json:"video"`
	Voice     *telegramFile `json:"voice"`
	VideoNote *telegramFile `json:"video_note"`
	Sticker   *telegramFile `json:"sticker"`
	Photo     []struct {
		FileID string `json:"file_id"`
		Size   int64  `json:"file_size"`
	} `json:"photo"`
	Contact *struct {
		PhoneNumber string `json:"phone_number"`
		FirstName   string `json:"first_name"`
		LastName    string `json:"last_name"`
		UserID      int64  `json:"user_id"`
	} `json:"contact"`
	Location *struct {
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
	} `json:"location"`
	Venue *struct {
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
		Title     string  `json:"title"`
		Address   string  `json:"address"`
	} `json:"venue"`
	Poll *struct {
		Question string `json:"question"`
		Options  []struct {
			Text string `json:"text"`
		} `json:"options"`
	} `json:"poll"`
}

type telegramFile struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MIME     string `json:"mime_type"`
	Size     int64  `json:"file_size"`
	Emoji    string `json:"emoji"`
}

func telegramResources(document, audio, video, voice, videoNote, sticker *telegramFile, photos []struct {
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
	if voice != nil && voice.FileID != "" {
		refs = append(refs, uvim.ResourceRef{Provider: "telegram", Connector: config.ConnectorID, Kind: uvim.ElementAudio, Name: voice.FileName, Key: voice.FileID, MIME: voice.MIME, SizeBytes: voice.Size})
	}
	if videoNote != nil && videoNote.FileID != "" {
		refs = append(refs, uvim.ResourceRef{Provider: "telegram", Connector: config.ConnectorID, Kind: uvim.ElementVideo, Name: videoNote.FileName, Key: videoNote.FileID, MIME: videoNote.MIME, SizeBytes: videoNote.Size})
	}
	if sticker != nil && sticker.FileID != "" {
		refs = append(refs, uvim.ResourceRef{Provider: "telegram", Connector: config.ConnectorID, Kind: uvim.ElementImage, Name: sticker.Emoji, Key: sticker.FileID, MIME: sticker.MIME, SizeBytes: sticker.Size})
	}
	if len(photos) > 0 {
		photo := photos[len(photos)-1]
		if photo.FileID != "" {
			refs = append(refs, uvim.ResourceRef{Provider: "telegram", Connector: config.ConnectorID, Kind: uvim.ElementImage, Key: photo.FileID, SizeBytes: photo.Size})
		}
	}
	return refs
}

func telegramMessageText(msg *telegramMessage) string {
	if msg == nil {
		return ""
	}
	if msg.Contact != nil {
		name := strings.TrimSpace(msg.Contact.FirstName + " " + msg.Contact.LastName)
		return strings.TrimSpace("Contact: " + firstNonEmpty(name, msg.Contact.PhoneNumber, fmt.Sprint(msg.Contact.UserID)))
	}
	if msg.Location != nil {
		return fmt.Sprintf("Location: %g,%g", msg.Location.Latitude, msg.Location.Longitude)
	}
	if msg.Venue != nil {
		return fmt.Sprintf("Venue: %s %s (%g,%g)", msg.Venue.Title, msg.Venue.Address, msg.Venue.Latitude, msg.Venue.Longitude)
	}
	if msg.Poll != nil {
		var parts []string
		if strings.TrimSpace(msg.Poll.Question) != "" {
			parts = append(parts, "Poll: "+strings.TrimSpace(msg.Poll.Question))
		}
		for _, option := range msg.Poll.Options {
			if text := strings.TrimSpace(option.Text); text != "" {
				parts = append(parts, "- "+text)
			}
		}
		return strings.Join(parts, "\n")
	}
	if msg.Sticker != nil {
		return firstNonEmpty("[Sticker: "+strings.TrimSpace(msg.Sticker.Emoji)+"]", "[Sticker]")
	}
	return ""
}

func Send(msg uvim.OutboundMessage, config httpchannel.Config) (httpchannel.Request, error) {
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return httpchannel.Request{}, fmt.Errorf("telegram send: target chat id is required")
	}
	body := map[string]any{"chat_id": target.ID, "text": msg.Text}
	if replyTo, err := strconv.ParseInt(msg.Referrer.MessageID, 10, 64); err == nil && replyTo != 0 {
		body["reply_parameters"] = map[string]any{"message_id": replyTo}
	}
	return httpchannel.Request{Path: "/bot" + config.Token + "/sendMessage", Body: body, NoAuth: true}, nil
}

func ParseSendResponse(raw []byte) (string, error) {
	var response struct {
		OK          bool   `json:"ok"`
		Description string `json:"description"`
		ErrorCode   int    `json:"error_code"`
		Result      struct {
			MessageID int64 `json:"message_id"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if !response.OK {
		businessErr := fmt.Errorf("error_code=%d description=%q", response.ErrorCode, response.Description)
		return "", uvim.NewProviderResponseError(raw, businessErr.Error(), businessErr)
	}
	if response.Result.MessageID == 0 {
		return "", nil
	}
	return strconv.FormatInt(response.Result.MessageID, 10), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
