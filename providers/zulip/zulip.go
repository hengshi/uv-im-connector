package zulip

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

func New(config Config) (*httpchannel.Provider, error) {
	return httpchannel.New(httpchannel.Config{
		ProviderID:        "zulip",
		ConnectorID:       firstNonEmpty(config.ConnectorID, "zulip"),
		BaseURL:           config.BaseURL,
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
			GroupMessage:    true,
			ThreadReply:     true,
			ReplyMessage:    true,
			ProactiveDirect: true,
			ProactiveGroup:  true,
			TargetKinds:     []string{uvim.TargetUser, uvim.TargetGroup},
			UploadResource:  config.ResourceStore != nil,
			ResourceKinds:   []string{uvim.ElementImage, uvim.ElementAudio, uvim.ElementVideo, uvim.ElementFile},
			ChannelTypes:    []string{uvim.ChannelDirect, uvim.ChannelGroup, uvim.ChannelThread},
		},
	})
}

const maxSimpleUploadBytes = 25 * 1024 * 1024

func prepareSend(ctx context.Context, msg uvim.OutboundMessage, config httpchannel.Config) (uvim.OutboundMessage, error) {
	if len(msg.Resources) == 0 {
		return msg, nil
	}
	if len(msg.Resources) != 1 {
		return msg, fmt.Errorf("zulip send: one resource per message is supported")
	}
	ref := msg.Resources[0]
	if config.ResourceStore == nil || !strings.HasPrefix(strings.TrimSpace(ref.InternalURL), "internal://") {
		return msg, fmt.Errorf("zulip upload: internal resource is required")
	}
	file, _, err := config.ResourceStore.Open(ref.InternalURL)
	if err != nil {
		return msg, uvim.NewProviderSendError("zulip resource is unavailable", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxSimpleUploadBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return msg, uvim.NewProviderSendError("zulip resource read failed", readErr)
	}
	if closeErr != nil {
		return msg, uvim.NewProviderSendError("zulip resource close failed", closeErr)
	}
	if len(data) == 0 {
		return msg, fmt.Errorf("zulip upload: empty resources are not supported")
	}
	if len(data) > maxSimpleUploadBytes {
		return msg, fmt.Errorf("zulip upload: resource exceeds %d bytes", maxSimpleUploadBytes)
	}
	name := uvim.ResourceUploadName(0, ref, ref.MIME)
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="filename"; filename=%q`, name))
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(config.BaseURL, "/")+"/api/v1/user_uploads", &body)
	if err != nil {
		return msg, err
	}
	if token := strings.TrimSpace(config.Token); token != "" {
		req.Header.Set("Authorization", httpchannel.Authorization(token))
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
		return msg, uvim.NewProviderSendError(fmt.Sprintf("zulip upload: http %d", resp.StatusCode), fmt.Errorf("zulip upload: http %d", resp.StatusCode))
	}
	var decoded struct {
		Result   string `json:"result"`
		Message  string `json:"msg"`
		URL      string `json:"url"`
		URI      string `json:"uri"`
		Filename string `json:"filename"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return msg, fmt.Errorf("zulip upload: decode response: %w", err)
	}
	if decoded.Result != "success" {
		businessErr := fmt.Errorf("zulip upload: result=%q msg=%q", decoded.Result, decoded.Message)
		return msg, uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	uploadURL := firstNonEmpty(decoded.URL, decoded.URI)
	if uploadURL == "" {
		return msg, fmt.Errorf("zulip upload: response missing url")
	}
	displayName := firstNonEmpty(decoded.Filename, name)
	displayName = strings.NewReplacer("[", "", "]", "").Replace(displayName)
	link := "[" + displayName + "](" + uploadURL + ")"
	if strings.TrimSpace(msg.Text) == "" {
		msg.Text = link
	} else {
		msg.Text = strings.TrimSpace(msg.Text) + "\n\n" + link
	}
	msg.Resources = nil
	return msg, nil
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var msg struct {
		ID          int64  `json:"id"`
		SenderID    int64  `json:"sender_id"`
		SenderEmail string `json:"sender_email"`
		DisplayName string `json:"sender_full_name"`
		StreamID    int64  `json:"stream_id"`
		Subject     string `json:"subject"`
		Content     string `json:"content"`
		Type        string `json:"type"`
		Attachments []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			URL  string `json:"url"`
			Path string `json:"path"`
			MIME string `json:"mime_type"`
			Size int64  `json:"size"`
		} `json:"attachments"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return uvim.Event{}, false, err
	}
	if msg.ID == 0 {
		return uvim.Event{}, false, nil
	}
	channelID := firstNonEmpty(num(msg.StreamID), msg.SenderEmail)
	channelType := uvim.ChannelGroup
	targetKind := uvim.TargetGroup
	if msg.Type == "private" {
		channelType = uvim.ChannelDirect
		targetKind = uvim.TargetUser
	}
	refs := make([]uvim.ResourceRef, 0, len(msg.Attachments))
	for _, attachment := range msg.Attachments {
		rawURL := firstNonEmpty(attachment.URL, attachment.Path)
		if rawURL == "" {
			continue
		}
		if strings.HasPrefix(rawURL, "/") && config.BaseURL != "" {
			rawURL = strings.TrimRight(config.BaseURL, "/") + rawURL
		}
		refs = append(refs, uvim.ResourceRef{
			Provider:  "zulip",
			Connector: config.ConnectorID,
			Kind:      uvim.ResourceKindFromMIME(attachment.MIME, uvim.ElementFile),
			Name:      attachment.Name,
			Key:       attachment.ID,
			URL:       rawURL,
			MIME:      attachment.MIME,
			SizeBytes: attachment.Size,
			Secret:    config.Token,
		})
	}
	return uvim.Event{
		ID:        num(msg.ID),
		Type:      uvim.EventMessageCreate,
		Provider:  "zulip",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: channelID, Type: channelType, Name: msg.Subject},
		User:      uvim.User{ID: num(msg.SenderID), Name: msg.DisplayName},
		Message:   uvim.Message{ID: num(msg.ID), Text: msg.Content, Type: msg.Type, Resources: refs},
		Referrer:  uvim.Referrer{MessageID: num(msg.ID), ChannelID: channelID, ThreadID: msg.Subject, Target: &uvim.OutboundTarget{ID: channelID, Kind: targetKind}},
		Addressed: true,
	}, true, nil
}

func Send(msg uvim.OutboundMessage, _ httpchannel.Config) (httpchannel.Request, error) {
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return httpchannel.Request{}, fmt.Errorf("zulip send: target id is required")
	}
	form := url.Values{"content": []string{msg.Text}}
	if target.Kind == uvim.TargetUser {
		form.Set("type", "private")
		form.Set("to", target.ID)
	} else {
		form.Set("type", "stream")
		form.Set("to", target.ID)
		form.Set("topic", firstNonEmpty(msg.Referrer.ThreadID, msg.Metadata["topic"]))
	}
	return httpchannel.Request{Path: "/api/v1/messages", Form: form}, nil
}

func ParseSendResponse(raw []byte) (string, error) {
	var response struct {
		Result  string `json:"result"`
		Message string `json:"msg"`
		ID      any    `json:"id"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if response.Result != "success" {
		businessErr := fmt.Errorf("result=%q msg=%q", response.Result, response.Message)
		return "", uvim.NewProviderSendError(businessErr.Error(), businessErr)
	}
	if response.ID == nil {
		return "", nil
	}
	return fmt.Sprint(response.ID), nil
}

func num(n int64) string {
	if n == 0 {
		return ""
	}
	return strconv.FormatInt(n, 10)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
