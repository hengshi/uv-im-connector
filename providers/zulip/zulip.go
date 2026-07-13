package zulip

import (
	"encoding/json"
	"fmt"
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
}

func New(config Config) (*httpchannel.Provider, error) {
	return httpchannel.New(httpchannel.Config{
		ProviderID:        "zulip",
		ConnectorID:       firstNonEmpty(config.ConnectorID, "zulip"),
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
			ThreadReply:     true,
			ReplyMessage:    true,
			ProactiveDirect: true,
			ProactiveGroup:  true,
			TargetKinds:     []string{uvim.TargetUser, uvim.TargetGroup},
			ChannelTypes:    []string{uvim.ChannelDirect, uvim.ChannelGroup, uvim.ChannelThread},
		},
	})
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
