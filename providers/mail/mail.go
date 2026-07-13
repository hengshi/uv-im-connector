package mail

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/mail"
	"net/smtp"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
)

type Config struct {
	ConnectorID   string
	SMTPAddr      string
	SMTPUsername  string
	SMTPPassword  string
	From          string
	WebhookSecret string
	ResourceStore *uvim.ResourceStore
	HTTPClient    *http.Client
	Now           func() time.Time
	Logger        *slog.Logger
}

type Provider struct {
	base   *httpchannel.Provider
	config Config
	now    func() time.Time
}

func New(config Config) (*Provider, error) {
	if config.Now == nil {
		config.Now = time.Now
	}
	base, err := httpchannel.New(httpchannel.Config{
		ProviderID:    "mail",
		ConnectorID:   firstNonEmpty(config.ConnectorID, "mail"),
		WebhookSecret: config.WebhookSecret,
		ResourceStore: config.ResourceStore,
		HTTPClient:    config.HTTPClient,
		Now:           config.Now,
		Logger:        config.Logger,
		Decode:        Decode,
		Capabilities: uvim.Capabilities{
			Inbound:          true,
			Outbound:         true,
			DirectMessage:    true,
			ReplyMessage:     true,
			ProactiveDirect:  true,
			TargetKinds:      []string{uvim.TargetUser},
			DownloadResource: true,
			ResourceKinds:    []string{uvim.ElementImage, uvim.ElementAudio, uvim.ElementVideo, uvim.ElementFile},
			ChannelTypes:     []string{uvim.ChannelDirect},
		},
	})
	if err != nil {
		return nil, err
	}
	return &Provider{base: base, config: config, now: config.Now}, nil
}

func (p *Provider) ID() string          { return p.base.ID() }
func (p *Provider) ConnectorID() string { return p.base.ConnectorID() }
func (p *Provider) Capabilities() uvim.Capabilities {
	return p.base.Capabilities()
}
func (p *Provider) Run(ctx context.Context, sink uvim.EventSink) error { return p.base.Run(ctx, sink) }
func (p *Provider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	return p.base.Download(ctx, req)
}
func (p *Provider) Health(ctx context.Context) uvim.Health {
	health := p.base.Health(ctx)
	if strings.TrimSpace(p.config.SMTPAddr) == "" {
		health.State = "degraded"
		health.Reason = "smtp_addr_missing"
	}
	return health
}
func (p *Provider) ServeWebhook(w http.ResponseWriter, req *http.Request, sink uvim.EventSink) {
	p.base.ServeWebhook(w, req, sink)
}

func (p *Provider) Send(ctx context.Context, msg uvim.OutboundMessage) (uvim.SendResult, error) {
	if err := uvim.ValidateOutboundTarget(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("mail send: %w", err)
	}
	if len(msg.Resources) > 0 || hasNonTextElements(msg.Elements) {
		return uvim.SendResult{}, fmt.Errorf("mail send: resources and rich elements are not supported")
	}
	if strings.TrimSpace(msg.Text) == "" && len(msg.Elements) > 0 {
		msg.Text = textFromElements(msg.Elements)
	}
	if strings.TrimSpace(msg.Text) == "" {
		return uvim.SendResult{}, fmt.Errorf("mail send: text is required")
	}
	to := msg.ResolvedTarget().ID
	if _, err := mail.ParseAddress(to); err != nil {
		return uvim.SendResult{}, fmt.Errorf("mail send: recipient is required")
	}
	from := strings.TrimSpace(p.config.From)
	if from == "" {
		from = p.config.SMTPUsername
	}
	if _, err := mail.ParseAddress(from); err != nil {
		return uvim.SendResult{}, fmt.Errorf("mail send: from address is required")
	}
	if strings.TrimSpace(p.config.SMTPAddr) == "" {
		return uvim.SendResult{}, fmt.Errorf("mail send: smtp addr is required")
	}
	if err := smtp.SendMail(p.config.SMTPAddr, p.auth(), from, []string{to}, mailBytes(from, to, subject(msg), msg.Text, msg.Referrer.MessageID, p.now().UTC())); err != nil {
		return uvim.SendResult{}, err
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: uvim.FirstNonEmpty(msg.ID, uvim.NewID("mail-msg")), Time: p.now().UTC()}, nil
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var msg struct {
		ID          string `json:"id"`
		MessageID   string `json:"message_id"`
		From        string `json:"from"`
		FromName    string `json:"from_name"`
		To          string `json:"to"`
		Subject     string `json:"subject"`
		Text        string `json:"text"`
		HTML        string `json:"html"`
		Attachments []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			URL  string `json:"url"`
			MIME string `json:"mime"`
			Size int64  `json:"size"`
		} `json:"attachments"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return uvim.Event{}, false, err
	}
	id := firstNonEmpty(msg.ID, msg.MessageID)
	if id == "" || strings.TrimSpace(msg.From) == "" {
		return uvim.Event{}, false, nil
	}
	refs := make([]uvim.ResourceRef, 0, len(msg.Attachments))
	for _, attachment := range msg.Attachments {
		if strings.TrimSpace(attachment.URL) == "" {
			continue
		}
		refs = append(refs, uvim.ResourceRef{
			Provider:  "mail",
			Connector: config.ConnectorID,
			Kind:      kindFromMIME(attachment.MIME),
			Name:      attachment.Name,
			Key:       attachment.ID,
			URL:       attachment.URL,
			MIME:      attachment.MIME,
			SizeBytes: attachment.Size,
		})
	}
	text := firstNonEmpty(msg.Text, msg.HTML)
	return uvim.Event{
		ID:        id,
		Type:      uvim.EventMessageCreate,
		Provider:  "mail",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: msg.From, Type: uvim.ChannelDirect, Name: msg.Subject},
		User:      uvim.User{ID: msg.From, Name: msg.FromName},
		Message:   uvim.Message{ID: id, Text: text, Type: "email", Resources: refs},
		Referrer:  uvim.Referrer{MessageID: id, ChannelID: msg.From, Target: &uvim.OutboundTarget{ID: msg.From, Kind: uvim.TargetUser}},
		Addressed: true,
	}, true, nil
}

func (p *Provider) auth() smtp.Auth {
	if p.config.SMTPUsername == "" || p.config.SMTPPassword == "" {
		return nil
	}
	host := p.config.SMTPAddr
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	return smtp.PlainAuth("", p.config.SMTPUsername, p.config.SMTPPassword, host)
}

func mailBytes(from, to, subject, body, replyTo string, now time.Time) []byte {
	var buf bytes.Buffer
	headers := map[string]string{
		"From":         from,
		"To":           to,
		"Subject":      subject,
		"Date":         now.Format(time.RFC1123Z),
		"MIME-Version": "1.0",
		"Content-Type": `text/plain; charset="utf-8"`,
	}
	if strings.TrimSpace(replyTo) != "" {
		headers["In-Reply-To"] = strings.TrimSpace(replyTo)
		headers["References"] = strings.TrimSpace(replyTo)
	}
	for key, value := range headers {
		buf.WriteString(key)
		buf.WriteString(": ")
		buf.WriteString(strings.ReplaceAll(value, "\n", " "))
		buf.WriteString("\r\n")
	}
	buf.WriteString("\r\n")
	buf.WriteString(body)
	buf.WriteString("\r\n")
	return buf.Bytes()
}

func subject(msg uvim.OutboundMessage) string {
	if msg.Metadata != nil && strings.TrimSpace(msg.Metadata["subject"]) != "" {
		return strings.TrimSpace(msg.Metadata["subject"])
	}
	return "uv-im-connector message"
}

func kindFromMIME(mime string) string {
	switch {
	case strings.HasPrefix(mime, "image/"):
		return uvim.ElementImage
	case strings.HasPrefix(mime, "audio/"):
		return uvim.ElementAudio
	case strings.HasPrefix(mime, "video/"):
		return uvim.ElementVideo
	default:
		return uvim.ElementFile
	}
}

func hasNonTextElements(elements []uvim.Element) bool {
	for _, element := range elements {
		if element.Type != "" && element.Type != uvim.ElementText {
			return true
		}
		if element.Resource != nil {
			return true
		}
		if hasNonTextElements(element.Children) {
			return true
		}
	}
	return false
}

func textFromElements(elements []uvim.Element) string {
	parts := make([]string, 0, len(elements))
	for _, element := range elements {
		if element.Type == uvim.ElementText && strings.TrimSpace(element.Text) != "" {
			parts = append(parts, element.Text)
		}
		if childText := textFromElements(element.Children); childText != "" {
			parts = append(parts, childText)
		}
	}
	return strings.Join(parts, "\n")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
