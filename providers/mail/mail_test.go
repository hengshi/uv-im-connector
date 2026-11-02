package mail

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"mime"
	"mime/multipart"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strings"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

func TestSendTextAndAttachmentsThroughSMTP(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(t.Context(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, reply string
		attachments int
	}{
		{"text", "", 0}, {"reply", "original@example.com", 0}, {"attachments", "", 11},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var raw []byte
			var sends int
			provider, err := New(Config{
				SMTPAddr: "smtp.example.com:25", SMTPUsername: "bot@example.com", SMTPPassword: "test-password", From: "bot@example.com",
				ResourceStore: store, Now: func() time.Time { return time.Unix(1, 0).UTC() },
				SendMail: func(addr string, auth smtp.Auth, from string, to []string, msg []byte) error {
					sends++
					if addr != "smtp.example.com:25" || from != "bot@example.com" || len(to) != 1 || to[0] != "user@example.com" {
						t.Errorf("addr=%q from=%q to=%v", addr, from, to)
					}
					if auth == nil {
						t.Error("missing SMTP auth")
					} else {
						mechanism, credentials, err := auth.Start(&smtp.ServerInfo{Name: "smtp.example.com", TLS: true})
						if err != nil || mechanism != "PLAIN" || string(credentials) != "\x00bot@example.com\x00test-password" {
							t.Errorf("SMTP authentication mechanism=%s err=%v", mechanism, err)
						}
					}
					raw = append([]byte(nil), msg...)
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			msg := uvim.OutboundMessage{ID: "sent1", Target: &uvim.OutboundTarget{ID: "user@example.com", Kind: uvim.TargetUser}, Text: "# 结论\n**完成**", Referrer: uvim.Referrer{MessageID: tt.reply}}
			for range tt.attachments {
				msg.Resources = append(msg.Resources, ref)
			}
			result, err := provider.Send(t.Context(), msg)
			if err != nil {
				t.Fatal(err)
			}
			if sends != 1 || result.Provider != "mail" || result.MessageID != "sent1" || !result.Time.Equal(time.Unix(1, 0)) {
				t.Fatalf("sends=%d result=%+v", sends, result)
			}
			message, err := mail.ReadMessage(bytes.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			if message.Header.Get("From") != "bot@example.com" || message.Header.Get("To") != "user@example.com" || message.Header.Get("In-Reply-To") != tt.reply {
				t.Fatalf("headers = %v", message.Header)
			}
			mediaType, params, err := mime.ParseMediaType(message.Header.Get("Content-Type"))
			if err != nil {
				t.Fatal(err)
			}
			if tt.attachments == 0 {
				body, err := io.ReadAll(message.Body)
				if err != nil || mediaType != "text/plain" || string(body) != "# 结论\n**完成**\r\n" {
					t.Fatalf("type=%s body=%q err=%v", mediaType, body, err)
				}
				return
			}
			if mediaType != "multipart/mixed" {
				t.Fatalf("media type = %s", mediaType)
			}
			reader := multipart.NewReader(message.Body, params["boundary"])
			for i := 0; ; i++ {
				part, err := reader.NextPart()
				if err == io.EOF {
					if i != tt.attachments+1 {
						t.Fatalf("MIME part count = %d", i)
					}
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				var body []byte
				if i == 0 {
					body, err = io.ReadAll(part)
					if string(body) != "# 结论\n**完成**\r\n" || part.Header.Get("Content-Type") != `text/plain; charset="utf-8"` {
						t.Errorf("text part = %q headers=%v", body, part.Header)
					}
				} else {
					body, err = io.ReadAll(base64.NewDecoder(base64.StdEncoding, part))
					if string(body) != "report" || part.FileName() != "report.txt" || part.Header.Get("Content-Type") != "text/plain" {
						t.Errorf("attachment = %q headers=%v", body, part.Header)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSendRejectsExternalResourceBeforeSMTP(t *testing.T) {
	called := false
	provider, err := New(Config{
		SMTPAddr:      "smtp.example.com:25",
		SMTPUsername:  "bot@example.com",
		ResourceStore: &uvim.ResourceStore{Dir: t.TempDir()},
		SendMail: func(string, smtp.Auth, string, []string, []byte) error {
			called = true
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Target:    &uvim.OutboundTarget{ID: "user@example.com", Kind: uvim.TargetUser},
		Resources: []uvim.ResourceRef{{Kind: uvim.ElementFile, URL: "https://example.com/report.txt"}},
	})
	if err == nil || !strings.Contains(err.Error(), "internal resource") {
		t.Fatalf("Send() error = %v", err)
	}
	if called {
		t.Fatal("SMTP called")
	}
}

func TestSendPreservesSMTPStatusWithoutMessageText(t *testing.T) {
	provider, err := New(Config{
		SMTPAddr:     "smtp.example.com:25",
		SMTPUsername: "bot@example.com",
		From:         "bot@example.com",
		SendMail: func(string, smtp.Auth, string, []string, []byte) error {
			return &textproto.Error{Code: 550, Msg: "recipient secret@example.test rejected"}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Target: &uvim.OutboundTarget{ID: "user@example.com", Kind: uvim.TargetUser},
		Text:   "hello",
	})
	if err == nil {
		t.Fatal("Send() error = nil")
	}
	if got := uvim.ProviderSendErrorLogDetail(err); got != "mail send: remote protocol error: code 550" {
		t.Fatalf("private log detail = %q", got)
	}
	if strings.Contains(uvim.ProviderSendErrorLogDetail(err), "secret@example.test") {
		t.Fatalf("private log detail leaked SMTP text: %q", uvim.ProviderSendErrorLogDetail(err))
	}
}

func TestSendMarksFixedLocalFailure(t *testing.T) {
	provider, err := New(Config{
		SMTPAddr:     "smtp.example.com:25",
		SMTPUsername: "bot@example.com",
		From:         "bot@example.com",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Target: &uvim.OutboundTarget{ID: "user@example.com", Kind: uvim.TargetUser},
	})
	if err == nil {
		t.Fatal("Send() error = nil")
	}
	if got := uvim.ProviderSendErrorLogDetail(err); got != "mail send: text or resource is required" {
		t.Fatalf("private log detail = %q", got)
	}
}
