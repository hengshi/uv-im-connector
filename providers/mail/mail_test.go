package mail

import (
	"bytes"
	"context"
	"net/smtp"
	"net/textproto"
	"strings"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

func TestSendIncludesInternalResourcesAsMIMEAttachments(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	provider, err := New(Config{
		SMTPAddr:      "smtp.example.com:25",
		SMTPUsername:  "bot@example.com",
		From:          "bot@example.com",
		ResourceStore: store,
		Now:           func() time.Time { return time.Unix(1, 0).UTC() },
		SendMail: func(_ string, _ smtp.Auth, from string, to []string, msg []byte) error {
			if from != "bot@example.com" || len(to) != 1 || to[0] != "user@example.com" {
				t.Fatalf("from=%q to=%v", from, to)
			}
			raw = append([]byte(nil), msg...)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !provider.Capabilities().UploadResource {
		t.Fatal("UploadResource = false")
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Target:    &uvim.OutboundTarget{ID: "user@example.com", Kind: uvim.TargetUser},
		Text:      "done",
		Resources: []uvim.ResourceRef{ref, ref, ref, ref, ref, ref, ref, ref, ref, ref, ref},
	})
	if err != nil {
		t.Fatal(err)
	}
	message := string(raw)
	if got := strings.Count(message, `filename="report.txt"`); got != 11 {
		t.Fatalf("attachments = %d", got)
	}
	for _, want := range []string{"Content-Type: multipart/mixed", `filename="report.txt"`, "Content-Transfer-Encoding: base64", "cmVwb3J0"} {
		if !strings.Contains(message, want) {
			t.Fatalf("message missing %q:\n%s", want, message)
		}
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
