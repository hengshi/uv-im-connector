package whatsapp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
)

func TestSendKeepsNativeAttachmentCaption(t *testing.T) {
	for _, tc := range []struct{ kind, mediaType, text, caption string }{
		{uvim.ElementFile, "document", "  summary\n", "  summary\n"},
		{uvim.ElementAudio, "audio", " \n", ""},
	} {
		t.Run(tc.mediaType, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/phone/media":
					fmt.Fprint(w, "{\"id\":\"asset\"}")
				case "/phone/messages":
					var body map[string]json.RawMessage
					if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					var media map[string]string
					if err := json.Unmarshal(body[tc.mediaType], &media); err != nil || media["id"] != "asset" || media["caption"] != tc.caption {
						t.Errorf("native %s = %+v, %v", tc.mediaType, media, err)
					}
					fmt.Fprint(w, "{\"messages\":[{\"id\":\"sent\"}]}")
				default:
					t.Errorf("unexpected request %s", req.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer api.Close()
			store := &uvim.ResourceStore{Dir: t.TempDir()}
			ref, err := store.Save(t.Context(), strings.NewReader("bytes"), uvim.ResourceRef{Kind: tc.kind, Name: "报告.pdf"})
			if err != nil {
				t.Fatal(err)
			}
			p, err := New(Config{BaseURL: api.URL, PhoneNumberID: "phone", ResourceStore: store, HTTPClient: api.Client()})
			if err != nil {
				t.Fatal(err)
			}
			msg := uvim.OutboundMessage{Text: tc.text, Target: &uvim.OutboundTarget{Kind: uvim.TargetUser, ID: "123"}, Resources: []uvim.ResourceRef{ref}}
			if result, err := p.Send(t.Context(), msg); err != nil || result.MessageID != "sent" {
				t.Fatalf("send = %+v, %v", result, err)
			}
		})
	}
}
