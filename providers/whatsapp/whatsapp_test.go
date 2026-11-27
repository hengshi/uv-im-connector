package whatsapp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
)

func TestSendDocumentKeepsCaptionInOneMessage(t *testing.T) {
	for _, tc := range []struct {
		name, kind, nativeType, caption string
		rejectUpload                    bool
	}{
		{"document with caption", uvim.ElementFile, "document", "  document description\n", false},
		{"upload rejected", uvim.ElementFile, "document", "  document description\n", true},
		{"audio with a blank caption", uvim.ElementAudio, "audio", " \n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := make(chan string, 4)
			native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls <- req.URL.Path
				switch req.URL.Path {
				case "/test-phone/media":
					if tc.rejectUpload {
						http.Error(w, "test rejected media", http.StatusBadRequest)
						return
					}
					fmt.Fprint(w, "{\"id\":\"media\"}")
				case "/test-phone/messages":
					var body map[string]json.RawMessage
					if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					var media map[string]string
					wantCaption := tc.caption
					if strings.TrimSpace(wantCaption) == "" {
						wantCaption = ""
					}
					if err := json.Unmarshal(body[tc.nativeType], &media); err != nil || media["id"] != "media" || media["caption"] != wantCaption || (tc.nativeType == "document" && media["filename"] != "报告.pdf") {
						t.Errorf("media = %+v, %v", media, err)
					}
					fmt.Fprint(w, "{\"messages\":[{\"id\":\"sent\"}]}")
				default:
					t.Errorf("unexpected path: %s", req.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer native.Close()

			store := &uvim.ResourceStore{Dir: t.TempDir()}
			ref, err := store.Save(t.Context(), strings.NewReader("attachment"), uvim.ResourceRef{Kind: tc.kind, Name: "报告.pdf", MIME: "application/pdf"})
			if err != nil {
				t.Fatal(err)
			}
			provider, err := New(Config{Token: "token", BaseURL: native.URL, PhoneNumberID: "test-phone", ResourceStore: store, HTTPClient: native.Client()})
			if err != nil {
				t.Fatal(err)
			}
			result, err := provider.Send(t.Context(), uvim.OutboundMessage{Target: &uvim.OutboundTarget{Kind: uvim.TargetUser, ID: "123"}, Text: tc.caption, Resources: []uvim.ResourceRef{ref}})
			want := []string{"/test-phone/media"}
			if tc.rejectUpload {
				if err == nil {
					t.Fatal("send succeeded after upload rejection")
				}
			} else {
				want = append(want, "/test-phone/messages")
				if err != nil || result.MessageID != "sent" {
					t.Fatalf("send result = %+v, %v", result, err)
				}
			}
			var got []string
			for len(calls) > 0 {
				got = append(got, <-calls)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("native calls %v, want %v", got, want)
			}
		})
	}
}
