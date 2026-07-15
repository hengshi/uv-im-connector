package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
)

func TestSendResourceUsesMultipartBotAPI(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/bottoken/sendDocument" {
			t.Errorf("path = %q", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := req.ParseMultipartForm(maxMediaBytes); err != nil {
			t.Error(err)
		}
		if req.FormValue("chat_id") != "123" || req.FormValue("reply_parameters") != `{"message_id":9}` {
			t.Errorf("form = %+v", req.MultipartForm.Value)
		}
		file, header, err := req.FormFile("document")
		if err != nil {
			t.Error(err)
		} else {
			defer file.Close()
			var data bytes.Buffer
			_, _ = data.ReadFrom(file)
			if header.Filename != "report.txt" || data.String() != "report" {
				t.Errorf("filename=%q data=%q", header.Filename, data.String())
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 10}})
	}))
	defer server.Close()
	provider, err := New(Config{Token: "token", BaseURL: server.URL, ResourceStore: store, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if !provider.Capabilities().UploadResource {
		t.Fatal("UploadResource = false")
	}
	result, err := provider.Send(context.Background(), uvim.OutboundMessage{
		ChannelID: "123",
		Resources: []uvim.ResourceRef{ref},
		Referrer:  uvim.Referrer{MessageID: "9"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "10" {
		t.Fatalf("result = %+v", result)
	}
}

func TestTelegramMediaRouteFallsBackForUnsupportedNativeFormats(t *testing.T) {
	tests := []struct {
		ref        uvim.ResourceRef
		wantMethod string
		wantField  string
	}{
		{ref: uvim.ResourceRef{Kind: uvim.ElementImage, MIME: "image/png"}, wantMethod: "sendPhoto", wantField: "photo"},
		{ref: uvim.ResourceRef{Kind: uvim.ElementAudio, MIME: "audio/mpeg"}, wantMethod: "sendAudio", wantField: "audio"},
		{ref: uvim.ResourceRef{Kind: uvim.ElementVideo, MIME: "video/mp4"}, wantMethod: "sendVideo", wantField: "video"},
		{ref: uvim.ResourceRef{Kind: uvim.ElementAudio, MIME: "audio/wav"}, wantMethod: "sendDocument", wantField: "document"},
		{ref: uvim.ResourceRef{Kind: uvim.ElementVideo, MIME: "video/quicktime"}, wantMethod: "sendDocument", wantField: "document"},
	}
	for _, test := range tests {
		method, field, _ := telegramMediaRoute(test.ref)
		if method != test.wantMethod || field != test.wantField {
			t.Fatalf("route(%+v) = %q/%q, want %q/%q", test.ref, method, field, test.wantMethod, test.wantField)
		}
	}
}
