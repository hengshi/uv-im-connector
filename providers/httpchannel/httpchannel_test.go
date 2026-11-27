package httpchannel

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
)

func TestSendRejectsProviderBusinessErrorOnHTTP200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":false,"error":"channel_not_found"}`))
	}))
	defer server.Close()
	provider, err := New(Config{
		ProviderID: "test",
		BaseURL:    server.URL,
		Capabilities: uvim.Capabilities{
			Outbound:       true,
			ProactiveGroup: true,
			TargetKinds:    []string{uvim.TargetConversation},
		},
		Send: func(uvim.OutboundMessage, Config) (Request, error) {
			return Request{Path: "/send"}, nil
		},
		ParseSendResponse: func([]byte) (string, error) {
			businessErr := errors.New("channel_not_found")
			return "", uvim.NewProviderSendError(businessErr.Error(), businessErr)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "c1", Text: "hello"})
	if err == nil || !strings.Contains(err.Error(), "channel_not_found") {
		t.Fatalf("Send() error = %v", err)
	}
	if got := uvim.ProviderSendErrorDetail(err); !strings.Contains(got, "channel_not_found") {
		t.Fatalf("public detail = %q", got)
	}
}

func TestReadPrivateSendResponseCarriesSafeNormalizedHTTPFailure(t *testing.T) {
	resp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header: http.Header{
			"Retry-After":  []string{"9"},
			"X-Request-Id": []string{"req-lark-1"},
		},
		Body: io.NopCloser(strings.NewReader(`{"code":230020,"msg":"access_token=secret"}`)),
	}
	_, err := ReadPrivateSendResponse(resp, "lark send")
	if err == nil {
		t.Fatal("ReadPrivateSendResponse() error = nil")
	}
	failure, ok := uvim.ProviderSendFailure(err)
	if !ok {
		t.Fatal("private HTTP error has no normalized failure")
	}
	if failure.Category != uvim.SendFailureRateLimited || !failure.Retryable || failure.DeliveryState != uvim.DeliveryRejected || failure.ProviderCode != "230020" || failure.RetryAfterSeconds != 9 || failure.RequestID != "req-lark-1" {
		t.Fatalf("failure = %+v", failure)
	}
	if got := uvim.ProviderSendErrorDetail(err); got != "" {
		t.Fatalf("private response exposed public detail %q", got)
	}
	if got := uvim.ProviderSendErrorLogDetail(err); strings.Contains(got, "secret") {
		t.Fatalf("private response leaked body in log detail %q", got)
	}
}

func TestSendIncludesBoundedHTTPErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid recipient"}`))
	}))
	defer server.Close()
	provider, err := New(Config{
		ProviderID: "test",
		BaseURL:    server.URL,
		Capabilities: uvim.Capabilities{
			Outbound:       true,
			ProactiveGroup: true,
			TargetKinds:    []string{uvim.TargetConversation},
		},
		Send: func(uvim.OutboundMessage, Config) (Request, error) {
			return Request{Path: "/send"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "c1", Text: "hello"})
	if err == nil || !strings.Contains(err.Error(), "http 400") || !strings.Contains(err.Error(), "invalid recipient") {
		t.Fatalf("Send() error = %v", err)
	}
	if got := uvim.ProviderSendErrorDetail(err); got != "test send: http 400" {
		t.Fatalf("public detail = %q", got)
	}
}

func TestSendPreservesHTTPStatusWhenErrorBodyIsUnreadable(t *testing.T) {
	provider, err := New(Config{
		ProviderID: "test",
		BaseURL:    "https://api.example.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadGateway,
				Body:       io.NopCloser(errorReader{err: errors.New("access_token=secret")}),
				Header:     make(http.Header),
			}, nil
		})},
		Capabilities: uvim.Capabilities{
			Outbound:       true,
			ProactiveGroup: true,
			TargetKinds:    []string{uvim.TargetConversation},
		},
		Send: func(uvim.OutboundMessage, Config) (Request, error) {
			return Request{Path: "/private/send?access_token=secret"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "c1", Text: "hello"})
	if err == nil {
		t.Fatal("Send() error = nil")
	}
	if got := uvim.ProviderSendErrorDetail(err); got != "test send: http 502" {
		t.Fatalf("public detail = %q", got)
	}
	if got := uvim.ProviderSendErrorLogDetail(err); got != "test send: http 502" {
		t.Fatalf("private log detail = %q", got)
	}
}

func TestReadSendResponsePreservesStatusAndStructuredReadCause(t *testing.T) {
	tests := []struct {
		name       string
		operation  string
		publicOp   string
		statusCode int
		readErr    error
		private    bool
		wantLog    string
		wantPublic string
	}{
		{
			name:       "public status before unreadable body",
			operation:  "matrix upload",
			publicOp:   "matrix send",
			statusCode: http.StatusBadGateway,
			readErr:    errors.New("access_token=secret"),
			wantLog:    "matrix upload: http 502",
			wantPublic: "matrix send: http 502",
		},
		{
			name:       "private status before unreadable body",
			operation:  "lark send",
			statusCode: http.StatusBadGateway,
			readErr:    errors.New("access_token=secret"),
			private:    true,
			wantLog:    "lark send: http 502",
		},
		{
			name:       "successful status with reset body",
			operation:  "slack upload",
			statusCode: http.StatusOK,
			readErr:    &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
			wantLog:    "slack upload: read response: read tcp: connection reset",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				StatusCode: tt.statusCode,
				Body:       io.NopCloser(errorReader{err: tt.readErr}),
				Header:     make(http.Header),
			}
			var err error
			if tt.private {
				_, err = ReadPrivateSendResponse(resp, tt.operation)
			} else if tt.publicOp != "" {
				_, err = ReadSendResponseWithPublicOperation(resp, tt.operation, tt.publicOp)
			} else {
				_, err = ReadSendResponse(resp, tt.operation)
			}
			if err == nil {
				t.Fatal("ReadSendResponse() error = nil")
			}
			if got := uvim.ProviderSendErrorLogDetail(err); got != tt.wantLog {
				t.Fatalf("private log detail = %q, want %q", got, tt.wantLog)
			}
			if got := uvim.ProviderSendErrorDetail(err); got != tt.wantPublic {
				t.Fatalf("public detail = %q, want %q", got, tt.wantPublic)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestSendRejectsUnsupportedExplicitTarget(t *testing.T) {
	provider, err := New(Config{
		ProviderID: "test",
		Capabilities: uvim.Capabilities{
			Outbound:        true,
			ProactiveDirect: true,
			TargetKinds:     []string{uvim.TargetUser},
		},
		Send: func(uvim.OutboundMessage, Config) (Request, error) {
			return Request{Path: "/send"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Target: &uvim.OutboundTarget{ID: "g1", Kind: uvim.TargetGroup},
		Text:   "hello",
	})
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("Send() error = %v", err)
	}
}

func TestMultipartRequestStreamsFilesWithExactLength(t *testing.T) {
	source := strings.NewReader("first-file")
	var redirected bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/upload" {
			http.Redirect(w, req, "/destination", http.StatusTemporaryRedirect)
			return
		}
		redirected = req.URL.Path == "/destination" && req.Method == http.MethodPost
		body, err := io.ReadAll(req.Body)
		if err != nil || int64(len(body)) != req.ContentLength || !strings.Contains(string(body), "first-file") {
			t.Errorf("redirected upload bytes=%d length=%d error=%v", len(body), req.ContentLength, err)
		}
	}))
	defer api.Close()
	req, err := MultipartRequest(t.Context(), api.URL+"/upload", MultipartBody{
		Fields: map[string]string{"key": "value"},
		Files: []MultipartFile{
			{Field: "first", Name: "报告.pdf", Reader: source, Size: source.Size()},
			{Field: "second", Name: "empty.txt", Data: []byte{}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if source.Len() != int(source.Size()) {
		t.Fatal("multipart request construction read the attachment")
	}
	response, err := api.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 || !redirected {
		t.Fatalf("did not replay upload: %s", response.Status)
	}
	req.Body, err = req.GetBody()
	if err != nil {
		t.Fatal(err)
	}
	if err := req.ParseMultipartForm(1 << 20); err != nil {
		t.Fatal(err)
	}
	defer req.MultipartForm.RemoveAll()
	for _, check := range []struct{ field, name, content string }{
		{"first", "报告.pdf", "first-file"}, {"second", "empty.txt", ""},
	} {
		file, header, err := req.FormFile(check.field)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(file)
		file.Close()
		if err != nil || header.Filename != check.name || string(raw) != check.content {
			t.Fatalf("%s: %q %q %v", check.field, header.Filename, raw, err)
		}
	}
	if req.FormValue("key") != "value" {
		t.Fatal("lost non-file form field")
	}
}
