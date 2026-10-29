package wecom

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

type directoryTransport func(*http.Request) (*http.Response, error)

func (f directoryTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func directoryTestClient(t *testing.T, handler http.HandlerFunc) *http.Client {
	t.Helper()
	api := httptest.NewServer(handler)
	t.Cleanup(api.Close)
	endpoint, err := url.Parse(api.URL)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{Transport: directoryTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "qyapi.weixin.qq.com" || req.URL.Scheme != "https" {
			t.Errorf("unexpected credential destination: %s", req.URL.Host)
		}
		copy := req.Clone(req.Context())
		copy.URL.Scheme, copy.URL.Host = endpoint.Scheme, endpoint.Host
		return http.DefaultTransport.RoundTrip(copy)
	})}
}

func TestDirectoryIdentityProof(t *testing.T) {
	for _, tc := range []struct {
		name, native, conversion, profile, wantID string
	}{
		{"plaintext including case-insensitive member lookup", "woPlainAccount", `{"errcode":0,"invalid_open_userid_list":["woPlainAccount"]}`, `{"errcode":0,"userid":"WOPLAINACCOUNT","name":"Directory Name"}`, "WOPLAINACCOUNT"},
		{"encrypted short callback", "alice", `{"errcode":0,"userid_list":[{"open_userid":"alice","userid":"employee-17"}]}`, `{"errcode":0,"userid":"employee-17","name":"Directory Name"}`, "employee-17"},
		{"trusted IP denial", "alice", `{"errcode":60020,"errmsg":"leaked application-secret access-token"}`, "", ""},
		{"invalid input is not plaintext proof", "alice", `{"errcode":0,"invalid_open_userid_list":["alice"]}`, `{"errcode":60111}`, ""},
		{"converted identity survives profile denial", "alice", `{"errcode":0,"userid_list":[{"open_userid":"alice","userid":"employee-17"}]}`, `{"errcode":48002}`, "employee-17"},
		{"another conversion subject", "alice", `{"errcode":0,"userid_list":[{"open_userid":"other","userid":"employee-17"}]}`, "", ""},
		{"another directory subject", "alice", `{"errcode":0,"invalid_open_userid_list":["alice"]}`, `{"errcode":0,"userid":"other","name":"Wrong Name"}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			client := directoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch r.URL.Path {
				case "/cgi-bin/gettoken":
					if r.Method != http.MethodGet || r.URL.Query().Get("corpid") != "corp" || r.URL.Query().Get("corpsecret") != "application-secret" || r.URL.Query().Has("agentid") {
						t.Error("token request must use enterprise ID and application Secret")
					}
					_, _ = io.WriteString(w, `{"errcode":0,"access_token":"access-token","expires_in":7200}`)
				case "/cgi-bin/batch/openuserid_to_userid":
					var body struct {
						IDs []string `json:"open_userid_list"`
					}
					if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&body) != nil || !reflect.DeepEqual(body.IDs, []string{tc.native}) {
						t.Error("conversion did not use the exact callback sender")
					}
					_, _ = io.WriteString(w, tc.conversion)
				case "/cgi-bin/user/get":
					if tc.profile == "" || r.URL.Query().Get("userid") == "" {
						t.Error("unexpected member lookup")
					}
					_, _ = io.WriteString(w, tc.profile)
				default:
					t.Error("unexpected API path")
				}
				if r.URL.Path != "/cgi-bin/gettoken" && r.URL.Query().Get("access_token") != "access-token" {
					t.Error("missing application token")
				}
			})
			var logs bytes.Buffer
			p, err := New(Config{BotID: "bot", Secret: "bot-secret", CorpID: "corp", CorpSecret: "application-secret", HTTPClient: client, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
			if err != nil {
				t.Fatal(err)
			}
			in := runTestMessage(0)
			in.Body["from"] = map[string]any{"userid": tc.native}
			event, ok := p.decodeMessage(in)
			if !ok {
				t.Fatal("message was not decoded")
			}
			originalTarget := *event.Referrer.Target
			p.enrichDirectoryUser(t.Context(), &event)
			if event.User.ID != tc.native || *event.Referrer.Target != originalTarget || event.Channel.ID != "chat-1" || event.User.Identities[0].ID != tc.native {
				t.Fatalf("native identity or routing changed: %+v", event)
			}
			wantLen := 1
			if tc.wantID != "" {
				wantLen++
			}
			if len(event.User.Identities) != wantLen {
				t.Fatalf("identities = %+v", event.User.Identities)
			}
			if tc.wantID != "" && event.User.Identities[1] != (uvim.UserIdentity{Type: "wecom.userid", ID: tc.wantID, Scope: "corp", Source: uvim.IdentitySourceLookup}) {
				t.Fatalf("enterprise identity = %+v", event.User.Identities[1])
			}
			if strings.Contains(tc.profile, "Directory Name") && event.User.DisplayName != "Directory Name" {
				t.Fatalf("directory name = %q", event.User.DisplayName)
			}
			if tc.wantID == "" && event.User.DisplayName != "" {
				t.Fatal("unverified name was attached")
			}
			before := calls.Load()
			p.enrichDirectoryUser(t.Context(), &event)
			if calls.Load() != before {
				t.Fatal("successful or failed lookup was not cached")
			}
			if strings.Contains(logs.String(), "application-secret") || strings.Contains(logs.String(), "access-token") {
				t.Fatal("credential leaked in lookup diagnostics")
			}
		})
	}
}

func TestDirectoryCacheAndTokenRefresh(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var tokens, conversions atomic.Int32
	client := directoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			tokens.Add(1)
			_, _ = io.WriteString(w, `{"errcode":0,"access_token":"token","expires_in":7200}`)
		case "/cgi-bin/batch/openuserid_to_userid":
			if conversions.Add(1) == 2 { // Token revoked before advertised expiry.
				_, _ = io.WriteString(w, `{"errcode":42001}`)
			} else {
				_, _ = io.WriteString(w, `{"errcode":0,"userid_list":[{"open_userid":"user-1","userid":"employee"}]}`)
			}
		case "/cgi-bin/user/get":
			_, _ = io.WriteString(w, `{"errcode":0,"userid":"employee","name":"Directory Name"}`)
		}
	})
	p, err := New(Config{BotID: "bot", Secret: "secret", CorpID: "corp", CorpSecret: "app", HTTPClient: client, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	for i, advance := range []time.Duration{0, 11 * time.Minute, 2 * time.Hour} {
		now = now.Add(advance)
		event, _ := p.decodeMessage(runTestMessage(0))
		event.User.DisplayName = "Existing Name"
		p.enrichDirectoryUser(t.Context(), &event)
		if tokens.Load() != int32(i+1) || len(event.User.Identities) != 2 || event.User.DisplayName != "Existing Name" {
			t.Fatalf("step=%d tokens=%d user=%+v", i, tokens.Load(), event.User)
		}
	}
	if conversions.Load() != 4 {
		t.Fatalf("conversion calls = %d", conversions.Load())
	}
}

func TestDirectoryEnrichmentIsOptional(t *testing.T) {
	for _, config := range []Config{{CorpID: "corp"}, {CorpSecret: "app"}} {
		config.BotID, config.Secret = "bot", "secret"
		if _, err := New(config); err == nil {
			t.Fatal("partial application credentials accepted")
		}
	}
	for _, kind := range []string{"", uvim.UserKindChat} {
		config := Config{BotID: "bot", Secret: "secret", HTTPClient: &http.Client{Transport: directoryTransport(func(*http.Request) (*http.Response, error) {
			t.Fatal("disabled or non-user enrichment made an HTTP call")
			return nil, nil
		})}}
		if kind != "" {
			config.CorpID, config.CorpSecret = "corp", "app"
		}
		p, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		event := uvim.Event{User: uvim.User{ID: "user-1", Kind: kind}}
		p.enrichDirectoryUser(context.Background(), &event)
		if len(event.User.Identities) != 0 {
			t.Fatal("unexpected enterprise identity")
		}
	}
}

func TestDirectoryLookupDoesNotBlockSocketACKs(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	client := directoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			_, _ = io.WriteString(w, `{"errcode":0,"access_token":"token","expires_in":7200}`)
		case "/cgi-bin/batch/openuserid_to_userid":
			close(started)
			select {
			case <-release:
				_, _ = io.WriteString(w, `{"errcode":0,"userid_list":[{"open_userid":"user-1","userid":"employee"}]}`)
			case <-r.Context().Done():
			}
		case "/cgi-bin/user/get":
			_, _ = io.WriteString(w, `{"errcode":0,"userid":"employee","name":"Directory Name"}`)
		}
	})
	wsURL, _, _ := newRunTestServer(t, []frame{runTestMessage(0)}, true)
	p, err := New(Config{BotID: "bot", Secret: "secret", CorpID: "corp", CorpSecret: "app", HTTPClient: client, WSURL: wsURL, AckTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events, done := make(chan uvim.Event, 1), make(chan error, 1)
	go func() {
		done <- p.Run(ctx, uvim.EventSinkFunc(func(_ context.Context, event uvim.Event) error {
			events <- event
			return nil
		}))
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("directory lookup did not start through Run")
	}
	if _, err := p.Send(ctx, uvim.OutboundMessage{Target: &uvim.OutboundTarget{Kind: uvim.TargetGroup, ID: "chat-1"}, Text: "ACK probe"}); err != nil {
		t.Fatalf("blocked lookup stalled the socket ACK dispatcher: %v", err)
	}
	close(release)
	select {
	case event := <-events:
		if len(event.User.Identities) != 2 || event.User.Identities[1].ID != "employee" || event.User.ID != "user-1" || event.Referrer.Target.ID != "chat-1" {
			t.Fatalf("Run did not emit enriched metadata with native routing: %+v", event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("enriched event was not emitted")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}
