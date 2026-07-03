package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/client"
	"github.com/hengshi/uv-im-connector/providers/line"
	"github.com/hengshi/uv-im-connector/providers/memory"
	"github.com/hengshi/uv-im-connector/providers/slack"
)

func TestHubEventsAndOutbound(t *testing.T) {
	dir := t.TempDir()
	log, err := uvim.NewEventLog(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	provider := memory.New("test")
	hub := NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(dir, "resources")})
	if err := hub.Emit(context.Background(), uvim.Event{ID: "evt-1", Type: uvim.EventMessageCreate, Provider: "test", Message: uvim.Message{ID: "m1", Text: "hello"}}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var events struct {
		Events []uvim.Event `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	if len(events.Events) != 1 || events.Events[0].Message.Text != "hello" {
		t.Fatalf("events = %+v", events.Events)
	}

	raw, _ := json.Marshal(uvim.OutboundMessage{Provider: "test", ChannelID: "c1", Text: "reply"})
	resp, err = http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(provider.Sent()) != 1 || provider.Sent()[0].Text != "reply" {
		t.Fatalf("sent = %+v", provider.Sent())
	}
}

func TestHubRoutesOutboundByConnector(t *testing.T) {
	first := memory.NewConnector("lark", "main")
	second := memory.NewConnector("lark", "sandbox")
	hub := NewHub(uvim.NewProviderRegistry(first, second), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw, _ := json.Marshal(uvim.OutboundMessage{Provider: "lark", Connector: "sandbox", ChannelID: "c1", Text: "reply"})
	resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(first.Sent()) != 0 {
		t.Fatalf("main connector sent = %+v", first.Sent())
	}
	if len(second.Sent()) != 1 {
		t.Fatalf("sandbox connector sent = %+v", second.Sent())
	}
}

func TestHubDownloadsInboundResourcesBeforeEventLog(t *testing.T) {
	dir := t.TempDir()
	provider := &downloadProvider{id: "test", connector: "main"}
	log := mustEventLog(t, filepath.Join(dir, "events.jsonl"))
	hub := NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(dir, "resources")})
	err := hub.Emit(context.Background(), uvim.Event{
		ID:        "evt-1",
		Type:      uvim.EventMessageCreate,
		Provider:  "test",
		Connector: "main",
		Message: uvim.Message{
			ID:   "m1",
			Text: "file",
			Resources: []uvim.ResourceRef{{
				Provider:  "test",
				Connector: "main",
				Kind:      uvim.ElementFile,
				Name:      "secret.txt",
				Key:       "provider-key",
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	events, err := log.ReadAfter(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || len(events[0].Message.Resources) != 1 {
		t.Fatalf("events = %+v", events)
	}
	ref := events[0].Message.Resources[0]
	if ref.InternalURL == "" || ref.Key != "" || ref.Metadata != nil {
		t.Fatalf("resource was not resolved and sanitized: %+v", ref)
	}
}

func TestHubAuthProtectsAPIs(t *testing.T) {
	hub := NewHub(uvim.NewProviderRegistry(memory.New("test")), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	hub.SetAuthToken("token")
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	resp, err := http.Get(server.URL + "/v1/meta")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without token = %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/meta", nil)
	req.Header.Set("Authorization", "Bearer token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status with token = %d", resp.StatusCode)
	}
}

func TestHubWebhookRoutesToProviderAndStoresEvent(t *testing.T) {
	dir := t.TempDir()
	provider, err := slack.New(slack.Config{WebhookSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	log := mustEventLog(t, filepath.Join(dir, "events.jsonl"))
	hub := NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(dir, "resources")})
	hub.SetAuthToken("api-token")
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw := `{"type":"event_callback","event":{"type":"message","user":"u1","channel":"c1","text":"hello","ts":"m1"}}`
	resp, err := http.Post(server.URL+"/v1/webhook/slack/slack", "application/json", strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without provider secret = %d", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/webhook/slack/slack", strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-UV-Webhook-Secret", "secret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status with provider secret = %d", resp.StatusCode)
	}
	events, err := log.ReadAfter(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Provider != "slack" || events[0].Message.Text != "hello" {
		t.Fatalf("events = %+v", events)
	}
}

func TestHubWebhookRejectsMissingProviderSecret(t *testing.T) {
	provider, err := slack.New(slack.Config{})
	if err != nil {
		t.Fatal(err)
	}
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw := `{"type":"event_callback","event":{"type":"message","user":"u1","channel":"c1","text":"hello","ts":"m1"}}`
	resp, err := http.Post(server.URL+"/v1/webhook/slack/slack", "application/json", strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without configured provider secret = %d", resp.StatusCode)
	}
}

func TestHubWebhookEmitsBatchedProviderEvents(t *testing.T) {
	dir := t.TempDir()
	provider, err := line.New(line.Config{WebhookSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	log := mustEventLog(t, filepath.Join(dir, "events.jsonl"))
	hub := NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(dir, "resources")})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw := `{"events":[{"replyToken":"r1","source":{"type":"user","userId":"u1"},"message":{"id":"m1","type":"text","text":"one"}},{"replyToken":"r2","source":{"type":"user","userId":"u2"},"message":{"id":"m2","type":"text","text":"two"}}]}`
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/webhook/line/line", strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-UV-Webhook-Secret", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	events, err := log.ReadAfter(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Message.Text != "one" || events[1].Message.Text != "two" {
		t.Fatalf("events = %+v", events)
	}
}

func TestHubClosesSubscriberOnOverflow(t *testing.T) {
	hub := NewHub(uvim.NewProviderRegistry(memory.New("test")), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	ch := make(chan uvim.Event, 1)
	hub.mu.Lock()
	hub.subscribers[ch] = struct{}{}
	hub.mu.Unlock()
	if err := hub.Emit(context.Background(), uvim.Event{ID: "evt-1", Type: uvim.EventMessageCreate, Provider: "test", Message: uvim.Message{ID: "m1"}}); err != nil {
		t.Fatal(err)
	}
	if err := hub.Emit(context.Background(), uvim.Event{ID: "evt-2", Type: uvim.EventMessageCreate, Provider: "test", Message: uvim.Message{ID: "m2"}}); err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	_, stillSubscribed := hub.subscribers[ch]
	hub.mu.Unlock()
	if stillSubscribed {
		t.Fatal("subscriber remained registered after overflow")
	}
	<-ch
	if _, ok := <-ch; ok {
		t.Fatal("subscriber channel is not closed after overflow")
	}
}

func TestClientResolveInternalURL(t *testing.T) {
	dir := t.TempDir()
	store := &uvim.ResourceStore{Dir: dir}
	ref, err := store.Save(context.Background(), strings.NewReader("hello"), uvim.ResourceRef{ID: "r1", Kind: uvim.ElementFile, Name: "hello.txt"})
	if err != nil {
		t.Fatal(err)
	}
	hub := NewHub(uvim.NewProviderRegistry(memory.New("test")), mustEventLog(t, ""), store)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	resp, err := client.New(server.URL).ResolveInternalURL(context.Background(), ref.InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("body = %q", string(data))
	}
}

func mustEventLog(t *testing.T, path string) *uvim.EventLog {
	t.Helper()
	log, err := uvim.NewEventLog(path)
	if err != nil {
		t.Fatal(err)
	}
	return log
}

type downloadProvider struct {
	id        string
	connector string
}

func (p *downloadProvider) ID() string          { return p.id }
func (p *downloadProvider) ConnectorID() string { return p.connector }
func (p *downloadProvider) Capabilities() uvim.Capabilities {
	return uvim.Capabilities{Inbound: true, Outbound: true, DownloadResource: true, ResourceKinds: []string{uvim.ElementFile}}
}
func (p *downloadProvider) Run(ctx context.Context, sink uvim.EventSink) error {
	<-ctx.Done()
	return ctx.Err()
}
func (p *downloadProvider) Send(context.Context, uvim.OutboundMessage) (uvim.SendResult, error) {
	return uvim.SendResult{Provider: p.id, Connector: p.connector, Time: time.Now().UTC()}, nil
}
func (p *downloadProvider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	store := &uvim.ResourceStore{Dir: req.Dir}
	ref := req.Resource
	return store.Save(ctx, strings.NewReader("downloaded"), ref)
}
func (p *downloadProvider) Health(context.Context) uvim.Health {
	return uvim.Health{Provider: p.id, Connector: p.connector, State: "ok", CheckedAt: time.Now().UTC(), Capabilities: p.Capabilities()}
}
