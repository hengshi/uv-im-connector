package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
)

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	Token      string
}

func New(baseURL string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTPClient: http.DefaultClient}
}

func (c *Client) Meta(ctx context.Context) (map[string]any, error) {
	var out map[string]any
	return out, c.getJSON(ctx, "/v1/meta", &out)
}

func (c *Client) ServiceMeta(ctx context.Context) (uvim.ServiceMeta, error) {
	var out uvim.ServiceMeta
	return out, c.getJSON(ctx, "/v1/meta", &out)
}

func (c *Client) Events(ctx context.Context, after int64) ([]uvim.Event, error) {
	var out struct {
		Events []uvim.Event `json:"events"`
	}
	path := fmt.Sprintf("/v1/events?after=%d", after)
	if err := c.getJSON(ctx, path, &out); err != nil {
		return nil, err
	}
	return out.Events, nil
}

func (c *Client) WatchEvents(ctx context.Context, after int64, emit func(uvim.Event) error) error {
	return c.WatchEventsWithConnect(ctx, after, nil, emit)
}

func (c *Client) WatchEventsWithConnect(ctx context.Context, after int64, onConnect func() error, emit func(uvim.Event) error) error {
	u := strings.TrimRight(c.BaseURL, "/") + fmt.Sprintf("/v1/events/ws?after=%d", after)
	u = strings.Replace(u, "http://", "ws://", 1)
	u = strings.Replace(u, "https://", "wss://", 1)
	header := http.Header{}
	if c.Token != "" {
		header.Set("Authorization", "Bearer "+c.Token)
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, u, header)
	if err != nil {
		return err
	}
	if onConnect != nil {
		if err := onConnect(); err != nil {
			_ = conn.Close()
			return err
		}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	defer close(done)
	defer conn.Close()
	for {
		var event uvim.Event
		if err := conn.ReadJSON(&event); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		if emit != nil {
			if err := emit(event); err != nil {
				return err
			}
		}
	}
}

func (c *Client) Send(ctx context.Context, msg uvim.OutboundMessage) (uvim.SendResult, error) {
	var out uvim.SendResult
	return out, c.postJSON(ctx, "/v1/message.create", msg, &out)
}

func (c *Client) Upload(ctx context.Context, ref uvim.ResourceRef, data []byte) (uvim.ResourceRef, error) {
	var out uvim.ResourceRef
	input := map[string]string{
		"kind":           ref.Kind,
		"name":           ref.Name,
		"mime":           ref.MIME,
		"content_base64": base64.StdEncoding.EncodeToString(data),
	}
	return out, c.postJSON(ctx, "/v1/upload.create", input, &out)
}

func (c *Client) DownloadProviderResource(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	var out uvim.ResourceRef
	return out, c.postJSON(ctx, "/v1/resource.download", req, &out)
}

func (c *Client) ResolveInternalURL(ctx context.Context, internalURL string) (*http.Response, error) {
	id := strings.TrimPrefix(strings.TrimSpace(internalURL), "internal://")
	if id == "" {
		return nil, fmt.Errorf("invalid internal url")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.BaseURL, "/")+"/v1/internal/"+id, nil)
	if err != nil {
		return nil, err
	}
	c.authorize(req)
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	return resp, nil
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.BaseURL, "/")+path, nil)
	if err != nil {
		return err
	}
	c.authorize(req)
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) postJSON(ctx context.Context, path string, in any, out any) error {
	raw, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	c.authorize(req)
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) authorize(req *http.Request) {
	if c != nil && c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
}

func (c *Client) httpClient() *http.Client {
	if c != nil && c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}
