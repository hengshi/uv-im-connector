package dingtalk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"
	streamsdk "github.com/open-dingtalk/dingtalk-stream-sdk-go/client"
)

type streamClient interface {
	RegisterChatBotCallbackRouter(chatbot.IChatBotMessageHandler)
	Start(context.Context) error
	Close()
}

type Provider struct {
	config Config
	base   *httpchannel.Provider

	stateMu sync.Mutex
	state   string

	newStreamClient func(clientID, clientSecret string) streamClient
}

var _ uvim.WebhookProvider = (*Provider)(nil)

func newProvider(config Config, base *httpchannel.Provider) *Provider {
	return &Provider{
		config: config,
		base:   base,
		state:  "configured",
		newStreamClient: func(clientID, clientSecret string) streamClient {
			return streamsdk.NewStreamClient(streamsdk.WithAppCredential(
				streamsdk.NewAppCredentialConfig(clientID, clientSecret),
			))
		},
	}
}

func (p *Provider) ID() string { return p.base.ID() }

func (p *Provider) ConnectorID() string { return p.base.ConnectorID() }

func (p *Provider) Capabilities() uvim.Capabilities { return p.base.Capabilities() }

func (p *Provider) streamEnabled() bool { return p.config.ClientID != "" }

func (p *Provider) Run(ctx context.Context, sink uvim.EventSink) error {
	if !p.streamEnabled() {
		return p.base.Run(ctx, sink)
	}

	client := p.newStreamClient(p.config.ClientID, p.config.ClientSecret)
	client.RegisterChatBotCallbackRouter(func(handlerCtx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
		raw, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("dingtalk stream: encode callback: %w", err)
		}
		event, ok, err := Decode(raw, httpchannel.Config{
			ProviderID:  p.ID(),
			ConnectorID: p.ConnectorID(),
			BaseURL:     p.config.BaseURL,
		})
		if err != nil {
			return nil, fmt.Errorf("dingtalk stream: decode callback: %w", err)
		}
		if !ok {
			return nil, nil
		}
		if err := sink.Emit(handlerCtx, event); err != nil {
			return nil, fmt.Errorf("dingtalk stream: emit callback: %w", err)
		}
		p.setState("event")
		return nil, nil
	})

	p.setState("connecting")
	if err := client.Start(ctx); err != nil {
		p.setState("error")
		return fmt.Errorf("dingtalk stream: start: %w", err)
	}
	defer client.Close()
	p.setState("connected")

	<-ctx.Done()
	return nil
}

func (p *Provider) Send(ctx context.Context, msg uvim.OutboundMessage) (uvim.SendResult, error) {
	return p.base.Send(ctx, msg)
}

func (p *Provider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	return p.base.Download(ctx, req)
}

func (p *Provider) Health(ctx context.Context) uvim.Health {
	if !p.streamEnabled() {
		return p.base.Health(ctx)
	}
	p.stateMu.Lock()
	state := p.state
	p.stateMu.Unlock()
	return uvim.Health{
		Provider:     p.ID(),
		Connector:    p.ConnectorID(),
		State:        state,
		CheckedAt:    time.Now().UTC(),
		Capabilities: p.Capabilities(),
	}
}

func (p *Provider) ServeWebhook(w http.ResponseWriter, req *http.Request, sink uvim.EventSink) {
	p.base.ServeWebhook(w, req, sink)
}

func (p *Provider) setState(state string) {
	p.stateMu.Lock()
	p.state = state
	p.stateMu.Unlock()
}
