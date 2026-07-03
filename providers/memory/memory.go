package memory

import (
	"context"
	"sync"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

type Provider struct {
	id        string
	connector string
	caps      uvim.Capabilities

	mu     sync.Mutex
	events []uvim.Event
	sent   []uvim.OutboundMessage
}

func New(id string) *Provider {
	return NewConnector(id, id)
}

func NewConnector(id, connector string) *Provider {
	if id == "" {
		id = "memory"
	}
	if connector == "" {
		connector = id
	}
	return &Provider{
		id:        id,
		connector: connector,
		caps: uvim.Capabilities{
			Inbound:          true,
			Outbound:         true,
			DirectMessage:    true,
			GroupMessage:     true,
			ThreadReply:      true,
			UploadResource:   true,
			DownloadResource: true,
			ResourceKinds:    []string{uvim.ElementImage, uvim.ElementAudio, uvim.ElementVideo, uvim.ElementFile},
			ChannelTypes:     []string{uvim.ChannelDirect, uvim.ChannelGroup, uvim.ChannelThread},
		},
	}
}

func (p *Provider) ID() string          { return p.id }
func (p *Provider) ConnectorID() string { return p.connector }
func (p *Provider) Capabilities() uvim.Capabilities {
	return p.caps
}

func (p *Provider) Run(ctx context.Context, sink uvim.EventSink) error {
	p.mu.Lock()
	events := append([]uvim.Event(nil), p.events...)
	p.events = nil
	p.mu.Unlock()
	for _, event := range events {
		if err := sink.Emit(ctx, event); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (p *Provider) Send(_ context.Context, msg uvim.OutboundMessage) (uvim.SendResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, msg)
	return uvim.SendResult{Provider: p.id, Connector: p.connector, MessageID: uvim.NewID("mem-msg"), Time: time.Now().UTC()}, nil
}

func (p *Provider) Download(_ context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	ref := req.Resource
	ref.Provider = p.id
	ref.Connector = p.connector
	return ref, nil
}

func (p *Provider) Health(context.Context) uvim.Health {
	return uvim.Health{Provider: p.id, Connector: p.connector, State: "ok", CheckedAt: time.Now().UTC(), Capabilities: p.caps}
}

func (p *Provider) Queue(event uvim.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if event.Provider == "" {
		event.Provider = p.id
	}
	if event.Connector == "" {
		event.Connector = p.connector
	}
	p.events = append(p.events, event)
}

func (p *Provider) Sent() []uvim.OutboundMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]uvim.OutboundMessage(nil), p.sent...)
}
