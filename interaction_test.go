package uvim_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/dingtalk"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
	"github.com/hengshi/uv-im-connector/providers/memory"
	"github.com/hengshi/uv-im-connector/providers/slack"
	"github.com/hengshi/uv-im-connector/providers/telegram"
)

func TestUnsupportedCardCannotFallThroughToTextOrResourceSend(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer server.Close()
	slackProvider, err := slack.New(slack.Config{BaseURL: server.URL, Token: "test"})
	if err != nil {
		t.Fatal(err)
	}
	telegramProvider, err := telegram.New(telegram.Config{BaseURL: server.URL, Token: "test"})
	if err != nil {
		t.Fatal(err)
	}
	dingProvider, err := dingtalk.New(dingtalk.Config{BaseURL: server.URL, ClientID: "test", ClientSecret: "test"})
	if err != nil {
		t.Fatal(err)
	}
	generic, err := httpchannel.New(httpchannel.Config{ProviderID: "test", ConnectorID: "main", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	providers := []uvim.Provider{slackProvider, telegramProvider, dingProvider, generic, memory.New("memory")}
	card := &uvim.InteractionCard{SchemaVersion: uvim.InteractionSchemaV1, CardID: "card", Revision: 1, Title: "Title"}
	for _, provider := range providers {
		for _, resources := range [][]uvim.ResourceRef{nil, {{Kind: uvim.ElementFile, URL: server.URL + "/resource", Name: "file.txt"}}} {
			_, err := provider.Send(context.Background(), uvim.OutboundMessage{Provider: provider.ID(), Connector: provider.ConnectorID(), ChannelID: "chat", Text: "must not be sent", Interaction: card, Resources: resources})
			if err == nil {
				t.Fatalf("%s silently accepted a card", provider.ID())
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("unsupported card caused %d native calls", calls.Load())
	}
	if err := uvim.ValidateMessageUpdate(uvim.MessageUpdate{OutboundMessage: uvim.OutboundMessage{Provider: "lark", Connector: "main", Text: "text", Interaction: card}, MessageID: "message"}, uvim.Capabilities{}); err == nil {
		t.Fatal("text update accepted a card")
	}
}
