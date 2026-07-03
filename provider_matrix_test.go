package uvim_test

import (
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/conformance"
	"github.com/hengshi/uv-im-connector/providers/dingtalk"
	"github.com/hengshi/uv-im-connector/providers/discord"
	"github.com/hengshi/uv-im-connector/providers/kook"
	"github.com/hengshi/uv-im-connector/providers/lark"
	"github.com/hengshi/uv-im-connector/providers/line"
	mailprovider "github.com/hengshi/uv-im-connector/providers/mail"
	"github.com/hengshi/uv-im-connector/providers/matrix"
	"github.com/hengshi/uv-im-connector/providers/memory"
	"github.com/hengshi/uv-im-connector/providers/onebot"
	"github.com/hengshi/uv-im-connector/providers/qq"
	"github.com/hengshi/uv-im-connector/providers/qqguild"
	"github.com/hengshi/uv-im-connector/providers/slack"
	"github.com/hengshi/uv-im-connector/providers/telegram"
	"github.com/hengshi/uv-im-connector/providers/wechatofficial"
	"github.com/hengshi/uv-im-connector/providers/wecom"
	"github.com/hengshi/uv-im-connector/providers/whatsapp"
	"github.com/hengshi/uv-im-connector/providers/zulip"
)

func TestProviderMetadataForAllChannels(t *testing.T) {
	providers := []struct {
		name string
		new  func() (providerMetadata, error)
	}{
		{"memory", func() (providerMetadata, error) { return memory.New("memory"), nil }},
		{"wecom", func() (providerMetadata, error) { return wecom.New(wecom.Config{BotID: "bot", Secret: "secret"}) }},
		{"lark", func() (providerMetadata, error) { return lark.New(lark.Config{AppID: "app", AppSecret: "secret"}) }},
		{"dingtalk", func() (providerMetadata, error) { return dingtalk.New(dingtalk.Config{BaseURL: "http://127.0.0.1"}) }},
		{"discord", func() (providerMetadata, error) { return discord.New(discord.Config{BaseURL: "http://127.0.0.1"}) }},
		{"kook", func() (providerMetadata, error) { return kook.New(kook.Config{BaseURL: "http://127.0.0.1"}) }},
		{"line", func() (providerMetadata, error) { return line.New(line.Config{BaseURL: "http://127.0.0.1"}) }},
		{"mail", func() (providerMetadata, error) { return mailprovider.New(mailprovider.Config{}) }},
		{"matrix", func() (providerMetadata, error) { return matrix.New(matrix.Config{BaseURL: "http://127.0.0.1"}) }},
		{"onebot", func() (providerMetadata, error) { return onebot.New(onebot.Config{BaseURL: "http://127.0.0.1"}) }},
		{"qq", func() (providerMetadata, error) { return qq.New(qq.Config{BaseURL: "http://127.0.0.1"}) }},
		{"qqguild", func() (providerMetadata, error) { return qqguild.New(qqguild.Config{BaseURL: "http://127.0.0.1"}) }},
		{"slack", func() (providerMetadata, error) { return slack.New(slack.Config{BaseURL: "http://127.0.0.1"}) }},
		{"telegram", func() (providerMetadata, error) {
			return telegram.New(telegram.Config{BaseURL: "http://127.0.0.1", Token: "token"})
		}},
		{"wechat-official", func() (providerMetadata, error) {
			return wechatofficial.New(wechatofficial.Config{BaseURL: "http://127.0.0.1"})
		}},
		{"whatsapp", func() (providerMetadata, error) { return whatsapp.New(whatsapp.Config{BaseURL: "http://127.0.0.1"}) }},
		{"zulip", func() (providerMetadata, error) { return zulip.New(zulip.Config{BaseURL: "http://127.0.0.1"}) }},
	}
	seen := map[string]bool{}
	for _, item := range providers {
		t.Run(item.name, func(t *testing.T) {
			provider, err := item.new()
			if err != nil {
				t.Fatal(err)
			}
			if seen[provider.ID()] {
				t.Fatalf("duplicate provider ID %q", provider.ID())
			}
			seen[provider.ID()] = true
			conformance.AssertProviderMetadata(t, provider)
		})
	}
}

type providerMetadata interface {
	uvim.Provider
}
