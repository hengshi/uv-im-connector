package uvim_test

import (
	"reflect"
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
		name            string
		new             func() (providerMetadata, error)
		direct          bool
		group           bool
		reply           bool
		proactiveDirect bool
		proactiveGroup  bool
		targetKinds     []string
	}{
		{"memory", func() (providerMetadata, error) { return memory.New("memory"), nil }, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetGroup, uvim.TargetChannel, uvim.TargetConversation}},
		{"wecom", func() (providerMetadata, error) { return wecom.New(wecom.Config{BotID: "bot", Secret: "secret"}) }, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetGroup, uvim.TargetConversation}},
		{"lark", func() (providerMetadata, error) { return lark.New(lark.Config{AppID: "app", AppSecret: "secret"}) }, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetGroup, uvim.TargetConversation}},
		{"dingtalk", func() (providerMetadata, error) { return dingtalk.New(dingtalk.Config{BaseURL: "http://127.0.0.1"}) }, true, true, true, false, true, []string{uvim.TargetUser, uvim.TargetGroup}},
		{"discord", func() (providerMetadata, error) { return discord.New(discord.Config{BaseURL: "http://127.0.0.1"}) }, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetChannel, uvim.TargetConversation}},
		{"kook", func() (providerMetadata, error) { return kook.New(kook.Config{BaseURL: "http://127.0.0.1"}) }, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetChannel}},
		{"line", func() (providerMetadata, error) { return line.New(line.Config{BaseURL: "http://127.0.0.1"}) }, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetGroup, uvim.TargetConversation}},
		{"mail", func() (providerMetadata, error) { return mailprovider.New(mailprovider.Config{}) }, true, false, true, true, false, []string{uvim.TargetUser}},
		{"matrix", func() (providerMetadata, error) { return matrix.New(matrix.Config{BaseURL: "http://127.0.0.1"}) }, true, true, true, true, true, []string{uvim.TargetConversation}},
		{"onebot", func() (providerMetadata, error) { return onebot.New(onebot.Config{BaseURL: "http://127.0.0.1"}) }, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetGroup}},
		{"qq", func() (providerMetadata, error) { return qq.New(qq.Config{BaseURL: "http://127.0.0.1"}) }, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetGroup}},
		{"qqguild", func() (providerMetadata, error) { return qqguild.New(qqguild.Config{BaseURL: "http://127.0.0.1"}) }, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetGroup, uvim.TargetChannel}},
		{"slack", func() (providerMetadata, error) { return slack.New(slack.Config{BaseURL: "http://127.0.0.1"}) }, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetChannel, uvim.TargetConversation}},
		{"telegram", func() (providerMetadata, error) {
			return telegram.New(telegram.Config{BaseURL: "http://127.0.0.1", Token: "token"})
		}, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetGroup, uvim.TargetConversation}},
		{"wechat-official", func() (providerMetadata, error) {
			return wechatofficial.New(wechatofficial.Config{BaseURL: "http://127.0.0.1"})
		}, true, false, true, true, false, []string{uvim.TargetUser}},
		{"whatsapp", func() (providerMetadata, error) { return whatsapp.New(whatsapp.Config{BaseURL: "http://127.0.0.1"}) }, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetGroup}},
		{"zulip", func() (providerMetadata, error) { return zulip.New(zulip.Config{BaseURL: "http://127.0.0.1"}) }, true, true, true, true, true, []string{uvim.TargetUser, uvim.TargetGroup}},
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
			caps := provider.Capabilities()
			if caps.DirectMessage != item.direct || caps.GroupMessage != item.group || caps.ReplyMessage != item.reply || caps.ProactiveDirect != item.proactiveDirect || caps.ProactiveGroup != item.proactiveGroup {
				t.Fatalf("capabilities = %+v", caps)
			}
			if !reflect.DeepEqual(caps.TargetKinds, item.targetKinds) {
				t.Fatalf("target kinds = %v, want %v", caps.TargetKinds, item.targetKinds)
			}
		})
	}
}

type providerMetadata interface {
	uvim.Provider
}
