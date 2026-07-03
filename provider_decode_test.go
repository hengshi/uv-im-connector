package uvim_test

import (
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/dingtalk"
	"github.com/hengshi/uv-im-connector/providers/discord"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
	"github.com/hengshi/uv-im-connector/providers/kook"
	"github.com/hengshi/uv-im-connector/providers/line"
	mailprovider "github.com/hengshi/uv-im-connector/providers/mail"
	"github.com/hengshi/uv-im-connector/providers/matrix"
	"github.com/hengshi/uv-im-connector/providers/onebot"
	"github.com/hengshi/uv-im-connector/providers/qq"
	"github.com/hengshi/uv-im-connector/providers/qqguild"
	"github.com/hengshi/uv-im-connector/providers/slack"
	"github.com/hengshi/uv-im-connector/providers/telegram"
	"github.com/hengshi/uv-im-connector/providers/wechatofficial"
	"github.com/hengshi/uv-im-connector/providers/whatsapp"
	"github.com/hengshi/uv-im-connector/providers/zulip"
)

func TestProviderDecodersNormalizeInboundMessages(t *testing.T) {
	tests := []struct {
		name        string
		decode      httpchannel.DecodeFunc
		config      httpchannel.Config
		raw         string
		want        string
		channelType string
		wantText    string
		wantRefs    int
	}{
		{
			name:        "dingtalk",
			decode:      dingtalk.Decode,
			raw:         `{"msgId":"m1","msgtype":"image","senderStaffId":"u1","senderNick":"Ada","conversationId":"c1","conversationType":"2","text":{"content":" hello "},"image":{"url":"https://cdn.test/pic.png","mime":"image/png","fileName":"pic.png","size":3}}`,
			want:        "dingtalk",
			channelType: uvim.ChannelGroup,
			wantText:    "hello",
			wantRefs:    1,
		},
		{
			name:        "discord",
			decode:      discord.Decode,
			raw:         `{"id":"m1","channel_id":"c1","content":"hello","author":{"id":"u1","username":"Ada"},"attachments":[{"id":"a1","filename":"pic.png","url":"https://cdn.test/pic.png","content_type":"image/png","size":3}]}`,
			want:        "discord",
			channelType: uvim.ChannelGroup,
			wantText:    "hello",
			wantRefs:    1,
		},
		{
			name:        "kook",
			decode:      kook.Decode,
			raw:         `{"s":0,"d":{"msg_id":"m1","target_id":"c1","author_id":"u1","content":"https://cdn.test/pic.png","type":2}}`,
			want:        "kook",
			channelType: uvim.ChannelGroup,
			wantText:    "https://cdn.test/pic.png",
			wantRefs:    1,
		},
		{
			name:        "line",
			decode:      line.Decode,
			raw:         `{"events":[{"replyToken":"r1","source":{"type":"group","userId":"u1","groupId":"c1"},"message":{"id":"m1","type":"image","text":"hello","fileName":"pic.png","fileSize":3}}]}`,
			want:        "line",
			channelType: uvim.ChannelGroup,
			wantText:    "hello",
			wantRefs:    1,
		},
		{
			name:        "mail",
			decode:      mailprovider.Decode,
			raw:         `{"id":"m1","from":"ada@example.test","from_name":"Ada","to":"bot@example.test","subject":"Hi","text":"hello","attachments":[{"id":"a1","name":"pic.png","url":"https://cdn.test/pic.png","mime":"image/png","size":3}]}`,
			want:        "mail",
			channelType: uvim.ChannelDirect,
			wantText:    "hello",
			wantRefs:    1,
		},
		{
			name:        "matrix",
			decode:      matrix.Decode,
			config:      httpchannel.Config{BaseURL: "https://matrix.test"},
			raw:         `{"event_id":"m1","room_id":"c1","sender":"u1","type":"m.room.message","content":{"body":"pic.png","msgtype":"m.image","url":"mxc://matrix.test/media","info":{"mimetype":"image/png","size":3}}}`,
			want:        "matrix",
			channelType: uvim.ChannelGroup,
			wantText:    "pic.png",
			wantRefs:    1,
		},
		{
			name:        "onebot",
			decode:      onebot.Decode,
			raw:         `{"post_type":"message","message_type":"group","message_id":1,"user_id":2,"group_id":3,"raw_message":"hello","message":[{"type":"image","data":{"url":"https://cdn.test/pic.png","file":"pic.png"}}]}`,
			want:        "onebot",
			channelType: uvim.ChannelGroup,
			wantText:    "hello",
			wantRefs:    1,
		},
		{
			name:        "qq",
			decode:      qq.Decode,
			raw:         `{"post_type":"message","message_type":"group","message_id":1,"user_id":2,"group_id":3,"raw_message":"hello","sender":{"nickname":"Ada"},"message":[{"type":"image","data":{"url":"https://cdn.test/pic.png","file":"pic.png"}}]}`,
			want:        "qq",
			channelType: uvim.ChannelGroup,
			wantText:    "hello",
			wantRefs:    1,
		},
		{
			name:        "qqguild",
			decode:      qqguild.Decode,
			raw:         `{"id":"m1","channel_id":"c1","content":"hello","author":{"id":"u1","username":"Ada"},"attachments":[{"id":"a1","filename":"pic.png","url":"https://cdn.test/pic.png","content_type":"image/png","size":3}]}`,
			want:        "qqguild",
			channelType: uvim.ChannelGroup,
			wantText:    "hello",
			wantRefs:    1,
		},
		{
			name:        "slack",
			decode:      slack.Decode,
			raw:         `{"type":"event_callback","event":{"type":"message","user":"u1","channel":"c1","text":"hello","ts":"m1","files":[{"id":"a1","name":"pic.png","url_private_download":"https://cdn.test/pic.png","mimetype":"image/png","size":3}]}}`,
			want:        "slack",
			channelType: uvim.ChannelGroup,
			wantText:    "hello",
			wantRefs:    1,
		},
		{
			name:        "telegram",
			decode:      telegram.Decode,
			raw:         `{"update_id":99,"message":{"message_id":1,"text":"hello","chat":{"id":2,"type":"private"},"from":{"id":3,"first_name":"Ada"}}}`,
			want:        "telegram",
			channelType: uvim.ChannelDirect,
			wantText:    "hello",
		},
		{
			name:        "wechat-official",
			decode:      wechatofficial.Decode,
			raw:         `<xml><ToUserName>bot</ToUserName><FromUserName>u1</FromUserName><CreateTime>1</CreateTime><MsgType>image</MsgType><PicUrl>https://cdn.test/pic.png</PicUrl><MediaId>media1</MediaId><MsgId>m1</MsgId></xml>`,
			want:        "wechat-official",
			channelType: uvim.ChannelDirect,
			wantRefs:    1,
		},
		{
			name:        "whatsapp",
			decode:      whatsapp.Decode,
			raw:         `{"entry":[{"changes":[{"value":{"messages":[{"id":"m1","from":"u1","type":"image","text":{"body":"hello"},"image":{"id":"media1","mime_type":"image/png","caption":"pic"}}]}}]}]}`,
			want:        "whatsapp",
			channelType: uvim.ChannelDirect,
			wantText:    "hello",
			wantRefs:    1,
		},
		{
			name:        "zulip",
			decode:      zulip.Decode,
			config:      httpchannel.Config{BaseURL: "https://zulip.test"},
			raw:         `{"id":1,"sender_id":2,"sender_full_name":"Ada","stream_id":3,"subject":"general","content":"hello","type":"stream","attachments":[{"id":"a1","name":"pic.png","path":"/user_uploads/pic.png","mime_type":"image/png","size":3}]}`,
			want:        "zulip",
			channelType: uvim.ChannelGroup,
			wantText:    "hello",
			wantRefs:    1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.config
			config.ConnectorID = "main"
			config.Token = "token"
			event, ok, err := tt.decode([]byte(tt.raw), config)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("decode ok = false")
			}
			if event.Provider != tt.want || event.Connector != "main" {
				t.Fatalf("event provider/connector = %s/%s", event.Provider, event.Connector)
			}
			if event.Type != uvim.EventMessageCreate || event.Channel.Type != tt.channelType {
				t.Fatalf("event type/channel = %+v", event)
			}
			if event.Message.Text != tt.wantText {
				t.Fatalf("message text = %q", event.Message.Text)
			}
			if event.Referrer.MessageID == "" || event.Referrer.ChannelID == "" {
				t.Fatalf("referrer missing: %+v", event.Referrer)
			}
			if len(event.Message.Resources) != tt.wantRefs {
				t.Fatalf("resources = %+v, want %d", event.Message.Resources, tt.wantRefs)
			}
		})
	}
}
