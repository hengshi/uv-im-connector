package uvim_test

import (
	"strings"
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
			raw:         `{"id":"m1","channel_id":"c1","guild_id":"g1","content":"hello","author":{"id":"u1","username":"Ada"},"attachments":[{"id":"a1","filename":"pic.png","url":"https://cdn.test/pic.png","content_type":"image/png","size":3}]}`,
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
			channelType: uvim.ChannelRoom,
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
			name:        "whatsapp group",
			decode:      whatsapp.Decode,
			raw:         `{"entry":[{"changes":[{"value":{"messages":[{"id":"m1","from":"u1","type":"text","text":{"body":"hello"},"context":{"group_id":"g1","group_subject":"Team"}}]}}]}]}`,
			want:        "whatsapp",
			channelType: uvim.ChannelGroup,
			wantText:    "hello",
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
			if event.Referrer.Target == nil || event.Referrer.Target.ID == "" || event.Referrer.Target.Kind == "" {
				t.Fatalf("reply target missing: event=%+v referrer=%+v", event.Channel, event.Referrer)
			}
			if len(event.Message.Resources) != tt.wantRefs {
				t.Fatalf("resources = %+v, want %d", event.Message.Resources, tt.wantRefs)
			}
		})
	}
}

func TestProviderDecodersCoverIssue20ProviderGaps(t *testing.T) {
	type resourceWant struct {
		kind        string
		name        string
		urlContains string
	}
	tests := []struct {
		name             string
		decode           httpchannel.DecodeFunc
		config           httpchannel.Config
		raw              string
		wantEventType    string
		wantText         string
		wantTextContains []string
		wantEmptyText    bool
		wantChannelID    string
		wantChannelType  string
		wantUserID       string
		wantMessageID    string
		wantParentID     string
		wantThreadID     string
		wantRefs         []resourceWant
	}{
		{
			name:            "slack app mention is delivered",
			decode:          slack.Decode,
			raw:             `{"type":"event_callback","event":{"type":"app_mention","user":"U1","channel":"C1","text":"<@BOT> hello","ts":"1700000000.000100"}}`,
			wantEventType:   uvim.EventMessageCreate,
			wantText:        "<@BOT> hello",
			wantChannelID:   "C1",
			wantChannelType: uvim.ChannelGroup,
			wantUserID:      "U1",
			wantMessageID:   "1700000000.000100",
		},
		{
			name:            "slack message changed uses nested message",
			decode:          slack.Decode,
			raw:             `{"type":"event_callback","event":{"type":"message","subtype":"message_changed","channel":"C1","message":{"user":"U1","text":"edited text","ts":"1700000000.000200","thread_ts":"1700000000.000100"}}}`,
			wantEventType:   uvim.EventMessageUpdate,
			wantText:        "edited text",
			wantChannelID:   "C1",
			wantChannelType: uvim.ChannelGroup,
			wantUserID:      "U1",
			wantMessageID:   "1700000000.000200",
			wantThreadID:    "1700000000.000100",
		},
		{
			name:            "slack message deleted preserves deleted timestamp",
			decode:          slack.Decode,
			raw:             `{"type":"event_callback","event":{"type":"message","subtype":"message_deleted","channel":"C1","deleted_ts":"1700000000.000300","previous_message":{"user":"U1","text":"removed","ts":"1700000000.000300","thread_ts":"1700000000.000100"}}}`,
			wantEventType:   uvim.EventMessageDelete,
			wantText:        "removed",
			wantChannelID:   "C1",
			wantChannelType: uvim.ChannelGroup,
			wantUserID:      "U1",
			wantMessageID:   "1700000000.000300",
			wantThreadID:    "1700000000.000100",
		},
		{
			name:            "qqguild c2c user openid is sender and direct channel",
			decode:          qqguild.Decode,
			raw:             `{"id":"m1","content":"hello","author":{"user_openid":"uo_1","username":"Ada"}}`,
			wantEventType:   uvim.EventMessageCreate,
			wantText:        "hello",
			wantChannelID:   "uo_1",
			wantChannelType: uvim.ChannelDirect,
			wantUserID:      "uo_1",
			wantMessageID:   "m1",
		},
		{
			name:            "qqguild group member openid is sender",
			decode:          qqguild.Decode,
			raw:             `{"id":"m2","group_openid":"go_1","content":"hello","author":{"member_openid":"mo_1","username":"Ada"}}`,
			wantEventType:   uvim.EventMessageCreate,
			wantText:        "hello",
			wantChannelID:   "go_1",
			wantChannelType: uvim.ChannelGroup,
			wantUserID:      "mo_1",
			wantMessageID:   "m2",
		},
		{
			name:            "line sticker is text not fake file",
			decode:          line.Decode,
			raw:             `{"events":[{"replyToken":"r1","source":{"type":"user","userId":"u1"},"message":{"id":"m1","type":"sticker","packageId":"pkg","stickerId":"stk","stickerResourceType":"STATIC"}}]}`,
			wantEventType:   uvim.EventMessageCreate,
			wantText:        "[Sticker: pkg/stk]",
			wantChannelID:   "u1",
			wantChannelType: uvim.ChannelDirect,
			wantUserID:      "u1",
			wantMessageID:   "m1",
		},
		{
			name:             "line location is text not fake file",
			decode:           line.Decode,
			raw:              `{"events":[{"replyToken":"r1","source":{"type":"group","userId":"u1","groupId":"g1"},"message":{"id":"m2","type":"location","title":"HQ","address":"1 Main St","latitude":31.2304,"longitude":121.4737}}]}`,
			wantEventType:    uvim.EventMessageCreate,
			wantTextContains: []string{"HQ", "1 Main St", "31.2304", "121.4737"},
			wantChannelID:    "g1",
			wantChannelType:  uvim.ChannelGroup,
			wantUserID:       "u1",
			wantMessageID:    "m2",
		},
		{
			name:            "kook kmarkdown url remains text",
			decode:          kook.Decode,
			raw:             `{"s":0,"d":{"msg_id":"m1","target_id":"c1","author_id":"u1","content":"https://example.test/article","type":9}}`,
			wantEventType:   uvim.EventMessageCreate,
			wantText:        "https://example.test/article",
			wantChannelID:   "c1",
			wantChannelType: uvim.ChannelGroup,
			wantUserID:      "u1",
			wantMessageID:   "m1",
		},
		{
			name:             "kook card extracts media modules",
			decode:           kook.Decode,
			raw:              `{"s":0,"d":{"msg_id":"m2","target_id":"c1","author_id":"u1","content":"[{\"type\":\"card\",\"modules\":[{\"type\":\"file\",\"src\":\"https://cdn.test/report.pdf\",\"title\":\"report.pdf\"},{\"type\":\"video\",\"src\":\"https://cdn.test/movie.mp4\",\"title\":\"movie.mp4\"}]}]","type":10}}`,
			wantEventType:    uvim.EventMessageCreate,
			wantTextContains: []string{"report.pdf", "movie.mp4"},
			wantChannelID:    "c1",
			wantChannelType:  uvim.ChannelGroup,
			wantUserID:       "u1",
			wantMessageID:    "m2",
			wantRefs: []resourceWant{
				{kind: uvim.ElementFile, name: "report.pdf", urlContains: "/report.pdf"},
				{kind: uvim.ElementVideo, name: "movie.mp4", urlContains: "/movie.mp4"},
			},
		},
		{
			name:            "telegram voice keeps resource and thread context",
			decode:          telegram.Decode,
			raw:             `{"update_id":99,"message":{"message_id":10,"message_thread_id":7,"caption":"voice note","voice":{"file_id":"voice_1","mime_type":"audio/ogg","file_size":123},"reply_to_message":{"message_id":8},"chat":{"id":2,"type":"supergroup","title":"Team"},"from":{"id":3,"first_name":"Ada"}}}`,
			wantEventType:   uvim.EventMessageCreate,
			wantText:        "voice note",
			wantChannelID:   "2",
			wantChannelType: uvim.ChannelGroup,
			wantUserID:      "3",
			wantMessageID:   "10",
			wantParentID:    "8",
			wantThreadID:    "7",
			wantRefs:        []resourceWant{{kind: uvim.ElementAudio}},
		},
		{
			name:             "telegram edited channel location is normalized",
			decode:           telegram.Decode,
			raw:              `{"update_id":100,"edited_channel_post":{"message_id":11,"location":{"latitude":31.2304,"longitude":121.4737},"chat":{"id":-100,"type":"channel","title":"News"}}}`,
			wantEventType:    uvim.EventMessageUpdate,
			wantTextContains: []string{"Location", "31.2304", "121.4737"},
			wantChannelID:    "-100",
			wantChannelType:  uvim.ChannelGroup,
			wantMessageID:    "11",
		},
		{
			name:            "whatsapp media caption is surfaced",
			decode:          whatsapp.Decode,
			raw:             `{"entry":[{"changes":[{"value":{"messages":[{"id":"m1","from":"u1","type":"image","image":{"id":"media1","mime_type":"image/png","caption":"please analyze this"}}]}}]}]}`,
			wantEventType:   uvim.EventMessageCreate,
			wantText:        "please analyze this",
			wantChannelID:   "u1",
			wantChannelType: uvim.ChannelDirect,
			wantUserID:      "u1",
			wantMessageID:   "m1",
			wantRefs:        []resourceWant{{kind: uvim.ElementImage}},
		},
		{
			name:             "whatsapp interactive button reply is surfaced",
			decode:           whatsapp.Decode,
			raw:              `{"entry":[{"changes":[{"value":{"messages":[{"id":"m2","from":"u1","type":"interactive","interactive":{"button_reply":{"id":"approve","title":"Approve"}}}]}}]}]}`,
			wantEventType:    uvim.EventMessageCreate,
			wantTextContains: []string{"Approve", "approve"},
			wantChannelID:    "u1",
			wantChannelType:  uvim.ChannelDirect,
			wantUserID:       "u1",
			wantMessageID:    "m2",
		},
		{
			name:             "discord embeds stickers polls and reference are normalized",
			decode:           discord.Decode,
			raw:              `{"id":"m1","channel_id":"c1","guild_id":"g1","content":"","type":19,"author":{"id":"u1","username":"Ada"},"embeds":[{"title":"Deploy","description":"finished"}],"sticker_items":[{"id":"s1","name":"Ship"}],"poll":{"question":{"text":"Ship?"},"answers":[{"poll_media":{"text":"Yes"}}]},"referenced_message":{"id":"parent1","content":"original"}}`,
			wantEventType:    uvim.EventMessageCreate,
			wantTextContains: []string{"Deploy", "finished", "Ship", "Ship?", "Yes", "original"},
			wantChannelID:    "c1",
			wantChannelType:  uvim.ChannelGroup,
			wantUserID:       "u1",
			wantMessageID:    "m1",
			wantParentID:     "parent1",
		},
		{
			name:            "discord message reference preserves parent id without expanded message",
			decode:          discord.Decode,
			raw:             `{"id":"m2","channel_id":"c1","guild_id":"g1","content":"reply","author":{"id":"u1","username":"Ada"},"message_reference":{"message_id":"parent2"}}`,
			wantEventType:   uvim.EventMessageCreate,
			wantText:        "reply",
			wantChannelID:   "c1",
			wantChannelType: uvim.ChannelGroup,
			wantUserID:      "u1",
			wantMessageID:   "m2",
			wantParentID:    "parent2",
		},
		{
			name:             "wechat official location content is kept",
			decode:           wechatofficial.Decode,
			raw:              `<xml><ToUserName>bot</ToUserName><FromUserName>u1</FromUserName><CreateTime>1</CreateTime><MsgType>location</MsgType><Location_X>31.2304</Location_X><Location_Y>121.4737</Location_Y><Scale>15</Scale><Label>HQ</Label><MsgId>m1</MsgId></xml>`,
			wantEventType:    uvim.EventMessageCreate,
			wantTextContains: []string{"HQ", "31.2304", "121.4737"},
			wantChannelID:    "u1",
			wantChannelType:  uvim.ChannelDirect,
			wantUserID:       "u1",
			wantMessageID:    "m1",
		},
		{
			name:             "wechat official link content is kept",
			decode:           wechatofficial.Decode,
			raw:              `<xml><ToUserName>bot</ToUserName><FromUserName>u1</FromUserName><CreateTime>1</CreateTime><MsgType>link</MsgType><Title>Docs</Title><Description>Read me</Description><Url>https://example.test/docs</Url><MsgId>m2</MsgId></xml>`,
			wantEventType:    uvim.EventMessageCreate,
			wantTextContains: []string{"Docs", "Read me", "https://example.test/docs"},
			wantChannelID:    "u1",
			wantChannelType:  uvim.ChannelDirect,
			wantUserID:       "u1",
			wantMessageID:    "m2",
		},
		{
			name:            "onebot cq string image becomes resource",
			decode:          onebot.Decode,
			raw:             `{"post_type":"message","message_type":"group","message_id":1,"user_id":2,"group_id":3,"raw_message":"[CQ:image,file=pic.png,url=https://cdn.test/pic.png]","message":"[CQ:image,file=pic.png,url=https://cdn.test/pic.png]"}`,
			wantEventType:   uvim.EventMessageCreate,
			wantChannelID:   "3",
			wantChannelType: uvim.ChannelGroup,
			wantUserID:      "2",
			wantMessageID:   "1",
			wantEmptyText:   true,
			wantRefs:        []resourceWant{{kind: uvim.ElementImage, name: "pic.png", urlContains: "/pic.png"}},
		},
		{
			name:            "qq cq string image becomes resource",
			decode:          qq.Decode,
			raw:             `{"post_type":"message","message_type":"private","message_id":1,"user_id":2,"raw_message":"[CQ:image,file=pic.png,url=https://cdn.test/pic.png]","message":"[CQ:image,file=pic.png,url=https://cdn.test/pic.png]"}`,
			wantEventType:   uvim.EventMessageCreate,
			wantChannelID:   "2",
			wantChannelType: uvim.ChannelDirect,
			wantUserID:      "2",
			wantMessageID:   "1",
			wantEmptyText:   true,
			wantRefs:        []resourceWant{{kind: uvim.ElementImage, name: "pic.png", urlContains: "/pic.png"}},
		},
		{
			name:            "matrix edit targets original message id",
			decode:          matrix.Decode,
			config:          httpchannel.Config{BaseURL: "https://matrix.test"},
			raw:             `{"event_id":"$edit","room_id":"!room:matrix.test","sender":"@ada:matrix.test","type":"m.room.message","content":{"body":" * edited","msgtype":"m.text","m.new_content":{"body":"edited","msgtype":"m.text"},"m.relates_to":{"rel_type":"m.replace","event_id":"$original"}}}`,
			wantEventType:   uvim.EventMessageUpdate,
			wantText:        "edited",
			wantChannelID:   "!room:matrix.test",
			wantChannelType: uvim.ChannelRoom,
			wantUserID:      "@ada:matrix.test",
			wantMessageID:   "$original",
		},
		{
			name:            "matrix encrypted attachment and reply are normalized",
			decode:          matrix.Decode,
			config:          httpchannel.Config{BaseURL: "https://matrix.test"},
			raw:             `{"event_id":"$m1","room_id":"!room:matrix.test","sender":"@ada:matrix.test","type":"m.room.message","content":{"body":"secret.pdf","msgtype":"m.file","file":{"url":"mxc://matrix.test/media1","key":{"alg":"A256CTR","k":"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY"},"iv":"MDEyMzQ1Njc4OWFiY2RlZg","hashes":{"sha256":"abc"}},"info":{"mimetype":"application/pdf","size":42},"m.relates_to":{"m.in_reply_to":{"event_id":"$parent"}}}}`,
			wantEventType:   uvim.EventMessageCreate,
			wantText:        "secret.pdf",
			wantChannelID:   "!room:matrix.test",
			wantChannelType: uvim.ChannelRoom,
			wantUserID:      "@ada:matrix.test",
			wantMessageID:   "$m1",
			wantParentID:    "$parent",
			wantRefs:        []resourceWant{{kind: uvim.ElementFile, name: "secret.pdf", urlContains: "/media/download/matrix.test/media1"}},
		},
		{
			name:            "matrix sticker event is normalized",
			decode:          matrix.Decode,
			config:          httpchannel.Config{BaseURL: "https://matrix.test"},
			raw:             `{"event_id":"$s1","room_id":"!room:matrix.test","sender":"@ada:matrix.test","type":"m.sticker","content":{"body":"ship it","url":"mxc://matrix.test/sticker1","info":{"mimetype":"image/png","size":12}}}`,
			wantEventType:   uvim.EventMessageCreate,
			wantText:        "ship it",
			wantChannelID:   "!room:matrix.test",
			wantChannelType: uvim.ChannelRoom,
			wantUserID:      "@ada:matrix.test",
			wantMessageID:   "$s1",
			wantRefs:        []resourceWant{{kind: uvim.ElementImage, name: "ship it", urlContains: "/media/download/matrix.test/sticker1"}},
		},
		{
			name:            "zulip upload links become resources",
			decode:          zulip.Decode,
			config:          httpchannel.Config{BaseURL: "https://zulip.test"},
			raw:             `{"id":1,"sender_id":2,"sender_full_name":"Ada","stream_id":3,"subject":"general","content":"see [report.pdf](/user_uploads/1/report.pdf)","type":"stream"}`,
			wantEventType:   uvim.EventMessageCreate,
			wantText:        "see [report.pdf](/user_uploads/1/report.pdf)",
			wantChannelID:   "3",
			wantChannelType: uvim.ChannelGroup,
			wantUserID:      "2",
			wantMessageID:   "1",
			wantThreadID:    "general",
			wantRefs:        []resourceWant{{kind: uvim.ElementFile, name: "report.pdf", urlContains: "/user_uploads/1/report.pdf"}},
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
			if event.Type != tt.wantEventType {
				t.Fatalf("event type = %q, want %q; event=%+v", event.Type, tt.wantEventType, event)
			}
			if tt.wantText != "" && event.Message.Text != tt.wantText {
				t.Fatalf("message text = %q, want %q", event.Message.Text, tt.wantText)
			}
			if tt.wantEmptyText && event.Message.Text != "" {
				t.Fatalf("message text = %q, want empty normalized text", event.Message.Text)
			}
			for _, want := range tt.wantTextContains {
				if !strings.Contains(event.Message.Text, want) {
					t.Fatalf("message text = %q, want to contain %q", event.Message.Text, want)
				}
			}
			if event.Channel.ID != tt.wantChannelID || event.Channel.Type != tt.wantChannelType {
				t.Fatalf("channel = %+v, want id=%q type=%q", event.Channel, tt.wantChannelID, tt.wantChannelType)
			}
			if event.User.ID != tt.wantUserID {
				t.Fatalf("user id = %q, want %q; event=%+v", event.User.ID, tt.wantUserID, event)
			}
			if event.Message.ID != tt.wantMessageID || event.Referrer.MessageID != tt.wantMessageID {
				t.Fatalf("message/referrer id = %q/%q, want %q", event.Message.ID, event.Referrer.MessageID, tt.wantMessageID)
			}
			if event.Referrer.ParentMessageID != tt.wantParentID {
				t.Fatalf("parent id = %q, want %q", event.Referrer.ParentMessageID, tt.wantParentID)
			}
			if event.Referrer.ThreadID != tt.wantThreadID {
				t.Fatalf("thread id = %q, want %q", event.Referrer.ThreadID, tt.wantThreadID)
			}
			if len(event.Message.Resources) != len(tt.wantRefs) {
				t.Fatalf("resources = %+v, want %d", event.Message.Resources, len(tt.wantRefs))
			}
			for i, want := range tt.wantRefs {
				ref := event.Message.Resources[i]
				if ref.Kind != want.kind {
					t.Fatalf("resource %d kind = %q, want %q; resources=%+v", i, ref.Kind, want.kind, event.Message.Resources)
				}
				if want.name != "" && ref.Name != want.name {
					t.Fatalf("resource %d name = %q, want %q", i, ref.Name, want.name)
				}
				if want.urlContains != "" && !strings.Contains(ref.URL, want.urlContains) {
					t.Fatalf("resource %d url = %q, want to contain %q", i, ref.URL, want.urlContains)
				}
			}
		})
	}
}

func TestProviderDecodersNormalizeDirectMessages(t *testing.T) {
	tests := []struct {
		name     string
		decode   httpchannel.DecodeFunc
		raw      string
		wantName string
	}{
		{name: "dingtalk", decode: dingtalk.Decode, raw: `{"msgId":"m1","msgtype":"text","senderStaffId":"u1","senderNick":"Ada","conversationId":"c1","conversationType":"1","text":{"content":"hello"}}`, wantName: "Ada"},
		{name: "discord", decode: discord.Decode, raw: `{"id":"m1","channel_id":"D1","content":"hello","author":{"id":"u1","username":"Ada"}}`, wantName: "Ada"},
		{name: "kook", decode: kook.Decode, raw: `{"d":{"msg_id":"m1","channel_type":"PERSON","target_id":"bot1","author_id":"u1","content":"hello","type":1,"extra":{"author":{"id":"u1","username":"ada","nickname":"Ada"}}}}`, wantName: "Ada"},
		{name: "line", decode: line.Decode, raw: `{"events":[{"replyToken":"r1","source":{"type":"user","userId":"u1"},"message":{"id":"m1","type":"text","text":"hello"}}]}`},
		{name: "mail", decode: mailprovider.Decode, raw: `{"id":"m1","from":"ada@example.test","from_name":"Ada","text":"hello"}`, wantName: "Ada"},
		{name: "onebot", decode: onebot.Decode, raw: `{"post_type":"message","message_type":"private","message_id":1,"user_id":2,"raw_message":"hello","sender":{"nickname":"Ada"}}`, wantName: "Ada"},
		{name: "qq", decode: qq.Decode, raw: `{"post_type":"message","message_type":"private","message_id":1,"user_id":2,"raw_message":"hello","sender":{"nickname":"Ada"}}`, wantName: "Ada"},
		{name: "qqguild", decode: qqguild.Decode, raw: `{"id":"m1","content":"hello","author":{"id":"u1","username":"Ada"}}`, wantName: "Ada"},
		{name: "slack", decode: slack.Decode, raw: `{"type":"event_callback","event":{"type":"message","user":"u1","channel":"D1","channel_type":"im","text":"hello","ts":"m1"}}`},
		{name: "telegram", decode: telegram.Decode, raw: `{"update_id":99,"message":{"message_id":1,"text":"hello","chat":{"id":2,"type":"private"},"from":{"id":3,"first_name":"Ada"}}}`, wantName: "Ada"},
		{name: "wechat-official", decode: wechatofficial.Decode, raw: `<xml><ToUserName>bot</ToUserName><FromUserName>u1</FromUserName><CreateTime>1</CreateTime><MsgType>text</MsgType><Content>hello</Content><MsgId>m1</MsgId></xml>`},
		{name: "whatsapp", decode: whatsapp.Decode, raw: `{"entry":[{"changes":[{"value":{"contacts":[{"profile":{"name":"Ada"},"wa_id":"u1"}],"messages":[{"id":"m1","from":"u1","type":"text","text":{"body":"hello"}}]}}]}]}`, wantName: "Ada"},
		{name: "zulip", decode: zulip.Decode, raw: `{"id":1,"sender_id":2,"sender_email":"ada@example.test","sender_full_name":"Ada","content":"hello","type":"private"}`, wantName: "Ada"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, ok, err := tt.decode([]byte(tt.raw), httpchannel.Config{ConnectorID: "main"})
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("decode ok = false")
			}
			event = event.Sanitized()
			if event.Channel.Type != uvim.ChannelDirect || event.Channel.ID == "" || event.User.ID == "" {
				t.Fatalf("event = %+v", event)
			}
			if event.User.Name != tt.wantName || event.Channel.Name != tt.wantName {
				t.Fatalf("display names user=%q channel=%q, want %q", event.User.Name, event.Channel.Name, tt.wantName)
			}
			if event.Referrer.Target == nil || event.Referrer.Target.ID == "" || event.Referrer.Target.Kind == "" {
				t.Fatalf("reply target missing: %+v", event.Referrer)
			}
		})
	}
}
