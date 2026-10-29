package uvim_test

import (
	"reflect"
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
			raw:         `{"event_id":"m1","room_id":"!room:matrix.test","sender":"@ada:matrix.test","type":"m.room.message","content":{"body":"pic.png","msgtype":"m.image","url":"mxc://matrix.test/media","info":{"mimetype":"image/png","size":3}}}`,
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
			identityType := map[string]string{
				"dingtalk": "dingtalk.userid", "discord": "discord.user_id", "kook": "kook.user_id",
				"line": "line.user_id", "mail": "mail.mailbox", "matrix": "matrix.mxid",
				"onebot": "onebot.v11.qq_user_id", "qq": "onebot.v11.qq_user_id", "qqguild": "qqguild.author_id",
				"slack": "slack.user_id", "telegram": "telegram.user_id", "wechat-official": "wechat-official.openid",
				"whatsapp": "whatsapp.wa_id", "zulip": "zulip.user_id",
			}[tt.want]
			if len(event.User.Identities) == 0 || event.User.Identities[0].Type != identityType || event.User.Identities[0].ID != event.User.ID || event.User.Identities[0].Source != uvim.IdentitySourceCallback {
				t.Fatalf("native user identity = %+v", event.User)
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

// These minimized fixtures follow the linked official schemas in docs/identities.md;
// they are not captured messages from live accounts.
func TestProviderIdentitySubjectsAndNativeTargets(t *testing.T) {
	tests := []struct {
		name, raw, userID, kind, channelID string
		decode                             httpchannel.DecodeFunc
		identities                         []uvim.UserIdentity
		target                             *uvim.OutboundTarget
	}{
		{name: "dingtalk distinct namespaces", decode: dingtalk.Decode,
			raw:    `{"msgId":"m","conversationId":"chat","conversationType":"1","senderStaffId":"staff","senderUnionId":"union","senderId":"encrypted","senderCorpId":"corp"}`,
			userID: "staff", channelID: "chat", target: &uvim.OutboundTarget{ID: "staff", Kind: uvim.TargetUser},
			identities: []uvim.UserIdentity{{Type: "dingtalk.userid", ID: "staff", Scope: "corp"}, {Type: "dingtalk.unionid", ID: "union"}, {Type: "dingtalk.sender_id", ID: "encrypted"}}},
		{name: "dingtalk external sender is not staff recipient", decode: dingtalk.Decode,
			raw:    `{"msgId":"m","conversationId":"chat","conversationType":"1","senderId":"encrypted","sessionWebhook":"https://reply.test/session"}`,
			userID: "encrypted", channelID: "chat", identities: []uvim.UserIdentity{{Type: "dingtalk.sender_id", ID: "encrypted"}}},
		{name: "discord webhook is not personal account", decode: discord.Decode,
			raw:    `{"id":"m","channel_id":"chat","webhook_id":"hook","author":{"id":"hook","username":"Alice","bot":true}}`,
			userID: "hook", kind: uvim.UserKindWebhook, channelID: "chat", target: &uvim.OutboundTarget{ID: "chat", Kind: uvim.TargetChannel},
			identities: []uvim.UserIdentity{{Type: "discord.webhook_id", ID: "hook"}}},
		{name: "kook system is not person", decode: kook.Decode,
			raw:    `{"d":{"msg_id":"m","target_id":"chat","author_id":"1","type":255}}`,
			userID: "1", kind: uvim.UserKindSystem, channelID: "chat", target: &uvim.OutboundTarget{ID: "chat", Kind: uvim.TargetChannel},
			identities: []uvim.UserIdentity{{Type: "kook.system_id", ID: "1"}}},
		{name: "line missing consent does not use destination bot", decode: line.Decode,
			raw:       `{"destination":"bot","events":[{"replyToken":"reply","source":{"type":"group","groupId":"chat"},"message":{"id":"m","type":"text","text":"hi"}}]}`,
			channelID: "chat", target: &uvim.OutboundTarget{ID: "chat", Kind: uvim.TargetGroup}},
		{name: "mail preserves case sensitive localpart", decode: mailprovider.Decode,
			raw:    `{"id":"m","from":"Alice <Alice@example.test>","text":"hi"}`,
			userID: "Alice <Alice@example.test>", channelID: "Alice <Alice@example.test>", target: &uvim.OutboundTarget{ID: "Alice <Alice@example.test>", Kind: uvim.TargetUser},
			identities: []uvim.UserIdentity{{Type: "mail.mailbox", ID: "Alice@example.test"}}},
		{name: "matrix full namespaced user stays distinct from room", decode: matrix.Decode,
			raw:    `{"event_id":"m","room_id":"!room:example.test","sender":"@alice:example.test","type":"m.room.message","content":{"body":"hi","msgtype":"m.text"}}`,
			userID: "@alice:example.test", channelID: "!room:example.test", target: &uvim.OutboundTarget{ID: "!room:example.test", Kind: uvim.TargetConversation},
			identities: []uvim.UserIdentity{{Type: "matrix.mxid", ID: "@alice:example.test"}}},
		{name: "onebot anonymous hides compatibility account", decode: onebot.Decode,
			raw:  `{"post_type":"message","message_type":"group","sub_type":"anonymous","message_id":1,"user_id":80000000,"group_id":3,"anonymous":{"id":1,"name":"Alice","flag":"credential"}}`,
			kind: uvim.UserKindAnonymous, channelID: "3", target: &uvim.OutboundTarget{ID: "3", Kind: uvim.TargetGroup}},
		{name: "qq anonymous hides compatibility account", decode: qq.Decode,
			raw: `{"post_type":"message","message_type":"group","sub_type":"anonymous","message_id":1,"user_id":80000000,"group_id":3}`, kind: uvim.UserKindAnonymous, channelID: "3", target: &uvim.OutboundTarget{ID: "3", Kind: uvim.TargetGroup}},
		{name: "qqguild modern C2C has no legacy author id", decode: qqguild.Decode,
			raw:    `{"id":"m","author":{"user_openid":"person","union_openid":"union","union_user_account":"account"}}`,
			userID: "person", channelID: "person", target: &uvim.OutboundTarget{ID: "person", Kind: uvim.TargetUser},
			identities: []uvim.UserIdentity{{Type: "qqguild.user_openid", ID: "person"}, {Type: "qqguild.union_openid", ID: "union"}, {Type: "qqguild.union_user_account", ID: "account"}}},
		{name: "qqguild group member is not group destination", decode: qqguild.Decode,
			raw:    `{"id":"m","group_openid":"group","author":{"id":"legacy","member_openid":"member"}}`,
			userID: "legacy", channelID: "group", target: &uvim.OutboundTarget{ID: "group", Kind: uvim.TargetGroup},
			identities: []uvim.UserIdentity{{Type: "qqguild.author_id", ID: "legacy"}, {Type: "qqguild.member_openid", ID: "member"}}},
		{name: "slack installation user is not sender", decode: slack.Decode,
			raw:    `{"type":"event_callback","team_id":"team","authorizations":[{"user_id":"installer"}],"event":{"type":"message","bot_id":"bot","channel":"chat","ts":"m"}}`,
			userID: "bot", kind: uvim.UserKindBot, channelID: "chat", target: &uvim.OutboundTarget{ID: "chat", Kind: uvim.TargetChannel},
			identities: []uvim.UserIdentity{{Type: "slack.bot_id", ID: "bot", Scope: "team"}}},
		{name: "telegram missing from does not invent zero", decode: telegram.Decode,
			raw:       `{"message":{"message_id":1,"chat":{"id":-100,"type":"supergroup"},"text":"hi"}}`,
			channelID: "-100", target: &uvim.OutboundTarget{ID: "-100", Kind: uvim.TargetGroup}},
		{name: "telegram sender chat overrides fake user", decode: telegram.Decode,
			raw:    `{"message":{"message_id":1,"chat":{"id":-100,"type":"supergroup"},"from":{"id":1087968824,"is_bot":true},"sender_chat":{"id":-200,"type":"channel","title":"News"},"text":"hi"}}`,
			userID: "-200", kind: uvim.UserKindChat, channelID: "-100", target: &uvim.OutboundTarget{ID: "-100", Kind: uvim.TargetGroup},
			identities: []uvim.UserIdentity{{Type: "telegram.chat_id", ID: "-200"}}},
		{name: "wechat scope is receiving account not appid", decode: wechatofficial.Decode,
			raw:    `<xml><ToUserName>account</ToUserName><FromUserName>openid</FromUserName><MsgType>text</MsgType><MsgId>m</MsgId></xml>`,
			userID: "openid", channelID: "openid", target: &uvim.OutboundTarget{ID: "openid", Kind: uvim.TargetUser},
			identities: []uvim.UserIdentity{{Type: "wechat-official.openid", ID: "openid", Scope: "account"}}},
		{name: "whatsapp BSUID only and matching contact", decode: whatsapp.Decode,
			raw:    `{"entry":[{"changes":[{"value":{"contacts":[{"user_id":"other","wa_id":"wrong","profile":{"name":"Alice"}},{"user_id":"bsuid","parent_user_id":"parent","profile":{"name":"Alice"}}],"messages":[{"id":"m","from":" ","from_user_id":"bsuid","type":"text"}]}}]}]}`,
			userID: "bsuid", channelID: "bsuid", target: &uvim.OutboundTarget{ID: "bsuid", Kind: uvim.TargetUser, IDType: "whatsapp.bsuid"},
			identities: []uvim.UserIdentity{{Type: "whatsapp.bsuid", ID: "bsuid"}, {Type: "whatsapp.parent_bsuid", ID: "parent"}}},
		{name: "whatsapp parent BSUID only", decode: whatsapp.Decode,
			raw:    `{"entry":[{"changes":[{"value":{"messages":[{"id":"m","from_parent_user_id":"parent","type":"text"}]}}]}]}`,
			userID: "parent", channelID: "parent", target: &uvim.OutboundTarget{ID: "parent", Kind: uvim.TargetUser, IDType: "whatsapp.parent_bsuid"},
			identities: []uvim.UserIdentity{{Type: "whatsapp.parent_bsuid", ID: "parent"}}},
		{name: "whatsapp root group takes precedence over legacy context", decode: whatsapp.Decode,
			raw:    `{"entry":[{"changes":[{"value":{"messages":[{"id":"m","from":"phone","from_user_id":"bsuid","group_id":"group","context":{"group_id":"legacy"},"type":"text"}]}}]}]}`,
			userID: "phone", channelID: "group", target: &uvim.OutboundTarget{ID: "group", Kind: uvim.TargetGroup},
			identities: []uvim.UserIdentity{{Type: "whatsapp.wa_id", ID: "phone"}, {Type: "whatsapp.bsuid", ID: "bsuid"}}},
		{name: "zulip API email is not real mailbox proof", decode: zulip.Decode,
			raw:    `{"id":1,"sender_id":2,"sender_email":"user2@hidden.example","type":"private"}`,
			userID: "2", channelID: "user2@hidden.example", target: &uvim.OutboundTarget{ID: "user2@hidden.example", Kind: uvim.TargetUser},
			identities: []uvim.UserIdentity{{Type: "zulip.user_id", ID: "2"}, {Type: "zulip.api_email", ID: "user2@hidden.example"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, ok, err := tt.decode([]byte(tt.raw), httpchannel.Config{ConnectorID: "main"})
			if err != nil || !ok {
				t.Fatalf("decode ok=%v err=%v", ok, err)
			}
			for i := range tt.identities {
				tt.identities[i].Source = uvim.IdentitySourceCallback
			}
			if event.User.ID != tt.userID || event.User.Kind != tt.kind || !reflect.DeepEqual(event.User.Identities, tt.identities) {
				t.Fatalf("user=%+v, want id=%q kind=%q identities=%+v", event.User, tt.userID, tt.kind, tt.identities)
			}
			if event.Channel.ID != tt.channelID || !reflect.DeepEqual(event.Referrer.Target, tt.target) {
				t.Fatalf("channel=%+v target=%+v, want channel=%q target=%+v", event.Channel, event.Referrer.Target, tt.channelID, tt.target)
			}
			if tt.name == "dingtalk external sender is not staff recipient" && event.Referrer.ReplyToken != "https://reply.test/session" {
				t.Fatal("lost webhook reply")
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
