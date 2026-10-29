# 用户身份与原生地址

`user.id` 是 adapter 选定的原生主体 ID。`user.identities` 保留平台提供的其他身份及其命名空间，调用方无需解析原始回调。身份属于用户主体；`channel.id` 表示会话，`referrer.target` 表示回复地址，三者不能互相代替。

## 统一契约

```json
{
  "provider": "lark",
  "connector": "work",
  "user": {
    "id": "open-id",
    "identities": [
      {"type": "lark.open_id", "id": "open-id", "scope": "app-id", "source": "callback"},
      {"type": "lark.user_id", "id": "employee-userid", "scope": "tenant-key", "source": "callback"},
      {"type": "lark.union_id", "id": "union-id", "source": "lookup"}
    ]
  },
  "channel": {"id": "chat-id", "type": "direct"},
  "referrer": {"target": {"id": "chat-id", "kind": "conversation"}}
}
```

每项身份包含 `type`、`id`、可选的 `scope` 和 `source`。`type` 命名标识方案；`scope` 只填写已知的实际发行组织、租户或应用，不猜测。即使平台原生 ID 具有更大范围，缺少显式 scope 的协议记录也只保证在事件的 `provider + connector` 内解释，不能直接跨连接合并。相同 type 的不同 scope 可以并存；同一 type/scope 保留第一项，优先保留平台事实。ID 保留大小写，不从前缀推断种类。

`source` 区分 `callback`（回调）、`lookup`（平台查询）和 `configured`（部署者确认的绑定）。来源记录不构成业务授权。显示名、用户名、拼音、手机号外观和昵称相同均不证明是同一个人。

`user.kind` 缺省表示普通账户，也可显式为 `user`；其他值包括 `bot`、`webhook`、`system`、`anonymous`、`chat`。匿名、系统、webhook 和 chat 主体不接受账户映射。缺少真实用户时省略个人身份，不能填入机器人安装者、群 ID 或兼容用的假用户。`bot` 标识的账户仍可有明确的账户绑定。

协议 v1 的新增字段均可省略。普通受支持消息保留原生主 ID 和回复目标；缺失发送者、非个人主体或新增官方地址格式按实际字段处理。`memory` 是内部测试 provider，身份由调用方显式提供。

## 16 个 provider 的入站身份

表中的字段是本实现实际保留的内容，查询接口与权限条件见下一节。

| Provider | 发布的 identity type 与字段 | 作用域与主体 | 原生回复目标 |
| --- | --- | --- | --- |
| WeCom | `wecom.callback_userid` ← `from.userid`；可选应用查询增加 `wecom.userid`。 | 回调可能已是企业明文账号或企业内加密 ID；经官方核验的 `wecom.userid` 使用配置的企业 ID 作为 scope。 | 单聊原生 userid；群聊 chatid。 |
| Lark / 飞书 | `lark.open_id`、`lark.user_id`、`lark.union_id` ← sender ID；已有用户查询也保留这三项。 | open_id 为应用 scope；user_id 为 tenant scope；union_id 属于开发者组织，其发行组织不在普通回调中，scope 留空。 | chat_id，保留会话目标。 |
| DingTalk | `dingtalk.userid` ← senderStaffId；`dingtalk.unionid` ← senderUnionId；`dingtalk.sender_id` ← senderId。 | userid 使用实际 senderCorpId；加密 senderId 不是 unionid。主 ID 优先 staff、sender、union。 | 群 conversationId；单聊仅已知 staff ID 可作用户地址；缺少 staff 时仍保留 session webhook。 |
| Discord | `discord.user_id` ← author.id；webhook 消息改为 `discord.webhook_id` ← webhook_id。 | Snowflake 原样保留；bot/webhook 区分主体，不用名称关联账户。 | 接收消息的 channel_id。 |
| KOOK | `kook.user_id` ← author_id；系统为 `kook.system_id`。 | author_id=`1` 是系统；保留 bot 标识；guild.open_id 不是用户别名。 | PERSON 为发送者；其他为频道。 |
| LINE | `line.user_id` ← source.userId。 | LINE developer provider scope 不在普通回调中；destination 是 bot，不作为发行方。缺少 consent 时可无用户 ID。 | source 的 user/group/room ID，及独立 reply token。 |
| Mail | `mail.mailbox` ← 成功解析的 from 邮箱。 | 原生 user.id 保留 from 字符串；邮箱本地部分不统一小写；邮件头不证明真实个人或通过认证。 | 原有 from 邮箱。 |
| Matrix | `matrix.mxid` ← 完整 sender。 | 保留 MXID 的 homeserver 命名空间，不把 localpart 当作邮箱/员工号。 | room_id，始终为 conversation。 |
| OneBot | `onebot.v11.qq_user_id` ← 数字 user_id。 | 本 adapter 是 v11；anonymous/notice 不发布个人账户 ID。 | 原生 QQ user_id 或 group_id。 |
| QQ | `onebot.v11.qq_user_id` ← 数字 user_id。 | 独立 provider envelope，同属 OneBot v11；不是 QQ 官方 OpenID API。 | 原生 QQ user_id 或 group_id。 |
| QQ Guild / QQ Bot | `qqguild.author_id`、`qqguild.user_openid`、`qqguild.member_openid`、`qqguild.union_openid`、`qqguild.union_user_account` ← 实际 author 字段。 | 旧 author.id 存在时保留为主 ID，缺少时回退 user/member OpenID；没有配置发行方时 scope 留空。 | C2C 优先 user_openid；群 group_openid；频道 channel_id。 |
| Slack | `slack.user_id` ← event.user；缺少 user 的 bot 消息为 `slack.bot_id`。 | 使用实际 team_id；authorizations 中的安装者不属于发送者身份。 | 接收消息的 conversation/channel。 |
| Telegram | `telegram.user_id` ← from.id；存在 sender_chat 时改为 `telegram.chat_id`。 | sender_chat 优先于兼容假 from；缺少 from 不生成 `"0"`；username 是展示字段。 | 接收消息的 chat.id，不换成发送者 ID。 |
| WeChat Official | `wechat-official.openid` ← FromUserName。 | scope 是实际 ToUserName（接收账号），不声称它是 AppID。普通 XML 没有 UnionID。 | 账号下的 OpenID。 |
| WhatsApp | `whatsapp.wa_id` ← from/contacts.wa_id；`whatsapp.bsuid` ← from_user_id/contacts.user_id；`whatsapp.parent_bsuid` ← 对应 parent 字段。 | BSUID 属于 business portfolio；entry.id 和 phone_number_id 不能代替 portfolio ID。仅用实际相同 type+ID 匹配 contact。 | 手机号或明确的 BSUID 地址；群优先根级 group_id，兼容旧 context.group_id。 |
| Zulip | `zulip.user_id` ← sender_id；`zulip.api_email` ← sender_email。 | 组织/部署内身份；API email 可能是隐私替代地址，不能声称是真实邮箱。 | stream/topic 或现有私聊地址。 |

## 官方转换、查询与权限

平台回调已提供的身份直接保留。WeCom 配置自建应用凭据后自动核验企业账号，未配置时不调用应用接口；[配置与验证指南](./guide/wecom.md)说明所有值的来源。Lark 复用已有用户资料请求和有界缓存，按真实主 ID 类型设置 `user_id_type`，保留已知 scope。两者查询失败都保留基础事件。其余 provider 没有新增必需的身份查询；表中的接口不代表 adapter 已自动集成。

| Provider | 官方文档与可用路径 | 条件与边界 |
| --- | --- | --- |
| WeCom | [长连接回调](https://developer.work.weixin.qq.com/document/path/101463)；[转换](https://developer.work.weixin.qq.com/document/path/101521)；[读取成员](https://developer.work.weixin.qq.com/document/path/90196)。 | 可选 `UV_WECOM_CORP_ID` + `UV_WECOM_CORP_SECRET` 自动获取应用 token、转换并核验企业成员；需应用可见范围和服务器可信出口 IP。已是明文的输入也会进入 invalid 列表，须由成员查询进一步证明。短回调账号可能已经正确，CLI 目录标识可能才是需转换的一端；不按外观判断，不做反向生成或姓名绑定。 |
| Lark | [用户标识](https://open.feishu.cn/document/platform-overveiw/basic-concepts/user-identity-introduction/introduction)、[消息事件](https://open.feishu.cn/document/server-docs/im-v1/message/events/receive)、[获取用户](https://open.feishu.cn/document/server-docs/contact-v3/user/get)。 | 使用应用 token、基本用户读取权限和可见范围；user_id 字段涉及 `contact:user.employee_id:readonly`。未返回的标识不补造，union_id 不当作跨开发者全局 ID。 |
| DingTalk | [消息回调](https://open.dingtalk.com/document/development/robot-message-type)、[用户详情](https://open.dingtalk.com/document/development/query-user-details)、[unionid 转 userid](https://open.dingtalk.com/document/development/query-a-user-by-the-union-id)。 | `/topapi/v2/user/get`、`/topapi/user/getbyunionid` 需要相应应用 token、通讯录权限（如 `qyapi_get_member`）及范围。外部用户/未发布机器人可能没有 senderStaffId，不能拿 senderId 充当 userid。 |
| Discord | [User](https://docs.discord.com/developers/resources/user)、[Message](https://docs.discord.com/developers/resources/message)。 | bot 的 User 查询不提供另一套普通稳定账户 ID；OAuth identify/email 针对授权用户，不是任意人的企业目录。webhook author 不作为个人。 |
| KOOK | [对象](https://developer.kookapp.cn/doc/objects)、[事件](https://developer.kookapp.cn/doc/event/event-introduction)、[用户 API](https://developer.kookapp.cn/doc/http/user)。 | 认证用户查询仍使用原生用户 ID；昵称、discriminator、guild open_id 不能制造第二套个人 ID。 |
| LINE | [用户 ID](https://developers.line.biz/en/docs/messaging-api/getting-user-ids/)、[API](https://developers.line.biz/en/reference/messaging-api/)、[账户绑定](https://developers.line.biz/en/docs/messaging-api/linking-accounts/)。 | 查询需要 channel access token 并满足好友/群成员等条件；profile 没有第二套普通用户 ID。跨系统绑定是显式授权流程。 |
| Mail | [RFC 5321 邮箱](https://www.rfc-editor.org/rfc/rfc5321.html#section-2.3.11)、[RFC 5322 地址](https://www.rfc-editor.org/rfc/rfc5322.html#section-3.4)。 | 没有协议通用的员工 ID 转换；SMTP、入站 webhook 和邮件认证是不同信任边界。 |
| Matrix | [用户标识](https://spec.matrix.org/latest/appendices/#user-identifiers)、[第三方用户查询](https://spec.matrix.org/latest/client-server-api/#get_matrixclientv3thirdpartyuser)。 | bridge 查询需已注册 application service、homeserver 支持和认证；可无映射。本 adapter 未接入 bridge。`/account/3pid` 只描述调用账户，不是任意发送者目录。 |
| OneBot | [v11 消息](https://github.com/botuniverse/onebot-11/blob/master/event/message.md)、[公共 API](https://github.com/botuniverse/onebot-11/blob/master/api/public.md)、[认证](https://github.com/botuniverse/onebot-11/blob/master/communication/authorization.md)。 | get_stranger_info 等需要部署的 access token/实现支持；仍返回 QQ 号，没有标准化 QQ Bot OpenID 转换。anonymous.flag 是凭据，不是别名。 |
| QQ | 同一 [v11 协议](https://github.com/botuniverse/onebot-11/blob/master/event/message.md) 和 [API](https://github.com/botuniverse/onebot-11/blob/master/api/public.md)，适用于此独立 adapter。 | 不能把 v12 或腾讯官方 Bot 字段套在 v11 回调上。 |
| QQ Guild | [C2C](https://bot.q.qq.com/wiki/develop/api-v2/autogen/event/c2c_message_create.html)、[群消息](https://bot.q.qq.com/wiki/develop/api-v2/autogen/event/group_at_message_create.html)、[消息概述](https://bot.q.qq.com/wiki/develop/api-v2/server-inter/message/overview.html)。 | 使用相应 bot/app token 和场景权限；union 字段按实际返回保留，不保证所有应用都有。成员资料权限不成为接收消息的新增前提。 |
| Slack | [Events](https://docs.slack.dev/apis/events-api/)、[users.info](https://docs.slack.dev/reference/methods/users.info/)、[migration.exchange](https://docs.slack.dev/reference/methods/migration.exchange/)。 | `users:read` 的 profile 可有 enterprise_user.id/enterprise_id；邮箱另需 `users:read.email`。migration.exchange 是 local/global 转换，要求 `tokens.basic`，反向用 `to_old`。本 adapter 未新增这些查询。 |
| Telegram | [Bot API](https://core.telegram.org/bots/api)、[用户名](https://telegram.org/faq#usernames-and-t-me)。 | Bot token 查询不提供另一套稳定个人 ID；用户名可变，转发、共享联系人不证明发消息者身份。 |
| WeChat Official | [普通消息](https://developers.weixin.qq.com/doc/service/guide/product/message/Receiving_standard_messages.html)、[UnionID](https://developers.weixin.qq.com/doc/service/guide/product/unionid.html)、[user/info](https://developers.weixin.qq.com/doc/service/api/usermanage/userinfo/api_userinfo.html)。 | 账号 access_token、接口资格、关注状态及开放平台绑定决定能否获得 unionid；不再假定能取到姓名头像。UnionID 不能直接替代客服发送用的 OpenID。 |
| WhatsApp | [BSUID](https://developers.facebook.com/documentation/business-messaging/whatsapp/business-scoped-user-ids)、[群回调](https://developers.facebook.com/documentation/business-messaging/whatsapp/webhooks/reference/messages/group/)、[权限](https://developers.facebook.com/documentation/business-messaging/whatsapp/permissions/)。 | `whatsapp_business_messaging` 覆盖消息；business portfolio scope 必须已知才能填写。没有把隐藏手机号反查回来的通用转换；身份轮换的 system message 不在本 adapter 的持久账户合并范围内。 |
| Zulip | [用户](https://zulip.com/api/get-user)、[消息](https://zulip.com/api/get-messages)、[发送](https://zulip.com/api/send-message)。 | 组织内 bot/user API 认证；delivery_email 可因隐私为空，sender_email 是 API email。查询不能绕过可见性，也不能据此跨组织合并。 |

## 经确认的账户绑定

当平台转换或明确的账户绑定已证明两个 ID 属于同一账户时，可使用共用映射层。独立服务设置 `UV_IM_USER_IDENTITIES_FILE=/etc/uv-im/user-identities.json`，文件内容是数组：

```json
[
  {
    "provider": "wecom",
    "connector": "work",
    "user_id": "callback-native-id",
    "identities": [
      {"type": "wecom.userid", "id": "directory-userid", "scope": "verified-corp-id"}
    ]
  }
]
```

只有 `provider + connector + user_id` 完全匹配的账户才添加身份，来源固定为 `configured`。不能按名称、通配符或群聊查用户。映射不会替换平台事实，不改 `user.id`、channel、回复/主动发送目标，也不自动形成权限授予。空选择器、重复账户、空 ID、重复 type/scope、未知字段和非数组 JSON 会在启动时拒绝。文件只在启动时读取，事件在进入日志前应用映射；重放历史日志不重新计算绑定。

Go 嵌入调用方使用 `NewUserIdentityMapper`，在启动前调用 `Hub.SetUserIdentityMapper`；直接消费 provider 的应用可调用不可变 mapper 的 `Apply(event)`。部署者负责验证绑定，connector 不通过模糊通讯录搜索生成绑定。

## 带类型的原生发送地址

身份列表不参与自动选路。普通发送继续使用既有 target；回复应复制完整 `referrer`。WhatsApp 手机号使用原生 `to` 字段，BSUID/parent BSUID 使用 `recipient`，因此其 target 需要保留 `id_type`：

```json
{"provider":"whatsapp","connector":"work","target":{"kind":"user","id":"opaque-bsuid","id_type":"whatsapp.bsuid"},"text":"hello"}
```

WhatsApp 在 `capabilities.target_id_types` 声明 `whatsapp.wa_id`、`whatsapp.bsuid`、`whatsapp.parent_bsuid`、`whatsapp.group_id`。后两种用户方案不能用作群地址，group_id 不能用作用户地址；文本和资源发送一致校验。未填 id_type 保留旧 `to` 行为。其他 adapter 未声明类型时会拒绝显式 id_type，避免将业务身份误当原生地址。

## 已知覆盖边界

QQ Guild 的 guild-DM 专用路由、Zulip 多人私聊 reply-all、Telegram 独立 `channel_post` 更新、Matrix 加密事件、OneBot v12 和跨平台 SSO/账户合并均不由身份保留自动实现。官方协议形状的回归用例验证解析与本地 HTTP 选路；它们不等同于所有平台账号的线上端到端验证。
