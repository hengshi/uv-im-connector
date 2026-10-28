# User identities and native addresses

`user.id` is the adapter's primary native subject ID. `user.identities` preserves additional platform identifiers with their namespaces. Callers do not need to parse raw callbacks. User identity, `channel.id` (conversation), and `referrer.target` (reply address) identify different things.

## Shared contract

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

An identity has `type`, `id`, optional `scope`, and `source`. Type names the identifier scheme. Scope records a known actual issuer, tenant, organization, or application; it is never guessed. Even when the platform's native identifier has wider uniqueness, a record without explicit scope is only guaranteed within the event's `provider + connector`. Do not merge such records across connectors. Different scopes may coexist for a type; the first assertion for a type/scope pair wins, preserving platform facts. IDs retain case, and their type is never inferred from their prefix.

Source is `callback`, `lookup`, or `configured`. Provenance is not business authorization. Matching display names, usernames, pinyin, nickname or phone-like strings do not establish a shared account.

Absent `user.kind` means an ordinary account; `user` is also accepted explicitly. Other values are `bot`, `webhook`, `system`, `anonymous`, and `chat`. Account mappings exclude webhook, system, anonymous and chat subjects. Missing users are not replaced with an installation user, group ID or compatibility fake sender. A bot account may still have an explicit account binding.

The new protocol-v1 fields are optional. Existing supported ordinary messages keep their primary IDs and destinations; absent senders, non-person subjects and newly supported native address formats follow their actual schema. `memory` is an internal test provider with caller-supplied identities.

## Inbound identities for all 16 providers

This table describes implemented output. Optional official lookups and their requirements are listed separately.

| Provider | Published identity types and fields | Scope and subject | Native reply destination |
| --- | --- | --- | --- |
| WeCom | `wecom.callback_userid` from `from.userid`. | The official callback can contain plaintext or an enterprise-encrypted ID; the field alone does not distinguish them. | Native callback userid for direct messages; chatid for groups. |
| Lark / Feishu | `lark.open_id`, `lark.user_id`, `lark.union_id` from sender IDs and the existing user-profile lookup. | Open ID: app; user ID: tenant; union ID: developer organization, whose issuer is absent from ordinary callbacks and remains unspecified. | chat_id, preserving the conversation target. |
| DingTalk | `dingtalk.userid` from senderStaffId; `dingtalk.unionid` from senderUnionId; `dingtalk.sender_id` from senderId. | Actual senderCorpId scopes userid. Encrypted senderId is not unionid. Primary preference: staff, sender, union. | Group conversationId; direct user address only with staff ID. Missing staff ID still retains the session webhook. |
| Discord | `discord.user_id` from author.id; webhook messages use `discord.webhook_id` from webhook_id. | Native Snowflake; bot/webhook subject distinctions; no name-based binding. | Incoming channel_id. |
| KOOK | `kook.user_id` from author_id; system uses `kook.system_id`. | author_id=`1` means system; preserve bot flag. Guild open_id is not a user alias. | Sender for PERSON, otherwise channel. |
| LINE | `line.user_id` from source.userId. | Developer-provider issuer is not in the ordinary callback. Destination is the bot, not the issuer. Missing consent may omit user ID. | Source user/group/room ID and a separate reply token. |
| Mail | `mail.mailbox` from successfully parsed from address. | Primary user.id keeps the original from string; preserve local-part case. Header identity is not authenticated personhood. | Existing from address. |
| Matrix | `matrix.mxid` from the full sender. | Preserve homeserver namespace; localpart is not an inferred email/employee ID. | room_id, always a conversation. |
| OneBot | `onebot.v11.qq_user_id` from numeric user_id. | This adapter implements v11; anonymous/notice subjects omit personal account IDs. | Native QQ user_id or group_id. |
| QQ | `onebot.v11.qq_user_id` from numeric user_id. | Separate provider envelope, same OneBot-v11 dialect; not the official QQ OpenID API. | Native QQ user_id or group_id. |
| QQ Guild / QQ Bot | `qqguild.author_id`, `qqguild.user_openid`, `qqguild.member_openid`, `qqguild.union_openid`, `qqguild.union_user_account` from actual author fields. | Retain legacy author.id as primary when present, otherwise user/member OpenID. Unknown issuer scope remains unspecified. | C2C prefers user_openid; groups use group_openid; channels use channel_id. |
| Slack | `slack.user_id` from event.user; bot-only messages use `slack.bot_id`. | Actual team_id scopes the record. Installation authorizations do not identify the sender. | Incoming conversation/channel. |
| Telegram | `telegram.user_id` from from.id; sender_chat uses `telegram.chat_id`. | sender_chat takes precedence over fake compatibility from; absent from never becomes `"0"`. Username is presentation data. | Receiving chat.id, never substituted with sender ID. |
| WeChat Official | `wechat-official.openid` from FromUserName. | Actual receiving ToUserName scopes the record; it is not asserted to be AppID. Ordinary XML contains no UnionID. | Account OpenID. |
| WhatsApp | `whatsapp.wa_id` from from/contacts.wa_id; `whatsapp.bsuid` from from_user_id/contacts.user_id; `whatsapp.parent_bsuid` from corresponding parent fields. | BSUID is business-portfolio scoped; entry.id/phone_number_id do not establish that portfolio. Contacts match only actual type+ID. | Phone or explicitly typed BSUID address; root group_id takes precedence over legacy context.group_id. |
| Zulip | `zulip.user_id` from sender_id; `zulip.api_email` from sender_email. | Organization/deployment scope. API email can be a privacy replacement, not a verified real mailbox. | Stream/topic or existing private address. |

## Official lookup and conversion paths

Callback identifiers are preserved directly. Lark reuses its existing optional user-profile request and bounded cache, chooses `user_id_type` from the actual primary identifier, and retains known callback scopes. Lookup errors preserve the base event. Other providers do not gain mandatory per-message identity requests. The following official APIs describe how extra identities can be obtained; their presence in this table does not imply automatic adapter integration.

| Provider | Official documentation and path | Requirements and limits |
| --- | --- | --- |
| WeCom | [Long connection](https://developer.work.weixin.qq.com/document/path/101463), [open_userid to userid](https://developer.work.weixin.qq.com/document/path/101521). | Callback ID is plaintext for a bot created by an enterprise super administrator, otherwise enterprise-encrypted. `POST /cgi-bin/batch/openuserid_to_userid` requires a **self-built application's access_token**, member visibility, batches up to 1,000. Already-plaintext IDs can appear in the invalid list. Bot Secret/contact authorization is not that token. Verified output can be bound as `wecom.userid` with enterprise scope. Automatic application-token acquisition/conversion is not implemented. |
| Lark | [Identifiers](https://open.feishu.cn/document/platform-overveiw/basic-concepts/user-identity-introduction/introduction), [callback](https://open.feishu.cn/document/server-docs/im-v1/message/events/receive), [get user](https://open.feishu.cn/document/server-docs/contact-v3/user/get). | Application token, basic user permission and visibility apply; user_id involves `contact:user.employee_id:readonly`. Missing fields remain absent. Union ID is not global across developers. |
| DingTalk | [Callback](https://open.dingtalk.com/document/development/robot-message-type), [user details](https://open.dingtalk.com/document/development/query-user-details), [unionid conversion](https://open.dingtalk.com/document/development/query-a-user-by-the-union-id). | `/topapi/v2/user/get` and `/topapi/user/getbyunionid` require the application's token, contact permissions such as `qyapi_get_member`, and visibility. External users/unpublished bots may lack staff ID; senderId is not a replacement userid. |
| Discord | [User](https://docs.discord.com/developers/resources/user), [Message](https://docs.discord.com/developers/resources/message). | Bot User lookup supplies no second ordinary stable account namespace. OAuth identify/email concerns the authorized user, not an arbitrary enterprise directory. Webhook authors are not people. |
| KOOK | [Objects](https://developer.kookapp.cn/doc/objects), [events](https://developer.kookapp.cn/doc/event/event-introduction), [user API](https://developer.kookapp.cn/doc/http/user). | Authenticated profile lookup retains the native user ID. Nickname, discriminator and guild open_id do not create another person ID. |
| LINE | [User IDs](https://developers.line.biz/en/docs/messaging-api/getting-user-ids/), [API](https://developers.line.biz/en/reference/messaging-api/), [account linking](https://developers.line.biz/en/docs/messaging-api/linking-accounts/). | Channel access token and friend/group membership rules apply. Profiles have no second ordinary ID. Cross-system linking is an explicit authorization flow. |
| Mail | [RFC 5321 mailbox](https://www.rfc-editor.org/rfc/rfc5321.html#section-2.3.11), [RFC 5322 address](https://www.rfc-editor.org/rfc/rfc5322.html#section-3.4). | No protocol-wide employee-ID conversion exists. SMTP, inbound webhook and mail authentication have separate trust boundaries. |
| Matrix | [User identifiers](https://spec.matrix.org/latest/appendices/#user-identifiers), [third-party user lookup](https://spec.matrix.org/latest/client-server-api/#get_matrixclientv3thirdpartyuser). | Requires registered application service, homeserver support and authentication; mappings may be absent. This adapter does not integrate bridge lookup. `/account/3pid` describes the caller, not arbitrary senders. |
| OneBot | [v11 message](https://github.com/botuniverse/onebot-11/blob/master/event/message.md), [public API](https://github.com/botuniverse/onebot-11/blob/master/api/public.md), [authentication](https://github.com/botuniverse/onebot-11/blob/master/communication/authorization.md). | get_stranger_info and similar methods depend on deployment/token support and still return QQ-number identities, with no standardized QQ Bot OpenID conversion. anonymous.flag is a credential, not an alias. |
| QQ | The same [v11 schema](https://github.com/botuniverse/onebot-11/blob/master/event/message.md) and [API](https://github.com/botuniverse/onebot-11/blob/master/api/public.md), checked for this separate adapter. | Do not apply v12 or Tencent Bot fields to the v11 callback. |
| QQ Guild | [C2C](https://bot.q.qq.com/wiki/develop/api-v2/autogen/event/c2c_message_create.html), [group](https://bot.q.qq.com/wiki/develop/api-v2/autogen/event/group_at_message_create.html), [overview](https://bot.q.qq.com/wiki/develop/api-v2/server-inter/message/overview.html). | Relevant bot/app token and scenario permissions apply. Preserve union fields only when supplied; not every app gets them. Member-profile permissions are not a new ingress prerequisite. |
| Slack | [Events](https://docs.slack.dev/apis/events-api/), [users.info](https://docs.slack.dev/reference/methods/users.info/), [migration.exchange](https://docs.slack.dev/reference/methods/migration.exchange/). | `users:read` can expose enterprise_user.id/enterprise_id; email additionally needs `users:read.email`. Migration local/global exchange requires `tokens.basic`; reverse with `to_old`. These lookups are not newly integrated. |
| Telegram | [Bot API](https://core.telegram.org/bots/api), [usernames](https://telegram.org/faq#usernames-and-t-me). | Bot-token queries do not supply another stable person ID. Mutable usernames, forwarded messages and shared contacts do not identify the sender. |
| WeChat Official | [Ordinary messages](https://developers.weixin.qq.com/doc/service/guide/product/message/Receiving_standard_messages.html), [UnionID](https://developers.weixin.qq.com/doc/service/guide/product/unionid.html), [user/info](https://developers.weixin.qq.com/doc/service/api/usermanage/userinfo/api_userinfo.html). | Account token, endpoint eligibility, subscription and Open Platform binding determine unionid availability. Do not assume names/avatar are returned. Customer-service sends still address OpenID. |
| WhatsApp | [BSUID](https://developers.facebook.com/documentation/business-messaging/whatsapp/business-scoped-user-ids), [group webhook](https://developers.facebook.com/documentation/business-messaging/whatsapp/webhooks/reference/messages/group/), [permissions](https://developers.facebook.com/documentation/business-messaging/whatsapp/permissions/). | `whatsapp_business_messaging` covers messages. Portfolio scope must be known explicitly. No generic hidden-phone reverse lookup is provided. Rotation system messages do not create persistent account merging in this adapter. |
| Zulip | [User](https://zulip.com/api/get-user), [messages](https://zulip.com/api/get-messages), [send](https://zulip.com/api/send-message). | Organization bot/user API authentication and visibility apply. delivery_email can be null; sender_email is an API email. No cross-organization merge follows. |

## Verified account bindings

After an official conversion or explicit account-linking process proves an association, deployments can use the shared mapper. Set `UV_IM_USER_IDENTITIES_FILE=/etc/uv-im/user-identities.json` to a JSON array:

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

The complete `provider + connector + user_id` selector must match. Added identities always have source `configured`. Names, wildcards and conversations are not selectors. Mappings cannot override platform assertions, change native user/channel IDs or send/reply destinations, or grant permissions. Empty selectors, duplicate accounts, empty IDs, duplicate type/scope pairs, unknown fields and non-array JSON fail startup. Configuration is read once at startup and applied before event persistence. Historical log replay does not recalculate bindings.

Embedded Go applications construct `NewUserIdentityMapper` and call `Hub.SetUserIdentityMapper` before startup. Applications consuming providers directly can use the immutable mapper's `Apply(event)`. Operators verify associations; the connector does not generate them from fuzzy directory searches.

## Typed native send addresses

Identity lists never select a destination automatically. Preserve the complete inbound `referrer` when replying. WhatsApp phone addresses use native `to`, while BSUID/parent BSUID use `recipient`, so targets retain `id_type`:

```json
{"provider":"whatsapp","connector":"work","target":{"kind":"user","id":"opaque-bsuid","id_type":"whatsapp.bsuid"},"text":"hello"}
```

WhatsApp advertises `whatsapp.wa_id`, `whatsapp.bsuid`, `whatsapp.parent_bsuid`, and `whatsapp.group_id` in `capabilities.target_id_types`. BSUID/parent BSUID are user addresses; group_id is a group address. Text and resource sending enforce the same rules. Omitted id_type retains legacy `to` behavior. Other adapters reject explicit id_type unless they advertise support, preventing business identities from being interpreted as native addresses.

## Coverage boundaries

Preserving identities does not implement QQ Guild's dedicated guild-DM route, Zulip group-DM reply-all, Telegram's separate `channel_post` update, Matrix encrypted events, OneBot v12, cross-platform SSO or account merging. Official-schema regression fixtures verify decoding and local HTTP addressing; they do not represent live end-to-end account tests for every platform.
