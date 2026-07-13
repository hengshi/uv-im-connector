# 架构

`uv-im-connector` 是 universal、channel-neutral 的 IM connector。它负责 provider 认证、入站事件、出站消息、provider health 和 resources。调用方应用负责 product workflow、bot behavior、agent tasks、runs、workspaces、原生 resume handles 和 writeback policy。

## 分层

```text
IM provider
  -> provider adapter
  -> normalized Event / ResourceRef
  -> event log + HTTP/WS API
  -> caller application

caller application
  -> OutboundMessage
  -> provider adapter
  -> IM provider
```

## Provider Contract

每个 provider 实现同一个 Go 接口：

- `Run(ctx, sink)` 接收 provider 事件并输出标准事件。
- `Send(ctx, message)` 发送标准出站消息。
- `Download(ctx, request)` 把 provider resource 解析为本地 / 内部资源。
- `Capabilities()` 声明支持的行为。
- `Health(ctx)` 报告当前状态。

没有任何 provider 能定义核心协议。Provider-specific 行为留在 provider package 内部，并只通过 capabilities 暴露给调用方。

## Protocol

标准协议包含这些稳定对象：

- `Event`：入站事件 envelope，包含 provider、connector、channel、user、message 和 referrer。
- `Message`：文本、结构化 elements 和 resource references。
- `ResourceRef`：文件 / 图片 / 音频 / 视频引用，包含 sanitized internal URL 和 provider-private 字段。
- `OutboundMessage`：显式 `target`、text/elements/resources，以及用于 reply/thread 的可选 referrer。
- `Capabilities`：每个 provider 的显式能力声明，包括 reply、Server 主动发送和可接受的 target kind。

`OutboundMessage.target` 由 `id` 和 `kind` 组成。`kind` 只能是 `user`、`group`、`channel` 或 `conversation`，调用方必须使用该 provider 在 `capabilities.target_kinds` 中声明的类型。入站事件的 `referrer.target` 是 provider adapter 给出的精确回复目标；回复时应原样带回完整 `referrer`。目标解析优先级是 outbound `target`、`referrer.target`、旧 channel 字段。协议 v1 继续接受旧的 `channel_id` / `channel_type`：`direct` 映射为 `user`，`group` 映射为 `group`，`thread` 映射为 `channel`，`room` 或空类型映射为 `conversation`。旧 `channel_id` 仍是 provider-native 的现有会话 / channel ID，只映射语义类型，不会被重新解释成主动发送的 user ID。旧请求必须提供 channel ID 或 message / reply handle；未知的非空旧类型会被拒绝，不做猜测。

`reply_message` 表示可以携带入站事件的 `referrer` 回复已有消息；`proactive_direct` 和 `proactive_group` 表示没有当前入站消息时，Server 能否主动发送私聊或群聊消息。调用方应从 `/v1/meta` 读取这些能力，不应根据 provider 名称推断。

## Provider 能力矩阵

下表只列 16 个外部 provider，不包含用于测试和本地开发的 `memory`。`有条件` 表示 adapter 已支持该操作，但必须满足“限制”列的平台条件。

| Provider | 私聊入站 | 群聊入站 | 回复已有消息 | Server 主动私聊 | Server 主动群聊 | 出站 target kind | 限制 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| WeCom | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`group`、`conversation` | 使用 AI Bot WebSocket API。 |
| Lark / Feishu | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`group`、`conversation` | 用户目标是 Open ID；群聊 / 会话目标是 chat ID。 |
| DingTalk | 支持 | 支持 | 支持 | 不支持 | 有条件 | `user`、`group` | 回复使用入站 session webhook；主动发送使用配置的群机器人 webhook。 |
| Discord | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`channel`、`conversation` | 用户目标会先创建或复用 Discord DM channel。 |
| KOOK | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`channel` | 私聊使用 KOOK direct-message API。 |
| LINE | 支持 | 支持 | 支持 | 有条件 | 有条件 | `user`、`group`、`conversation` | Push target 必须满足 LINE 的好友、近期联系或群成员规则。 |
| Mail | 支持 | 不支持 | 支持 | 支持 | 不支持 | `user` | 用户目标是 email address。 |
| Matrix | 支持（room） | 支持（room） | 支持 | 有条件 | 支持 | `conversation` | Matrix 消息事件不区分私聊 / 群聊 room；调用方必须提供已知 room ID。 |
| OneBot | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`group` | 需要兼容的 OneBot endpoint。 |
| QQ | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`group` | 这是 OneBot-style QQ adapter，不是 QQ 官方 Bot API。 |
| QQ Guild | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`group`、`channel` | 使用 QQ 官方 Bot 的用户、群和频道消息 endpoint。 |
| Slack | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`channel`、`conversation` | `chat.postMessage` 接受 user ID 并建立私聊会话。 |
| Telegram | 支持 | 支持 | 支持 | 有条件 | 支持 | `user`、`group`、`conversation` | 用户必须先联系 bot，bot 才能主动发送给该用户。 |
| WeChat Official Account | 支持 | 不支持 | 支持 | 有条件 | 不支持 | `user` | 客服消息受平台交互时间窗口和账号规则限制。 |
| WhatsApp | 支持 | 有条件 | 支持 | 有条件 | 有条件 | `user`、`group` | Groups API 要求符合资格的 business account；主动私聊自由文本受客服窗口限制。 |
| Zulip | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`group` | 群聊目标是 Zulip stream；有 topic 时会保留。 |

## Resources

Provider 下载 URL、resource key、encrypted payload key、provider resource lookup 所需 metadata 和 raw payload 都是 provider-private。标准化后不能暴露给调用方。

公开 resource 形态是：

```text
ResourceRef
  id
  provider / connector
  kind
  name
  internal_url
  mime
  size_bytes
  sha256
```

`internal_url` 通过 `GET /v1/internal/<id>` 或 `client.ResolveInternalURL` 解析。调用方应该保存 sanitized reference，而不是 provider 下载凭证。

## Server API

| Endpoint | 用途 |
| --- | --- |
| `GET /health` | 进程健康检查。 |
| `GET /v1/meta` | 服务版本、协议版本、Provider 列表、capabilities 和 health。 |
| `GET /v1/events?after=<seq>` | 读取标准事件日志。 |
| `GET /v1/events/ws?after=<seq>` | 订阅标准事件。 |
| `POST /v1/message.create` | 发送出站消息。 |
| `POST /v1/upload.create` | 从本地字节创建内部资源。 |
| `POST /v1/resource.download` | 可信请求：把 provider-private resource 解析为内部资源。 |
| `POST /v1/webhook/{provider}/{connector}` | Provider webhook ingress。Webhook verification 由 provider adapter 负责。 |
| `GET /v1/internal/<id>` | 解析内部资源。 |

配置 `UV_IM_AUTH_TOKEN` 后，除 `/health` 和 provider webhook ingress 外，所有 endpoint 都要求 `Authorization: Bearer <UV_IM_AUTH_TOKEN>`。Provider webhook ingress 必须在输出标准事件前使用 provider-level webhook verification；支持 webhook 的 provider 在没有配置 provider webhook secret 时会拒绝 ingress。

`/v1/meta` 是调用方的兼容性检查入口。调用方应基于 `service`、`protocol_version` 和 capabilities 判断是否继续启动；不要只根据 provider 名称推断能力。

## Conformance

Provider conformance 由 capabilities 驱动。一个 provider 合格的条件是：

- 至少声明 inbound 或 outbound capability；
- 以相同 provider ID 报告 health；
- 声明支持 outbound 时能发送出站消息；
- 声明支持 download 时能解析 resource request；
- 输出 sanitized events，不泄露 provider secrets。

## Provider Set

独立二进制可以注册 `memory`、`wecom`、`lark`、`dingtalk`、`discord`、`kook`、`line`、`mail`、`matrix`、`onebot`、`qq`、`qqguild`、`slack`、`telegram`、`wechat-official`、`whatsapp` 和 `zulip`。

Provider set 不是协议层级。每个 adapter 的边界都相同：把 provider-native inbound、outbound、auth 和 resource 行为翻译成根协议。
