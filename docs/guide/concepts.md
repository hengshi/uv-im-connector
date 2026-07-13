# 核心概念

`uv-im-connector` 围绕少量标准协议对象建模。Provider 特有细节留在 provider package 内部，不泄漏给调用方应用、机器人或 agent service。

## Provider

Provider 是 IM 平台 adapter，例如 `wecom`、`lark`、`slack`、`telegram` 或 `matrix`。

每个 provider 都实现同一个 Go 接口：

- `Run(ctx, sink)` 接收平台原生事件并输出标准事件。
- `Send(ctx, message)` 发送标准出站消息。
- `Download(ctx, request)` 把 provider 资源解析为内部资源。
- `Capabilities()` 声明支持的行为。
- `Health(ctx)` 返回当前健康状态。

## Connector

Connector 是某个 provider 下的一组具体账号身份。典型例子：

- 一个生产 Lark app；
- 一个沙箱 Lark app；
- 某个企业下的 WeCom bot；
- 某个 Slack workspace 的 bot token。

同一个 provider 下存在多个身份时，调用方发送消息或下载资源必须同时传 `provider` 和 `connector`。

```json
{
  "provider": "lark",
  "connector": "sandbox",
  "target": {"kind": "conversation", "id": "oc_xxx"},
  "text": "hello"
}
```

## Target

Target 是出站消息的精确目的地，由 `id` 和 `kind` 组成。`kind` 是 `user`、`group`、`channel` 或 `conversation`。Server 主动发送必须显式提供 target，并使用 provider 在 `capabilities.target_kinds` 中声明的 kind。入站事件已经在 `referrer.target` 中提供精确回复目标，调用方回复时不应重新推断。

## Channel

Channel 是 provider 原生会话目标，归一化为：

| 类型 | 含义 |
| --- | --- |
| `direct` | 一对一会话。 |
| `group` | 群聊、频道、stream、guild channel 或等价共享会话。 |
| `thread` | provider 把 thread 暴露为独立目标时使用。 |
| `room` | 不适合归为 direct 或 group 的 room-like 会话。 |

调用方的长期状态应该按 `provider + connector + channel.id` 路由，不要只用 `channel.id`。

## Addressed

`addressed` 表示当 provider 能判断时，这条消息是否指向 bot。

- `true`：私聊、mention、command，或其他明确指向 bot 的消息。
- `false`：群里的 ambient traffic，或 provider 无法证明它指向 bot。

调用方默认应该把 `addressed=false` 的群消息视为 ambient/non-actionable。只有应用明确要处理群内所有消息时，才把它路由进 workflow。

## Referrer

`Referrer` 携带 provider 原生回复或 thread 上下文：

```json
{
  "message_id": "1710000000.000100",
  "parent_message_id": "1710000000.000099",
  "root_message_id": "1710000000.000001",
  "channel_id": "C123",
  "thread_id": "1710000000.000100",
  "reply_token": "...",
  "target": {"kind": "channel", "id": "C123"}
}
```

`message_id` 和 `reply_token` 用于回复当前消息；provider 能提供引用祖先时，`parent_message_id` 和 `root_message_id` 分别保留直接父消息和根消息。回复某个 event 时，把 event 的 `referrer` 原样带回 outbound message。Provider adapter 会把回复字段映射成平台原生 reply/thread 字段。

## Capabilities

Capabilities 描述 provider 支持什么：

- inbound events；
- outbound messages；
- 私聊和群聊；
- thread reply；
- reply message、Server 主动私聊和主动群聊；
- outbound target kind；
- resource upload / download；
- resource kind；
- channel type。

调用方应该在启动时读取 `/v1/meta`，基于 capabilities 做策略判断，不要在业务代码里硬编码 provider 名称。
