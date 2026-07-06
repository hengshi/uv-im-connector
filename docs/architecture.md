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
- `OutboundMessage`：channel、text/elements/resources，以及用于 reply/thread 的可选 referrer。
- `Capabilities`：每个 provider 的显式能力声明。

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
