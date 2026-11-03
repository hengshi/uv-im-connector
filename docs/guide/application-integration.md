# 应用接入

本页描述应用、机器人、agent service、workflow worker 或自动化服务接入 `uv-im-connector` 时应遵守的边界。

## 启动流程

1. 在 connector 服务中配置 provider 凭证。
2. 使用私有监听地址和 `UV_IM_AUTH_TOKEN` 启动 `uv-im-connector`。
3. 调用 `GET /v1/meta` 检查 service/protocol 兼容性，并记录 provider ID、connector ID、capabilities 和 health。
4. 从最后处理过的 sequence 开始订阅事件。

```text
GET /v1/events/ws?after=<last-sequence>
```

启动检查至少应该确认：

- `service` 是 `uv-im-connector`；
- `protocol_version` 在调用方支持范围内；
- 所需 provider/connector 存在；
- 所需能力在 `capabilities` 中为 true。

同一 `protocol_version` 的 connector bugfix 可以只升级 connector 服务；协议不兼容或调用方要使用新的 client/API 时，调用方应用才需要发版。

## Inbound Flow

```text
provider event
  -> provider adapter
  -> normalized Event
  -> event log
  -> /v1/events/ws
  -> caller application
```

调用方应用应该：

- 按 event `sequence` 和协议 ID 去重；
- 使用 `provider + connector + channel.id` 映射会话目标；
- 默认把 `addressed=false` 的群消息视为 ambient；
- 在启动长耗时任务前，把允许的资源复制到调用方自己的存储；
- 持久化足够的 run state，以便后续通过 `POST /v1/message.create` 回复。

## Outbound Flow

```text
caller application
  -> OutboundMessage
  -> /v1/message.create
  -> provider adapter
  -> provider send API
```

使用 event 字段发送回复：

```json
{
  "provider": "lark",
  "connector": "main",
  "text": "done",
  "referrer": {
    "message_id": "om_xxx",
    "channel_id": "oc_xxx",
    "target": {"kind": "conversation", "id": "oc_xxx"}
  }
}
```

Server 主动发送没有入站 `referrer`，必须显式传 `target`，并先检查 provider 的 `proactive_direct` / `proactive_group` 和 `target_kinds`。

回复流程还应保留完整 `referrer`，并遵守 `expires_at` 和 provider 的 `reply_max_uses`。reply handle 过期或耗尽后，只能在主动发送能力允许时清除 handle、改用 `referrer.target`；不要根据 provider 名称硬编码超时时间。

发送失败时，`POST /v1/message.create` 仍返回兼容的 HTTP `502` 和 `error: "provider_send_failed"`，同时始终返回 provider-neutral `failure`。外层 `502` 表示 connector 没有完成发送，不等于上游 provider 返回了 502；上游 HTTP 状态只看 `failure.http_status`。非 HTTP 失败没有该字段。

```json
{
  "ok": false,
  "error": "provider_send_failed",
  "detail": "lark send: http 429",
  "failure": {
    "category": "rate-limited",
    "retryable": true,
    "delivery_state": "rejected",
    "http_status": 429,
    "provider_code": "230020",
    "retry_after_seconds": 3,
    "request_id": "request-123"
  }
}
```

`detail` 是可选的人类可读说明，只适合展示；调用方不得解析它、`error` 或日志来决定行为。机器策略只使用 `failure`：

| 条件 | 调用方策略 |
| --- | --- |
| `retryable=true` 且 `delivery_state` 为 `rejected` 或 `not-attempted` | 可执行有界自动重试，并遵守 `retry_after_seconds` |
| `delivery_state=unknown` | 不自动重放；重复发送风险高于自动恢复收益 |
| `retryable=false` | 不重试，直接展示结构化原因和可执行的配置/目标修复入口 |

`category` 的稳定值包括 `invalid-request`、`authentication`、`permission`、`target-unavailable`、`rate-limited`、`provider-unavailable`、`timeout`、`transport`、`provider-rejected`、`payload-too-large` 和 `unknown`。`provider_code` 与 `request_id` 只保留机器代码/标识；provider response body、任意错误文本和凭证不会进入 `failure`。旧 client 可以继续只读取外层字段，新 client 应把 `failure` 持久化到自己的 delivery/writeback artifact。

调用方不应该直接调用 provider-native send API。Provider 特有发送逻辑属于 provider adapter。

## 完整文本、流式回复和编辑

`capabilities.text_max_bytes` 是单次发送或编辑的标准化 UTF-8 文本字节上限，包含调用方添加的可见前缀和格式。WeCom 和 DingTalk 当前声明并在上传或发送前校验 16384 字节；超限返回 `category=payload-too-large`、`delivery_state=not-attempted` 和包含精确上限的安全说明。`0` 或字段缺失表示未知上限。Lark 的原生限制针对实际序列化请求体，无法准确声明为固定的原始 UTF-8 文本预算，因此其 `text_max_bytes` 保持未知，由原生 API 校验请求体。Connector 不自动截断或分页；调用方按完整逻辑内容分页，并在无法合法继续分页时使用受支持的文件资源。

`reply_stream=true` 只表示有效被动 reply token 路由可以替换同一条流式回复，不表示主动发送支持流式更新。WeCom 使用现有 `message.create`：保留同一 `referrer` 更新全文，最后以 `final=true` 关闭流。切换主动发送路由后须重新判断模式。DingTalk 的普通 Markdown 是追加消息。调用方根据当前 connector 和实际路由决定呈现策略。

`edit_message=true` 表示实现了可选 `MessageUpdater.Update(ctx, MessageUpdate)` 和 `POST /v1/message.update`（Go client 为 `Client.Update`）。编辑沿用服务认证和发送失败协议，只接受完整替换文本，不接受 elements 或 resources，失败后不会自动创建新消息：

```json
{
  "provider": "lark",
  "connector": "main",
  "message_id": "om_bot_created",
  "target": {"kind": "conversation", "id": "oc_original_chat"},
  "text": "更新后的完整正文"
}
```

顶层 `message_id` 来自先前成功发送的 `SendResult`，与入站 `referrer.message_id` 不同。Lark 编辑前通过原生 GET 核验精确消息的 app 发送者、文本类型和会话，再调用原生 PUT；外部 app、已撤回消息、其他会话及指定但不匹配的话题均被拒绝。所有提供的会话字段必须一致。可提供已知 chat ID，或仅提供可查询到同一会话的原始 `referrer.message_id`；用户 Open ID 不足以证明会话，须使用原始 conversation target。读取失败不会执行编辑；应用需要消息读取和编辑权限。原生接口见[编辑消息](https://open.feishu.cn/document/server-docs/im-v1/message/update)及[获取指定消息](https://open.feishu.cn/document/server-docs/im-v1/message/get)。

这些字段和 endpoint 是协议 v1 的增量能力。新调用方须先读取精确 provider/connector 的 metadata；旧服务缺少能力字段时保持追加策略，不调用编辑 endpoint。旧调用方继续使用 `message.create`。使用新 Go client/API 的应用须固定包含这些 API 的依赖版本，并升级服务二进制后再启用能力；仅更新本地 Go 依赖不会升级运行中的服务。

## 恢复

事件日志按 sequence 递增。Consumer 可以带上最后处理过的 sequence 重连：

```text
/v1/events/ws?after=42
```

Connector 会先发送该 sequence 之后的 backlog，再继续推送新事件。

## 调用方边界

`uv-im-connector` 不负责：

- product workflow lifecycle；
- bot behavior；
- agent task lifecycle；
- run artifacts；
- 原生 resume handle；
- workspace 创建或清理；
- 用户 / 团队可见性策略；
- 基于 normalized `failure` 的有界重试、升级和任务生命周期策略。

这些职责属于调用方应用。

一次发送可同时提交 text 和多个 resources，connector 不设附件数量上限。Discord、Mail、Zulip 将它们合并为一条平台消息；其他支持上传的适配器先发送文本，再按顺序发送各附件（Slack 单附件仍可携带说明文字）。顺序发送的结果通过 `message_ids` 返回全部 ID，`message_id` 为最后一条 ID。中途失败时，`failure.delivered_count` 和 `failure.delivered_message_ids` 标明已完成部分，`retryable=false`、`delivery_state=unknown` 表示不能重放整批；失败的那一条仍可能已送达。
