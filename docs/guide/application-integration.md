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
