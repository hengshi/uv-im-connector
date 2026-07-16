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

发送失败时，`POST /v1/message.create` 返回 HTTP `502` 和 `error: "provider_send_failed"`。如果 adapter 拿到了可安全公开的平台业务失败原因，响应还会通过限长的 `detail` 返回；可能包含凭证的任意网络错误不会原样回传。调用方可以在 `detail` 存在时展示该原因，或据此决定重试和 fallback。

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
- 业务重试或升级策略。

这些职责属于调用方应用。
