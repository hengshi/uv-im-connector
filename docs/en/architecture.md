# Architecture

`uv-im-connector` is a universal, channel-neutral IM connector. It owns provider authentication, inbound events, outbound messages, provider health, and resources. Caller applications own product workflows, bot behavior, agent tasks, runs, workspaces, native resume handles, and writeback policy.

## Layers

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

Every provider implements the same Go interface:

- `Run(ctx, sink)` receives provider events and emits normalized events.
- `Send(ctx, message)` sends a normalized outbound message.
- `Download(ctx, request)` resolves a provider resource into a local/internal resource.
- `Capabilities()` declares supported behavior.
- `Health(ctx)` reports current state.

No provider defines the core protocol. Provider-specific behavior stays inside the provider package and is exposed only through capabilities.

## Protocol

The normalized protocol has these stable objects:

- `Event`: inbound event envelope with provider, connector, channel, user, message, and referrer.
- `Message`: text, structured elements, and resource references.
- `ResourceRef`: file/image/audio/video reference with sanitized internal URL and private provider fields.
- `OutboundMessage`: explicit target, text/elements/resources, and optional referrer for replies or threads.
- `Capabilities`: explicit feature declaration for each provider, including reply, proactive send, and accepted target kinds.

`OutboundMessage.target` contains an `id` and a `kind`. The kind is `user`, `group`, `channel`, or `conversation`, and callers must use a kind declared by that provider in `capabilities.target_kinds`. An inbound event's `referrer.target` is the exact reply destination selected by the provider adapter; callers should copy the complete `referrer` when replying. Target resolution prefers outbound `target`, then `referrer.target`, then legacy channel fields. Protocol v1 continues to accept the legacy `channel_id` / `channel_type` fields: `direct` maps to `user`, `group` to `group`, `thread` to `channel`, and `room` or an empty type maps to `conversation`. The legacy `channel_id` remains the provider-native existing conversation/channel ID; only its semantic kind is mapped, so it is not reinterpreted as a proactive user ID. A legacy request must include a channel ID or a message/reply handle. Unknown non-empty legacy channel types are rejected instead of being guessed.

`reply_message` means that an outbound message can carry an inbound event's `referrer` to reply to an existing message. `proactive_direct` and `proactive_group` mean the server can send to a direct or group target without a current inbound message. Callers should read these capabilities from `/v1/meta` instead of inferring them from a provider name.

## Provider Capability Matrix

This table covers the 16 external providers and excludes `memory`, which is used for tests and local development. `Conditional` means the adapter supports the operation when the platform condition in the last column is met.

| Provider | Direct inbound | Group inbound | Reply | Proactive direct | Proactive group | Outbound target kinds | Constraint |
| --- | --- | --- | --- | --- | --- | --- | --- |
| WeCom | Yes | Yes | Yes | Yes | Yes | `user`, `group`, `conversation` | Uses the AI Bot WebSocket API. |
| Lark / Feishu | Yes | Yes | Yes | Yes | Yes | `user`, `group`, `conversation` | User targets are Open IDs; group/conversation targets are chat IDs. |
| DingTalk | Yes | Yes | Yes | No | Conditional | `user`, `group` | Replies use the inbound session webhook; proactive sends use the configured group webhook. |
| Discord | Yes | Yes | Yes | Yes | Yes | `user`, `channel`, `conversation` | A user target opens or reuses a Discord DM channel before sending. |
| KOOK | Yes | Yes | Yes | Yes | Yes | `user`, `channel` | Direct messages use the KOOK direct-message API. |
| LINE | Yes | Yes | Yes | Conditional | Conditional | `user`, `group`, `conversation` | Push targets must satisfy LINE friendship, recent-contact, or group-membership rules. |
| Mail | Yes | No | Yes | Yes | No | `user` | The user target is an email address. |
| Matrix | Yes (room) | Yes (room) | Yes | Conditional | Yes | `conversation` | Matrix message events do not identify direct versus group rooms; the target must be a known room ID. |
| OneBot | Yes | Yes | Yes | Yes | Yes | `user`, `group` | Requires a compatible OneBot endpoint. |
| QQ | Yes | Yes | Yes | Yes | Yes | `user`, `group` | This provider is the OneBot-style QQ adapter, not the official QQ Bot API. |
| QQ Guild | Yes | Yes | Yes | Yes | Yes | `user`, `group`, `channel` | Uses official QQ Bot user, group, and channel message endpoints. |
| Slack | Yes | Yes | Yes | Yes | Yes | `user`, `channel`, `conversation` | Slack accepts a user ID in `chat.postMessage` to open a direct conversation. |
| Telegram | Yes | Yes | Yes | Conditional | Yes | `user`, `group`, `conversation` | A user must contact the bot before the bot can send to that user. |
| WeChat Official Account | Yes | No | Yes | Conditional | No | `user` | Customer-service messages are subject to the platform's interaction window and account rules. |
| WhatsApp | Yes | Conditional | Yes | Conditional | Conditional | `user`, `group` | Groups API requires an eligible business account; free-form direct messages require the customer-service window. |
| Zulip | Yes | Yes | Yes | Yes | Yes | `user`, `group` | Group targets are Zulip streams; the outbound topic is preserved when available. |

## Resources

Provider download URLs, resource keys, encrypted payload keys, metadata needed for provider resource lookup, and raw payloads are provider-private. They must not be exposed to callers after normalization.

The public resource shape is:

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

`internal_url` is resolved through `GET /v1/internal/<id>` or `client.ResolveInternalURL`. Callers should store the sanitized reference, not provider download credentials.

## Server API

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Process health. |
| `GET /v1/meta` | Service version, protocol version, provider list, capabilities, and health. |
| `GET /v1/events?after=<seq>` | Read normalized event log. |
| `GET /v1/events/ws?after=<seq>` | Watch normalized events. |
| `POST /v1/message.create` | Send outbound message. |
| `POST /v1/upload.create` | Create internal resource from local bytes. |
| `POST /v1/resource.download` | Trusted request to resolve a provider-private resource into an internal resource. |
| `POST /v1/webhook/{provider}/{connector}` | Provider webhook ingress. Webhook verification is owned by the provider adapter. |
| `GET /v1/internal/<id>` | Resolve internal resource. |

All endpoints except `/health` and provider webhook ingress require `Authorization: Bearer <UV_IM_AUTH_TOKEN>` when `UV_IM_AUTH_TOKEN` is configured. Provider webhook ingress must use provider-level webhook verification before emitting normalized events, and webhook-capable providers reject ingress when no provider webhook secret is configured.

`/v1/meta` is the caller compatibility check entrypoint. Callers should decide whether to continue startup from `service`, `protocol_version`, and capabilities, not from provider names alone.

## Conformance

Provider conformance is capability-driven. A provider is valid when it:

- declares at least inbound or outbound capability;
- reports health under the same provider ID;
- sends outbound messages when outbound is supported;
- resolves resource requests when download is supported;
- emits sanitized events without leaking provider secrets.

## Provider Set

The standalone binary can register `memory`, `wecom`, `lark`, `dingtalk`, `discord`, `kook`, `line`, `mail`, `matrix`, `onebot`, `qq`, `qqguild`, `slack`, `telegram`, `wechat-official`, `whatsapp`, and `zulip`.

The provider set is not a protocol hierarchy. Each adapter has the same boundary: translate provider-native inbound, outbound, auth, and resource behavior into the root protocol.
