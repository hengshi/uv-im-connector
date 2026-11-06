# Application Integration

This page describes the integration contract for an application, bot, agent service, workflow worker, or automation service that consumes `uv-im-connector`.

## Startup

1. Configure provider credentials in the connector service.
2. Start `uv-im-connector` with a private listener and `UV_IM_AUTH_TOKEN`.
3. Call `GET /v1/meta` to verify service/protocol compatibility and record provider IDs, connector IDs, capabilities, and health.
4. Start the event consumer from the last processed sequence.

```text
GET /v1/events/ws?after=<last-sequence>
```

The startup check should at least verify:

- `service` is `uv-im-connector`;
- `protocol_version` is supported by the caller;
- required provider/connector pairs exist;
- required capabilities are true.

Connector bugfixes within the same `protocol_version` can be deployed by upgrading only the connector service. Caller applications need their own release when the protocol is incompatible or when they consume a new client/API surface.

## Inbound Flow

```text
provider event
  -> provider adapter
  -> normalized Event
  -> event log
  -> /v1/events/ws
  -> caller application
```

The caller application should:

- dedupe by event `sequence` and protocol IDs;
- map a conversation target by `provider + connector + channel.id`;
- treat `addressed=false` group events as ambient unless explicitly enabled;
- copy allowed resources into caller-owned storage before starting long-running work;
- persist enough run state to reply later through `POST /v1/message.create`.

## Outbound Flow

```text
caller application
  -> OutboundMessage
  -> /v1/message.create
  -> provider adapter
  -> provider send API
```

Use the event fields to send a reply:

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

A proactive server send has no inbound `referrer`, so it must provide `target` explicitly after checking the provider's `proactive_direct` / `proactive_group` and `target_kinds` capabilities.

A reply flow should also retain the complete `referrer` and honor its `expires_at` value plus the provider's `reply_max_uses`. After a reply handle expires or is exhausted, clear the handle and use `referrer.target` only when proactive delivery is supported; do not hard-code deadlines from provider names.

When a send fails, `POST /v1/message.create` keeps the compatible HTTP `502` and `error: "provider_send_failed"` fields and always adds a provider-neutral `failure`. The outer `502` means that the connector did not complete the send; it does not mean that the upstream provider returned 502. Read the upstream HTTP status from `failure.http_status`. Non-HTTP failures omit that field.

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

`detail` is an optional human-readable display value. A caller must not parse it, `error`, or logs to make decisions. Machine policy uses only `failure`:

| Condition | Caller policy |
| --- | --- |
| `retryable=true` and `delivery_state` is `rejected` or `not-attempted` | A bounded automatic retry is safe; honor `retry_after_seconds` |
| `delivery_state=unknown` | Do not replay automatically because delivery may have happened |
| `retryable=false` | Do not retry; expose the structured reason and an actionable configuration/target repair path |

Stable `category` values are `invalid-request`, `authentication`, `permission`, `target-unavailable`, `rate-limited`, `provider-unavailable`, `timeout`, `transport`, `provider-rejected`, `payload-too-large`, and `unknown`. `provider_code` and `request_id` contain only machine-like codes/IDs. Provider response bodies, arbitrary error text, and credentials never enter `failure`. Older clients can continue reading only the outer fields; new clients should persist `failure` in their delivery/writeback artifact.

Callers should not call provider-native send APIs directly. Provider-specific send behavior belongs in provider adapters.

## Candidate explicit native conversation search

`conversation_search_version=v1` is a candidate, trusted-infrastructure-only
surface for a future current-Run lookup. It is separate from the automatic
conversation window. A caller would send the persisted trigger tuple and
sanitized-trigger digest, plus the SHA-256 audience digest of its currently
authorized Run capability. Agents and users never choose chat/thread scope,
native page tokens, or the connector service Bearer.

The connector would return only an opaque, short-lived Hub cursor. It is bound
to the verified trigger, local filters, verified native scope, and current
capability audience; a capability rotation invalidates it. Candidate output
would report native provenance, immutable anchor, effective bounds, and bounded
scan coverage so that `none`, partial scans, expiry, permission/rate limits, and
unavailability are not represented as complete-history absence. This document
does not claim that the associated caller CLI or TaskService handoff has
shipped.

## Complete text, reply streams, and editing

`capabilities.text_max_bytes` bounds normalized UTF-8 text bytes per send or edit, including visible prefixes and formatting added by the caller. WeCom declares 20480 bytes for [passive stream](https://developer.work.weixin.qq.com/document/path/101031) `stream.content` and [long-connection proactive](https://developer.work.weixin.qq.com/document/path/101463) `markdown.content`, and validates it before upload or send. Oversized text returns `category=payload-too-large`, `delivery_state=not-attempted`, and a safe detail containing the exact limit. Zero or an absent field means unknown. Lark's native limits apply to serialized request bodies and cannot be declared accurately as a fixed raw UTF-8 text budget, so its `text_max_bytes` remains unknown and the native API validates the request body. The connector does not truncate or paginate; callers should page complete logical content and use a supported file resource when further pages cannot legally be sent.

DingTalk also leaves the raw-text budget unknown. Its [group server API](https://open.dingtalk.com/document/development/the-robot-sends-a-group-message) limits the serialized `msgParam` JSON string to 15000 bytes, including title, text, and JSON escaping. This is not a plain UTF-8 text limit shared by its direct-message and session-webhook routes. Unknown does not mean unlimited; native APIs enforce their route constraints and the connector returns normalized failures.

`reply_stream=true` applies only to a live passive reply-token route. It does not advertise proactive streaming. WeCom uses the existing `message.create` path: retain the same referrer, replace the complete stream text, and close with `final=true`. Re-evaluate the mode after switching to proactive delivery. Ordinary DingTalk Markdown appends messages. Presentation policy belongs to the caller and depends on the exact connector and actual route.

`edit_message=true` advertises the optional `MessageUpdater.Update(ctx, MessageUpdate)` and `POST /v1/message.update` (Go client: `Client.Update`). Updates use the same authentication and structured failures as sends. They accept complete replacement text without elements or resources, and never fall back to creating a new message:

```json
{
  "provider": "lark",
  "connector": "main",
  "message_id": "om_bot_created",
  "target": {"kind": "conversation", "id": "oc_original_chat"},
  "text": "Complete replacement text"
}
```

The top-level `message_id` comes from a successful outbound `SendResult`, separately from the inbound `referrer.message_id`. Before a native PUT, Lark uses a native GET to verify the exact message's app sender, text type, and conversation. A foreign app, deleted message, different chat, or mismatched specified thread is rejected. All supplied conversation fields must agree. Provide a known chat ID, or an original `referrer.message_id` that resolves to the same chat. A user Open ID is insufficient conversation proof; use the original conversation target. Failed reads never execute an edit; the app needs message read and edit permissions. See the native [edit message](https://open.feishu.cn/document/server-docs/im-v1/message/update) and [get message](https://open.feishu.cn/document/server-docs/im-v1/message/get) contracts.

These fields and the endpoint are additive protocol v1 capabilities. New callers must first read metadata for the exact provider/connector. Missing capabilities on an older service mean append-only presentation and no update calls. Older callers continue using `message.create`. Applications using the new Go client/API must pin a dependency that includes it and upgrade the service binary before enabling it; updating a local Go dependency does not upgrade a running service.

## Confirmed outbound publication locators

An internal caller that needs to quote a notification later may add `request_publication_locator: true` to its `message.create` request. This is not an end-user setting or connector configuration. Ordinary sends omit it and do not gain a message lookup. The caller must first verify `capabilities.publication_locator_version: "v1"` for the exact provider and connector. The service-level `publication_locator_version` only says that this HTTP service can serialize the schema; it does not imply every provider can confirm a locator.

The Lark / Feishu adapter currently supports `v1`. It completes the native POST first and performs a GET for the exact returned `message_id` only when requested. A successful `SendResult.publications` entry with `delivery_state: "confirmed"` requires the same message, a non-deleted record, the configured app as sender (`app_id`), and a nonempty native `chat_id`. It contains the provider, connector, message ID, actual native conversation target, and bot app ID. A proactive recipient Open ID is never guessed to be the conversation ID. Adapters that do not advertise the capability must not synthesize a locator.

Once the native POST is explicitly successful, a failed GET—whether from permission, network, deletion, or response validation—does **not** turn the send into an error or `unknown`, and never triggers a retry. The successful result still contains its original `message_id`; it simply omits `publications`. Callers may treat only a `confirmed` locator as quote evidence and must use their own safe fallback when it is absent.

When one logical send is split into text/resource native parts, confirmed entries are returned in `publications` with one-based `part_ordinal`. If a later part fails, only the confirmed successful prefix appears in `failure.delivered_publications`; the failed or unknown part never gets a locator and the whole batch must not be replayed.

## Recovery

The event log is sequence-based. A consumer can reconnect with the last processed sequence:

```text
/v1/events/ws?after=42
```

The connector sends backlog events after that sequence before streaming fresh events.

## Caller Boundary

`uv-im-connector` does not own:

- product workflow lifecycle;
- bot behavior;
- agent task lifecycle;
- run artifacts;
- native resume handles;
- workspace creation or cleanup;
- user/team visibility policy;
- bounded retry, escalation, and task lifecycle policy driven by normalized `failure`.

Those responsibilities belong to the caller application.

A send accepts text and multiple resources without a connector-imposed count limit. Discord, Mail, and Zulip combine them in one native message; the other upload-capable adapters send text first, then each resource in order (Slack also keeps its single-file caption form). Ordered sends return all IDs in `message_ids` and the last ID in `message_id`. On partial failure, `failure.delivered_count` and `failure.delivered_message_ids` identify completed messages; `retryable=false` and `delivery_state=unknown` prohibit replaying the whole sequence. The failed part may have an ambiguous outcome.
