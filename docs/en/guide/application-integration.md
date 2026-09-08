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
