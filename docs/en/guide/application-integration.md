# Application Integration

This page describes the integration contract for an application, bot, agent service, workflow worker, or automation service that consumes `uv-im-connector`.

## Startup

1. Configure provider credentials in the connector service.
2. Start `uv-im-connector` with a private listener and `UV_IM_AUTH_TOKEN`.
3. Call `GET /v1/meta` to record provider IDs, connector IDs, capabilities, and health.
4. Start the event consumer from the last processed sequence.

```text
GET /v1/events/ws?after=<last-sequence>
```

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
  "channel_id": "oc_xxx",
  "channel_type": "group",
  "text": "done",
  "referrer": {
    "message_id": "om_xxx",
    "channel_id": "oc_xxx"
  }
}
```

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
- business retry or escalation policy.

Those responsibilities belong to the caller application.
