# Concepts

`uv-im-connector` is built around a small set of normalized protocol objects. Provider-specific details stay inside provider packages.

## Provider

A provider is the IM platform adapter, such as `wecom`, `lark`, `slack`, `telegram`, or `matrix`.

Every provider implements the root Go interface:

- `Run(ctx, sink)` receives provider-native events and emits normalized events.
- `Send(ctx, message)` sends normalized outbound messages.
- `Download(ctx, request)` resolves provider resources into internal resources.
- `Capabilities()` declares supported behavior.
- `Health(ctx)` reports the current state.

## Connector

A connector is one configured account identity for a provider. Examples:

- a production Lark app;
- a sandbox Lark app;
- a WeCom bot in one enterprise;
- a Slack bot token for one workspace.

When multiple identities exist for the same provider, callers must send both `provider` and `connector`.

```json
{
  "provider": "lark",
  "connector": "sandbox",
  "channel_id": "oc_xxx",
  "text": "hello"
}
```

## Channel

A channel is the provider-native conversation target, normalized into:

| Type | Meaning |
| --- | --- |
| `direct` | One-to-one conversation. |
| `group` | Group chat, channel, stream, guild channel, or equivalent shared conversation. |
| `thread` | Thread conversation when the provider exposes one as a distinct target. |
| `room` | Room-like conversation where the provider model is not direct or group. |

Route long-lived caller state by `provider + connector + channel.id`, not by `channel.id` alone.

## Addressed

`addressed` tells a caller whether a message is directed at the bot when the provider can determine it.

- `true`: the message is a direct message, mention, command, or otherwise addressed to the bot.
- `false`: the message is ambient group traffic or the provider cannot prove it is addressed.

Caller applications should treat `addressed=false` group events as ambient and non-actionable by default. Route them into workflows only when the caller explicitly opts into ambient group traffic.

## Referrer

`Referrer` carries provider-native reply or thread context:

```json
{
  "message_id": "1710000000.000100",
  "channel_id": "C123",
  "thread_id": "1710000000.000100",
  "reply_token": "..."
}
```

When replying to an event, copy its `referrer` into the outbound message. Provider adapters map it into the platform-specific reply or thread field.

## Capabilities

Capabilities describe what a provider supports:

- inbound events;
- outbound messages;
- direct and group messages;
- thread replies;
- upload and download resources;
- supported resource kinds;
- supported channel types.

Callers should read `/v1/meta` at startup and make policy decisions from capabilities instead of hardcoding provider names.
