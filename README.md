# uv-im-connector

`uv-im-connector` is a provider-neutral Go connector for IM inbound events, outbound messages, and resources.

The core contract treats every channel equally:

- Providers emit normalized events.
- Providers send outbound messages through the same API.
- Resources use one internal reference model for files, images, audio, and video.
- Provider differences are expressed as capabilities, not as product hierarchy.
- Runtime systems consume normalized events and do not depend on channel-specific payloads.

## Packages

| Package | Purpose |
| --- | --- |
| `github.com/hengshi/uv-im-connector` | Protocol types, provider contract, event log, resource store. |
| `server` | HTTP and WebSocket API for events, metadata, outbound messages, uploads, and internal resources. |
| `client` | Go client for the connector HTTP and WebSocket API. |
| `conformance` | Shared provider behavior tests. |
| `providers/memory` | In-memory provider for tests and local development. |
| `providers/wecom` | WeCom provider implementation. |
| `providers/lark` | Lark provider implementation. |
| `providers/dingtalk` | DingTalk provider implementation. |
| `providers/discord` | Discord provider implementation. |
| `providers/kook` | KOOK provider implementation. |
| `providers/line` | LINE provider implementation. |
| `providers/mail` | Mail provider implementation. |
| `providers/matrix` | Matrix provider implementation. |
| `providers/onebot` | OneBot-compatible provider implementation. |
| `providers/qq` | QQ provider implementation. |
| `providers/qqguild` | QQ Guild provider implementation. |
| `providers/slack` | Slack provider implementation. |
| `providers/telegram` | Telegram provider implementation. |
| `providers/wechatofficial` | WeChat Official Account provider implementation. |
| `providers/whatsapp` | WhatsApp provider implementation. |
| `providers/zulip` | Zulip provider implementation. |

## Boundary

This repository owns IM connection, authentication, normalized events, outbound messages, provider health, and resource download/upload.

It does not own runtime-agent sessions, task state, run artifacts, native resume handles, or workspace lifecycle. Those belong to the caller.

## Run

```bash
UV_IM_PROVIDERS=memory go run ./cmd/uv-im-connector
```

Production deployments should set `UV_IM_AUTH_TOKEN` and keep the listener on a private interface.

Webhook-capable providers receive inbound events at:

```text
POST /v1/webhook/{provider}/{connector}
```

Public API auth is separate from provider webhook verification. Configure each provider webhook secret before enabling webhook ingress; requests are rejected when the provider secret is absent.
