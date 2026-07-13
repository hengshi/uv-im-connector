# Getting Started

This guide starts a local connector, verifies the HTTP API, and sends one outbound message through the in-memory provider.

## Install

Install the standalone binary:

```bash
go install github.com/hengshi/uv-im-connector/cmd/uv-im-connector@<tag>
```

Or run it from a checkout:

```bash
go run ./cmd/uv-im-connector
```

Use the Go packages directly when embedding the connector in another process:

```bash
go get github.com/hengshi/uv-im-connector@<tag>
```

You can also use the published container image:

```bash
docker run --rm \
  -p 127.0.0.1:8787:8787 \
  -v uv-im-connector-state:/var/lib/uv-im-connector \
  -e UV_IM_AUTH_TOKEN=dev-token \
  -e UV_IM_PROVIDERS=memory \
  ghcr.io/hengshi/uv-im-connector:<tag>
```

## Start a Local Connector

```bash
export UV_IM_AUTH_TOKEN=dev-token
export UV_IM_PROVIDERS=memory
uv-im-connector
```

The default listener is `127.0.0.1:8787`.

```bash
curl http://127.0.0.1:8787/health
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:8787/v1/meta
```

Production deployments should set `UV_IM_AUTH_TOKEN` and keep the listener on a private interface or behind an authenticated reverse proxy. When `UV_IM_AUTH_TOKEN` is empty, public HTTP and WebSocket endpoints are unauthenticated.

## Send a Message

```bash
curl -X POST http://127.0.0.1:8787/v1/message.create \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "provider": "memory",
    "connector": "memory",
    "target": {"kind": "user", "id": "local"},
    "text": "hello"
  }'
```

The response is a normalized send result:

```json
{
  "provider": "memory",
  "connector": "memory",
  "message_id": "mem-msg_xxx",
  "time": "2026-01-01T00:00:00Z"
}
```

## Read or Watch Events

Read persisted normalized events:

```bash
curl -H "Authorization: Bearer dev-token" \
  "http://127.0.0.1:8787/v1/events?after=0"
```

Watch events over WebSocket:

```bash
websocat -H "Authorization: Bearer dev-token" \
  "ws://127.0.0.1:8787/v1/events/ws?after=0"
```

Callers should persist the last processed event `sequence` and reconnect with `/v1/events/ws?after=<seq>`.

## Enable a Real Provider

WeCom and Lark use provider-specific credentials:

```bash
# WeCom
export UV_IM_PROVIDERS=wecom
export UV_WECOM_CONNECTOR_ID=main
export UV_WECOM_BOT_ID=...
export UV_WECOM_BOT_SECRET=...
```

```bash
# Lark / Feishu
export UV_IM_PROVIDERS=lark
export UV_LARK_CONNECTOR_ID=main
export UV_LARK_APP_ID=...
export UV_LARK_APP_SECRET=...
export UV_LARK_REGION=feishu
```

HTTP webhook providers use the generic form documented in [Configuration](/en/configuration):

```text
UV_<PROVIDER>_CONNECTOR_ID
UV_<PROVIDER>_BASE_URL
UV_<PROVIDER>_TOKEN
UV_<PROVIDER>_WEBHOOK_SECRET
```

## Next Steps

- Read [Why It Exists](/en/guide/why-uv) to understand why `uv-im-connector` extracts IM connector logic from product-specific systems.
- Read [Concepts](/en/guide/concepts) for provider, connector, channel, addressed, referrer, and resource semantics.
- Read [Application Integration](/en/guide/application-integration) before wiring an application, bot, or agent service.
- Read [Deployment](/en/guide/deployment) before deploying the standalone service.
- Read [Resources](/en/guide/resources) before accepting user files.
- Read [Contributing](/en/guide/contributing) before adding a provider.
