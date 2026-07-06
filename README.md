# uv-im-connector

`uv-im-connector` is a universal, provider-neutral Go connector for IM inbound events, outbound messages, and resources.

It gives caller applications one stable IM surface:

```text
IM provider -> provider adapter -> normalized Event -> HTTP/WS API -> caller application
caller application -> OutboundMessage -> HTTP API -> provider adapter -> IM provider
```

The core contract treats every channel equally:

- Providers emit normalized events through the same event model.
- Providers send outbound messages through the same API.
- Files, images, audio, and video use one resource reference model.
- Provider differences are exposed through capabilities, not product hierarchy.
- Caller applications consume normalized events and do not parse provider-native payloads.

## Documentation Site

This repository is configured to publish the VitePress documentation site with GitHub Pages:

```text
https://hengshi.github.io/uv-im-connector/
```

The site defaults to Simplified Chinese. English documentation is served under:

```text
https://hengshi.github.io/uv-im-connector/en/
```

Run the site locally with:

```bash
npm install
npm run docs:dev
```

## Boundary

`uv-im-connector` owns:

- IM provider credentials and connection lifecycle;
- inbound event normalization;
- outbound message delivery;
- provider health and capability metadata;
- resource download, upload, redaction, and internal resource URLs.

Caller applications own:

- product workflows, bot behavior, agent sessions, tasks, runs, workspaces, and resume handles;
- business routing and permission policy;
- durable run artifacts and status UI;
- retry, escalation, and writeback policy.

In other words, `uv-im-connector` is IM infrastructure. It is not a bot framework or an agent execution engine.

## Install

Use the standalone binary when you want one connector service for another application:

```bash
go install github.com/hengshi/uv-im-connector/cmd/uv-im-connector@<tag>
```

Or run it from a checkout:

```bash
go run ./cmd/uv-im-connector
```

Or run the published container image:

```bash
docker run --rm \
  -p 127.0.0.1:8787:8787 \
  -v uv-im-connector-state:/var/lib/uv-im-connector \
  -e UV_IM_AUTH_TOKEN=dev-token \
  -e UV_IM_PROVIDERS=memory \
  ghcr.io/hengshi/uv-im-connector:<tag>
```

Use the Go packages directly when embedding the connector in another Go process:

```bash
go get github.com/hengshi/uv-im-connector@<tag>
```

## Quick Start

Start a local memory-backed connector:

```bash
export UV_IM_AUTH_TOKEN=dev-token
export UV_IM_PROVIDERS=memory
uv-im-connector
```

The service listens on `127.0.0.1:8787` by default. Check it with:

```bash
curl http://127.0.0.1:8787/health
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:8787/v1/meta
```

Send a test outbound message through the memory provider:

```bash
curl -X POST http://127.0.0.1:8787/v1/message.create \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "provider": "memory",
    "connector": "memory",
    "channel_id": "local",
    "channel_type": "direct",
    "text": "hello"
  }'
```

Read normalized events:

```bash
curl -H "Authorization: Bearer dev-token" \
  "http://127.0.0.1:8787/v1/events?after=0"
```

Watch normalized events over WebSocket:

```bash
websocat -H "Authorization: Bearer dev-token" \
  "ws://127.0.0.1:8787/v1/events/ws?after=0"
```

## Configuration

The standalone binary reads these top-level settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `UV_IM_ADDR` | `127.0.0.1:8787` | HTTP listen address. |
| `UV_IM_STATE_DIR` | `.uv-im-connector` | Event log and resource storage directory. |
| `UV_IM_PROVIDERS` | auto-detected | Comma-separated provider list. |
| `UV_IM_AUTH_TOKEN` | empty | Optional bearer token for all public HTTP/WS endpoints except `/health`. |

Production deployments should set `UV_IM_AUTH_TOKEN` and keep the listener on a private interface or behind an authenticated reverse proxy. When `UV_IM_AUTH_TOKEN` is empty, public HTTP/WS endpoints are unauthenticated.

Supported standalone provider IDs:

```text
memory,wecom,lark,dingtalk,discord,kook,line,mail,matrix,onebot,qq,qqguild,slack,telegram,wechat-official,whatsapp,zulip
```

When `UV_IM_PROVIDERS` is empty, the binary auto-loads providers that have detected credentials or webhook configuration. The `memory` provider is never auto-loaded.

Provider-specific settings are documented in [docs/configuration.md](docs/configuration.md). The important naming rule is:

```text
UV_<PROVIDER>_CONNECTOR_ID
UV_<PROVIDER>_BASE_URL
UV_<PROVIDER>_TOKEN
UV_<PROVIDER>_WEBHOOK_SECRET
```

For WeCom and Lark, the connector uses provider-specific credentials:

```bash
# WeCom
export UV_IM_PROVIDERS=wecom
export UV_WECOM_CONNECTOR_ID=main
export UV_WECOM_BOT_ID=...
export UV_WECOM_BOT_SECRET=...

# Lark / Feishu
export UV_IM_PROVIDERS=lark
export UV_LARK_CONNECTOR_ID=main
export UV_LARK_APP_ID=...
export UV_LARK_APP_SECRET=...
export UV_LARK_REGION=feishu
```

Connector IDs identify one concrete bot/app/workspace identity for a provider. If the same provider has multiple configured identities, callers must include both `provider` and `connector` when sending outbound messages or resolving provider resources.

## HTTP API

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Process health. |
| `GET /v1/meta` | Service version, protocol version, providers, connector IDs, capabilities, and health. |
| `GET /v1/events?after=<seq>` | Read persisted normalized events. |
| `GET /v1/events/ws?after=<seq>` | Watch normalized events over WebSocket. |
| `POST /v1/message.create` | Send an outbound message. |
| `POST /v1/upload.create` | Create an internal resource from local bytes. |
| `POST /v1/resource.download` | Resolve a provider-private resource into an internal resource. |
| `POST /v1/webhook/{provider}/{connector}` | Provider webhook ingress. |
| `GET /v1/internal/<id>` | Resolve an internal resource URL. |

When `UV_IM_AUTH_TOKEN` is configured, pass it as:

```text
Authorization: Bearer <token>
```

Provider webhook ingress is intentionally separate from public API auth. Webhook-capable providers verify their own provider-level secret and reject webhook requests when the secret is not configured.

`GET /v1/meta` is the runtime compatibility contract:

```json
{
  "service": "uv-im-connector",
  "connector_version": "v0.0.4",
  "protocol_version": "v1",
  "providers": []
}
```

Caller applications should check `service`, supported `protocol_version`, and required provider capabilities at startup. A connector bugfix with the same protocol version can be deployed by upgrading only the `uv-im-connector` service. Caller applications need their own release only when they consume a new incompatible protocol/API or change their integration behavior.

## Normalized Events

Every inbound event uses the root `Event` shape:

```json
{
  "sequence": 1,
  "id": "evt-1",
  "type": "message.create",
  "provider": "lark",
  "connector": "main",
  "time": "2026-01-01T00:00:00Z",
  "channel": {
    "id": "oc_xxx",
    "type": "group",
    "name": "engineering"
  },
  "user": {
    "id": "ou_xxx",
    "name": "alice"
  },
  "message": {
    "id": "om_xxx",
    "type": "text",
    "text": "build passed"
  },
  "referrer": {
    "message_id": "om_xxx",
    "channel_id": "oc_xxx"
  },
  "addressed": true
}
```

Important fields:

- `provider`: the IM platform adapter, such as `wecom`, `lark`, or `slack`.
- `connector`: the concrete configured account identity for that provider.
- `channel.id`: the provider-native conversation ID.
- `channel.type`: normalized conversation type, such as `direct`, `group`, `thread`, or `room`.
- `addressed`: whether the message is addressed to the bot when the provider can tell.
- `referrer`: provider information needed for replies or thread-aware outbound messages.

## Send Messages

Send outbound text with `POST /v1/message.create`:

```json
{
  "provider": "wecom",
  "connector": "main",
  "channel_id": "wr_xxx",
  "channel_type": "group",
  "text": "done"
}
```

Reply to a known message or thread by carrying the event referrer back:

```json
{
  "provider": "slack",
  "connector": "main",
  "channel_id": "C123",
  "channel_type": "group",
  "text": "done",
  "referrer": {
    "message_id": "1710000000.000100",
    "channel_id": "C123",
    "thread_id": "1710000000.000100"
  }
}
```

Provider adapters map the normalized outbound request into the provider-native API. Unsupported rich elements or resource types should return explicit errors instead of being silently dropped.

## Resources

Inbound files, images, audio, and video are normalized as sanitized `ResourceRef` values:

```json
{
  "id": "res_xxx",
  "provider": "lark",
  "connector": "main",
  "kind": "file",
  "name": "report.pdf",
  "internal_url": "internal://res_xxx",
  "mime": "application/pdf",
  "size_bytes": 12043,
  "sha256": "..."
}
```

Provider-private download URLs, keys, secrets, encrypted payload fields, and lookup metadata are removed from public events. Callers should store the sanitized `ResourceRef` and resolve `internal_url` through:

```text
GET /v1/internal/<id>
```

The Go client also exposes `ResolveInternalURL`.

## Go Client

```go
package main

import (
	"context"
	"log"
	"os"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/client"
)

func main() {
	ctx := context.Background()
	c := client.New("http://127.0.0.1:8787")
	c.Token = os.Getenv("UV_IM_AUTH_TOKEN")
	meta, err := c.ServiceMeta(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if meta.Service != uvim.ServiceName || meta.ProtocolVersion != uvim.ProtocolVersion {
		log.Fatalf("unsupported connector metadata: %+v", meta)
	}

	err = c.WatchEvents(ctx, 0, func(event uvim.Event) error {
		if event.Type != uvim.EventMessageCreate || !event.Addressed {
			return nil
		}
		_, err := c.Send(ctx, uvim.OutboundMessage{
			Provider:    event.Provider,
			Connector:   event.Connector,
			ChannelID:   event.Channel.ID,
			ChannelType: event.Channel.Type,
			Text:        "received",
			Referrer:    event.Referrer,
		})
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

## Webhook Providers

Webhook-capable providers receive inbound events at:

```text
POST /v1/webhook/{provider}/{connector}
```

Webhook authentication is provider-specific. For generic HTTP webhook providers, configure:

```bash
export UV_SLACK_WEBHOOK_SECRET=...
```

Then pass the secret in one of the accepted forms:

```text
X-UV-Webhook-Secret: <secret>
X-Webhook-Secret: <secret>
?secret=<secret>
```

Do not use the public API bearer token as a provider webhook secret.

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

## Development

Run the local test suite:

```bash
go test ./...
```

Provider quality expectations are documented in [docs/conformance.md](docs/conformance.md). Local and live E2E guidance is documented in [docs/e2e-tests.md](docs/e2e-tests.md).

Release and deployment guidance is documented in [docs/guide/deployment.md](docs/guide/deployment.md). Tag releases publish GitHub Release binaries and `ghcr.io/hengshi/uv-im-connector:<tag>` container images.

## Integration Notes

- Use `GET /v1/meta` at startup to verify service/protocol compatibility and record provider capabilities and connector IDs.
- Persist the last processed event `sequence` and resume `/v1/events/ws?after=<seq>` after reconnect.
- Route conversations by `provider + connector + channel.id`, not by `channel.id` alone.
- Treat `addressed=false` group events as ambient and non-actionable by default; route them into workflows only when the caller explicitly opts into ambient group traffic.
- Resolve and copy allowed `internal_url` resources into caller-owned storage before starting long-running work.
- Send replies through `POST /v1/message.create`; do not call provider-native send APIs from the caller application.
