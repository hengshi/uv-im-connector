# Architecture

`uv-im-connector` is a channel-neutral IM connector. It owns provider authentication, inbound events, outbound messages, provider health, and resources. Callers own runtime-agent tasks, runs, workspaces, native resume handles, and writeback policy.

## Layers

```text
IM provider
  -> provider adapter
  -> normalized Event / ResourceRef
  -> event log + HTTP/WS API
  -> caller runtime

caller runtime
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
- `OutboundMessage`: channel, text/elements/resources, and optional referrer for replies or threads.
- `Capabilities`: explicit feature declaration for each provider.

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
| `GET /v1/meta` | Provider list, capabilities, and health. |
| `GET /v1/events?after=<seq>` | Read normalized event log. |
| `GET /v1/events/ws?after=<seq>` | Watch normalized events. |
| `POST /v1/message.create` | Send outbound message. |
| `POST /v1/upload.create` | Create internal resource from local bytes. |
| `POST /v1/resource.download` | Trusted request to resolve a provider-private resource into an internal resource. |
| `POST /v1/webhook/{provider}/{connector}` | Provider webhook ingress. Webhook verification is owned by the provider adapter. |
| `GET /v1/internal/<id>` | Resolve internal resource. |

All endpoints except `/health` and provider webhook ingress require `Authorization: Bearer <UV_IM_AUTH_TOKEN>` when `UV_IM_AUTH_TOKEN` is configured. Provider webhook ingress must use provider-level webhook verification before emitting normalized events, and webhook-capable providers reject ingress when no provider webhook secret is configured.

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
