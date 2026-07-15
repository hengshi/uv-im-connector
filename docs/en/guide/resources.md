# Resources

Inbound files, images, audio, and video are normalized into `ResourceRef` values.

## Public Shape

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

Provider-private fields are removed from public events:

- temporary download URLs;
- encrypted payload keys;
- provider resource IDs that should not be exposed;
- webhook secrets;
- raw provider payload metadata.

## Resolve an Internal Resource

Use the internal URL through the connector HTTP API:

```text
GET /v1/internal/<id>
```

The Go client exposes the same operation:

```go
resp, err := c.ResolveInternalURL(ctx, event.Message.Resources[0].InternalURL)
```

Callers should copy allowed files into caller-owned storage before starting long-running work. The connector resource store is infrastructure state, not the caller application's artifact store.

## Explicit Provider Download

Trusted callers can ask the provider adapter to resolve a provider-private resource:

```text
POST /v1/resource.download
```

The request uses `ResourceDownloadRequest` and returns a sanitized `ResourceRef`.

## Upload Local Bytes

Use `POST /v1/upload.create` to create an internal resource from local bytes before sending it through a provider that supports outbound resources.

```json
{
  "kind": "file",
  "name": "report.txt",
  "mime": "text/plain",
  "content_base64": "..."
}
```

Before sending, inspect the exact provider and connector in `GET /v1/meta`, require `upload_resource` and the desired `resource_kinds`, and pass the complete `ResourceRef` returned by `upload.create` in `OutboundMessage.resources`. Do not construct an `internal_url` or reuse one from another uv-im-connector process.

In the standalone binary, WeCom, Lark / Feishu, Discord, KOOK, Telegram, Matrix, Slack, WhatsApp, Zulip, WeChat Official Account, and Mail share the HTTP upload resource store and declare `upload_resource=true`. See the [provider capability matrix](/en/architecture.html#provider-capability-matrix) for every provider.

- WeCom: one resource per outbound message with no mixed text; up to 100 raw 512 KiB chunks, about 50 MiB.
- Lark / Feishu: native images up to 10 MiB; other kinds are delivered as file attachments up to 30 MiB.
- Discord: direct multipart upload with a default 10 MiB attachment limit.
- KOOK: asset upload followed by an image or attachment-card message; adapter cap 100 MiB, with a possibly lower platform policy.
- Telegram: native photos up to 10 MiB and other files up to 50 MiB; unsupported native media formats fall back to documents.
- Matrix: content-repository upload followed by an `mxc://` room message; adapter cap 100 MiB, with a possibly lower homeserver limit.
- Slack: external upload URL, raw byte upload, then completion and channel share; adapter cap 100 MiB, with a possibly lower workspace policy.
- WhatsApp: Cloud API media upload followed by a message referencing the media ID; images 5 MiB, audio/video 16 MiB, documents 100 MiB.
- Zulip: simple user upload followed by a Markdown attachment link; adapter cap 25 MiB, with a possibly lower server policy.
- WeChat Official Account: temporary image, voice, and video media only, with no arbitrary file message; images/video 10 MiB, voice 2 MiB.
- Mail: text and up to 10 MIME attachments in one message, with a 25 MiB raw attachment total.

The remaining providers can currently receive and download resources but cannot send bytes from `internal://`; the matrix names the missing provider-native upload flow for each one. Whether text and multiple attachments can be combined is provider-specific. A provider-neutral caller can send one resource per message in order, followed by final text.

Providers that do not support a requested outbound resource kind should return explicit errors instead of silently dropping content.
