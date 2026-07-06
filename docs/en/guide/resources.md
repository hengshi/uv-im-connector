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

Providers that do not support a requested outbound resource kind should return explicit errors instead of silently dropping content.
