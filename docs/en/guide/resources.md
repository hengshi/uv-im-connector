# Resources

Inbound files, images, audio, and video are normalized into `ResourceRef` values.

The connector imposes no additional attachment size limit on inbound downloads, local storage, HTTP uploads, or outbound delivery. The IM provider or mail server enforces its size policy.

Group and direct attachments use the same download path, before event persistence. WeCom extracts files, images, videos, and quoted text from the message and its `quote`, including a group-file quote sent with an @mention of the bot. DingTalk exchanges inbound `downloadCode` values for temporary file URLs when Stream credentials are configured. Lark extracts images and videos from rich-text posts, and also extracts downloadable attachments from quoted parent messages. Lark `folder` messages are recognized as file resources, but the platform resource API does not guarantee that folders can be downloaded. Explicitly quoted `merge_forward` messages include attributable child text and supported image/file resources from the same response, including nested forwards. Downloads use the top-level container message ID. `interactive` cards still provide text or summary context without child resources. WeCom AI Bot callbacks have `addressed=true`; Lark group messages still derive that flag from bot mentions. Ingress processes only platform-delivered events; the native recent window is a separately authorized read. Caller applications perform content recognition.

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

Lark quoted-parent context, attachments and lookup-error resources carry optional `source_message_id` pointing to the explicitly quoted parent in the same event. It survives sanitization so callers can distinguish reference material from newly attached files without guessing from filenames. It does not prove a successful content lookup or authorize Task selection or continuation; callers still check `error` and their own ownership and authorization evidence. Do not fabricate lineage for historical events or adapters that omit the field. When the native lookup also verifies the parent sender as the configured app/app_id, resources include `source_bot_app_id`. Callers can use it to prevent an unindexed own-bot reply from falling into the current default Task; it does not recover Task ownership or grant access or execution. Other or unavailable senders omit it.

`message.elements` preserves `quote` and `children` hierarchy. Opaque resource IDs or unambiguous internal URLs associate leaves with downloaded resources; filenames, provider keys and private metadata are not relation keys. Missing or conflicting relations carry `resource_relation_unavailable`. The complete flat `message.resources` list remains available to resource-only consumers. Lark child IDs identify positions inside the forward, while every child resource retains the explicitly quoted top-level parent as `source_message_id`. WeCom inline quotes without a trustworthy parent ID preserve structure without inventing parent, thread or Task identity.

Lark `quoted-message.txt` lists every accepted root and forwarded node in native hierarchy: `Message ID`, `Message type`, `Sender`, the original millisecond `Create time` when present, an indented `Body`, and the node's own `Resource: ID/kind/name` entries. `Container message ID` names the selected top-level quote/download container; `Upper message ID` appears only when the platform supplied the immediate native relation. `Resource ID` is the sanitized protocol `ResourceRef.ID`, which can be correlated with resources and their internal URLs in the same event; equal filenames are not a relation key. A missing native sender tuple is shown as `unknown`, a missing time is not fabricated, and label values are escaped.

For an explicitly quoted Lark `post`, the body is quote-only rendering rather than ordinary inbound-post flattening. It selects a direct native body or one locale (`zh_cn`, then `en_us`, then `ja_jp`, then a sorted remaining locale) and labels a selected locale. Nonempty `content_v2` is rendered in preference to legacy `content`, with legacy and v2 native image/media references both retained once per kind/key. Title, adjacent text, links and their hrefs, stable mention ID plus supplied name, Markdown, bounded code with language, separators and emotion are preserved as reference material. Image/media use safe kind placeholders; structured resource keys and provider metadata maps are not rendered as body text. User-authored text, code, URLs and Markdown are retained, without blanket string-based redaction. Top-level post files become ordinary sanitized resources, while a post-zone folder is described with ZIP/individual-file guidance and is never fetched as a single resource. Unsupported or malformed post parts remain visible diagnostics, but diagnostics alone do not turn an empty quote into content. Styling, full raw-schema fidelity, and downloading media inferred from Markdown URLs are not provided. These facts apply to both root and forwarded quote elements, not to ordinary command decoding. They are descriptive reference material only: they do not recover a source Task, thread, user identity, or permission, and grant no access.

A missing Lark `upper_message_id` keeps the native CLI direct-child behavior. Explicit broken links, duplicate IDs, cycles, deleted items and conflicting chats are omitted with an unavailable indication; valid siblings remain usable. Downloads and relation handling follow the [official CLI resource contract](https://github.com/larksuite/cli/blob/05ac58ffa089fe7a171205ac1d1e97ee50196ec8/skills/lark-im/references/lark-im-message-enrichment.md) and [tree builder](https://github.com/larksuite/cli/blob/05ac58ffa089fe7a171205ac1d1e97ee50196ec8/shortcuts/im/convert_lib/merge.go). Quote handling itself does not traverse group history or grant source-system permissions. The separately authorized [recent conversation window](/en/architecture.html#recent-conversation-window) returns background media kind/name descriptors without downloading bytes; actual media still uses explicit quotes or current attachments.

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

In the standalone binary, WeCom, Lark / Feishu, DingTalk (with application credentials), Discord, KOOK, Telegram, Matrix, Slack, WhatsApp, Zulip, WeChat Official Account, and Mail share the HTTP upload resource store and declare `upload_resource=true`. See the [provider capability matrix](/en/architecture.html#provider-capability-matrix) for every provider.

- WeCom: WebSocket upload uses 512 KiB chunks, and the provider validates total size and chunk count.
- Lark / Feishu: Supported image formats use the image API regardless of size; other resources use file upload. The provider validates size.
- DingTalk: Application credentials upload media and send to an explicit user or group. Images use image messages; files, audio and video use downloadable file messages. A current session webhook is not required.
- Discord: Direct multipart message upload; the provider validates attachment size.
- KOOK: Asset upload followed by an image or attachment-card message.
- Telegram: Multipart Bot API upload; unsupported native formats fall back to documents.
- Matrix: Content-repository upload followed by an `mxc://` room message.
- Slack: External upload URL, raw upload, then completion and channel share.
- WhatsApp: Cloud API media upload followed by a message referencing the media ID.
- Zulip: Simple user upload followed by a Markdown attachment link.
- WeChat Official Account: Temporary-media upload plus customer-service send; no arbitrary file message.
- Mail: Sent as MIME attachments; the mail server validates size and count.

The remaining providers can currently receive and download resources but cannot send bytes from `internal://`; the matrix names the missing provider-native upload flow for each one. A send accepts text and multiple resources without a connector-imposed count limit. Discord, Mail, and Zulip combine them in one native message; the other upload-capable adapters send text first, then each resource in order (Slack also keeps its single-file caption form). Ordered sends return all IDs in `message_ids` and the last ID in `message_id`. On partial failure, `failure.delivered_count` and `failure.delivered_message_ids` identify completed messages; `retryable=false` and `delivery_state=unknown` prohibit replaying the whole sequence. The failed part may have an ambiguous outcome.

Providers that do not support a requested outbound resource kind should return explicit errors instead of silently dropping content.

The connector does not impose byte/count caps on resources, webhook bodies, Lark event assembly, event-log records, or provider-response reads. Empty resources reach the provider for validation. Text, safe filename components, and error metadata are not length-truncated. Authentication, path/header safety, supported message formats, and native protocol validation still apply. Memory, filesystem, standard-library transport, proxy, and provider constraints remain external boundaries.
