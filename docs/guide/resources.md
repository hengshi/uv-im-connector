# 资源与文件

入站文件、图片、音频和视频会被标准化为 `ResourceRef`。

## 公开形态

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

公开事件会移除 provider-private 字段：

- 临时下载 URL；
- 加密 payload key；
- 不应暴露的 provider resource ID；
- webhook secret；
- provider 原始 payload metadata。

## 解析内部资源

通过 connector HTTP API 解析 internal URL：

```text
GET /v1/internal/<id>
```

Go client 暴露同等能力：

```go
resp, err := c.ResolveInternalURL(ctx, event.Message.Resources[0].InternalURL)
```

调用方应该在启动长耗时任务前，把允许的文件复制到调用方自己的存储。Connector 的 resource store 是基础设施状态，不是调用方应用的 artifact store。

## 显式下载 Provider 资源

可信调用方可以请求 provider adapter 把 provider-private resource 解析为内部资源：

```text
POST /v1/resource.download
```

请求使用 `ResourceDownloadRequest`，返回 sanitized `ResourceRef`。

## 上传本地字节

使用 `POST /v1/upload.create` 从本地字节创建内部资源，再交给支持 outbound resource 的 provider 发送。

```json
{
  "kind": "file",
  "name": "report.txt",
  "mime": "text/plain",
  "content_base64": "..."
}
```

发送前必须从 `GET /v1/meta` 校验目标 provider + connector 的 `upload_resource` 和 `resource_kinds`，并把 `upload.create` 返回的完整 `ResourceRef` 原样放进 `OutboundMessage.resources`。不能自行拼装 `internal_url`，也不能拿另一个 uv-im-connector 进程的内部 URL 来用。

standalone binary 中，WeCom、Lark / Feishu、Discord、KOOK、Telegram、Matrix、Slack、WhatsApp、Zulip、WeChat Official Account 和 Mail 与 HTTP upload endpoint 使用同一个 resource store，并真正声明 `upload_resource=true`。完整逐 provider 清单见 [Provider 能力矩阵](/architecture.html#provider-能力矩阵)。

- WeCom：每条 outbound message 只接受一个 resource，且 resource 不能与 text 混发；单资源最多 100 个 512 KiB 原始分片，约 50 MiB。
- Lark / Feishu：原生图片上限 10 MiB；其他 kind 作为文件附件交付，上限 30 MiB。
- Discord：资源随消息直接 multipart 上传，默认单附件上限 10 MiB。
- KOOK：先上传 asset，再发送图片消息或附件卡片；adapter 上限 100 MiB，平台策略可能更低。
- Telegram：原生图片上限 10 MiB，其他文件上限 50 MiB；不符合原生 audio/video/image 格式的内容降级为 document。
- Matrix：先上传 content repository，再用 `mxc://` URI 发送 room message；adapter 上限 100 MiB，homeserver 可能配置更低上限。
- Slack：先申请 external upload URL、上传原始字节，再 complete 并分享到 channel；adapter 上限 100 MiB，workspace 策略可能更低。
- WhatsApp：先上传 Cloud API media，再引用 media ID 发消息；图片 5 MiB，音频 / 视频 16 MiB，文档 100 MiB。
- Zulip：simple user upload 后发送 Markdown 附件链接；adapter 上限 25 MiB，server 策略可能更低。
- WeChat Official Account：仅支持图片、语音和视频临时素材，不支持任意文件；图片 / 视频 10 MiB，语音 2 MiB。
- Mail：支持同一封邮件带 text 和多个 MIME 附件，最多 10 个、合计 25 MiB。

其余 provider 当前只能接收 / 下载资源，不能从 `internal://` 直接发送；矩阵逐项记录了缺失的 provider-native 上传环节。多个附件及最终文本是否可混发取决于 provider；provider-neutral 调用方可以按顺序拆成一资源一消息，再发送最终文本。

Provider 不支持某种 outbound resource 时，应该返回显式错误，而不是静默丢弃内容。
