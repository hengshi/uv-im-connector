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

Provider 不支持某种 outbound resource 时，应该返回显式错误，而不是静默丢弃内容。
