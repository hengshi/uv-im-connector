# 快速开始

本页会启动一个本地 connector，验证 HTTP API，并通过内存 provider 发送第一条出站消息。

## 安装

安装独立二进制：

```bash
go install github.com/hengshi/uv-im-connector/cmd/uv-im-connector@<tag>
```

也可以在仓库 checkout 内直接运行：

```bash
go run ./cmd/uv-im-connector
```

如果要嵌入到另一个 Go 进程中，直接使用 Go 包：

```bash
go get github.com/hengshi/uv-im-connector@<tag>
```

也可以使用发布的容器镜像：

```bash
docker run --rm \
  -p 127.0.0.1:8787:8787 \
  -v uv-im-connector-state:/var/lib/uv-im-connector \
  -e UV_IM_AUTH_TOKEN=dev-token \
  -e UV_IM_PROVIDERS=memory \
  ghcr.io/hengshi/uv-im-connector:<tag>
```

## 启动本地 Connector

```bash
export UV_IM_AUTH_TOKEN=dev-token
export UV_IM_PROVIDERS=memory
uv-im-connector
```

默认监听地址是 `127.0.0.1:8787`。

```bash
curl http://127.0.0.1:8787/health
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:8787/v1/meta
```

生产部署必须设置 `UV_IM_AUTH_TOKEN`，并把监听地址放在私有网络或受鉴权的反向代理之后。`UV_IM_AUTH_TOKEN` 为空时，公开 HTTP 和 WebSocket 端点不做鉴权。

## 发送一条消息

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

响应是标准化的发送结果：

```json
{
  "provider": "memory",
  "connector": "memory",
  "message_id": "mem-msg_xxx",
  "time": "2026-01-01T00:00:00Z"
}
```

## 读取或订阅事件

读取已持久化的标准事件：

```bash
curl -H "Authorization: Bearer dev-token" \
  "http://127.0.0.1:8787/v1/events?after=0"
```

通过 WebSocket 订阅事件：

```bash
websocat -H "Authorization: Bearer dev-token" \
  "ws://127.0.0.1:8787/v1/events/ws?after=0"
```

调用方应该持久化最后处理过的 event `sequence`，并在重连时使用 `/v1/events/ws?after=<seq>` 恢复。

## 启用真实 Provider

WeCom 和 Lark 使用各自平台的专用凭证：

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

HTTP webhook 类 provider 使用 [配置](/configuration) 中描述的通用形式：

```text
UV_<PROVIDER>_CONNECTOR_ID
UV_<PROVIDER>_BASE_URL
UV_<PROVIDER>_TOKEN
UV_<PROVIDER>_WEBHOOK_SECRET
```

## 下一步

- 阅读 [为什么存在](/guide/why-uv)，理解为什么 `uv-im-connector` 要把 IM connector 从上层产品里抽出来。
- 阅读 [核心概念](/guide/concepts)，理解 provider、connector、channel、addressed、referrer 和 resource。
- 接入应用、机器人或 agent service 前，阅读 [应用接入](/guide/application-integration)。
- 部署独立服务前，阅读 [部署与发布](/guide/deployment)。
- 接收用户文件前，阅读 [资源与文件](/guide/resources)。
- 新增 provider 前，阅读 [参与贡献](/guide/contributing)。
