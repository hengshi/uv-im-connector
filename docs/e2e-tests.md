# E2E 测试

`uv-im-connector` 把 E2E 覆盖分为 local connector E2E 和 live-provider E2E。

## Local Connector E2E

这些测试不需要外部凭证：

- `server.TestHubEventsAndOutbound`：标准事件可以被存储，出站消息可以路由到 provider。
- `server.TestHubRoutesOutboundByConnector`：provider + connector 能选择准确的账号实例。
- `server.TestHubDownloadsInboundResourcesBeforeEventLog`：provider resources 在事件持久化前被解析。
- `server.TestHubWebhookRoutesToProviderAndStoresEvent`：provider webhook ingress 验证 provider secret 并写入标准事件。
- `server.TestClientResolveInternalURL`：调用方可以通过 connector HTTP API 解析 internal resources。

运行：

```bash
go test ./server
```

## Provider Contract E2E

这些测试不需要外部凭证，用于覆盖 provider protocol shape：

- `TestProviderMetadataForAllChannels`
- `TestProviderDecodersNormalizeInboundMessages`
- `TestSlackSendUsesBearerJSON`
- `TestDiscordSendUsesBotAuthorization`
- `TestTelegramSendUsesTokenPathWithoutBearer`
- `TestZulipSendUsesFormEncoding`

运行：

```bash
go test .
```

## Live Provider E2E

Live-provider tests 必须由显式 credentials 保护。每个 provider 应使用相同的场景名称补充：

- direct inbound text message；
- group/channel inbound text message；
- inbound file、image、audio 或 video resource；
- 使用 `Referrer` 的 outbound text reply；
- reconnect 或 webhook retry dedupe；
- persisted events 和 public API responses 中的 secret redaction。

Live-provider credentials 必须按环境隔离。Production credentials 不能复用于 E2E、development 或临时 debug worker。

Lark 原生文本编辑可使用已有 provider API 的显式探针。先为测试应用导出 `UV_LARK_APP_ID`、`UV_LARK_APP_SECRET` 和精确会话 `UV_LARK_LIVE_CHAT_ID`；可选区域、connector ID 和 API 地址沿用 `UV_LARK_REGION`、`UV_LARK_CONNECTOR_ID`、`UV_LARK_BASE_URL`。再执行：

```bash
UV_LARK_LIVE_UPDATE=1 go test ./providers/lark -run '^TestLiveTextUpdateRoundTrip$' -count=1 -v
```

默认测试跳过此探针。启用后，它通过真实 `Send`、`Update` 和原生 GET 核验同一 message ID、会话及替换全文，不调用 `Run` 或启动第二条 bot WebSocket 连接。探针留下单条编辑后的测试消息，并输出其 ID 供测试目标 owner 清理。这只证明原生文本创建/编辑/读回，不证明入站、被动流式回复或调用方任务生命周期。
