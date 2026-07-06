# 参与贡献

贡献代码时应保持 provider-neutral 协议边界。Provider 内部可以有平台差异，但调用方看到的模型必须稳定。

## 本地开发

运行 Go 测试：

```bash
go test ./...
```

运行文档站：

```bash
npm install
npm run docs:dev
```

构建文档站：

```bash
npm run docs:build
```

## 新增 Provider

Provider 必须实现根接口：

```go
type Provider interface {
    ID() string
    ConnectorID() string
    Capabilities() Capabilities
    Run(context.Context, EventSink) error
    Send(context.Context, OutboundMessage) (SendResult, error)
    Download(context.Context, ResourceDownloadRequest) (ResourceRef, error)
    Health(context.Context) Health
}
```

支持 webhook 的 provider 还应该实现 `WebhookProvider`。

## Provider 必需行为

每个 provider 应该：

- 如实声明 capabilities；
- 在平台支持时标准化私聊和群聊；
- 把平台文件、图片、音频、视频暴露为 `ResourceRef`；
- 声明支持 download 时，在事件持久化前把可下载资源解析为 `internal_url`；
- 确保 provider-native secrets 不出现在 sanitized events 和 resources 中；
- 平台提供 reply/thread 能力时，通过 `Referrer` 保留上下文；
- 对不支持的 outbound resource 或 rich element 返回显式错误。

## 测试期望

Provider 行为质量门见 [Conformance](/conformance)。

Provider 测试应覆盖：

- inbound text normalization；
- inbound group/channel normalization；
- 支持的 resource kind normalization；
- outbound direct message；
- 带 `Referrer` 的 outbound reply；
- resource download；
- provider health；
- duplicate event key stability。

需要 live credentials 的 provider 应把测试拆成始终可跑的本地 decoder/contract tests，以及显式凭证保护的 live tests。

## 文档期望

新增或修改 provider 时：

- 在 [配置](/configuration) 中更新环境变量；
- 如果协议边界变化，更新 [架构](/architecture)；
- 如果需要新的 live-provider 覆盖，更新 [E2E 测试](/e2e-tests)；
- 除非 provider-specific 行为会影响调用方策略，否则公开指南保持 provider-neutral。

