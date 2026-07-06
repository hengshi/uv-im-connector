# Conformance

Provider conformance 是新增渠道的质量门。

## 必需行为

每个 provider 必须：

- 实现根 `Provider` interface；
- 如实声明 capabilities；
- 不在 sanitized events 和 resources 中暴露 provider-native secrets；
- 声明支持 download 时，在 event persistence 前把可下载 inbound provider resources 解析为 `internal_url`；
- 当渠道提供文件 / 图片 / 音频 / 视频时，暴露为 `ResourceRef`；
- 当渠道提供 reply/thread 能力时，支持 `Referrer`；
- 对不支持的 outbound resources 或 rich elements 返回显式错误，而不是静默丢弃。

## 测试形态

Provider tests 应覆盖：

- inbound text message normalization；
- inbound group/channel message normalization；
- 支持的 resource kind inbound resource normalization；
- outbound direct message；
- 使用 `Referrer` 的 outbound reply；
- resource download；
- provider health；
- duplicate event key stability。

需要 live credentials 的 provider 应拆分为：

- 始终运行的 local decoder / contract tests；
- 受显式 test credentials 保护的 live provider tests。

## 当前本地覆盖

仓库当前包含这些本地测试：

- 所有内置 provider 的 provider metadata 和 health shape；
- DingTalk、Discord、KOOK、LINE、Mail、Matrix、OneBot、QQ、QQ Guild、Slack、Telegram、WeChat Official Account、WhatsApp 和 Zulip 的 inbound decoder normalization；
- WeCom 和 Lark provider-specific inbound/resource 行为；
- bearer token、bot token、URL token 和 form-encoded providers 的代表性 outbound HTTP shape；
- 从 `/v1/webhook/{provider}/{connector}` 到 normalized event log 的 webhook routing；
- event persistence 前的 resource download，以及 public resource sanitization。
