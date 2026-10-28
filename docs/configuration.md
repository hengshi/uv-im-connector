# 配置

独立二进制读取 `UV_IM_*` 环境变量。

| 变量 | 用途 |
| --- | --- |
| `UV_IM_ADDR` | 监听地址，默认 `127.0.0.1:8787`。 |
| `UV_IM_STATE_DIR` | 事件日志和资源目录，默认 `.uv-im-connector`。 |
| `UV_IM_PROVIDERS` | 逗号分隔的 provider 列表。支持值：`memory`、`wecom`、`lark`、`dingtalk`、`discord`、`kook`、`line`、`mail`、`matrix`、`onebot`、`qq`、`qqguild`、`slack`、`telegram`、`wechat-official`、`whatsapp`、`zulip`。 |
| `UV_IM_AUTH_TOKEN` | 可选 bearer token。配置后，除 `/health` 外所有公共 HTTP/WS endpoint 都需要该 token。 |
| `UV_WECOM_CONNECTOR_ID` | WeCom connector ID，默认 `wecom`。 |
| `UV_WECOM_BOT_ID` | WeCom bot ID。 |
| `UV_WECOM_BOT_SECRET` | WeCom bot secret。 |
| `UV_WECOM_WS_URL` | 可选 WeCom WebSocket endpoint override。 |
| `UV_WECOM_USER_NAMES` | 可选 JSON 对象，把 AI Bot 回调中的 `userid` 映射为展示名，例如 `{"zhangsan":"张三"}`。只影响事件展示字段，不影响路由或授权。 |
| `UV_WECOM_CONVERSATION_NAMES` | 可选 JSON 对象，把 AI Bot 回调中的群 `chatid` 映射为展示名，例如 `{"wrxxxx":"研发群"}`。只影响事件展示字段。 |
| `UV_WECOM_USER_EXTERNAL_IDS` | 可选 JSON 对象，把 AI Bot 回调中的 `userid` 映射为企业通讯录 ID，例如 `{"CaoGuLiang":"enterprise-user-id"}`。 |
| `UV_WECOM_CONVERSATION_EXTERNAL_IDS` | 可选 JSON 对象，把 AI Bot 回调中的 `chatid` 映射为外部会话 ID，例如 `{"wrxxxx":"external-conversation-id"}`。 |
| `UV_LARK_CONNECTOR_ID` | Lark connector ID，默认 `lark`。 |
| `UV_LARK_APP_ID` | Lark app ID。 |
| `UV_LARK_APP_SECRET` | Lark app secret。 |
| `UV_LARK_REGION` | `feishu` 或 `lark`，默认 `feishu`。 |
| `UV_LARK_BOT_OPEN_ID` | 可选 bot open ID，用于识别群消息是否 @ 本机器人（`addressed`）及移除 mention 文本。显式配置会跳过身份自动查询。 |
| `UV_LARK_BOT_UNION_ID` | 可选 bot union ID，同样用于群 @ 识别及移除 mention 文本。两个 ID 都留空时，启动时自动查询 bot open ID；查询失败会停止启动。 |
| `UV_LARK_BASE_URL` | 可选 OpenAPI base URL override。 |
| `UV_LARK_CALLBACK_BASE_URL` | 可选 callback WebSocket endpoint base URL override。 |
| `UV_DINGTALK_CLIENT_ID` | DingTalk 应用 Client ID。必须与 `UV_DINGTALK_CLIENT_SECRET` 同时配置；配置后使用 Stream 模式接收入站消息，不需要公网 callback。 |
| `UV_DINGTALK_CLIENT_SECRET` | DingTalk 应用 Client Secret。必须与 `UV_DINGTALK_CLIENT_ID` 同时配置。 |
| `UV_<PROVIDER>_CONNECTOR_ID` | HTTP/webhook 类 provider 的 connector ID，默认 provider ID。 |
| `UV_<PROVIDER>_BASE_URL` | Provider API base URL override。 |
| `UV_<PROVIDER>_TOKEN` | Provider API token。值以 `Bearer `、`Bot ` 或 `Basic ` 开头时，会原样作为 Authorization；否则当 provider 需要 Authorization header 时作为 bearer token 发送。 |
| `UV_<PROVIDER>_WEBHOOK_SECRET` | Provider webhook shared secret。可通过 `X-UV-Webhook-Secret`、`X-Webhook-Secret` 或 `?secret=` 传入。未配置时 webhook 请求会被拒绝。 |
| `UV_WHATSAPP_PHONE_NUMBER_ID` | WhatsApp outbound sender phone number ID。 |
| `UV_MAIL_SMTP_ADDR` | Mail outbound SMTP 地址，例如 `smtp.example.com:587`。 |
| `UV_MAIL_SMTP_USERNAME` | Mail outbound SMTP 用户名。 |
| `UV_MAIL_SMTP_PASSWORD` | Mail outbound SMTP 密码。 |
| `UV_MAIL_FROM` | Mail outbound 发件人地址，默认 `UV_MAIL_SMTP_USERNAME`。 |
| `UV_MAIL_WEBHOOK_SECRET` | Mail inbound webhook secret。 |

Provider credentials 是独占 deployment identity。Production、E2E、development 和临时 debug worker 不应共用同一套 provider credentials。

Lark 身份自动查询通过现有应用凭据调用 `GET /open-apis/bot/v3/info`，在打开 WebSocket 前完成；身份查询及所需 token 请求共用 15 秒启动时限（调用方更短的 context 或 HTTPClient 超时仍生效）。成功日志记录身份来源和 bot ID，失败日志保留安全的阶段、错误码或传输原因。

升级注意：两个 bot ID 都留空时，该 API 是新的启动前置依赖。身份无法确认时不会降级接收群消息；共享监督器会退避重试该 provider，HTTP API 和其他 provider 保持运行。若需跳过身份查询，可设置与当前应用对应的 `UV_LARK_BOT_OPEN_ID`（或已核实的 union ID），再重启服务；这不会绕过应用凭据校验或其他启动 API。

Lark 入站事件会在确认回调后，使用现有应用凭据对发送人和群聊名称做可失败的缓存查询。应用需要具备读取用户基本信息和群聊信息的 OpenAPI 权限；权限缺失或 API 返回错误时，事件省略名称；查询没有默认超时，无响应的请求需由调用方 context 或显式 HTTPClient 超时取消。企业微信 AI Bot 长连接回调及其 Bot secret 不提供通讯录或群聊名称查询能力，因此需要名称时使用上面的显式映射；未配置时保留 provider-native ID。

通用 provider 变量中的 `<PROVIDER>` 替换为以下值之一：

```text
DINGTALK DISCORD KOOK LINE MATRIX ONEBOT QQ QQGUILD SLACK TELEGRAM WECHAT_OFFICIAL WHATSAPP ZULIP
```

`UV_IM_PROVIDERS` 为空时，二进制只会自动加载检测到 credentials 或 webhook 配置的 provider。`memory` 不会在生产模式下自动加载。

DingTalk 有两种入站模式。配置完整的 `UV_DINGTALK_CLIENT_ID` 和 `UV_DINGTALK_CLIENT_SECRET` 时使用 Stream 模式，并可下载含 `downloadCode` 的入站图片、文件、语音、视频和富文本资源；两者都不配置时保留原有 webhook 模式，并由 `UV_DINGTALK_WEBHOOK_SECRET` 验证入站请求。只配置其中一个会直接启动失败，不能静默降级。

配置应用凭据后，可通过机器人 OpenAPI 向明确的原生用户或群目标发送消息，无需有效的 session webhook；图片和文件使用同一凭据上传后发送，standalone 与 HTTP upload endpoint 共用资源存储。音频和视频作为可下载文件交付，不模拟原生播放器。普通文本回复仍可复用未过期的入站 session webhook。未配置应用凭据时不声明主动单聊或附件发送；`UV_DINGTALK_TOKEN` 仅支持已配置的旧群机器人主动群消息。应用需具有相应的机器人发消息和媒体 API 权限，目标用户/群须满足钉钉应用范围要求。

connector 创建的 HTTP client 不设置请求总超时。企微/飞书 WebSocket 握手、读写/ACK 超时以及飞书分片过期默认关闭；Go 调用方显式传入的正值配置和自定义 client 仍生效。心跳/ping 周期和展示名缓存淘汰不会拒绝消息或附件。嵌入使用时应传入可取消的 context。

## WeCom 外部身份

AI Bot 回调中的 `userid` 可能与企业通讯录 ID 不同。部署方可以通过 `UV_WECOM_USER_EXTERNAL_IDS` 和 `UV_WECOM_CONVERSATION_EXTERNAL_IDS` 提供显式映射；嵌入 Go 程序时使用相同的配置字段：

```go
provider, err := wecom.New(wecom.Config{
    BotID: "bot-id",
    Secret: "bot-secret",
    UserExternalIDs: map[string]string{
        "CaoGuLiang": "enterprise-user-id",
    },
    ConversationExternalIDs: map[string]string{
        "wrxxxx": "external-conversation-id",
    },
})
```

用户映射填入 `user.external_id`；会话映射填入 `channel.external_id` 和 `referrer.target.external_id`。单聊没有会话映射时，后两者回退到用户的外部身份；群聊只使用会话映射。没有配置或没有匹配项时，对应的 `external_id` 保持为空并从 JSON 省略。原生 `id` 始终保留，connector 仍使用它进行路由和去重；映射不会自动查询通讯录，也不会替换发送目标 ID。

环境变量必须是 ID 到字符串的 JSON 对象；键和值会去除首尾空白，空条目被忽略。无效 JSON、非字符串值或 `null` 会导致启动报错。
