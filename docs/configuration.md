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
| `UV_LARK_CONNECTOR_ID` | Lark connector ID，默认 `lark`。 |
| `UV_LARK_APP_ID` | Lark app ID。 |
| `UV_LARK_APP_SECRET` | Lark app secret。 |
| `UV_LARK_REGION` | `feishu` 或 `lark`，默认 `feishu`。 |
| `UV_LARK_BOT_OPEN_ID` | 可选 bot open ID，用于 mention stripping。 |
| `UV_LARK_BOT_UNION_ID` | 可选 bot union ID，用于 mention stripping。 |
| `UV_LARK_BASE_URL` | 可选 OpenAPI base URL override。 |
| `UV_LARK_CALLBACK_BASE_URL` | 可选 callback WebSocket endpoint base URL override。 |
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

通用 provider 变量中的 `<PROVIDER>` 替换为以下值之一：

```text
DINGTALK DISCORD KOOK LINE MATRIX ONEBOT QQ QQGUILD SLACK TELEGRAM WECHAT_OFFICIAL WHATSAPP ZULIP
```

`UV_IM_PROVIDERS` 为空时，二进制只会自动加载检测到 credentials 或 webhook 配置的 provider。`memory` 不会在生产模式下自动加载。
