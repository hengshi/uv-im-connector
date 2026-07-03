# Configuration

The standalone binary reads `UV_IM_*` variables.

| Variable | Purpose |
| --- | --- |
| `UV_IM_ADDR` | Listen address. Defaults to `127.0.0.1:8787`. |
| `UV_IM_STATE_DIR` | Event log and resource directory. Defaults to `.uv-im-connector`. |
| `UV_IM_PROVIDERS` | Comma-separated provider list. Supported values: `memory`, `wecom`, `lark`, `dingtalk`, `discord`, `kook`, `line`, `mail`, `matrix`, `onebot`, `qq`, `qqguild`, `slack`, `telegram`, `wechat-official`, `whatsapp`, `zulip`. |
| `UV_IM_AUTH_TOKEN` | Optional bearer token required by all HTTP/WS endpoints except `/health`. |
| `UV_WECOM_CONNECTOR_ID` | WeCom connector ID. Defaults to `wecom`. |
| `UV_WECOM_BOT_ID` | WeCom bot ID. |
| `UV_WECOM_BOT_SECRET` | WeCom bot secret. |
| `UV_WECOM_WS_URL` | Optional WeCom WebSocket endpoint override. |
| `UV_LARK_CONNECTOR_ID` | Lark connector ID. Defaults to `lark`. |
| `UV_LARK_APP_ID` | Lark app ID. |
| `UV_LARK_APP_SECRET` | Lark app secret. |
| `UV_LARK_REGION` | `feishu` or `lark`. Defaults to `feishu`. |
| `UV_LARK_BOT_OPEN_ID` | Optional bot open ID for mention stripping. |
| `UV_LARK_BOT_UNION_ID` | Optional bot union ID for mention stripping. |
| `UV_LARK_BASE_URL` | Optional OpenAPI base URL override. |
| `UV_LARK_CALLBACK_BASE_URL` | Optional callback WebSocket endpoint base URL override. |
| `UV_<PROVIDER>_CONNECTOR_ID` | Connector ID for HTTP/webhook providers. Defaults to the provider ID. |
| `UV_<PROVIDER>_BASE_URL` | Provider API base URL override. |
| `UV_<PROVIDER>_TOKEN` | Provider API token. If the value starts with `Bearer `, `Bot `, or `Basic ` it is used as the full Authorization value. Otherwise it is sent as bearer token when the provider requires Authorization headers. |
| `UV_<PROVIDER>_WEBHOOK_SECRET` | Required shared secret for provider webhooks. Accepted on `X-UV-Webhook-Secret`, `X-Webhook-Secret`, or `?secret=`. Webhook requests are rejected when this is not configured. |
| `UV_WHATSAPP_PHONE_NUMBER_ID` | WhatsApp sender phone number ID used by outbound messages. |
| `UV_MAIL_SMTP_ADDR` | Mail outbound SMTP address, for example `smtp.example.com:587`. |
| `UV_MAIL_SMTP_USERNAME` | Mail outbound SMTP username. |
| `UV_MAIL_SMTP_PASSWORD` | Mail outbound SMTP password. |
| `UV_MAIL_FROM` | Mail outbound sender address. Defaults to `UV_MAIL_SMTP_USERNAME`. |
| `UV_MAIL_WEBHOOK_SECRET` | Mail inbound webhook secret. |

Provider credentials are exclusive runtime identity. Production, E2E, development, and temporary debug workers must not share the same provider credential set.

For the generic provider variables, replace `<PROVIDER>` with one of:

```text
DINGTALK DISCORD KOOK LINE MATRIX ONEBOT QQ QQGUILD SLACK TELEGRAM WECHAT_OFFICIAL WHATSAPP ZULIP
```

When `UV_IM_PROVIDERS` is empty, the binary auto-loads only providers with detected credentials or webhook configuration. `memory` is never auto-loaded in production mode.
