# Configuration

The standalone binary reads `UV_IM_*` variables.

| Variable | Purpose |
| --- | --- |
| `UV_IM_ADDR` | Listen address. Defaults to `127.0.0.1:8787`. |
| `UV_IM_STATE_DIR` | Event log and resource directory. Defaults to `.uv-im-connector`. |
| `UV_IM_PROVIDERS` | Comma-separated provider list. Supported values: `memory`, `wecom`, `lark`, `dingtalk`, `discord`, `kook`, `line`, `mail`, `matrix`, `onebot`, `qq`, `qqguild`, `slack`, `telegram`, `wechat-official`, `whatsapp`, `zulip`. |
| `UV_IM_AUTH_TOKEN` | Optional bearer token required by all HTTP/WS endpoints except `/health`. |
| `UV_IM_USER_IDENTITIES_FILE` | Optional JSON file of [verified account bindings](./identities.md#verified-account-bindings), selected by exact provider, connector and native user_id. Adds identity metadata only; validated and read at startup. |
| `UV_WECOM_CONNECTOR_ID` | WeCom connector ID. Defaults to `wecom`. |
| `UV_WECOM_BOT_ID` | WeCom bot ID. |
| `UV_WECOM_BOT_SECRET` | WeCom bot secret. |
| `UV_WECOM_CORP_ID` | Optional enterprise ID from My Enterprise → Enterprise Information, not AgentId. Configure with `UV_WECOM_CORP_SECRET` to enable [enterprise identity lookup](./guide/wecom.md). |
| `UV_WECOM_CORP_SECRET` | The same enterprise's **self-built application Secret**, from Application Management → Self-built → application details, not the bot Secret. Token acquisition/refresh is automatic; application visibility and the server's trusted outbound IP are required. |
| `UV_WECOM_WS_URL` | Optional WeCom WebSocket endpoint override. |
| `UV_WECOM_USER_NAMES` | Optional JSON object mapping AI Bot callback `userid` values to display names, for example `{"zhangsan":"Zhang San"}`. It never changes routing or authorization. |
| `UV_WECOM_CONVERSATION_NAMES` | Optional JSON object mapping AI Bot group `chatid` values to display names, for example `{"wrxxxx":"Engineering"}`. It affects display metadata only. |
| `UV_LARK_CONNECTOR_ID` | Lark connector ID. Defaults to `lark`. |
| `UV_LARK_APP_ID` | Lark app ID. |
| `UV_LARK_APP_SECRET` | Lark app secret. |
| `UV_LARK_REGION` | `feishu` or `lark`. Defaults to `feishu`. |
| `UV_LARK_BOT_OPEN_ID` | Optional bot open ID for group mention admission (`addressed`) and mention stripping. An explicit value bypasses identity discovery. |
| `UV_LARK_BOT_UNION_ID` | Optional bot union ID for group mention admission and mention stripping. When both IDs are empty, startup discovers the bot open ID; lookup failure stops startup. |
| `UV_LARK_BASE_URL` | Optional OpenAPI base URL override. |
| `UV_LARK_CALLBACK_BASE_URL` | Optional callback WebSocket endpoint base URL override. |
| `UV_DINGTALK_CLIENT_ID` | DingTalk application Client ID. Must be configured with `UV_DINGTALK_CLIENT_SECRET`; enables Stream ingress without a public callback. |
| `UV_DINGTALK_CLIENT_SECRET` | DingTalk application Client Secret. Must be configured with `UV_DINGTALK_CLIENT_ID`. |
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

Provider credentials are exclusive deployment identity. Production, E2E, development, and temporary debug workers must not share the same provider credential set.

WeCom bot credentials and self-built application credentials have separate roles. Messaging alone does not require an application. Enterprise lookup is optional, and failures preserve the base event. See [WeCom setup and directory identities](./guide/wecom.md) for exact value locations, trusted IPs, environment injection, CLI identifier relationships and verification.

Lark identity discovery calls `GET /open-apis/bot/v3/info` with the existing application credentials before opening the WebSocket. Discovery and its token request share a 15-second startup deadline; a shorter caller context or HTTPClient timeout still applies. Success logs record the identity source and bot ID; failure logs retain safe stage, error-code or transport diagnostics.

Upgrade note: with both bot IDs empty, this API is a new startup prerequisite. Unknown identity does not fall back to receiving group messages. The shared supervisor retries that provider with backoff while the HTTP API and other providers remain available. To bypass identity discovery, set `UV_LARK_BOT_OPEN_ID` for the current application (or a verified union ID) and restart. This does not bypass credential validation or other startup APIs.

After acknowledging an inbound callback, Lark makes best-effort cached lookups for sender and group-chat names with the existing application credentials and preserves returned open_id, user_id and union_id. Basic user/chat OpenAPI permissions apply. Errors retain callback identities and omit lookup information. Lookups have no default timeout; cancellation needs caller context or an explicit HTTPClient timeout. A WeCom Bot secret alone provides no contact/chat-name lookup; display names can use explicit name maps. Enterprise application ID conversion requires a different token. See [platform identities, conversion APIs and permissions](./identities.md).

For the generic provider variables, replace `<PROVIDER>` with one of:

```text
DINGTALK DISCORD KOOK LINE MATRIX ONEBOT QQ QQGUILD SLACK TELEGRAM WECHAT_OFFICIAL WHATSAPP ZULIP
```

When `UV_IM_PROVIDERS` is empty, the binary auto-loads only providers with detected credentials or webhook configuration. `memory` is never auto-loaded in production mode.

DingTalk supports two ingress modes. A complete `UV_DINGTALK_CLIENT_ID` and `UV_DINGTALK_CLIENT_SECRET` pair enables Stream mode and downloads inbound image, file, voice, video, and rich-text resources that carry `downloadCode`; when both are absent, the existing webhook mode remains available and validates ingress with `UV_DINGTALK_WEBHOOK_SECRET`. Supplying only one Stream credential fails startup instead of silently falling back.

Application credentials also enable robot OpenAPI delivery to an explicit native user or group without a current session webhook. Images and files are uploaded with the same credentials; the standalone adapter shares the HTTP upload resource store. Audio and video are delivered as downloadable files rather than native players. Ordinary text replies can still use an unexpired inbound session webhook. Without application credentials, proactive direct and resource delivery are not advertised; `UV_DINGTALK_TOKEN` only enables legacy group-robot proactive messages. The application needs the corresponding robot messaging and media API permissions, and recipients must satisfy DingTalk application visibility and membership rules.

Connector-created HTTP clients have no total request timeout. WeCom/Lark WebSocket handshake, read/write/ACK timeouts and Lark chunk expiry are disabled by default; positive Go configuration values and caller-supplied clients remain explicit opt-ins. Heartbeat/ping scheduling and display-name cache eviction do not reject messages or attachments. Provide a cancellable context when embedding the connector.
