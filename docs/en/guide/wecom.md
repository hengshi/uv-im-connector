# WeCom setup and directory identities

Receiving and replying to WeCom messages requires the smart bot's BotID and Secret. To identify the enterprise member behind a sender, optionally configure an enterprise ID and a self-built application's Secret from the same enterprise. The connector obtains, caches and refreshes the application token and calls the official identity APIs. The two Secrets serve different purposes.

## Choose the capability

| Goal | Required configuration |
| --- | --- |
| Receive messages, stream replies and send proactively | BotID and the bot's Secret. |
| Add a verified enterprise account and available member name to events | Enterprise ID, self-built application Secret, application visibility and the server's trusted outbound IP. |
| Use wecom-cli for documents, calendars and other office operations | Follow the CLI's separate authorization flow. It is not a prerequisite for connector messaging or enterprise account lookup. |

## Where each value comes from

| UI field | Where to find it | Connector variable | Purpose |
| --- | --- | --- | --- |
| **BotID** | WeCom client → Workbench → Smart Bots → the bot's API-mode long-connection settings. | `UV_WECOM_BOT_ID` | Opens the bot message connection. |
| **Bot Secret** | The same bot long-connection settings page. | `UV_WECOM_BOT_SECRET` | Authenticates that bot connection. |
| **Enterprise ID / CorpID** | WeCom admin console → **My Enterprise → Enterprise Information → Enterprise ID**; requires administrator access. | `UV_WECOM_CORP_ID` | Used with the application Secret to obtain a token. |
| **Self-built application Secret** | Admin console → **Application Management → Self-built → select the application → Secret**. Create and enable an application if necessary. | `UV_WECOM_CORP_SECRET` | Authorizes the application's identity conversion and member queries. |
| **AgentId** | The same self-built application's detail page. This is the application number. | **Not required for this flow.** | Neither token acquisition nor this smart-bot conversion variant uses it. |
| **access_token** | Obtained programmatically using the enterprise ID and self-built application Secret. | **No manual value required.** | Temporary credential, cached and refreshed by the connector. |

CorpID identifies the enterprise; AgentId identifies an application. They are not interchangeable. `UV_WECOM_CORP_SECRET` is the **self-built application's Secret**, not the bot Secret. Official references: [field definitions](https://developer.work.weixin.qq.com/document/path/90665), [token acquisition](https://developer.work.weixin.qq.com/document/path/91039), [bot long connection](https://developer.work.weixin.qq.com/document/path/101463).

`UV_IM_AUTH_TOKEN` is a separate password chosen by the deployment operator for the connector HTTP API. It is unrelated to either WeCom Secret or WeCom's application token.

## Configure the self-built application

1. Use an administrator account in the **bot's enterprise** to create or select an enabled self-built application. Obtain the enterprise ID and that application's Secret. Credentials from another enterprise are not valid for this bot.
2. Add the relevant members or departments to the application's **visibility scope**, including group-chat senders who need identification. Being able to message the bot does not imply application visibility.
3. In the application's **Enterprise Trusted IP** settings, add the **public outbound IP used by the connector server when calling WeCom APIs**. This is neither its private LAN address nor the operator laptop's current public IP. Account for NAT, proxies and container egress.
4. Inject the following variables into the connector process and restart or recreate that service using its deployment procedure. Set both application variables together, or leave both empty.

```dotenv
UV_IM_PROVIDERS=wecom
UV_WECOM_BOT_ID=your-bot-id
UV_WECOM_BOT_SECRET=your-bot-secret
UV_WECOM_CORP_ID=your-enterprise-id
UV_WECOM_CORP_SECRET=your-self-built-application-secret
```

The standalone binary reads **process environment variables**. It does not scan secret directories or automatically load arbitrary `.env` files. Saving credentials in a file alone does not activate them. Use the service manager or your existing secret manager to inject the environment:

- Repository Compose example: configure the project `.env` and follow [Docker deployment](./deployment.md#docker); `deploy/compose.yaml` forwards the values into the container.
- Repository systemd example: fill `/etc/uv-im-connector/uv-im-connector.env`, the file named by the unit's `EnvironmentFile`.
- Other deployments: update the environment configuration actually loaded by that service.

Restrict secret-file access to the service account. Do not commit Secrets, tokens or real member lists. Trusted IPs should cover the service's stable egress; `60020` on another computer does not establish that the server's configuration is wrong.

Embedded Go applications use `wecom.Config{BotID: ..., Secret: ..., CorpID: ..., CorpSecret: ...}`. Omitting the last two fields disables enterprise identity lookup.

## How sender verification works

The callback's `from.userid` can already be a plaintext enterprise account or can be an enterprise-encrypted identifier. The official contract specifies plaintext when an enterprise super administrator created the bot; other creators receive encrypted identifiers. **Length, pinyin-looking text and a `wo` prefix are not identifier-type tests.**

With application credentials configured, the connector performs these steps before publishing an event:

1. Obtain or reuse the application token; refresh it when expired or rejected early by the platform.
2. Submit the callback sender to the [official conversion API](https://developer.work.weixin.qq.com/document/path/101521). A matching conversion establishes the enterprise member account.
3. If the input is in `invalid_open_userid_list`, call [get member](https://developer.work.weixin.qq.com/document/path/90196) with the original identifier. **Only a successful matching member lookup proves that it was already plaintext.** Invalid conversion alone is insufficient. The member API compares account names without case sensitivity; the connector preserves the returned spelling.
4. Add `wecom.userid` to `user.identities`, with the configured enterprise ID as `scope` and `lookup` as `source`. Fill `user.display_name` only when a name is available and the event has no existing name.

```json
{
  "user": {
    "id": "callback-sender",
    "display_name": "Example Member",
    "identities": [
      {"type": "wecom.callback_userid", "id": "callback-sender", "source": "callback"},
      {"type": "wecom.userid", "id": "enterprise-account", "scope": "your-enterprise-id", "source": "lookup"}
    ]
  }
}
```

Both identities can contain the same ID when the callback is already plaintext. The primary `user.id`, channel, complete reply address and existing callback/configured display names retain their semantics. Enterprise identity metadata does not change native delivery addresses or grant business permissions.

Lookup has a five-second total deadline and runs in the event-emission worker without blocking WebSocket ACK dispatch. Tokens follow their returned lifetime. The member cache holds at most 1,024 entries: ten minutes for success, thirty seconds for failures or partial failures. IP, permission, network and missing-member failures produce safe diagnostics while the original message remains available. An account already proven by conversion survives a failed display-name lookup.

## Matching wecom-cli directory identifiers

CLI contact search and bot callbacks can use different identifiers. **A CLI search result's `userid` is not automatically a plaintext enterprise account, and a short callback account is not evidence of missing identity.**

Callers that need to associate a CLI candidate with a sender should convert that candidate through the same enterprise's official API, then compare it with the event's `wecom.userid` in the same enterprise `scope`. Name search discovers candidates; it does not prove account identity.

```text
CLI encrypted candidate -> official conversion -> enterprise account
                                                       ^
message sender -> connector verification -> wecom.userid
```

The conversion API provides the encrypted-to-enterprise direction. Do not assume it can reverse an enterprise account into a CLI identifier. The connector neither invokes wecom-cli nor asks every sender to log into the CLI. CLI bot binding and human authorization for office operations follow its [own official flow](https://github.com/WecomTeam/wecom-cli/blob/main/docs/cli-reference.md).

## Verification and troubleshooting

Activate the configuration using the [deployment procedure](./deployment.md), send a new message to the bot and inspect `GET /v1/events?after=<sequence>`. The new event should include an enterprise-scoped `wecom.userid`; name availability depends on actual application access. `/health` and a connected WebSocket do not establish successful optional directory lookup. Replaying historical events does not recalculate identities.

| Symptom | Action |
| --- | --- |
| Startup reports `corp_id and corp_secret must be configured together` | Complete both application values, or clear both. |
| `gettoken` fails | Check enterprise ID, application Secret and enabled status. Do not substitute AgentId or the bot Secret. |
| Logs show `errcode=60020` | Add the **running server's** actual public egress to the application's trusted IPs. Token acquisition can succeed while later calls are denied by IP restrictions. |
| Member lookup fails or enterprise identity is absent | Check application visibility and member-read permission. The base event preserves its original identity when verification fails. |
| Conversion reports invalid | The input might already be plaintext or might be invalid. Require successful member lookup before treating it as plaintext. |
| Old results remain after changing settings | Confirm the service loaded its new configuration and inspect a new event. Failure cache lasts up to 30 seconds; success cache up to 10 minutes. Restart clears both caches. |
| A manually verified association is temporarily needed | Use the shared [verified account binding file](../identities.md#verified-account-bindings), with the intended namespace and enterprise scope. |
