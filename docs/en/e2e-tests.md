# E2E Tests

`uv-im-connector` splits E2E coverage into local connector E2E and live-provider E2E.

## Local Connector E2E

These tests run without external credentials:

- `server.TestHubEventsAndOutbound`: normalized events can be stored and outbound messages route through a provider.
- `server.TestHubRoutesOutboundByConnector`: provider plus connector chooses the exact account instance.
- `server.TestHubDownloadsInboundResourcesBeforeEventLog`: provider resources are resolved before event persistence.
- `server.TestHubWebhookRoutesToProviderAndStoresEvent`: provider webhook ingress verifies provider secret and writes normalized events.
- `server.TestClientResolveInternalURL`: callers can resolve internal resources through the connector HTTP API.

Run them with:

```bash
go test ./server
```

## Provider Contract E2E

These tests run without external credentials and exercise provider protocol shape:

- `TestProviderMetadataForAllChannels`
- `TestProviderDecodersNormalizeInboundMessages`
- `TestSlackSendUsesBearerJSON`
- `TestDiscordSendUsesBotAuthorization`
- `TestTelegramSendUsesTokenPathWithoutBearer`
- `TestZulipSendUsesFormEncoding`

Run them with:

```bash
go test .
```

## Live Provider E2E

Live-provider tests must be guarded by explicit credentials. They should be added per provider with the same scenario names:

- direct inbound text message;
- group/channel inbound text message;
- inbound file, image, audio, or video resource;
- outbound text reply using `Referrer`;
- reconnect or webhook retry dedupe;
- secret redaction in persisted events and public API responses.

Live-provider credentials must be isolated per environment. Production credentials must not be reused by E2E, development, or temporary debug workers.

To qualify native Lark text editing through the existing provider API, export the test app's `UV_LARK_APP_ID`, `UV_LARK_APP_SECRET`, and exact `UV_LARK_LIVE_CHAT_ID`. Optional region, connector ID, and API address use `UV_LARK_REGION`, `UV_LARK_CONNECTOR_ID`, and `UV_LARK_BASE_URL`. Run:

```bash
UV_LARK_LIVE_UPDATE=1 go test ./providers/lark -run '^TestLiveTextUpdateRoundTrip$' -count=1 -v
```

Normal tests skip this probe. When enabled, it uses real `Send`, `Update`, and native GET calls to verify the same message ID, chat, and complete replacement text. It never invokes `Run` or opens a second bot WebSocket connection. It leaves one edited test message and logs its ID for cleanup by the test destination owner. This qualifies native text create/edit/readback, not inbound events, passive reply streams, or caller task lifecycle.
