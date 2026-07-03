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
