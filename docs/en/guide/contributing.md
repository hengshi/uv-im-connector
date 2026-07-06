# Contributing

Contributions should preserve the provider-neutral protocol boundary. A provider can behave differently internally, but the caller-facing model must remain stable.

## Local Development

Run the Go test suite:

```bash
go test ./...
```

Run the documentation site:

```bash
npm install
npm run docs:dev
```

Build the documentation site:

```bash
npm run docs:build
```

## Add a Provider

A provider must implement the root `Provider` interface:

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

Webhook-capable providers should also implement `WebhookProvider`.

## Required Provider Behavior

Every provider should:

- declare capabilities honestly;
- normalize direct and group messages when the provider supports them;
- expose provider files, images, audio, and video as `ResourceRef` values;
- resolve downloadable provider resources into `internal_url` before event persistence when download is supported;
- keep provider-native secrets out of sanitized events and resources;
- preserve reply and thread context through `Referrer` when the provider exposes it;
- return explicit errors for unsupported outbound resources or rich elements.

## Test Expectations

Use [Conformance](/en/conformance) as the quality gate for provider behavior.

Provider tests should include:

- inbound text normalization;
- inbound group/channel normalization;
- resource normalization for supported resource kinds;
- outbound direct message;
- outbound reply with `Referrer`;
- resource download;
- provider health;
- duplicate event key stability.

Providers that require live credentials should split tests into local decoder/contract tests and live tests guarded by explicit credentials.

## Documentation Expectations

When adding or changing a provider:

- update [Configuration](/en/configuration) with environment variables;
- update [Architecture](/en/architecture) if the protocol boundary changes;
- update [E2E Tests](/en/e2e-tests) when new live-provider coverage is required;
- keep the public guide provider-neutral unless a provider-specific behavior changes caller policy.
