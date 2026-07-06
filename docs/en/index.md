---
layout: home

hero:
  name: uv-im-connector
  text: Universal IM Connector
  tagline: Normalize inbound events, outbound messages, and files across IM providers so applications, bots, agents, and automation systems do not reimplement every channel.
  image:
    src: /connector-flow-en.svg
    alt: uv-im-connector data flow
  actions:
    - theme: brand
      text: Get Started
      link: /en/guide/getting-started
    - theme: alt
      text: Why It Exists
      link: /en/guide/why-uv
    - theme: alt
      text: Read the Architecture
      link: /en/architecture

features:
  - title: One event model
    details: Providers emit the same Event shape with provider, connector, channel, user, message, resources, referrer, and addressed semantics.
  - title: One outbound API
    details: Caller applications send replies through OutboundMessage instead of calling platform-native send APIs directly.
  - title: Safe resource handling
    details: Provider-private URLs, keys, and secrets are converted into sanitized internal resource references.
  - title: Provider parity
    details: WeCom, Lark, Slack, Telegram, Discord, DingTalk, Matrix, OneBot, QQ, WhatsApp, and other channels share the same contract.
  - title: Clear caller boundary
    details: The connector owns IM infrastructure. The caller owns product workflows, bot behavior, agent sessions, artifacts, policy, and writeback.
  - title: Human and agent friendly
    details: Human guides, reference docs, conformance rules, and llms.txt describe the same universal protocol surface.
---

## Why This Exists

Applications, bots, and agents should not need a custom WeCom parser, a separate Lark sender, a Slack file downloader, and a different retry model for every IM provider. `uv-im-connector` centralizes the provider-specific work behind one Go protocol and one HTTP/WebSocket surface.

The result is a clear split:

| Layer | Owns |
| --- | --- |
| IM provider | Native messaging, webhooks, files, auth, rate limits. |
| `uv-im-connector` | Provider credentials, connection lifecycle, normalized events, outbound messages, provider health, resource redaction. |
| Caller application | Product workflows, bot behavior, agent sessions, workspace lifecycle, artifacts, policy, retries, user-facing status. |

## Common Scenarios

- Connect applications, bots, and agents to direct messages and group mentions without provider-specific connector code.
- Download user-sent files, images, audio, and video into an internal resource store before starting work.
- Route conversations by `provider + connector + channel.id` when multiple bot identities are configured.
- Send final replies or threaded replies through a single outbound API.
- Add a new IM channel by implementing the provider interface and shared conformance tests.

## First Working Loop

```bash
export UV_IM_AUTH_TOKEN=dev-token
export UV_IM_PROVIDERS=memory
uv-im-connector
```

```bash
curl -H "Authorization: Bearer dev-token" \
  http://127.0.0.1:8787/v1/meta
```

Continue with [Getting Started](/en/guide/getting-started) for installation, local testing, provider configuration, and the first outbound message.
