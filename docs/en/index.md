---
layout: home

hero:
  name: uv-im-connector
  text: Universal IM Connector
  tagline: Normalize inbound events, outbound messages, and files across IM providers so applications, bots, agents, and automation systems do not reimplement every channel.
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

<div class="home-section">
  <span class="home-kicker">Why it exists</span>
  <h2>Do not rebuild the IM connector layer in every project.</h2>
  <p>
    An application, bot, or agent needs to know who spoke in which conversation, what files arrived, and how to reply.
    It should not parse Lark events, manage WeCom streams, resolve Slack files, and duplicate webhook auth for every channel.
    <code>uv-im-connector</code> keeps those differences inside provider adapters and exposes one stable HTTP/WebSocket protocol.
  </p>
  <div class="signal-grid">
    <div class="signal-card">
      <strong>Inbound</strong>
      <span>Normalize provider-native events into <code>Event</code> and persist them in a recoverable stream.</span>
    </div>
    <div class="signal-card">
      <strong>Outbound</strong>
      <span>Translate caller-owned <code>OutboundMessage</code> requests into platform-native send calls.</span>
    </div>
    <div class="signal-card">
      <strong>Resources</strong>
      <span>Move files, images, audio, and video into an internal resource model without leaking provider secrets.</span>
    </div>
  </div>
</div>

<div class="home-section">
  <span class="home-kicker">Data flow</span>
  <h2>Inbound and outbound traffic cross the same protocol boundary.</h2>
  <div class="flow-grid">
    <figure class="flow-figure">
      <img src="/connector-flow-en.svg" alt="uv-im-connector data flow" />
    </figure>
    <div class="flow-panel">
      <div class="flow-row">
        <b>IM Provider</b>
        <span>Receives native events, webhooks, streams, file notices, and provider auth results.</span>
      </div>
      <div class="flow-row">
        <b>Adapter</b>
        <span>Translates platform differences into the provider-neutral protocol and declares capabilities.</span>
      </div>
      <div class="flow-row">
        <b>Connector API</b>
        <span><code>/v1/meta</code>, <code>/v1/events/ws</code>, <code>/v1/message.create</code>, and <code>/v1/internal/&lt;id&gt;</code>.</span>
      </div>
      <div class="flow-row">
        <b>Caller</b>
        <span>Owns product workflow, bot behavior, agent sessions, permissions, artifacts, and reply policy.</span>
      </div>
    </div>
  </div>
  <div class="scenario-list">
    <div>A group mention starts a bot action, agent run, or business workflow.</div>
    <div>A user uploads a file and the caller receives only a sanitized <code>ResourceRef</code>.</div>
    <div>Multiple Lark or WeCom bots run through distinct <code>connector</code> identities.</div>
    <div>A new provider adds an adapter and shared conformance tests.</div>
  </div>
</div>

<div class="home-section">
  <span class="home-kicker">Providers</span>
  <h2>Providers are peers; differences are expressed as capabilities.</h2>
  <p>
    Each provider implements the same inbound, outbound, download, health, and capability surface.
    When channels differ, callers read <code>/v1/meta</code> and choose policy instead of hard-coding platform names into business logic.
  </p>
  <div class="provider-strip">
    <span>WeCom</span><span>Lark / Feishu</span><span>DingTalk</span><span>Slack</span><span>Telegram</span>
    <span>Discord</span><span>Matrix</span><span>OneBot</span><span>QQ</span><span>QQ Guild</span>
    <span>LINE</span><span>KOOK</span><span>WhatsApp</span><span>Zulip</span><span>Mail</span>
  </div>
  <div class="cta-row">
    <a href="/uv-im-connector/en/guide/getting-started.html">Run a local loop</a>
    <a href="/uv-im-connector/en/guide/why-uv.html">Why it exists</a>
    <a href="/uv-im-connector/en/guide/application-integration.html">Integrate an app</a>
    <a href="/uv-im-connector/en/guide/deployment.html">Deploy service</a>
    <a href="/uv-im-connector/en/guide/contributing.html">Add a provider</a>
  </div>
</div>
