---
layout: home

hero:
  name: uv-im-connector
  text: Universal IM Connector
  tagline: uv 代表 universal。把飞书、企业微信、Slack、Telegram 等渠道的消息、回复和文件统一成一套协议，让应用、机器人、Agent 和自动化系统不用再重复实现 IM 接入。
  image:
    src: /connector-flow.svg
    alt: uv-im-connector 数据流
  actions:
    - theme: brand
      text: 快速开始
      link: /guide/getting-started
    - theme: alt
      text: 为什么存在
      link: /guide/why-uv
    - theme: alt
      text: 查看架构
      link: /architecture

features:
  - title: 一套事件模型
    details: 所有 provider 都输出同一种 Event：provider、connector、channel、user、message、resource、referrer 和 addressed。
  - title: 一套发送 API
    details: 调用方通过 OutboundMessage 回复消息，不再直接调用各平台原生 send API。
  - title: 文件资源安全收口
    details: Provider 私有下载 URL、临时 key、密钥和原始 payload 会被转换为内部资源引用。
  - title: 渠道平等
    details: WeCom、Lark、Slack、Telegram、Discord、DingTalk、Matrix、OneBot、QQ、WhatsApp 等都走同一个 contract。
  - title: 调用方边界清晰
    details: Connector 只负责 IM 基础设施；调用方负责产品 workflow、机器人行为、agent session、权限和 writeback。
  - title: 人和 Agent 都容易读
    details: 人类指南、参考文档、conformance 规则和 llms.txt 描述同一套 universal protocol。
---

<div class="home-section">
  <span class="home-kicker">为什么需要它</span>
  <h2>不要让每个项目都重新实现一遍 IM connector。</h2>
  <p>
    一个应用、机器人或 agent 真正关心的是“谁在什么会话里说了什么、带了哪些文件、应该如何回复”。
    它不应该同时理解飞书事件格式、企业微信长连接、Slack thread_ts、Telegram 文件下载和不同平台的 webhook 鉴权。
    <code>uv-im-connector</code> 把这些差异收敛到 provider adapter 内部，对调用方暴露稳定的 HTTP/WebSocket 协议。
  </p>
  <div class="signal-grid">
    <div class="signal-card">
      <strong>Inbound</strong>
      <span>把 provider-native 事件规范化为 <code>Event</code>，并写入可恢复的事件流。</span>
    </div>
    <div class="signal-card">
      <strong>Outbound</strong>
      <span>把调用方的 <code>OutboundMessage</code> 转换成平台原生发送请求。</span>
    </div>
    <div class="signal-card">
      <strong>Resources</strong>
      <span>把文件、图片、音频和视频落到内部资源模型，避免泄露 provider 私有凭证。</span>
    </div>
  </div>
</div>

<div class="home-section">
  <span class="home-kicker">数据流</span>
  <h2>Inbound / outbound 是同一条协议边界的两个方向。</h2>
  <div class="flow-grid">
    <div class="flow-panel">
      <div class="flow-row">
        <b>IM Provider</b>
        <span>接收原生事件、webhook、长连接消息、文件通知和平台鉴权结果。</span>
      </div>
      <div class="flow-row">
        <b>Adapter</b>
        <span>把平台差异翻译为 provider-neutral protocol，并声明 capabilities。</span>
      </div>
      <div class="flow-row">
        <b>Connector API</b>
        <span><code>/v1/events/ws</code>、<code>/v1/message.create</code>、<code>/v1/internal/&lt;id&gt;</code>。</span>
      </div>
      <div class="flow-row">
        <b>Caller</b>
        <span>只处理产品 workflow、机器人行为、agent session、权限、产物和回复策略。</span>
      </div>
    </div>
    <ul class="scenario-list">
      <li>群聊 mention 触发一次 bot action、agent run 或业务 workflow。</li>
      <li>用户上传文件，调用方只拿到 sanitized <code>ResourceRef</code>。</li>
      <li>多个 Lark / WeCom bot 使用不同 <code>connector</code> 并行接入。</li>
      <li>新增 provider 时只实现 adapter 和 conformance tests。</li>
    </ul>
  </div>
</div>

<div class="home-section">
  <span class="home-kicker">支持渠道</span>
  <h2>Provider 不分三六九等，差异通过 capabilities 表达。</h2>
  <p>
    每个 provider 都围绕同一个接口实现 inbound、outbound、download、health 和 capabilities。
    渠道功能不完全一致时，调用方读取 <code>/v1/meta</code> 做策略判断，而不是在业务代码里硬编码平台名。
  </p>
  <div class="provider-strip">
    <span>WeCom</span><span>Lark / Feishu</span><span>DingTalk</span><span>Slack</span><span>Telegram</span>
    <span>Discord</span><span>Matrix</span><span>OneBot</span><span>QQ</span><span>QQ Guild</span>
    <span>LINE</span><span>KOOK</span><span>WhatsApp</span><span>Zulip</span><span>Mail</span>
  </div>
  <div class="cta-row">
    <a href="/uv-im-connector/guide/getting-started.html">跑通本地示例</a>
    <a href="/uv-im-connector/guide/why-uv.html">为什么存在</a>
    <a href="/uv-im-connector/guide/application-integration.html">接入应用</a>
    <a href="/uv-im-connector/guide/contributing.html">贡献新渠道</a>
  </div>
</div>
