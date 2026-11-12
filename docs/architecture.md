# 架构

`uv-im-connector` 是 universal、channel-neutral 的 IM connector。它负责 provider 认证、入站事件、出站消息、provider health 和 resources。调用方应用负责 product workflow、bot behavior、agent tasks、runs、workspaces、原生 resume handles 和 writeback policy。

## 分层

```text
IM provider
  -> provider adapter
  -> normalized Event / ResourceRef
  -> event log + HTTP/WS API
  -> caller application

caller application
  -> OutboundMessage
  -> provider adapter
  -> IM provider
```

## Provider Contract

每个 provider 实现同一个 Go 接口：

- `Run(ctx, sink)` 接收 provider 事件并输出标准事件。
- `Send(ctx, message)` 发送标准出站消息。
- `Download(ctx, request)` 把 provider resource 解析为本地 / 内部资源。
- `Capabilities()` 声明支持的行为。
- `Health(ctx)` 报告当前状态。

没有任何 provider 能定义核心协议。Provider-specific 行为留在 provider package 内部，并只通过 capabilities 暴露给调用方。

`Hub.RunProviders` 为每个注册的 provider/connector identity 串行监督 `Run`。父 context 未取消时，错误或正常返回都会重试；单个 provider 失败不会关闭 HTTP API 或停止其他 provider。指数退避从 1 秒开始，加入 ±20% 抖动，上限 1 分钟；单次运行至少 1 分钟后重置退避。取消会中断退避，Hub 返回前等待各 `Run` 结束；正在执行的传输调用能否立即取消取决于 provider 或 SDK。此机制只重启入站 runner，不重放出站消息。

WeCom、Lark WebSocket 断线及 DingTalk Stream 首次启动失败由共享监督器恢复；DingTalk SDK 已建立连接后的重连仍由 SDK 负责。Discord、KOOK、LINE、Mail、Matrix、OneBot、QQ、QQ Guild、Slack、Telegram、WeChat Official Account、WhatsApp、Zulip 和 DingTalk webhook 模式通过 HTTP ingress 接收事件，其 `Run` 等待取消；memory 使用同一监督入口。

## Protocol

标准协议包含这些稳定对象：

- `Event`：入站事件 envelope，包含 provider、connector、channel、user、message 和 referrer。
- `User.Identities`：带类型、作用域和来源的账户身份，详见 [用户身份与原生地址](./identities.md)；与会话、发送地址分别建模。
- `Message`：文本、结构化 elements 和 resource references。钉钉回调的 `createAt` 保留为原生 `message.created_at`，`conversationTitle` 保留为 `channel.name`；缺少时间时保持缺失，由既有入站事件时间提供已收上下文的回退锚点。名称不参与身份或地址选择。
- `ResourceRef`：文件 / 图片 / 音频 / 视频引用，包含 sanitized internal URL 和 provider-private 字段；可选 `source_message_id` 保留显式引用材料的消息来源；成功的合成引用上下文可选 `referenced_message_ids` 标识其中实际交付的祖先材料。两者都不赋予任务、访问或执行权限。
- `OutboundMessage`：显式 `target`、text/elements/resources，以及用于 reply/thread 的可选 referrer。
- `MessageUpdate`：嵌入 `OutboundMessage`，顶层 `message_id` 指定本 bot 已发送的精确消息，只替换文本。
- `SendResult`：已发消息 ID，及 provider 已确认时的 `publications` native 定位符；定位符不是由收件人地址推测出来的。
- `Capabilities`：每个 provider 的显式能力声明，包括 reply、Server 主动发送、可接受的 target kind 和可选 `publication_locator_version`。

交互卡片有两种互斥的原生身份：可引用消息以真实 `native_message_id` 确认；`card-instance.v1` 以 `native_card_id`、当前 bot 和既有 CardID/revision/digest 确认可回调的实例，不提供消息定位。后者不写原生消息 PATCH 记录，也不生成 `PublicationLocator`。呈现上限、模板准备、原生回调及验证边界统一见[交互卡片](./interactions.md)。Task 与权限仍属于调用方，不由运输回执决定。

`OutboundMessage.target` 由 `id` 和 `kind` 组成。`kind` 只能是 `user`、`group`、`channel` 或 `conversation`，调用方必须使用该 provider 在 `capabilities.target_kinds` 中声明的类型。入站事件的 `referrer.target` 是 provider adapter 给出的精确回复目标；回复时应原样带回完整 `referrer`。目标解析优先级是 outbound `target`、`referrer.target`、旧 channel 字段。协议 v1 继续接受旧的 `channel_id` / `channel_type`：`direct` 映射为 `user`，`group` 映射为 `group`，`thread` 映射为 `channel`，`room` 或空类型映射为 `conversation`。旧 `channel_id` 仍是 provider-native 的现有会话 / channel ID，只映射语义类型，不会被重新解释成主动发送的 user ID。旧请求必须提供 channel ID 或 message / reply handle；未知的非空旧类型会被拒绝，不做猜测。

`reply_message` 表示可以携带入站事件的 `referrer` 回复已有消息；`proactive_direct` 和 `proactive_group` 表示没有当前入站消息时，Server 能否主动发送私聊或群聊消息。调用方应从 `/v1/meta` 读取这些能力，不应根据 provider 名称推断。

调用方仅在 provider 声明 `publication_locator_version` 时，才可为内部通知在 `OutboundMessage.request_publication_locator` 显式请求已确认发布定位符。普通发送不请求也不触发查询。POST 已成功而定位符读取或校验失败时，发送仍成功、不得重试，只省略定位符；分段发送失败时仅 `failure.delivered_publications` 中的已确认成功前缀可用。服务级 `publication_locator_version` 只声明 HTTP schema，不替代 provider capability。`native_conversation_type` 是可空的证据字段；Lark 消息查询不能证明会话类型时必须保留为空，调用方不得从主动收件人推断 `direct` 或 `group`。

部分入站 `referrer` 带有短效或限次 reply handle。`referrer.expires_at` 表示 provider 给出或 adapter 推导的截止时间；`capabilities.reply_max_uses` 表示该 handle 最多可用于多少次出站尝试，字段缺失或为 `0` 表示没有声明有限次数。超过截止时间或次数后，调用方必须移除 message / reply handle，并且只能在对应主动发送能力和 target kind 可用时改用 `referrer.target`。不具备 reply-handle 语义的平台业务会话窗口仍属于 provider 限制，不能统一重试为同一条主动消息。

可选的 `target.id_type` 区分实际原生地址方案，只接受 `capabilities.target_id_types` 声明的值；必须随 referrer 一起保留。用户身份列表不用于自动选路，详见[带类型的原生发送地址](./identities.md#带类型的原生发送地址)。

WeCom 的可选企业身份查询在事件发送 worker 中完成；应用 token 和成员查询有界缓存，查询失败仍发布原始消息，不阻塞 socket ACK 调度。配置和账号核验规则见[企微接入与通讯录](./guide/wecom.md)。

`reply_stream` 只适用于有效被动 reply token；`edit_message` 要求真实的可选 `MessageUpdater` 实现；`text_max_bytes` 是单次标准化 UTF-8 文本的字节预算，零值或缺失表示未知。流式更新、编辑和长内容的集成及兼容性见[应用接入](./guide/application-integration.md#完整文本流式回复和编辑)。

## 近期会话窗口

`conversation_window_version=v1` 是可选的 provider 能力，Lark/Feishu 的 `ConversationWindowReader` 已提供原生实现；不改变必需的 `Provider` 接口。原生 reader capability 缺失时，已认证且有持久 received-context projection 的 Hub 才以同一版本声明 retained-ingress fallback。`POST /v1/conversation.window` 必须携带匹配的 Bearer 请求头，未配置认证时也拒绝读取；不能用 URL token 代替。调用方先完成自己的用户/会话准入，再以受信基础设施身份调用，不能把服务令牌交给 Agent 或将此接口当作 Task 授权。

请求仅含 `provider`、`connector`、`trigger_event_id`、`trigger_message_id`，不接受会话、话题、时间或路径覆盖。Hub 只在事件日志尾部 64 MiB、最后 10,000 条记录内核对同一已持久化、被唤起的群触发消息；缺失、损坏或身份冲突时不访问原生历史。事件日志用于触发证明，不是历史来源，也不建立另一份正文索引。Lark 再以当前 bot 权限读取精确触发消息，核对会话、时间和真实 `thread_id`；root/parent 不提供话题证明。

随后只取原生接口的一页、至多 10 个候选，固定倒序查询，再按时间顺序返回严格早于触发的同群/同话题消息。触发本身、明确父引用、同时间与未来项、重复、删除、错误 scope 及 app 卡片被排除；话题失败不退回全群，不翻页或重试。自动元数据读取总期限为 5 秒，不限制显式附件下载。结果 `version=v1` 含来源、状态、scope、锚点、候选数量、边界省略数、`has_more` 及消息的原生身份/时间/可读正文。背景资源只返回 kind/name 描述，不含下载 key、URL、资源句柄或文件字节；图文复用引用的可读表示，转发不递归展开。

近期窗口的请求和原生筛选不接收也不使用 `referenced_message_ids`。调用方若在自己的已准备引用材料中避免重复，只能在已取得准确的 `quoted-message.txt` 字节、并与同一脱敏 `ResourceRef.sha256` 核验一致后，将列表中的精确 ID 从已验证的窗口结果移除；不得按正文、文件名或位置 ID 推断。该列表没有访问或 Task 权限，缺少、错误或未验证时必须保留窗口项。调用方局部移除后若该页没有剩余项，可将其本地参考表述为 `none`，但不得改变原始候选数量、边界省略数、`has_more` 或 `scan_limited`，也不得翻页、重试或扩大原生读取。

`available` 只说明该页可用，`none` 是该页无符合项，不能宣称完整历史。时间边界排除且还有更多或页面已满时用 `scan_limited`；`permission_denied`、`rate_limited`、`unavailable` 分别表示拒读、限流与不可用，原因经过归一化，不返回原生错误正文。仅在原生 reader 缺失而被选择的 received fallback 固定读取同一 generation-bound EventLog projection 的一页 10 条元数据，携带 generation/trigger digest/lower bound，以 ingress `sequence` 而不是时间证明早于触发，并只保留安全媒体描述（kind/name/MIME/availability/provenance），绝不返回 resource handle、私有引用或下载媒体；原生 reader 调用失败绝不降级。调用方保留显式材料并显示缺失/覆盖说明，不将背景当作新指令、引用绑定或 Task 权限。

## 显式原生会话搜索

`conversation_search_version=v1` 是独立的可选能力，目前只由 Lark/Feishu 提供。它不改变 `conversation_window_version` 的四字段请求、单页预算或自动调用行为。Hub 未配置服务 Bearer 时，`/v1/meta` 同时隐藏两个能力；未声明搜索能力的 connector 在通过 Bearer 认证后仅返回规范化 `unsupported`，不读取原生历史。

`POST /v1/conversation.search` 仅供已完成自身 ACL 和当前 Run 授权的可信基础设施调用。请求必须带精确 Bearer、已持久触发的 provider/connector/event/message 元组、该脱敏触发事件的 SHA-256 摘要及当前 Run capability 的 SHA-256 audience 摘要；不接受 chat、thread、路径或原生 `page_token`。Hub 重新证明触发和摘要后，Lark 每次重新核对原生触发、chat、时间及真实话题；令牌轮换后的 audience 不能重用旧 cursor。

`conversation_materialize_version=v1` 是已选择 search 媒体描述符的独立可选配套能力。search 只返回不透明的 `materialize_handle`、逐资源 `source_epoch`、脱敏来源和可读元数据，绝不返回 provider key、私有 URL、`internal://` 或字节。可信基础设施可携带同一精确 trigger proof 与 audience digest 调用 `POST /v1/conversation.materialize`。Hub 在呈现前把私有引用写入 EventLog owner 目录下的 journal、绑定已验证 scope，并在唯一 provider 尝试前持久写入 `effect_started`；此点重启一律返回 `unknown`，不进行第二次下载。每个不透明 handle 精确绑定一个 source record/provenance tuple；只有每个来源均独立重新校验后，才可复用相同物理字节。Lark 在下载前重新证明 trigger 与精确 source message/resource 仍属于该 scope；失配、过期或撤回返回规范化 `unavailable` 或 `revoked`。只有 `ready` 向可信调用方返回脱敏 internal `ResourceRef`。该路由不授予 Task/Run 权限，也不能访问任意 provider resource。

增量 service capability `conversation_context_query_version=v1` 允许在 `conversation.search` 请求中显式指定 `source="received"`。它只读取 connector 保留的 EventLog projection，从不请求 provider 历史，并与默认 native source 分开。每个 received 结果携带 `event_log_generation`、精确 `trigger_entry_digest` 和 index lower bound；每个 received resource 还携带自身 generation、entry digest 和 sequence。调用方使用 received 材料时必须校验这些 source proof。projection 限制为 EventLog 最后 64 MiB/10,000 records，不保存第二份正文；generation、entry digest、sequence 或删除不匹配时资源失效。

原生筛选仅在已验证 scope 内本地执行：`from` 包含、`to` 排除且不得晚于不可变锚点，sender 为 OR，keywords 为 Unicode 小写后的 AND 子串。不可变 source time 优先取已持久化的 `Message.CreatedAt`，缺失时回退到已持久化的 `Event.Time`，但绝不改写用于摘要的事件。结果可选以 `anchor_time_source`（`message_created_at` 或 `event_time`）如实表明该选择。received 使用同样的显式谓词，但不隐式按触发时间设上界：只有 ingress sequence 证明项目早于触发，时间仅用于排序/展示。received search 每次至多扫描五个 1–10 条的元数据页（最多 50 candidates），在第一个有匹配的页停止，按时间正序返回，并报告实际 scanned/candidate/match/returned 计数。其 projection 持久化 exact-scope 的排序元数据和 EventLog EOF/inode/generation cursor：热读只增量处理新追加 record，并仅重读/复核已选 candidate；替换、截断或 source proof 无效时 fail closed 重建。结果只暴露 Hub 的随机短期 cursor，绝不暴露原生 continuation、凭据、下载句柄或媒体字节。cursor 记录保存在 Hub 私有的 0600 journal 中，原生 continuation 仅以密文保存；仅当触发摘要、过滤器/limit、已验证 scope、audience、received 的 source generation/lower bound 和 15 分钟有效期仍完全匹配时，重启后才可续用，过滤器不能借 continuation 扩大范围。

覆盖字段记录有效请求时间和实际扫描的安全候选时间区间、页数、候选/匹配/返回/边界省略计数。`has_more` 而没有 `next_cursor` 只表示明确的 cursor 元数据/链深度容量边界；其余可继续的 `available` 或 `scan_limited` 结果必须有 Hub cursor。未知、错误、循环或畸形 continuation 均不返回行或 cursor；零匹配、部分扫描和容量边界均不得表述为完整群历史为空。

## Provider 能力矩阵

下表只列 16 个外部 provider，不包含用于测试和本地开发的 `memory`。`有条件` 表示 adapter 已支持该操作，但必须满足“限制”列的平台条件。

| Provider | 私聊入站 | 群聊入站 | 回复已有消息 | Server 主动私聊 | Server 主动群聊 | 出站 target kind | 限制 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| WeCom | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`group`、`conversation` | AI Bot stream reply handle 在 10 分钟后过期；入站 target 可用于主动发送 fallback。 |
| Lark / Feishu | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`group`、`conversation` | 用户目标是 Open ID；群聊 / 会话目标是 chat ID。 |
| DingTalk | 支持 | 支持 | 支持 | 有条件 | 有条件 | `user`、`group` | 应用凭据启用主动单聊和群聊发送；文本回复仍可使用入站 session webhook，旧群机器人 webhook 需单独配置 token。 |
| Discord | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`channel`、`conversation` | 用户目标会先创建或复用 Discord DM channel。 |
| KOOK | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`channel` | 私聊使用 KOOK direct-message API。 |
| LINE | 支持 | 支持 | 支持 | 有条件 | 有条件 | `user`、`group`、`conversation` | Reply token 单次且短效；push fallback target 必须满足 LINE 的好友、近期联系或群成员规则。 |
| Mail | 支持 | 不支持 | 支持 | 支持 | 不支持 | `user` | 用户目标是 email address。 |
| Matrix | 支持（room） | 支持（room） | 支持 | 有条件 | 支持 | `conversation` | Matrix 消息事件不区分私聊 / 群聊 room；调用方必须提供已知 room ID。 |
| OneBot | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`group` | 需要兼容的 OneBot endpoint。 |
| QQ | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`group` | 这是 OneBot-style QQ adapter，不是 QQ 官方 Bot API。 |
| QQ Guild | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`group`、`channel` | 使用 QQ 官方 Bot 的用户、群和频道消息 endpoint。 |
| Slack | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`channel`、`conversation` | `chat.postMessage` 接受 user ID 并建立私聊会话。 |
| Telegram | 支持 | 支持 | 支持 | 有条件 | 支持 | `user`、`group`、`conversation` | 用户必须先联系 bot，bot 才能主动发送给该用户。 |
| WeChat Official Account | 支持 | 不支持 | 支持 | 有条件 | 不支持 | `user` | 客服消息受平台交互时间窗口和账号规则限制。 |
| WhatsApp | 支持 | 有条件 | 支持 | 有条件 | 有条件 | `user`、`group` | Groups API 要求符合资格的 business account；主动私聊自由文本受客服窗口限制。 |
| Zulip | 支持 | 支持 | 支持 | 支持 | 支持 | `user`、`group` | 群聊目标是 Zulip stream；有 topic 时会保留。 |

资源能力与文本 / 会话能力分开统计。“入站资源”表示 adapter 能把 provider media 标准化并复制为 `internal://` 资源；“出站 internal resource”表示 `POST /v1/upload.create` 创建的字节可以由该 adapter 真正发送。调用方仍必须针对精确 connector 实时检查 `upload_resource` 和 `resource_kinds`。

| Provider | 入站资源 | 出站 internal resource | 接受的出站 kind | Adapter / 平台限制 |
| --- | --- | --- | --- | --- |
| WeCom | 支持 | 支持 | `file`、`image`、`audio`、`video` | AI Bot WebSocket 上传，每片 512 KiB；总大小和分片数量由平台校验。 |
| Lark / Feishu | 支持 | 支持 | `file`、`image`、`audio`、`video` | 支持格式的图片始终使用图片 API，不按大小转为文件；其他资源走文件上传，由平台校验大小。 |
| DingTalk | 支持 | 支持 | `file`、`image`、`audio`、`video` | 需要应用凭据和共享资源存储；图片使用图片消息，文件、音频和视频作为可下载文件发送。 |
| Discord | 支持 | 支持 | `file`、`image`、`audio`、`video` | 资源随消息直接 multipart 上传，由平台校验大小。 |
| KOOK | 支持 | 支持 | `file`、`image`、`audio`、`video` | 先上传 asset，再发送图片消息或附件卡片。 |
| LINE | 支持 | 不支持 | — | LINE 出站媒体要求 provider 可访问的 HTTPS 内容 URL；uv-im-connector 当前不提供公网 media origin。 |
| Mail | 支持 | 支持 | `file`、`image`、`audio`、`video` | 作为 MIME 附件发送；大小和数量由邮件服务器校验。 |
| Matrix | 支持 | 支持 | `file`、`image`、`audio`、`video` | 先上传 content repository，再以 `mxc://` room message 发送。 |
| OneBot | 支持 | 不支持 | — | 不同兼容 endpoint 的文件 / CQ 上传行为尚未归一化。 |
| QQ | 支持 | 不支持 | — | 与 OneBot-style QQ adapter 的限制相同。 |
| QQ Guild | 支持 | 不支持 | — | 尚未实现官方富媒体上传握手。 |
| Slack | 支持 | 支持 | `file`、`image`、`audio`、`video` | 申请 external upload URL、上传原始字节，再 complete 并分享到 channel。 |
| Telegram | 支持 | 支持 | `file`、`image`、`audio`、`video` | Bot API multipart 上传；不符合原生格式时降级为 document。 |
| WeChat Official Account | 支持 | 支持（仅媒体） | `image`、`audio`、`video` | 临时素材上传后通过客服消息发送；不支持任意文件。 |
| WhatsApp | 支持 | 支持 | `file`、`image`、`audio`、`video` | Cloud API media upload 后再引用 media ID 发送消息。 |
| Zulip | 支持 | 支持 | `file`、`image`、`audio`、`video` | simple user upload 后发送 Markdown 附件链接。 |

## Resources

Provider 下载 URL、resource key、encrypted payload key、provider resource lookup 所需 metadata 和 raw payload 都是 provider-private。标准化后不能暴露给调用方。

公开 resource 形态是：

```text
ResourceRef
  id
  provider / connector
  kind
  name
  internal_url
  mime
  size_bytes
  sha256
```

`internal_url` 通过 `GET /v1/internal/<id>` 或 `client.ResolveInternalURL` 解析。调用方应该保存 sanitized reference，而不是 provider 下载凭证。

## Server API

| Endpoint | 用途 |
| --- | --- |
| `GET /health` | 进程健康检查。 |
| `GET /v1/meta` | 服务版本、协议版本、Provider 列表、capabilities 和 health。 |
| `GET /v1/events?after=<seq>` | 读取标准事件日志。 |
| `GET /v1/events/ws?after=<seq>` | 订阅标准事件。 |
| `POST /v1/message.create` | 发送出站消息。 |
| `POST /v1/message.update` | 编辑精确 connector 的本 bot 文本消息，需能力和 `MessageUpdater` 实现。 |
| `POST /v1/conversation.window` | 在已持久触发 scope 下读取单页原生背景；仅原生 capability 缺失时退到 retained received fallback；强制 Bearer 认证及可选 provider 能力。 |
| `POST /v1/conversation.search` | 在同一已验证触发 scope 下按本地筛选读取有限的显式原生或明确请求的 retained received 材料；强制 Bearer、audience 摘要和可选 provider 能力。 |
| `POST /v1/conversation.materialize` | 重新验证触发/来源后材料化一个不透明、当前查询的 context resource；强制 Bearer、audience 摘要和可选 provider 能力。 |
| `POST /v1/upload.create` | 从本地字节创建内部资源。 |
| `POST /v1/resource.download` | 可信请求：把 provider-private resource 解析为内部资源。 |
| `POST /v1/webhook/{provider}/{connector}` | Provider webhook ingress。Webhook verification 由 provider adapter 负责。 |
| `GET /v1/internal/<id>` | 解析内部资源。 |

配置 `UV_IM_AUTH_TOKEN` 后，除 `/health` 和 provider webhook ingress 外，所有 endpoint 都要求 `Authorization: Bearer <UV_IM_AUTH_TOKEN>`。Provider webhook ingress 必须在输出标准事件前使用 provider-level webhook verification；支持 webhook 的 provider 在没有配置 provider webhook secret 时会拒绝 ingress。

`/v1/meta` 是调用方的兼容性检查入口。调用方应基于 `service`、`protocol_version` 和 capabilities 判断是否继续启动；不要只根据 provider 名称推断能力。

## Conformance

Provider conformance 由 capabilities 驱动。一个 provider 合格的条件是：

- 至少声明 inbound 或 outbound capability；
- 以相同 provider ID 报告 health；
- 声明支持 outbound 时能发送出站消息；
- 声明支持 download 时能解析 resource request；
- 输出 sanitized events，不泄露 provider secrets。

## Provider Set

独立二进制可以注册 `memory`、`wecom`、`lark`、`dingtalk`、`discord`、`kook`、`line`、`mail`、`matrix`、`onebot`、`qq`、`qqguild`、`slack`、`telegram`、`wechat-official`、`whatsapp` 和 `zulip`。

Provider set 不是协议层级。每个 adapter 的边界都相同：把 provider-native inbound、outbound、auth 和 resource 行为翻译成根协议。
