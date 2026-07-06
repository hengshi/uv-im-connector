# 为什么存在

`uv-im-connector` 存在的理由很直接：IM 接入已经变成应用、机器人、Agent 和自动化系统的基础设施，但很多项目仍然需要在自己的产品里重新实现一套 inbound、outbound、文件下载、群聊、thread、referrer、鉴权和连接生命周期。

这不是某个项目做错了，而是产品边界自然导致的结果。只要一个系统要“从 IM 收到用户意图，并把结果发回 IM”，它就必须先解决连接、认证、消息解析、附件、权限、会话和发送问题。没有独立可复用的 IM connector 时，这些能力就会被写进每个产品里。

## 为什么会重复实现

IM 不是一个简单的 transport。对上层系统来说，它同时是：

- 用户入口：谁在说话、是否被 mention、群消息是否应该触发动作。
- 权限边界：哪些用户、群、租户、机器人 identity 可以访问系统。
- 会话边界：一条消息属于哪个 channel、thread、任务、run 或业务 workflow。
- 文件边界：附件下载、临时 URL、密钥、资源落盘和脱敏策略。
- 交付边界：进度消息、最终回复、thread reply、引用原消息、失败重试。
- 运维边界：长连接、webhook、公网回调、私网部署、健康检查和凭证轮换。

如果这些问题没有被抽成独立协议，每个上层产品都会把它们和自己的 agent loop、任务模型、UI、权限模型、部署方式绑在一起。

## 这些项目为什么会各做一套

下面的判断基于这些项目公开 README / Docs 的产品定位，而不是对内部实现做价值判断。

| 项目 | 公开定位 | 为什么自然会内嵌 IM connector |
| --- | --- | --- |
| [OpenClaw](https://github.com/openclaw/openclaw) | 本地运行的 personal AI assistant，通过 Gateway 连接用户已经使用的多个 chat channel。 | 它的产品是“随时在 IM 里可达的个人助手”。channel、pairing、allowlist、session、skills 和本地 gateway 是一个完整体验，所以 IM connector 很容易成为 gateway 的一部分。 |
| [Hermes Agent](https://github.com/nousresearch/hermes-agent) | 带学习闭环的 agent，可以通过 Telegram、Discord、Slack、WhatsApp、Signal 和 CLI 与用户交互。 | 它关注 conversation continuity、memory、scheduled automation、技能沉淀和 agent execution。IM 不只是消息管道，也是学习循环和任务交付的入口。 |
| [Multica](https://github.com/multica-ai/multica) | 管理人和 coding agent 的平台，把 agent 当成团队成员分配任务、报告进度、复用技能。 | 它的核心是 team / task / issue / daemon / agent lifecycle。消息入口如果接进来，通常会直接映射到平台任务、成员、权限、状态和协作流。 |
| [cc-connect](https://github.com/chenhg5/cc-connect) | 把本机 AI coding agent 桥接到飞书、钉钉、Slack、Telegram、Discord、LINE、企业微信等消息平台。 | 它提供的是从 IM 远程控制本地 agent 的完整产品闭环，因此平台 adapter、agent adapter、权限、进度展示、取消、恢复和文件处理会一起演进。 |

这些实现的共同点是：IM connector 是主产品体验的一部分。这样做能让项目快速交付完整闭环，但代价是 connector 很难被其他系统直接复用。

## uv-im-connector 抽出的边界

`uv-im-connector` 把 IM 这层单独拿出来，目标不是再做一个 agent 平台，而是提供所有人都能直接使用的 universal IM connector。

它负责：

- provider 认证和连接生命周期；
- inbound event normalization；
- outbound message delivery；
- file / image / audio / video resource handling；
- thread、referrer、addressed、capabilities 和 health；
- provider conformance tests。

它不负责：

- agent loop；
- bot behavior；
- product workflow；
- task / run / workspace lifecycle；
- 用户可见 UI；
- 业务权限、审批、升级和 writeback 策略。

这条边界让任意调用方都可以复用同一套 IM 接入层：个人助手可以用，企业 bot 可以用，coding agent 平台可以用，运维自动化、客服自动化、审批自动化也可以用。

## 结果应该是什么

理想状态下，新项目不需要再问“飞书文件怎么下载、企业微信群聊 mention 怎么解析、Slack thread_ts 怎么映射、Telegram 文件 URL 怎么落地”。它只需要：

1. 配好 provider credentials。
2. 订阅标准 `Event`。
3. 复制允许访问的 `ResourceRef`。
4. 执行业务、bot 或 agent workflow。
5. 通过 `OutboundMessage` 回复。

新增渠道也不应该影响调用方代码。provider 差异应该进入 adapter 和 capabilities，而不是散落在每个上层产品里。

## 什么时候用 uv-im-connector

适合使用：

- 你在做一个应用、机器人、agent service 或自动化系统，并且需要多个 IM 渠道。
- 你希望 WeCom、Lark、Slack、Telegram、Discord、DingTalk、Matrix、OneBot、QQ、WhatsApp 等渠道走同一套事件和资源模型。
- 你不想把 provider-native payload、文件下载逻辑、发送 API 和鉴权细节写进业务代码。
- 你希望新增 provider 时有明确 conformance tests。

不适合使用：

- 你只需要某个完整的个人助手或 coding agent 产品，而不是一个 connector。
- 你希望 connector 负责 agent 执行、任务编排、产品 UI 或团队工作流。
- 你只接一个渠道，并且已经接受直接绑定该渠道 SDK。

`uv-im-connector` 的定位是 IM infrastructure。它应该让更多项目不再重复实现 connector，而不是把自己变成另一个绑定主产品逻辑的 bridge。
