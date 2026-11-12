# 交互卡片与回调

`interaction.v1` 在统一消息协议中表达卡片、问题和操作。它只负责呈现与运输；Task、问题是否仍有效、用户权限和一次性操作由调用方的业务服务验证。能力来自 `GET /v1/meta` 的 provider/connector 记录，无需另设开关。交互运输要求配置已有的 `UV_IM_AUTH_TOKEN`；未配置时不声明交互能力，拒绝交互发送、更新和入站，事件接口也不返回历史交互 handle。普通消息保留既有行为。

## 三端能力与应用准备

调用方根据 `interactive_message`、`interactive_action_callback`、`interaction_schema_versions` 和确认身份类型选择呈现，不能按渠道名称推断。原有文字 `MessageUpdate` 不接受卡片。

| 连接 | 原生交互 | 准备与边界 |
| --- | --- | --- |
| 飞书/Lark | Card JSON 2.0、原生消息回调与独立 PATCH | 使用既有应用；须发布消息、卡片回调和读取权限。 |
| 企业微信 | 单聊/群聊固定按钮模板卡、`template_card_event` | 使用既有 BotID/Secret；最多6个按钮，自由输入或整题不适配时用完整文字。不声明可引用消息定位或持久 PATCH。 |
| 钉钉 | 原生群互动卡、Stream 卡片回调 | 既有 Client ID/Secret 加同应用已发布模板及卡片权限；无模板、单聊或不适配问题用完整文字。不把卡片实例当聊天消息。 |

企微 `task_id` 必须与已有 `CardID` 完全相同，满足平台字符及128字节限制；按钮 `key` 使用既有操作值编码、不得超过1024字节。不能截断选项、另造ID或保存第二份权限映射。

钉钉模板由 `UV_DINGTALK_CARD_TEMPLATE_ID` 指定，这是必要的外部模板引用，不是逐群启用开关。空值不声明卡片能力；有值而无完整应用凭据时启动失败。应用须拥有卡片实例写权限并接收 `/v1.0/card/instances/callback`。配置值不证明模板内容、授权或实际投递已经成功。单聊原生卡空间与现有会话 ID 不同，连接器不会改写旧会话身份来拼接，使用完整文字保持任务连续。

钉钉模板须完整显示 `cardData.cardParamMap`：`title`、`summary`、`facts`（每行 `Label: Value`）、`question`、`question_help`、`detail_url`、`action_count`；第 n 个按钮（n=1至8）使用 `action_n_id`、`action_n_label`、`action_n_callback`、`action_n_disabled`。只显示实际操作，遵守禁用状态，不截断问题或选项。点击时以自身 ID 作为 `cardPrivateData.actionIds` 的唯一元素，并把自身 callback 值作为 `cardPrivateData.params.jarvis_callback` 的唯一参数；不能显示或让用户编辑该值。连接器只提供固定参数，不接收任意厂商 JSON；公开设计器导入格式未核实时，不提供猜测的导入文件。发布后需以真实应用核对完整呈现、按钮和回调。

飞书应用除现有消息发送/读取权限和 `im.message.receive_v1` 外，还须订阅 `card.action.trigger` 并发布应用配置，使该事件经现有应用长连接到达。更新会读取目标消息，核对当前应用所有权、原生会话和线程，再 PATCH 该消息；消息已删除、权限不足或目标不一致均不会尝试更新。代码声明能力不证明某个租户已经授予权限或发布事件订阅，安装后需使用实际应用验证。

协议上限为卡片16 KiB、最多8个操作、回调文字8 KiB；实际渠道更低时按其预算。`card-instance.v1` 通过 `interaction_card_instance_version`、`card_max_actions` 和 `interaction_free_text` 声明实例确认与呈现范围；未知版本或不支持整题时用完整文字。文本按纯文本/转义 Markdown 呈现，不接受调用方传入厂商卡片 JSON。操作 token 是调用方已有的私有不透明 handle；不得在 token 中放命令、凭据或用户正文。

## 回调持久受理

回调进入现有受认证事件流，类型为 `interaction.action`，结构化内容在 `Event.interaction`；普通 `Message.text/elements/resources` 为空。每次原生回调用其独立 event ID 去重，不能用同一张卡片的 message ID 合并不同点击。actor 使用原生 `operator.open_id`，保留 provider、connector、chat、message、card、presentation revision 和应用证据摘要；厂商更新凭据不写入事件。

adapter 先用原生消息查询核对当前应用、消息和 chat，恢复真实 parent/root/thread，然后在 EventLog 文件及其父目录同步成功后才 ACK；未配置持久日志、解析失败或持久化失败时不会回复成功 ACK。事件流沿现有 bearer 认证，日志文件为私有文件。启动只截断没有换行结束的未提交尾部，完整但损坏的记录仍拒绝恢复；已落盘而 ACK 丢失的回调重投不会增加第二条事件。跨会话接力对 callback 的私人目标另行查询当前 chat 信息，仅 `chat_mode=p2p` 才返回原生私人会话目标；群聊、拒绝或无法核验时保留已验证 actor/完整身份集合，不从历史 link 继承私人目标。该能力需要获取会话信息的既有平台权限。普通私聊消息的身份解析不额外依赖该查询。消费者必须把 callback 当作操作请求，重验当前业务 owner、actor、有效期、generation 和操作策略；卡片 revision 不代替业务 fence。

上述原生查询是飞书消息型回调的证明方式。企微与钉钉的卡片实例型回调使用显式 `card-instance.v1` profile：`native_card_id` 必须等于 `card_id`；原卡 `message_id` 和 `referrer.message_id` 留空。`Event.id` 只标识本次真实回调以去重。企微从已认证 WebSocket 核对 `aibotid`、原生操作者及显式 `chattype`，群必须有 `chatid`，单聊使用本次 `from.userid`；缺失会话类型不能猜成单聊。钉钉从当前应用的已认证 Stream 取得应用归属，并校验当前群空间、用户类型、操作者和 `outTrackId`，不接受正文声称的应用或从旧卡记录补造会话。缺失、错误或不匹配的范围均拒绝，不改投默认任务。

成功的实例型发送返回 `SendResult.interaction.native_card_id`、`bot_app_id`、原有 card ID/revision/digest，且不返回原卡 `native_message_id` 或 `publications`。这只能确认一张可接受回调的卡，不能用来建立引用索引、恢复外部通知或授予私人会话权限。业务调用方仍须持久化原问题与交付证据；已验证回调可以由其同一业务事务观察原未知交付，但不能触发连接器重发。

## 卡片更新与恢复

飞书消息型发送的 `SendResult.interaction` 包括真实原生 message ID、card ID、revision 和 payload SHA-256。后续使用同一 bearer 保护的 `POST /v1/interaction.update`，提交 `interaction-update.v1`、exact provider/connector/target/native message、card ID、expected/replacement revision、完整卡片及 digest、operation ID。replacement 必须为 expected + 1，最小更新间隔500 ms。

Hub 在现有 state 根下保存私有的运输记录：消息范围、已提交 revision/digest、操作摘要和 typed receipt，不保存卡片正文、操作 token 或 Task。先持久预留再释放锁进行平台 I/O，最后落回执。同 operation ID、同 payload 重放返回原回执；不同 payload 拒绝。未知/丢失记录、过期、旧 revision 和目标不一致在平台调用前拒绝。

进程在已开始调用后退出、平台超时或 ACK 丢失时，结果标为 `unknown`，禁止自动重发和继续覆盖。飞书 PATCH 成功回执本身没有 message ID 字段；adapter 依据已验证请求身份记录成功，不伪造厂商回显或 native CAS。当前不声称能从飞书查询证明一次未知 PATCH 的 exact operation。调用方应继续展示可证明的旧状态，并从自己的业务 fence 拒绝过期操作。

运输记录最多保留七日，清理沿普通调用的有界批次推进。记录缺失后的旧卡片只能由调用方判为过期，不能凭 callback 重建授权。刷新卡片不得改变 Task 生命周期或充当失败消息的重新发送。

实例型卡片不写入这份原生消息 PATCH 记录。企微官方更新依赖当前回调的短效请求，不是可持久恢复的消息 PATCH；钉钉实例也不接受把 `outTrackId` 填成 `NativeMessageID`。这两个适配器当前不声明持久卡片更新；旧按钮是否可用由调用方的当前问题/任务证明判断，已处理或过期时明确反馈。

## 验证边界

回归验证分别执行实际 adapter 的厂商协议、Hub HTTP/持久事件和调用方的原问题流程；消息型与实例型确认不得互相冒充。这类受控协议验证证明软件行为，不证明实际租户授权、模板发布、移动端呈现或厂商生产传输已经认证，后者需用部署后的应用验证。

协议依据：[Card JSON 2.0](https://open.feishu.cn/document/feishu-cards/card-json-v2-components/component-json-v2-overview)、[卡片回调](https://open.feishu.cn/document/feishu-cards/card-callback-communication)、[更新卡片](https://open.feishu.cn/document/server-docs/im-v1/message-card/patch)。

企微依据：[官方卡片与事件类型](https://github.com/WecomTeam/aibot-node-sdk/tree/main/src/types)、[官方收发与短效更新](https://github.com/WecomTeam/aibot-node-sdk/blob/main/src/client.ts)。钉钉依据：[官方 Stream 卡片回调模型](https://github.com/open-dingtalk/dingtalk-stream-sdk-go/blob/main/card/model.go)、[创建并投放卡片](https://open.dingtalk.com/document/orgapp/create-and-deliver-cards)、[模板创建与发布](https://open.dingtalk.com/document/orgapp/card-template-building-and-publishing)。
