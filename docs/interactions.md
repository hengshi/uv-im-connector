# 交互卡片与回调

`interaction.v1` 在统一消息协议中表达卡片、问题和操作。它只负责呈现与运输；Task、问题是否仍有效、用户权限和一次性操作由调用方的业务服务验证。能力来自 `GET /v1/meta` 的 provider/connector 记录，无需另设开关。交互运输要求配置已有的 `UV_IM_AUTH_TOKEN`；未配置时不声明交互能力，拒绝交互发送、更新和入站，事件接口也不返回历史交互 handle。普通消息保留既有行为。

## 能力与飞书应用准备

当前 Lark/飞书 adapter 支持 Card JSON 2.0 初始发送、`card.action.trigger` 长连接回调，以及独立的卡片 PATCH 更新。其他 adapter 对交互卡片在发送前明确拒绝；调用方应根据 `interactive_message`、`interactive_action_callback` 和 `interactive_message_update` 选择等义文字呈现。原有文字 `MessageUpdate` 不接受卡片。

飞书应用除现有消息发送/读取权限和 `im.message.receive_v1` 外，还须订阅 `card.action.trigger` 并发布应用配置，使该事件经现有应用长连接到达。更新会读取目标消息，核对当前应用所有权、原生会话和线程，再 PATCH 该消息；消息已删除、权限不足或目标不一致均不会尝试更新。代码声明能力不证明某个租户已经授予权限或发布事件订阅，安装后需使用实际应用验证。

协议上限为卡片 16 KiB、最多 8 个操作、回调文字 8 KiB，同一消息更新最小间隔 500 ms。文本按纯文本/转义 Markdown 呈现，不接受调用方传入厂商卡片 JSON。操作 token 是调用方签发的私有不透明 handle；不得在 token 中放命令、凭据或用户正文。

## 回调持久受理

回调进入现有受认证事件流，类型为 `interaction.action`，结构化内容在 `Event.interaction`；普通 `Message.text/elements/resources` 为空。每次原生回调用其独立 event ID 去重，不能用同一张卡片的 message ID 合并不同点击。actor 使用原生 `operator.open_id`，保留 provider、connector、chat、message、card、presentation revision 和应用证据摘要；厂商更新凭据不写入事件。

adapter 先用原生消息查询核对当前应用、消息和 chat，恢复真实 parent/root/thread，然后在 EventLog 文件及其父目录同步成功后才 ACK；未配置持久日志、解析失败或持久化失败时不会回复成功 ACK。事件流沿现有 bearer 认证，日志文件为私有文件。启动只截断没有换行结束的未提交尾部，完整但损坏的记录仍拒绝恢复；已落盘而 ACK 丢失的回调重投不会增加第二条事件。跨会话接力对 callback 的私人目标另行查询当前 chat 信息，仅 `chat_mode=p2p` 才返回原生私人会话目标；群聊、拒绝或无法核验时保留已验证 actor/完整身份集合，不从历史 link 继承私人目标。该能力需要获取会话信息的既有平台权限。普通私聊消息的身份解析不额外依赖该查询。消费者必须把 callback 当作操作请求，重验当前业务 owner、actor、有效期、generation 和操作策略；卡片 revision 不代替业务 fence。

## 卡片更新与恢复

初始成功发送返回 `SendResult.interaction`，包括真实原生 message ID、card ID、revision 和 payload SHA-256。后续使用同一 bearer 保护的 `POST /v1/interaction.update`，提交 `interaction-update.v1`、exact provider/connector/target/native message、card ID、expected/replacement revision、完整卡片及 digest、operation ID。replacement 必须为 expected + 1。

Hub 在现有 state 根下保存私有的运输记录：消息范围、已提交 revision/digest、操作摘要和 typed receipt，不保存卡片正文、操作 token 或 Task。先持久预留再释放锁进行平台 I/O，最后落回执。同 operation ID、同 payload 重放返回原回执；不同 payload 拒绝。未知/丢失记录、过期、旧 revision 和目标不一致在平台调用前拒绝。

进程在已开始调用后退出、平台超时或 ACK 丢失时，结果标为 `unknown`，禁止自动重发和继续覆盖。飞书 PATCH 成功回执本身没有 message ID 字段；adapter 依据已验证请求身份记录成功，不伪造厂商回显或 native CAS。当前不声称能从飞书查询证明一次未知 PATCH 的 exact operation。调用方应继续展示可证明的旧状态，并从自己的业务 fence 拒绝过期操作。

运输记录最多保留七日，清理沿普通调用的有界批次推进。记录缺失后的旧卡片只能由调用方判为过期，不能凭 callback 重建授权。刷新卡片不得改变 Task 生命周期或充当失败消息的重新发送。

## 验证边界

回归测试使用真实 adapter、Hub HTTP、持久 EventLog 和飞书协议形状的 HTTP/长连接 fixture，覆盖不同点击、ACK 前持久化、目标校验、PATCH 无 ID 回显、重启幂等、结算窗口恢复及结果未知后零重发。这证明本地协议与恢复契约；实际租户权限、事件订阅、移动端呈现及生产平台调用仍需用部署后的应用验证。

协议依据：[Card JSON 2.0](https://open.feishu.cn/document/feishu-cards/card-json-v2-components/component-json-v2-overview)、[卡片回调](https://open.feishu.cn/document/feishu-cards/card-callback-communication)、[更新卡片](https://open.feishu.cn/document/server-docs/im-v1/message-card/patch)。
