# 企业微信接入与通讯录身份

接收、回复企微消息只需要智能机器人的 BotID 和 Secret。若还要确定发送者对应的企业成员账号，可额外配置同企业自建应用的企业 ID 和应用 Secret；connector 自动获取、缓存和刷新 token，并查询官方身份接口。两套 Secret 用途不同。

## 先确定需要哪种能力

| 目标 | 需要配置 |
| --- | --- |
| 接收消息、流式回复、主动发送 | 机器人 BotID、机器人 Secret。 |
| 在事件中取得经核验的企业成员账号及可读取的姓名 | 增加企业 ID、自建应用 Secret、应用可见范围和运行服务器的可信 IP。 |
| 使用 wecom-cli 处理文档、日程等办公操作 | 按 CLI 的独立授权流程配置；不属于 connector 收发消息或企业账号查询的前置条件。 |

## 每个值去哪里拿、填到哪里

| 界面名称 | 获取位置 | connector 配置 | 用途 |
| --- | --- | --- | --- |
| **BotID** | 企业微信客户端 → 工作台 → 智能机器人 → 对应机器人的 API 模式长连接设置。 | `UV_WECOM_BOT_ID` | 建立机器人消息连接。 |
| **机器人 Secret** | 与 BotID 相同的机器人长连接设置页。 | `UV_WECOM_BOT_SECRET` | 验证机器人连接身份。 |
| **企业 ID / CorpID** | 企业微信管理后台 → **我的企业 → 企业信息 → 企业 ID**，需要管理员权限。 | `UV_WECOM_CORP_ID` | 与自建应用 Secret 换取应用 token。 |
| **自建应用 Secret** | 管理后台 → **应用管理 → 自建 → 选择应用 → Secret**。没有应用时先创建一个，并启用它。 | `UV_WECOM_CORP_SECRET` | 调用该应用获准的身份转换和成员查询接口。 |
| **AgentId** | 同一个自建应用的详情页。它是应用编号。 | **这条链路不需要填写。** | 不用于获取 token，也不用于智能机器人这版身份转换接口。 |
| **access_token** | 程序使用企业 ID 和自建应用 Secret 向官方接口申请。 | **无需手工填写。** | 临时凭据，由程序缓存和刷新。 |

企业 ID 属于企业，自建应用详情显示的 AgentId 属于应用，不能互换。`UV_WECOM_CORP_SECRET` 必须填**该自建应用的 Secret**，不能填机器人 Secret。官方依据：[字段定义](https://developer.work.weixin.qq.com/document/path/90665)、[获取 token](https://developer.work.weixin.qq.com/document/path/91039)、[机器人长连接](https://developer.work.weixin.qq.com/document/path/101463)。

`UV_IM_AUTH_TOKEN` 则是部署者为 connector HTTP API 自行设置的访问口令，与企微两套凭据及企微 token 都无关。

## 配置自建应用

1. 使用**机器人所属企业**的管理员账号进入管理后台，创建或选择一个启用的自建应用，取得企业 ID 和应用 Secret。一个企业的凭据不能用于另一个企业的机器人。
2. 在该应用的**可见范围**加入需要识别的成员或部门，包括群聊中需要识别的发言人。能给机器人发消息不代表成员已经在应用可见范围内。
3. 在应用详情的 **企业可信 IP** 中配置**运行 connector 的服务器访问企微 API 时的公网出口 IP**。填写实际出口；不是服务器的内网地址，也不是操作员笔记本当前的公网地址。经 NAT、代理或容器出网时，应以企微实际看到的出口为准。
4. 把以下配置注入 connector 进程，并按部署方式重启或重建该服务。两项自建应用凭据必须同时填写，或同时留空。

```dotenv
UV_IM_PROVIDERS=wecom
UV_WECOM_BOT_ID=your-bot-id
UV_WECOM_BOT_SECRET=your-bot-secret
UV_WECOM_CORP_ID=your-enterprise-id
UV_WECOM_CORP_SECRET=your-self-built-application-secret
```

独立二进制读取**进程环境变量**，不会自动扫描 secret 目录，也不会自动加载任意 `.env` 文件。只有把凭据保存到某个文件，并不能使配置生效。使用服务管理器或已有的秘密管理工具注入环境变量：

- 仓库 Compose 示例：在项目的 `.env` 中设置这些变量，并沿用[部署说明](./deployment.md#docker)启动；`deploy/compose.yaml` 将变量传入容器。
- 仓库 systemd 示例：填写 `/etc/uv-im-connector/uv-im-connector.env`，该路径由 unit 的 `EnvironmentFile` 指定。
- 其他部署：修改该服务实际加载的环境配置，不要把值写进另一份无人读取的文件。

Secret 文件只供运行服务的账号读取。不要把 Secret、token 或真实成员列表提交到仓库。可信 IP 应覆盖运行服务的稳定出口；在另一台电脑复现 `60020`，并不能证明服务器配置有误。

Go 嵌入场景使用 `wecom.Config{BotID: ..., Secret: ..., CorpID: ..., CorpSecret: ...}`；省略后两项即关闭企业身份查询。

## 程序如何确认发送者

官方回调中的 `from.userid` 可能已经是企业明文成员账号，也可能是企业内加密标识。机器人由企业超级管理员创建时，官方说明回调为明文；其他创建者通常得到加密标识。**长度、拼音形式、`wo` 前缀都不能用来判定种类。**

配置应用凭据后，connector 在发布事件前执行以下流程：

1. 自动申请或复用应用 token；过期或被平台提前作废时刷新。
2. 将回调发送者交给[官方转换接口](https://developer.work.weixin.qq.com/document/path/101521)。转换返回对应关系时，取得企业成员账号。
3. 若输入出现在 `invalid_open_userid_list`，再使用[读取成员接口](https://developer.work.weixin.qq.com/document/path/90196)按原标识查询。**只有查询确认该账号后，才按已是明文处理**；invalid 本身不能证明账号有效。成员接口不区分账号大小写，输出保留官方返回的写法。
4. 在 `user.identities` 增加 `wecom.userid`，`scope` 是配置的企业 ID，`source` 是 `lookup`。可读取到姓名且事件尚无姓名时，补充 `user.display_name`。

```json
{
  "user": {
    "id": "callback-sender",
    "display_name": "Example Member",
    "identities": [
      {"type": "wecom.callback_userid", "id": "callback-sender", "source": "callback"},
      {"type": "wecom.userid", "id": "enterprise-account", "scope": "your-enterprise-id", "source": "lookup"}
    ]
  }
}
```

回调已经是明文时，两项身份的 `id` 可以相同。`user.id`、`channel.id`、完整回复地址和回调/配置中已有的显示名保持原来的语义。企业账号用于调用方关联企业成员，不会替换机器人的发送地址，也不自动授予业务权限。

查询有 5 秒总时限，在独立的事件发送 worker 中运行，不阻塞 WebSocket ACK 处理。应用 token 按有效期缓存；用户结果最多缓存 1024 项，成功缓存 10 分钟，失败或部分失败缓存 30 秒。IP、权限、网络、成员不存在等失败只记录安全诊断，基础消息仍会发布。已由转换证明的企业账号可在姓名查询失败时保留。

## 与 wecom-cli 通讯录标识的关系

wecom-cli 的通讯录搜索结果和机器人回调可能使用不同的标识。**不要把搜索结果的 `userid` 无条件视为企业明文账号，也不要把短回调账号视为丢失了真实身份。**

调用方需要把 CLI 搜索候选与消息发送者对应时，应把候选标识交给同企业的官方转换接口，再与事件中 `type=wecom.userid`、相同企业 `scope` 的账号比较。姓名搜索只能找到候选，不能证明两者属于同一个人。

```text
CLI 搜索候选的加密标识 → 官方转换 → 企业成员账号
                                   ↑
消息发送者 → connector 官方核验 → wecom.userid
```

转换接口只提供加密标识到企业账号的方向；不能假定它可以反向生成 CLI 标识。connector 不调用 wecom-cli，也不为每个发消息的员工发起 CLI 登录。CLI 自己的机器人绑定和真人办公授权仍按其[官方流程](https://github.com/WecomTeam/wecom-cli/blob/main/docs/cli-reference.md)处理。

## 验证与排错

保存配置后，按[部署说明](./deployment.md)启用配置，向机器人发送一条新消息，再读取 connector 的 `GET /v1/events?after=<sequence>`。新事件应包含企业作用域的 `wecom.userid`；姓名是否返回取决于应用实际权限。`/health` 或 WebSocket 已连接，只能证明相应服务可用，不能证明可选通讯录查询已经成功。旧事件重放不会重新查询或补写身份。

| 现象 | 检查与处理 |
| --- | --- |
| 启动报 `corp_id and corp_secret must be configured together` | 企业 ID 和自建应用 Secret 只填了一项；补齐另一项，或两项同时清空。 |
| `gettoken` 失败 | 核对企业 ID、自建应用 Secret、应用启用状态；不要使用 AgentId 或机器人 Secret 代替。 |
| 日志出现 `errcode=60020` | 在该自建应用的企业可信 IP 中加入**实际运行服务器**的公网出口。获取 token 成功仍可能在后续接口被 IP 限制拒绝。 |
| 成员查询失败或没有新增企业身份 | 检查应用可见范围及接口查看权限；没有核验结果时事件只保留原始身份。 |
| 转换返回 invalid | 可能已经是明文，也可能输入无效；以成员查询的成功结果为准。 |
| 改配置或权限后仍看到旧结果 | 确认服务加载了新配置，并使用新事件验证；失败缓存最多 30 秒，成功缓存最多 10 分钟，重启服务会清空缓存。 |
| 已知人工核验的绑定需要暂时使用 | 可使用通用的[账户绑定文件](../identities.md#经确认的账户绑定)，明确填写目标身份类型和企业作用域。 |
