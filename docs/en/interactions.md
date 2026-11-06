# Interactive cards and callbacks

`interaction.v1` adds neutral cards, questions and actions to the existing message protocol. It owns presentation and transport only. The caller remains responsible for current Task ownership, authorization and single-use business actions. Select behavior from each provider/connector's `/v1/meta` capabilities; no feature switch is required. Interactive transport requires the existing `UV_IM_AUTH_TOKEN`. Without it the Hub disables interactive capabilities, rejects card sends/updates/ingress and filters historical private action handles from both event readers; ordinary messaging retains its released behavior.

## Capabilities and application setup

The Lark adapter supports Card JSON 2.0 sends, `card.action.trigger` callbacks on its existing application WebSocket, and a separate card PATCH API. Other adapters reject cards before sending; callers should provide equivalent text when interactive capabilities are absent. The existing text `MessageUpdate` rejects cards.

In addition to existing send/read permissions and `im.message.receive_v1`, subscribe to `card.action.trigger` and publish the application configuration. Updating a card first verifies the current app's ownership and the exact native chat/thread. A deleted message, permission failure or target mismatch prevents PATCH. Advertised implementation support does not prove a tenant's permissions, published event subscription or live operation.

Limits are 16 KiB per card, eight actions, 8 KiB submitted text and at least 500 ms between updates of one message. The renderer escapes Markdown/plain text and does not accept arbitrary vendor card JSON. Action tokens must be private opaque handles, without commands, credentials or user text.

## Durable callback ingress

Callbacks enter the existing authenticated event stream as `interaction.action`, with typed `Event.interaction` and empty ordinary message bodies. Each native callback event ID is deduplicated independently; distinct clicks on one card are not collapsed by message ID. The actor comes from `operator.open_id`; provider, connector, chat, message, card, presentation revision and app proof digest are preserved. Vendor update credentials are discarded.

The adapter first verifies the app, message and chat with the native message lookup and restores the actual parent/root/thread. It acknowledges only after the EventLog and its parent directory have synced. Startup truncates only an unterminated, uncommitted tail; invalid complete records still fail closed. A durable callback whose ACK was lost remains deduplicated after restart. Missing durable storage, malformed callbacks or failed persistence do not receive a success ACK. The stream retains existing bearer authentication and private-file access. Cross-target identity resolution separately queries the current native chat for card callbacks. Only `chat_mode=p2p` proves a private conversation target; group/denied/unavailable lookups retain the verified actor and full identity set without inheriting an old link target. This needs native chat-info read permission; ordinary direct-message identity resolution keeps its existing proof and gains no extra lookup dependency. Consumers must revalidate business ownership, actor, expiry, generation and action policy; presentation revisions are not business fences.

## Update receipts and recovery

A successful initial send includes `SendResult.interaction`: actual native message ID, card ID, revision and payload SHA-256. The bearer-protected `POST /v1/interaction.update` accepts `interaction-update.v1`, exact native scope, card ID, expected and replacement revisions, complete card/digest and operation ID. Replacement must equal expected + 1.

A private ledger under the existing state root retains scope, committed revision/digest and operation receipts, without card bodies, action tokens or Tasks. It durably reserves the operation before unlocked provider I/O. Replaying the same operation/payload returns its receipt; changed payload, missing records, expiry, stale revision or scope mismatches are rejected before I/O.

An interrupted call, timeout or lost acknowledgement becomes `unknown`, blocking automatic resend and further updates. Lark's successful PATCH response has no message-ID echo; the adapter retains the verified request identity without inventing an echo or claiming native CAS. Exact observation of an unknown Lark PATCH is not supported. Callers should preserve the last proven presentation and reject stale actions using business fences.

Records expire within seven days and are pruned in bounded batches during normal use. An old callback cannot recreate missing authorization. Presentation refreshes never change Task lifecycle or resend failed messages.

Protocol-shaped regression fixtures exercise actual adapter, Hub HTTP, durable EventLog and WebSocket paths, including distinct clicks, persistence before ACK, target mismatch, PATCH without an ID echo, restart replay, interrupted settlement and zero resend after unknown outcomes. Tenant permissions, subscriptions, mobile rendering and live platform operations require deployed-application qualification.

References: [Card JSON 2.0](https://open.feishu.cn/document/feishu-cards/card-json-v2-components/component-json-v2-overview), [card callbacks](https://open.feishu.cn/document/feishu-cards/card-callback-communication), [card updates](https://open.feishu.cn/document/server-docs/im-v1/message-card/patch).
