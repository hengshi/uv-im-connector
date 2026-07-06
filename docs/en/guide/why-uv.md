# Why It Exists

`uv-im-connector` exists because IM integration has become infrastructure for applications, bots, agents, and automation systems, but many projects still have to rebuild inbound events, outbound messages, file downloads, group chat semantics, threads, referrers, authentication, and connection lifecycle inside their own product.

That repetition is not a sign that those projects are wrong. It follows naturally from their product boundary. Once a system receives user intent from IM and sends results back to IM, it must solve connection, auth, parsing, attachments, permissions, conversations, and delivery. Without a reusable connector contract, that work gets embedded into every product.

## Why This Keeps Happening

IM is not just a simple transport. For the caller, it is also:

- a user entry point: who spoke, whether the bot was mentioned, and whether a group message should trigger work;
- a permission boundary: which users, groups, tenants, and bot identities may access the system;
- a conversation boundary: which channel, thread, task, run, or product workflow owns the message;
- a file boundary: attachment download, temporary URLs, secrets, storage, and redaction;
- a delivery boundary: progress messages, final replies, threaded replies, message references, and retry behavior;
- an operations boundary: long connections, webhooks, public callbacks, private deployment, health checks, and credential rotation.

If these concerns are not expressed as a standalone protocol, every product ends up coupling them to its own agent loop, task model, UI, permissions, and deployment model.

## Why These Projects Implement Their Own

The notes below are based on the public README / Docs positioning of these projects. They are about product boundaries, not a judgment on implementation quality.

| Project | Public positioning | Why an embedded IM connector is natural |
| --- | --- | --- |
| [OpenClaw](https://github.com/openclaw/openclaw) | A local personal AI assistant that connects to the chat channels users already use through a Gateway. | Its product experience is an assistant that is always reachable from IM. Channels, pairing, allowlists, sessions, skills, and the local gateway are one user experience, so connector logic naturally lives in the gateway. |
| [Hermes Agent](https://github.com/nousresearch/hermes-agent) | A self-improving agent with a learning loop, reachable through Telegram, Discord, Slack, WhatsApp, Signal, and CLI. | It cares about conversation continuity, memory, scheduled automations, skill creation, and agent execution. IM is not only transport; it is part of the learning and delivery loop. |
| [Multica](https://github.com/multica-ai/multica) | A managed agents platform where agents behave like teammates that receive tasks, report blockers, and compound skills. | Its core model is team, task, issue, daemon, and agent lifecycle. When messages enter the system, they naturally map to platform tasks, members, permissions, status, and collaboration flows. |
| [cc-connect](https://github.com/chenhg5/cc-connect) | A bridge from local AI coding agents to Feishu/Lark, DingTalk, Slack, Telegram, Discord, LINE, WeChat Work, and other messaging platforms. | It offers an end-to-end remote-control loop for local agents. Platform adapters, agent adapters, permissions, progress display, cancellation, resume, and file handling evolve together. |

The shared pattern is that IM connector logic is part of the main product experience. That helps a project deliver a complete loop quickly, but it also makes the connector hard for unrelated systems to reuse.

## The Boundary uv-im-connector Extracts

`uv-im-connector` extracts the IM layer into its own component. The goal is not to build another agent platform; the goal is to provide a universal IM connector that everyone can use directly.

It owns:

- provider authentication and connection lifecycle;
- inbound event normalization;
- outbound message delivery;
- file, image, audio, and video resource handling;
- thread, referrer, addressed, capabilities, and health semantics;
- provider conformance tests.

It does not own:

- an agent loop;
- bot behavior;
- product workflows;
- task, run, or workspace lifecycle;
- user-facing UI;
- business permission, approval, escalation, or writeback policy.

That boundary lets any caller reuse the same IM layer: personal assistants, enterprise bots, coding agent platforms, operations automation, support automation, and approval automation.

## The Desired Outcome

New projects should not need to ask how to download Lark files, parse WeCom group mentions, map Slack `thread_ts`, or persist Telegram file URLs. They should only need to:

1. Configure provider credentials.
2. Subscribe to standard `Event` objects.
3. Copy allowed `ResourceRef` objects.
4. Run the business, bot, or agent workflow.
5. Reply through `OutboundMessage`.

Adding a provider should not change caller code. Provider differences should live in adapters and capabilities, not leak into every product.

## When to Use uv-im-connector

Use it when:

- you are building an application, bot, agent service, or automation system that needs multiple IM providers;
- you want WeCom, Lark, Slack, Telegram, Discord, DingTalk, Matrix, OneBot, QQ, WhatsApp, and similar providers to share one event and resource model;
- you do not want provider-native payloads, file download logic, send APIs, and auth details in product code;
- you want new providers to pass explicit conformance tests.

Do not use it when:

- you only need a complete personal assistant or coding agent product rather than a connector;
- you expect the connector to own agent execution, task orchestration, product UI, or team workflows;
- you only connect one provider and are comfortable binding directly to that provider SDK.

`uv-im-connector` is IM infrastructure. It should help more projects stop rebuilding connectors, not become another bridge tied to one product workflow.
