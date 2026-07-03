# jarvis-box Integration

jarvis-box should treat `uv-im-connector` as an external IM infrastructure dependency.

## Boundary

`uv-im-connector` owns:

- provider credentials and connection lifecycle;
- inbound IM event normalization;
- outbound IM messages;
- provider resource download and internal resource references;
- provider health and capabilities.

jarvis-box owns:

- Target, Task, Run, Workspace, and runtime-agent lifecycle;
- native resume and handoff;
- run artifacts such as `prompt.txt`, `run-context.json`, and `reply.md`;
- team visibility, redaction, status UI, and retry policy.

## Replacement Path

1. Configure `uv-im-connector` providers in service setup.
2. jarvis-box watches `/v1/events/ws`.
3. Each normalized `message.create` event maps to a jarvis-box Target using `provider + connector + channel.id`.
4. jarvis-box resolves resources through `internal_url`, then copies allowed files into the Run attachment directory.
5. jarvis-box starts or continues a Task/Run using its existing runtime-agent model.
6. jarvis-box sends final replies through `POST /v1/message.create`.
7. Legacy provider-specific IM code is removed after the connector client path passes equivalent E2E coverage.

## Required E2E Coverage

- Direct conversation message creates a Run.
- Group/channel mention creates a Run under the same Target model.
- File, image, audio, and video resources resolve through `internal_url`.
- Reply uses `OutboundMessage.Referrer`.
- Provider credentials and raw payload fields do not appear in public status or artifacts.
- Duplicate event IDs do not create duplicate Runs.
- Connector reconnect does not require jarvis-box to know provider-specific state.

## Non-Goals

jarvis-box must not parse provider-native payloads after the replacement. If a provider needs special handling, that handling belongs in `uv-im-connector` and must be exposed through normalized fields or capabilities.
