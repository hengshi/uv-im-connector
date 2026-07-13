# Conformance

Provider conformance is the quality gate for adding channels.

## Required Behavior

Every provider must:

- implement the root `Provider` interface;
- declare capabilities honestly;
- keep provider-native secrets out of sanitized events and resources;
- resolve downloadable inbound provider resources into `internal_url` before event persistence when the provider declares download support;
- expose file/image/audio/video as `ResourceRef` values when the channel provides them;
- support `Referrer` for reply/thread behavior when the channel provides it;
- include the exact reply destination in `referrer.target` for reply-capable inbound events;
- declare at least one of `reply_message`, `proactive_direct`, or `proactive_group` when outbound is enabled;
- declare accepted `target_kinds` honestly for outbound messages and reject unsupported target kinds;
- return explicit errors for unsupported outbound resources or rich elements instead of silently dropping them.

## Test Shape

Provider tests should cover:

- inbound text message normalization;
- inbound group/channel message normalization;
- inbound resource normalization for all supported resource kinds;
- outbound direct message;
- outbound group message;
- outbound reply with `Referrer`;
- explicit errors when a provider API returns an HTTP 2xx response with a failed business status;
- resource download;
- provider health;
- duplicate event key stability.

Providers that require live credentials should split tests into:

- local decoder/contract tests that always run;
- live provider tests guarded by explicit test credentials.

## Current Local Coverage

The repository contains local tests for:

- provider metadata and health shape across all built-in providers;
- inbound decoder normalization across DingTalk, Discord, KOOK, LINE, Mail, Matrix, OneBot, QQ, QQ Guild, Slack, Telegram, WeChat Official Account, WhatsApp, and Zulip;
- WeCom and Lark provider-specific inbound/resource behavior;
- the direct/group/reply/proactive capability matrix for all 16 external providers;
- representative outbound targets, HTTP shapes, and business ACK parsing for bearer token, bot token, URL token, and form-encoded providers;
- webhook routing from `/v1/webhook/{provider}/{connector}` into the normalized event log;
- resource download before event persistence and public resource sanitization.
