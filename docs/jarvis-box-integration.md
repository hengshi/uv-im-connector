# jarvis-box Integration

jarvis-box should treat `uv-im-connector` as an external IM infrastructure dependency.

## Boundary

`uv-im-connector` owns:

- provider credentials and connection lifecycle;
- inbound IM event normalization;
- outbound IM messages;
- provider resource download and internal resource references;
- provider health and capabilities;
- connector service version and protocol version.

jarvis-box owns:

- Target, Task, Run, Workspace, and runtime-agent lifecycle;
- native session continuity;
- run artifacts such as `prompt.txt`, `run-context.json`, and `reply.md`;
- team visibility, redaction, status UI, and retry policy.

## Replacement Path

1. Configure and deploy `uv-im-connector` as an external service.
2. jarvis-box reads `/v1/meta` at startup and verifies `service == "uv-im-connector"` plus a supported `protocol_version`.
3. jarvis-box watches `/v1/events/ws`.
4. Each normalized `message.create` event maps to a jarvis-box Target using `provider + connector + channel.id`.
   Optional `channel.name`, `user.name`, and `user.display_name` are presentation snapshots only; jarvis-box must not use them for Target identity, ACL, dedupe, or reply routing.
5. jarvis-box resolves resources through `internal_url`, then copies allowed files into the Run attachment directory.
6. jarvis-box starts or continues a Task/Run using its existing runtime-agent model.
7. jarvis-box sends final replies through `POST /v1/message.create`.
8. On failure, jarvis-box persists the normalized `failure`, applies its own bounded retry policy, and keeps delivery state separate from agent Run state.

### Quoted Lark messages

When no `UV_LARK_BOT_OPEN_ID` or `UV_LARK_BOT_UNION_ID` is configured,
the Lark connector resolves its own open ID through the bot-info API before
opening its WebSocket. Identity lookup failures stop provider startup with an
explicit error; they must not silently turn bot mentions into unaddressed events. Explicit identity settings still take precedence.

For an addressed Lark message with a parent message ID, the connector reads that
selected parent with its bot identity and verifies the returned message and chat
IDs. It exposes the quoted text and source ID as a `quoted-message.txt` resource,
and adds the selected parent's files to the same resource list. When the verified
selected response has a native `parent_id`, it can append older replies to that
same reference file. Each extra read must uniquely return the requested ID in the
same chat, be non-deleted and usable, and have the exact native `thread_id` of the
selected quote (including an empty value). The connector follows only those native
parent IDs; it does not infer an edge from callback thread data, root IDs, forward
relations, text, or a list/history request. Up to four extra reads share five
seconds and append at most 24 KiB of complete ancestor node blocks. An unavailable
or rejected ancestor ends that chain with a safe reason while retaining the
selected quote and earlier accepted nodes. Ancestor media are kind/name
descriptions only: they are not `ResourceRef`s, elements, resource IDs or implicit
downloads. Older merged forwards remain a placeholder with direct-quote guidance;
their child messages and files are not expanded. The Hub resolves selected quoted context and provider files into its
own resource store before event persistence; the optional provider-local store is
not a second cache owner. File downloads use each resource's source message ID.
The current command text, addressing decision and reply target are unchanged;
quoted text is reference material, not a new command. These bounded ancestor
rules do not limit the selected quote, attachment downloads or provider response
bytes, and do not create a group-history query.

An unavailable, deleted or inaccessible parent produces a resource with a
normalized `quoted_message_*` error; a file download failure remains
`download_failed`. Consumers must preserve these errors in the Agent input rather
than reporting that the user supplied no attachment. Existing clients can consume
this through the resource protocol without gaining provider credentials.

A successfully prepared `quoted-message.txt` can carry optional
`referenced_message_ids`: only exact native IDs of complete earlier-reply nodes
actually rendered in that context file. It is producer-side material identity,
not a resource locator, access grant, message binding or Task authority. Jarvis
may omit exact matching native-window items only after it has obtained that exact
context attachment for the current Run and verified its bytes against the same
sanitized `ResourceRef.sha256`, while also retaining the resource's event,
provider, connector and explicit-parent correspondence. Missing, legacy, errored
or unverified provenance leaves background candidates unchanged and never blocks
the Turn.

Lark `folder` messages are recognized as file resources, including when quoted.
Recognition does not guarantee that Lark's resource API can download the folder.
If it fails, the context identifies the original folder and asks for a ZIP archive
or individual files. Do not claim that folder contents were retrieved based only
on its metadata or a successful local transport stub.

Quoted `merge_forward` responses include attributable child text and supported
resources, including nested forwards. Downloads use the selected top-level
container message ID; child IDs are positions, not download or Task authority.
Quote elements retain the hierarchy while the complete flat resources remain
usable by current Jarvis attachment preparation. Resource leaves correlate only
by opaque IDs or unambiguous internal URLs. Missing/conflicting relations are
unavailable rather than guessed from names or private keys. Invalid child rows
are omitted with an unavailable indication without losing valid siblings.
`quoted-message.txt` renders each accepted root/forwarded node under that same
hierarchy with its escaped native message/type/sender tuple, the raw native create
time when present, its quote-specific body representation, and each owned public
`ResourceRef.ID`, kind and quoted name. An explicitly quoted Lark `post` selects
one direct or deterministic locale body, prefers nonempty `content_v2`, retains
legacy and v2 native media references once per kind/key, and preserves title,
links, stable mentions, Markdown, bounded language-tagged code, separators and
emotion as reference material. Structured image/media keys and provider metadata maps
are not rendered as body text; user-authored text, code, URLs and Markdown are
retained without blanket string-based redaction. Top post files are normal resources; post-zone folders only
receive ZIP/individual-file guidance and are not fetched. Unsupported or malformed
parts remain visible but cannot by themselves make an empty quote available. This
does not offer style/full-schema fidelity or Markdown-URL media downloads, and
does not change ordinary command decoding. `Container message ID` names the
selected top-level quote/download container; `Upper message ID` appears only when
the platform supplied the immediate native relation. A missing sender is `unknown`;
a missing time is omitted. jarvis-box may use the public resource ID to make an
attachment correspondence visible, but must obtain bytes through the normalized
resource and must not treat cache facts, child IDs, names or senders as Task, Run,
conversation, ACL or identity authority.
`interactive` cards still provide summary context without child resources.
Neither representation creates group history or new Task selection rules.

### Interactive identity and text fallback

The neutral question flow can use either a provider-confirmed native message or
the explicit `card-instance.v1` profile. The latter confirms an existing CardID
as a callback-addressable WeCom/DingTalk instance without inventing a message ID.
Jarvis must persist this scoped card ACK under its existing question effect and
recheck the original actor, current native audience, presentation and Task/Run.
It must not put the ACK in a quote binding or external-notification alias. An
authenticated callback can observe unknown delivery through the same business
transaction; it does not authorize resending the card.

WeCom fixed-button cards reuse bot credentials. DingTalk native group cards need
a published application template; direct chats and unsupported whole questions
use complete text. Only a typed zero-attempt transport refusal permits switching
the same complete question to text; unknown or partial sends never do. Templates,
limits and callback profiles are owned by [interactions](./interactions.md).
Both providers reuse the Hub's retained received context and event-bound private
identity proof; those are not native full-history or quoteable-publication claims.

### Native group background

After its existing ACL, native addressing and Task selection, Jarvis may request a
[recent conversation window](./architecture.md#近期会话窗口) for the persisted
group trigger. This is separate from explicit quote enrichment. The trusted
infrastructure request contains only provider/connector and trigger event/message
IDs; no Agent-selected chat/thread/time. Require the exact provider's
`conversation_window_version=v1` and the existing service Bearer credential.
When no native window reader capability exists, that version can instead describe
the Hub's retained received-ingress fallback: exactly one metadata page of at most
10 sequence-proved items, with generation/trigger-digest/lower-bound source proof
and no materialization handle or download. A native permission, rate, timeout or
other reader result is never replaced by that fallback. Unsupported capability
preserves explicit inputs and produces an unavailable background note, without
trying another scope or scanning the full event log.

The Lark reader verifies the native trigger and uses its real thread identity,
then returns one time-anchored page with sanitized text, sender/time provenance,
coverage and media descriptors. It never downloads background media. Jarvis
keeps a bounded reference file in the selected Run rather than putting history
arrays into the current command. After verified local composition removes every
item already rendered by a verified quote context, its local reference may say
`none`; the native candidate count, boundary omissions, `has_more` and any
`scan_limited` condition remain unchanged, and Jarvis does not refill, paginate,
retry or widen the query. A denied, limited or unavailable window stays visible
and does not block explicit materials. Direct/control/unaddressed inputs and
source-notification preparation do not request it. Window reads do not change
Task binding, authorize another source, start background work, or create a new
history store or per-group setting.

### Explicit scoped search and materialization

The connector exposes `conversation_search_version=v1` for a consumer's
current-Run-only lookup. It is distinct from the automatic window: Jarvis must
authenticate an internal request with the current
Run capability, send its SHA-256 as the audience binding together with the
persisted trigger digest, and request only local time/sender/keyword narrowing.
Neither an Agent nor a user supplies a chat, topic, provider continuation, or
service Bearer. A returned cursor is an opaque, short-lived connector value;
it is invalid after a Run capability rotation and must not be placed in a
prompt, public status, or durable Run artifact.

Native output identifies source/provider/conversation/verified thread,
immutable anchor, effective time bounds, and bounded scan coverage. It makes
`none`, scan limits, cursor expiry, permission, rate and unavailable
states explicit rather than claiming complete group-history absence. Connector
API support alone does not establish which Jarvis release consumes it or qualify
an installed client's capability handoff.

When a search row has media, a trusted caller may use the additive
`conversation_materialize_version=v1` capability. The row contains an opaque
`materialize_handle`, per-resource `source_epoch`, readable descriptor and
provenance. `POST /v1/conversation.materialize` repeats the same trigger/audience
proof and accepts no source-message override, URL, key, path or native scope.
The handle already binds its source; `source_epoch` can be omitted, while an
explicit value must match. The connector caches complete, verified files only.
Interrupted reads can retry after scope revalidation instead of becoming a
permanent unknown attempt. Replays of completed handles preserve the original
bytes and source, and different handles do not share a private-resource cache.

Jarvis may pass this opaque handle through its current-Run Agent channel while
keeping service credentials and trigger/audience fields server-owned. It checks
current authorization before and after transport, then atomically publishes the
verified complete file as Run-local material. It does not mint a second handle or
persist a Task query/materialization lifecycle. Only a revalidated `ready`
result carries an internal resource reference; the Agent receives the verified
Run-local path, never a provider reference or service token.

The separate service capability `conversation_context_query_version=v1` permits
`source="received"` on the same search request. This is connector-retained ingress
only, not older provider history. Its result and resources carry generation,
digest and sequence proof. The caller validates the search result and the Hub
retains that proof with the opaque handle. A changed generation, missing/deleted
entry or lower-bound limit invalidates the source. It never authorizes a wider
native lookup or a retry with a provider key.

### Event-bound inbound identity

Authenticated consumers can call `POST /v1/identity.resolve-inbound` with only
`provider`, `connector` and `event_id`. The connector reads that exact durable
event and returns its native actor, canonical digest and the complete current
set of non-revoked business identities for the same provider, connector and
native user. The result has no preferred identity; Jarvis must calculate a
unique permitted intersection with its original Task admission authority.
Names, an Agent-supplied actor and an arbitrary conversation ID are not inputs.

A direct event additionally reuses the adapter's current private-participant
proof. Room adapters can return a conversation target without treating it as a
user target. A changed private-room audience, missing event, explicit non-user
actor, absent link or uncertain link state returns an unavailable error. A group
actor can establish identity evidence without obtaining a private reply target.
The identity set, native target and link revisions remain service-private and
must not enter public Task views, logs or user messages. This endpoint creates
no Task grant and does not make identity revocation atomic with a Jarvis input
transaction; the consumer must bind a short-lived assertion to the exact event
and recheck authorization before subsequent effects.

## Release Boundary

jarvis-box does not host, spawn, or auto-update `uv-im-connector`. A connector bugfix that keeps `protocol_version` compatible is deployed by upgrading the connector service. jarvis-box needs a dependency bump and release only when it consumes new Go client/API behavior or when the connector protocol becomes incompatible with the supported protocol set.

## Required E2E Coverage

- Direct conversation message creates a Run.
- Group/channel mention creates a Run under the same Target model.
- Optional sender/conversation display names survive event-log replay and appear in status without replacing provider-native IDs.
- Startup fails clearly when `/v1/meta` reports a wrong service or unsupported protocol version.
- File, image, audio, and video resources resolve through `internal_url`.
- Addressed authorized Lark groups receive scoped, pre-trigger native background as a Run reference; wrong-topic/future items and media downloads are absent, and unavailable/limited coverage remains visible.
- Reply uses `OutboundMessage.Referrer`.
- Provider credentials and raw payload fields do not appear in public status or artifacts.
- Duplicate event IDs do not create duplicate Runs.
- Connector reconnect does not require jarvis-box to know provider-specific state.
- Every provider send failure reaches jarvis-box through the same normalized failure contract.
- Safe retryable rejection/not-attempted failures are retried without parsing text; ambiguous delivery is not replayed automatically.
- A failed final reply reuses the persisted provider-ready result and does not rerun the agent.

## Non-Goals

jarvis-box must not parse provider-native payloads after the replacement. If a provider needs special handling, that handling belongs in `uv-im-connector` and must be exposed through normalized fields or capabilities.
