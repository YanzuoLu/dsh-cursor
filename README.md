# DSH Cursor

Cursor subscription models as an in-process DeepSeek Harness provider.

- No Cursor SDK, CLI, ACP agent, local model proxy or MCP server.
- Browser sign-in through DSH native authorization. One grant in the native credential store.
- Account-scoped model discovery, reasoning selections and image capability metadata.
- Native DSH tools, approvals, sandbox and durable tool records. No additional model tools. Cursor receives a names-only MCP directory; full schemas are returned through its MCP-state request rather than repeatedly changing the Run's tool preamble.
- A Cursor Run stays alive across tool handoffs. Actual tool results resume the original execution ids, with execution heartbeats while DSH tools run.
- Small histories use direct HTTP/2. Histories of at least 4 MiB use Cursor's native RunSSE/BidiAppend transport with bounded parallel gzip uploads. Image bytes and context are unchanged.
- Stable per-session blob-cache keys and checkpoints survive plugin/process restarts through DSH's native storage domain. Warm Runs can reuse Cursor's remote blob cache without retransmitting historical images.
- Existing DSH file attachments, request image versions and durable image offloading are preserved.
- Opening settings, reading status and listing cached models do not trigger Cursor requests.

## Install

```sh
dsh plugin --profile web add github:YanzuoLu/dsh-cursor
```

The GitHub package includes the built client and content-addressed host entry. No local checkout is required at runtime. Keep the provider loaded while Cursor requests are active; changing only the installation source does not require restarting it when the installed host artifact is unchanged.

## Use

Enable the plugin in the web profile, refresh the GUI, then open **Settings → Cursor**. Select **登录** and open the returned browser link. Successful authorization refreshes the model directory. Choose a model from the **Cursor** provider group. **刷新模型** explicitly reloads the directory without signing in again.

The plugin does not open applications, import desktop/CLI credentials, change the default model or sign in automatically. Logging out removes only its own native grant.

## Model picker

For a model family with explicit context metadata, the picker excludes variants below that family's largest declared window. It does not invent unavailable routes or infer limits from model names. Genuine Fast and other non-context lanes remain distinct when they support the full window; known reasoning variants stay in the reasoning selector.

Display names use plain spaces between model names and variant labels such as `Thinking` and `Fast`, and omit context-size annotations and `No ZDR` labels. This is a presentation preference, not a change to the upstream data-retention policy. Legacy entries without structured variant metadata remain separate unless an explicit, verified compatibility table is provided.

Fable 5 and Fable 5.1 use such a table for their observed Low/Medium/High/Extra High/Max route ids. The picker shows each family in normal and `Thinking` modes, with effort selected separately. Only currently advertised, capability-compatible routes are grouped. Dispatch still uses the exact original route id, with no invented protocol parameters. Previously selected effort-specific ids preserve their default effort; an explicit effort choice overrides it within the same mode. The two modes are distinct Cursor routes; the normal label does not assert that the model's internal thinking is disabled. Anthropic documents Fable 5/5.1 as always-adaptive thinking, and effort controls token spending separately. Cursor does not document the exact backend mapping of its additional Thinking route. No claim is made about fixed reasoning budgets or one route always outperforming the other.

Saved ids for removed short-context rows resolve to the matching full-context lane where one is advertised. Fast/other lane parameters are not silently changed. These compatibility ids are not listed as additional models, and do not dispatch the old shortened-context route.

## Scope

This is an initial, unofficial implementation of Cursor's private agent protocol. It is not a public raw inference API. Cursor has [warned that unofficial clients can lead to account restrictions](https://forum.cursor.com/t/does-using-oh-my-pi-s-cursor-provider-or-an-openai-compatible-proxy-to-the-same-endpoints-violate-cursor-s-tos/167778/5).

The protocol may change. Offline contracts are not proof that a particular subscription, endpoint or model currently works. Actual sign-in and live model/tool validation require separate authorization.

Direct HTTP/2 resolves the account's regional Run origin. Split transport uses `RunSSE` and binary `BidiAppend` on Cursor's API gateway, not an OpenAI proxy or external component. At most 16 appends are in flight, and no more than 64 sequence positions can precede the oldest unacknowledged append. Tool results and steering await acknowledgements. A failed append is not internally resent. Explicit proxy settings are rejected rather than silently bypassed. OAuth and unary model metadata use the host's existing fetch transport.

The optional `runTransport` setting on the `cursor` plugin accepts `auto` (default), `http2`, or `split`. Auto is selected before each new Run from the byte size of the history referenced by its checkpoint; it never changes an active Run's transport. The byte threshold is a client performance policy, not an upstream context limit. A cold or expired remote blob cache still requires history upload; a warm blob cache can avoid that independently of the model's inference cache.

A transient failure can resume an eligible checkpoint for the exact same uncommitted host request. Checkpointed text/reasoning is replayed before new output. Two recovery attempts without a newer checkpoint stop automatic recovery. Once a real host tool call or injected message has crossed the boundary, recovery instead rebuilds from DSH's committed history rather than replaying an earlier execution state. User Stop revokes interrupted-step recovery. Healthy handoffs keep the original Run. No exactly-once recovery across process restarts is claimed.

DSH's effective context remains authoritative. Tool-result pruning, image offloading, instruction changes and summary compaction invalidate a mismatched checkpoint, including between user turns. Rebuilds while tools are pending require every emitted invocation to have an exact recorded result. The plugin does not silently retain content that DSH has removed.

The private `cursor_conversations` domain persists checkpoint updates, eligibility and the SDK-style `x-blob-encryption-key`. The key is not a login token and is never included in diagnostics. It remains stable across user turns, transport selection and checkpoint reseeds; changing account or workspace establishes a new binding. Records expire after 24 hours, with a 64-session cap. Immutable compressed KV blobs are stored once and shared by checkpoint references. Serialized saves and garbage collection prevent dangling references. The native per-record backend bootstraps prior whole-file records without deleting the old file.

`turnEnded` starts terminal draining rather than immediately closing the connection. Later checkpoints and KV writes are retained before publishing done. A saved terminal checkpoint is final, not an invitation to resume the already-completed turn. A terminal checkpoint reporting zero tokens does not erase the last measured context occupancy. Older snapshots without a proven host prefix establish a fresh authoritative baseline once.

Checkpoint reuse is not the same as an upstream model cache hit. Final assistant replay metadata records opening mode, transport, history bytes, transmitted bytes, last large-payload acknowledgement time, first-output timing and aggregate Cursor usage counters separately from DSH's per-step context occupancy. Backend latency can still vary even when cache reads are high.

Native titles and compaction use isolated, tool-free text requests. Closing an ordinary turn does not cancel an independently running title request. Cursor-hosted native execution and unknown permission queries are never locally executed.

## Development

```sh
pnpm install --ignore-scripts --config.auto-install-peers=false
pnpm run build
```

The only runtime dependency outside DSH is the JavaScript protobuf codec. The build emits an immutable content-addressed host entry. DSH core files are not patched. Offline tests and intermediate artifacts stay in the ignored project cache.

Source provenance and retained upstream licenses are in [UPSTREAM.md](UPSTREAM.md). MIT.
