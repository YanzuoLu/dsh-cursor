# Upstream provenance and protocol scope

## Referenced and vendored sources

- [NOirBRight/dsh-llm-cursor](https://github.com/NOirBRight/dsh-llm-cursor), fixed revision `119b04e28fba86571488f88d47fcad8490814f36`, MIT. The generated `src/wire/vendor/agent_pb.ts` is vendored intact apart from a provenance header as [agent_pb.ts](<src/protocol/vendor/agent_pb.ts>). Framing, KV/context handshakes, structured conversation turns and history informed this implementation. The original generated file says protoc-gen-es v2.13.0, `agent.proto`, package `agent.v1`. Its exact underlying private proto provenance is not established. This is not represented as an official or current Cursor schema.
- [can1357/oh-my-pi](https://github.com/can1357/oh-my-pi), fixed revision `650f3a108229ee11bf68ba3377f4e8c3391cd424`, MIT. Reference files were `packages/ai/src/providers/cursor.ts`, its `cursor/` helpers, and `packages/catalog/src/{wire/cursor.ts,discovery/cursor-proto.ts}`. The external-executor restriction, modern root-message tool records, typed denials and client-version headers informed this implementation. No Bun, native bindings, SDK, tool executor, proxy helper, retry machinery or server was copied.

- [oakimov/cursor-opencode-provider](https://github.com/oakimov/cursor-opencode-provider), fixed revision `b2d2cd84df44edb6ac42e8749d4212974a0faf70`, MIT. The standard `RequestContext` global-rule instruction carrier follows the public schema and `src/context/build.ts`. Account-specific Run routing, the 1 KiB gzip threshold, ordered compression, and 3-second exec heartbeats were cross-checked against `src/agent-url.ts`, `src/transport/connect.ts` and `src/protocol/request.ts`. Terminal checkpoint persistence is implemented through DSH's own storage domain, not OpenCode's filesystem integration. The text is DSH's existing instruction content, without OpenCode integration or filesystem rule loading.

[The supplemental descriptor](<src/protocol/schema.ts>) adds `InteractionUpdate.tool_requests_listed = 27`, `ToolRequestsListedUpdate.call_count = 1`, optional int64 `TurnEndedUpdate` token counters at fields 1 through 5, the run-id/context-injection messages, and `RequestContext.mcp_meta_tool_options = 34` / `mcp_info_complete = 36` documented by oakimov. The vendored Noir descriptor itself remains unchanged. The complete schema is not claimed to match every current Cursor field.

## Split transport contract

The installed `@cursor/sdk` 1.0.27 was inspected only as a wire-contract reference. It is not imported, executed or added as a dependency. Its `executor-common.ts` bundle maps bidi Run to `RunSSE`, uses `x-request-id` plus `x-cursor-streaming: true`, and appends messages through `aiserver.v1.BidiService/BidiAppend` with monotonically increasing sequence numbers and up to 16 parallel requests. Its protobuf descriptors define `BidiRequestId.request_id = 1`, and `BidiAppendRequest.request_id = 2`, `append_seqno = 3`, `data_binary = 4`.

The independently written [split transport](<src/protocol/split.ts>) uses those fields with Node HTTP and Connect framing. Independent-byte tests and live image-history probes cover the contract. A 64-position acknowledgement window is a tested local backpressure policy; an unbounded sequence gap caused real server failures during long image uploads. No SDK implementation or generated SDK code is shipped.

## Checkpoint and cache lifecycle contract

SDK 1.0.27's `RunExecutorOptions.blobEncryptionKey`, `agent-session-runner` and `blob-encryption-key-header` carry a stable, per-agent 32-byte random key as 64 lowercase hex digits in `x-blob-encryption-key`. Its agent metadata persists and restores this key. This provider owns the corresponding per-session key in private native storage, separately from the login grant. The key is neither a telemetry field nor a caller header override. Live validation confirmed zero historical KV fetches on a subsequent Run when the stable key was supplied.

The SDK stream splitter continues reading checkpoints after `turnEnded` and marks terminal checkpoints ineligible for same-turn retry. Its `onCheckpoint` persistence chain is awaited before final publication. This provider likewise drains the terminal stream, persists each checkpoint, and distinguishes completed continuation from interrupted-request recovery. A bounded six-second terminal drain avoids leaving a completed stream indefinitely parked; a transport close after a saved terminal checkpoint is not a new retry opportunity.

Recovery is adapted to DSH's external execution ownership. Only an exact unchanged, uncommitted host request before any real host tool call or context injection may replay a checkpoint and its already-checkpointed text. Host-side action barriers force recovery from the durable DSH history instead. User Stop revokes interrupted-step recovery. This is an explicit native-host constraint, not a claim to reproduce every SDK internal executor path.

## Runtime boundaries

The public [protocol entry](<src/protocol-entry.ts>) exports Run/catalog operations, conversation creation and snapshot codecs, and `CursorProtocolError`. The [build helper](<scripts/build-protocol.mjs>) emits one Node ESM [bundle](<lib/protocol.js>) with native DSH and Bufbuild imports external. Importing it does not authenticate, read settings or open sockets.

Inference uses public Node HTTP/2 and Connect/protobuf on `/agent.v1.AgentService/Run`, or the same stateful Run protocol through `/agent.v1.AgentService/RunSSE` and `/aiserver.v1.BidiService/BidiAppend` for large histories. Bounded unary metadata calls use the existing host `fetch`, preserving its HTTP/1 and routing hooks; callers can explicitly choose HTTP/2 unary transport. Metadata and split transport use `https://api2.cursor.sh`; the direct Run origin is resolved from `GetServerConfig.agentUrlConfig` for the current account. Explicit endpoint overrides bypass discovery. Proxy options fail with `UNSUPPORTED_PROXY` rather than bypassing an explicit proxy. A bidirectional Run is never replaced by a stateless HTTP/1 inference fallback. The client-version compatibility header comes from the fixed OMP source, not an installed Cursor executable.

Only actual `execServerMessage.mcpArgs` requests may become host tool events. Display updates, model text and native exec requests are not executable instructions. Tool names and JSON schemas are the exact advertised host set, including names overlapping native tools. RequestContext advertises names through MCP meta-tool descriptors; full definitions are returned by `mcpStateExecArgs`. The legacy `RequestContext.tools` list is omitted to preserve cross-turn prompt-cache continuity. MCP here describes wire definitions only. No MCP listener/server exists. Each opaque call key binds a precise pending raw exec envelope. Replies preserve text, image bytes, MIME type and MCP `isError` using `McpSuccess.content` and `McpSuccess.is_error`, even for tool-level failures. Native operations are rejected through generated response schemas where a denial exists. Unknown exec/query variants or variants without a safe denial close the Run with an explicit unsupported error. Nothing autoapproves unknown fields.

The background control pump, 5-second connection heartbeats and 3-second pending-exec heartbeats outlive individual event-reader leases. Terminal exec results are followed by their stream-close control frame. Outgoing messages of at least 1 KiB are gzipped. Direct streams serialize writes; split streams reserve ordered sequence numbers before bounded parallel compression and upload. Only KV replies return on queue admission; exec results/actions wait for individual acknowledgements, and terminal completion drains outstanding acknowledgements. Returning an iterator does not cancel a Run. The true request signal, `cancel()` or `close()` disposes that Run. Frames, gzip inflation, events, queued writes, blobs and pending calls are bounded. EOF without `turnEnded` is a transport failure. Session/account ownership and rebuilding after disconnection belong to the adapter. Rebuilding does not guarantee exactly-once tool execution.

Structured root JSON and conversation-turn blobs carry user/assistant/tool history, tool-call/result identities and images. System and developer instruction text is carried by the standard `RequestContext.rules` global rule. A full history rebuild also seeds system and rule records in the root. A resumed conversation echoes the server's checkpoint. No provider-authored persona, tool-boundary manifest, filesystem rule scan, special skill prompt or character-budget truncation is inserted. A tool-result tail uses `resumeAction`, not a repeated user message. Invalid historical call IDs are deterministically hashed consistently on both sides. Synthetic live validation covers tool-image delivery and a subsequent image question, but does not establish every model's behavior or service limits. SelectedImage, global rules and live MCP image protobuf encoding are directly schema-backed.

`RequestedModel.parameters` carries explicit `{key,value}` selections as wire `{id,value}` strings. The adapter quietly omits generic sampling controls for which this schema has no verified request field. `ConversationTokenDetails.max_tokens` is context metadata, not an output-token request limit.

Offline tests use loopback H2 and split-HTTP fake services, real Bufbuild encoding/decoding and a fixture for DSH attribution only. No actual Cursor auth, model or catalog call, installed SDK, real credentials/configuration, process-global patch, subprocess runtime, or private profile is used. Separate isolated live validation uses the explicitly authorized account. Its measurements do not imply universal compatibility or a guaranteed latency.

## MIT notices

Copyright (c) 2026 NOirBRight

Copyright (c) 2026 Oleksiy Akimov

Copyright (c) 2025 Mario Zechner

Copyright (c) 2025-2026 Can Bölük

Copyright (c) 2026 Stencil Labs, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
