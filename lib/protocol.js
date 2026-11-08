/* dsh-cursor in-process wire core. MIT upstream notices in UPSTREAM.md. */

// src/protocol/run.ts
import { createHash as createHash4, randomBytes, randomUUID as randomUUID3 } from "node:crypto";
import { create as create5, fromBinary as fromBinary2, toBinary as toBinary4, toJson as toJson2 } from "@bufbuild/protobuf";

// src/protocol/blobs.ts
import { createHash } from "node:crypto";

// src/protocol/errors.ts
var CursorProtocolError = class extends Error {
  constructor(code, message2, status) {
    super(message2);
    this.code = code;
    this.status = status;
    this.name = "CursorProtocolError";
  }
};
function safeError(error, fallback = "Cursor HTTP/2 transport failed") {
  return error instanceof CursorProtocolError ? error : new CursorProtocolError("TRANSPORT", fallback);
}
function httpError(status) {
  const code = status === 401 || status === 403 ? "AUTH" : status === 429 ? "RATE_LIMIT" : status >= 500 ? "SERVER" : "INVALID_REQUEST";
  return new CursorProtocolError(code, `Cursor request returned HTTP ${status}`, status);
}
function serverDetail(text) {
  if (typeof text !== "string" || !text.trim()) return "";
  return text.replace(/bearer\s+\S+/giu, "[redacted]").replace(/[A-Za-z0-9_\-.]{32,}/gu, "[redacted]").replace(/\s+/gu, " ").trim().slice(0, 300);
}
function statusError(code, message2) {
  const known = {
    unauthenticated: "AUTH",
    permission_denied: "AUTH",
    "16": "AUTH",
    "7": "AUTH",
    resource_exhausted: "RATE_LIMIT",
    "8": "RATE_LIMIT",
    canceled: "ABORTED",
    cancelled: "ABORTED",
    "1": "ABORTED",
    deadline_exceeded: "TIMEOUT",
    "4": "TIMEOUT",
    invalid_argument: "INVALID_REQUEST",
    "3": "INVALID_REQUEST"
  };
  const detail = serverDetail(message2);
  return new CursorProtocolError(
    known[code] ?? "SERVER",
    `Cursor returned ${code || "an unknown"} error${detail ? `: ${detail}` : ""}`
  );
}
var LIMITS = Object.freeze({
  frameBytes: 16 * 1024 * 1024,
  inflatedBytes: 32 * 1024 * 1024,
  eventBytes: 8 * 1024 * 1024,
  eventCount: 2048,
  blobBytes: 256 * 1024 * 1024,
  blobCount: 65536,
  /** A conversation store above this size is reset and rebuilt from host history. */
  conversationBytes: 192 * 1024 * 1024,
  pendingCalls: 128,
  pendingBytes: 16 * 1024 * 1024,
  seenCalls: 8192,
  toolCount: 2048,
  toolBytes: 16 * 1024 * 1024,
  queuedWriteBytes: 32 * 1024 * 1024,
  queuedWrites: 256,
  handshakeMs: 15e3,
  unaryMs: 3e4,
  idleMs: 12e4,
  terminalDrainMs: 6e3,
  heartbeatMs: 5e3
});
function limitError() {
  return new CursorProtocolError("LIMIT_EXCEEDED", "Cursor protocol buffer limit exceeded");
}

// src/protocol/blobs.ts
var BlobStore = class {
  #items = /* @__PURE__ */ new Map();
  #bytes = 0;
  get bytes() {
    return this.#bytes;
  }
  get(id) {
    return this.#items.get(Buffer.from(id).toString("hex"));
  }
  put(data) {
    const id = new Uint8Array(createHash("sha256").update(data).digest());
    this.set(id, data);
    return id;
  }
  set(id, data) {
    if (id.byteLength > 128) throw limitError();
    const key = Buffer.from(id).toString("hex");
    const previous = this.#items.get(key);
    const bytes = this.#bytes - (previous?.byteLength ?? 0) + data.byteLength;
    if (!previous && this.#items.size >= LIMITS.blobCount || bytes > LIMITS.blobBytes) throw limitError();
    this.#items.set(key, Uint8Array.from(data));
    this.#bytes = bytes;
  }
  entries() {
    return [...this.#items].map(([id, data]) => [new Uint8Array(Buffer.from(id, "hex")), data]);
  }
  clear() {
    this.#items.clear();
    this.#bytes = 0;
  }
};

// src/protocol/endpoint.ts
import { createHash as createHash2 } from "node:crypto";

// src/protocol/transport.ts
import { randomUUID } from "node:crypto";
import { connect, constants } from "node:http2";
import { gzip, gunzipSync } from "node:zlib";
import { promisify } from "node:util";
import { attributionHeaders } from "@deepseek-ai/dsh-llm";
var RUN_PATH = "/agent.v1.AgentService/Run";
var DEFAULT_ENDPOINT = "https://api2.cursor.sh";
var CLIENT_VERSION = "cli-2026.09.02-c22c1a3";
var compress = promisify(gzip);
var COMPRESS_MIN_BYTES = 1024;
var FORBIDDEN = /* @__PURE__ */ new Set(["connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "http2-settings", "host", "content-length", "te", "content-type", "connect-protocol-version", "authorization", "x-cursor-agent-allowed-tools"]);
function protocolHeaders(options) {
  const headers = {
    "x-cursor-client-type": "cli",
    "x-cursor-client-version": CLIENT_VERSION,
    "x-ghost-mode": "true",
    "x-request-id": randomUUID(),
    "x-dsh-plugin": "dsh-cursor/0.1.0"
  };
  for (const [name, value] of Object.entries(options.headers ?? {})) {
    const field2 = name.toLowerCase();
    if (!field2.startsWith(":") && !FORBIDDEN.has(field2)) headers[field2] = value;
  }
  for (const [name, value] of Object.entries(attributionHeaders())) headers[name.toLowerCase()] = value;
  headers.authorization = `Bearer ${options.accessToken}`;
  headers["x-cursor-agent-allowed-tools"] = "mcp_tool_call,get_mcp_tools_tool_call";
  return headers;
}
function validateTransport(options) {
  if (options.proxyUrl !== void 0) throw new CursorProtocolError("UNSUPPORTED_PROXY", "Cursor HTTP/2 proxy transport is not supported");
  if (options.signal?.aborted) throw new CursorProtocolError("ABORTED", "Cursor Run was aborted");
  let endpoint;
  try {
    endpoint = new URL(options.endpoint ?? DEFAULT_ENDPOINT);
  } catch {
    throw new CursorProtocolError("INVALID_REQUEST", "Invalid Cursor endpoint");
  }
  if (!["http:", "https:"].includes(endpoint.protocol) || endpoint.username || endpoint.password || endpoint.pathname !== "/" || endpoint.search || endpoint.hash) {
    throw new CursorProtocolError("INVALID_REQUEST", "Cursor endpoint must be an HTTP(S) origin");
  }
  return endpoint.origin;
}
function nodeCode(error) {
  return error && typeof error === "object" && "code" in error && typeof error.code === "string" ? error.code : "unknown";
}
function frameMessage(payload, flags = 0) {
  if (payload.byteLength > LIMITS.frameBytes) throw limitError();
  const frame = Buffer.allocUnsafe(payload.byteLength + 5);
  frame[0] = flags;
  frame.writeUInt32BE(payload.byteLength, 1);
  frame.set(payload, 5);
  return frame;
}
var FrameDecoder = class {
  #header = Buffer.alloc(5);
  #headerUsed = 0;
  #payload;
  #payloadUsed = 0;
  #flags = 0;
  push(chunk) {
    const frames = [];
    let offset = 0;
    while (offset < chunk.byteLength) {
      if (!this.#payload) {
        const count2 = Math.min(5 - this.#headerUsed, chunk.byteLength - offset);
        this.#header.set(chunk.subarray(offset, offset + count2), this.#headerUsed);
        this.#headerUsed += count2;
        offset += count2;
        if (this.#headerUsed < 5) break;
        this.#flags = this.#header[0];
        if ((this.#flags & ~3) !== 0) throw new CursorProtocolError("INVALID_RESPONSE", "Unsupported Cursor frame flags");
        const length = this.#header.readUInt32BE(1);
        if (length > LIMITS.frameBytes) throw limitError();
        this.#payload = Buffer.allocUnsafe(length);
        this.#payloadUsed = 0;
      }
      const count = Math.min(this.#payload.length - this.#payloadUsed, chunk.byteLength - offset);
      this.#payload.set(chunk.subarray(offset, offset + count), this.#payloadUsed);
      this.#payloadUsed += count;
      offset += count;
      if (this.#payloadUsed === this.#payload.length) {
        frames.push({ flags: this.#flags, payload: this.#payload });
        this.#payload = void 0;
        this.#headerUsed = 0;
      }
    }
    return frames;
  }
  finish() {
    if (this.#headerUsed !== 0 || this.#payload) throw new CursorProtocolError("INVALID_RESPONSE", "Cursor sent an incomplete frame");
  }
};
function terminalError(payload) {
  let result;
  try {
    result = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(payload));
  } catch {
    throw new CursorProtocolError("INVALID_RESPONSE", "Invalid Cursor end-stream envelope");
  }
  if (!result || typeof result !== "object" || Array.isArray(result)) throw new CursorProtocolError("INVALID_RESPONSE", "Invalid Cursor end-stream envelope");
  if ("error" in result && result.error !== void 0) {
    const error = result.error;
    const code = error && typeof error === "object" && "code" in error && typeof error.code === "string" ? error.code : "";
    const message2 = error && typeof error === "object" && "message" in error ? error.message : void 0;
    throw statusError(code, message2);
  }
}
var H2Channel = class {
  session;
  stream;
  ready;
  #readyResolve;
  #readyReject;
  #readySettled = false;
  #handshake;
  #signal;
  #onAbort;
  #failure;
  #closed = false;
  #streaming;
  #trace;
  #writes = 0;
  #writeBytes = 0;
  #writeTail = Promise.resolve();
  #responseHeaders = {};
  #trailers = {};
  constructor(path, options, streaming) {
    const origin = validateTransport(options);
    this.#streaming = streaming;
    this.#trace = options.trace;
    if (!path.startsWith("/") || path.startsWith("//") || /[\r\n?#]/u.test(path)) throw new CursorProtocolError("INVALID_REQUEST", "Invalid Cursor RPC path");
    this.#onAbort = () => this.close(new CursorProtocolError("ABORTED", "Cursor request was aborted"));
    this.#signal = options.signal;
    this.ready = new Promise((resolve, reject) => {
      this.#readyResolve = resolve;
      this.#readyReject = reject;
    });
    void this.ready.catch(() => {
    });
    try {
      this.session = connect(origin);
      this.session.on("error", (error) => this.close(new CursorProtocolError("TRANSPORT", `Cursor HTTP/2 session failed (${nodeCode(error)})`)));
      this.stream = this.session.request({
        ...protocolHeaders(options),
        ":method": "POST",
        ":path": path,
        "content-type": streaming ? "application/connect+proto" : "application/proto",
        "connect-protocol-version": "1",
        ...streaming ? { "connect-accept-encoding": "gzip", "connect-content-encoding": "gzip" } : {},
        te: "trailers"
      });
    } catch {
      this.session?.destroy();
      throw new CursorProtocolError("TRANSPORT", "Cursor HTTP/2 request could not be opened");
    }
    this.stream.on("error", (error) => this.close(new CursorProtocolError("TRANSPORT", `Cursor HTTP/2 stream failed (${nodeCode(error)}, rst ${this.stream.rstCode ?? "none"})`)));
    this.stream.on("response", (headers) => {
      this.#responseHeaders = headers;
      const status = Number(headers[":status"] ?? 0);
      if (status < 200 || status >= 300) {
        this.close(httpError(status));
        return;
      }
      const contentType = String(headers["content-type"] ?? "").split(";")[0];
      if (contentType !== (streaming ? "application/connect+proto" : "application/proto")) {
        this.close(new CursorProtocolError("INVALID_RESPONSE", "Unexpected Cursor response content type"));
        return;
      }
      this.#settleReady();
    });
    this.stream.on("trailers", (headers) => {
      this.#trailers = headers;
    });
    this.stream.once("close", () => {
      if (!this.#readySettled) this.#settleReady(this.#failure ?? new CursorProtocolError("TRANSPORT", "Cursor stream closed before response headers"));
    });
    this.#handshake = setTimeout(() => this.close(new CursorProtocolError("TIMEOUT", "Cursor response header timeout")), LIMITS.handshakeMs);
    this.#handshake.unref();
    this.#signal?.addEventListener("abort", this.#onAbort, { once: true });
    if (this.#signal?.aborted) this.#onAbort();
  }
  #settleReady(error) {
    if (this.#readySettled) return;
    this.#readySettled = true;
    clearTimeout(this.#handshake);
    this.#handshake = void 0;
    if (error) this.#readyReject(error);
    else this.#readyResolve();
  }
  /** Serialized, bounded writes await both the write callback and drain. */
  write(data) {
    if (this.#closed) return Promise.reject(this.#failure ?? new CursorProtocolError("CLOSED", "Cursor connection is closed"));
    if (data.byteLength > LIMITS.queuedWriteBytes || this.#writes >= LIMITS.queuedWrites || this.#writeBytes + data.byteLength > LIMITS.queuedWriteBytes) {
      const error = limitError();
      this.close(error);
      return Promise.reject(error);
    }
    this.#writes++;
    this.#writeBytes += data.byteLength;
    const bytes = Buffer.from(data);
    const written = this.#writeTail.then(async () => {
      if (this.#closed) throw this.#failure ?? new CursorProtocolError("CLOSED", "Cursor connection is closed");
      const started = Date.now();
      const frame = this.#streaming && bytes[0] === 0 && bytes.length - 5 >= COMPRESS_MIN_BYTES ? frameMessage(await compress(bytes.subarray(5)), 1) : bytes;
      return new Promise((resolve, reject) => {
        if (this.#closed) {
          reject(this.#failure ?? new CursorProtocolError("CLOSED", "Cursor connection is closed"));
          return;
        }
        let callbackDone = false;
        let drained = false;
        const cleanup = () => {
          this.stream.off("drain", onDrain);
          this.stream.off("close", onClose);
        };
        const finish = () => {
          if (callbackDone && drained) {
            cleanup();
            this.#trace?.("upload", { bytes: bytes.length, wireBytes: frame.length, elapsedMs: Date.now() - started });
            resolve();
          }
        };
        const onDrain = () => {
          drained = true;
          finish();
        };
        const onClose = () => {
          cleanup();
          reject(this.#failure ?? new CursorProtocolError("TRANSPORT", "Cursor connection closed during write"));
        };
        this.stream.once("close", onClose);
        try {
          const accepted = this.stream.write(frame, (error) => {
            if (error) {
              cleanup();
              reject(safeError(error));
              return;
            }
            callbackDone = true;
            finish();
          });
          drained = accepted;
          if (!accepted) this.stream.once("drain", onDrain);
        } catch {
          cleanup();
          reject(new CursorProtocolError("TRANSPORT", "Cursor write failed"));
        }
      });
    }).finally(() => {
      this.#writes--;
      this.#writeBytes -= bytes.length;
    });
    this.#writeTail = written.catch((error) => {
      this.close(safeError(error));
    });
    return written;
  }
  async *frames() {
    const decoder = new FrameDecoder();
    let ended2 = false;
    try {
      await this.ready;
      for await (const chunk of this.stream) {
        if (this.#failure) throw this.#failure;
        for (const frame of decoder.push(chunk)) {
          if (ended2) throw new CursorProtocolError("INVALID_RESPONSE", "Cursor sent data after end-stream");
          if ((frame.flags & 1) !== 0) {
            if (this.#responseHeaders["connect-content-encoding"] !== "gzip") throw new CursorProtocolError("INVALID_RESPONSE", "Unsupported Cursor frame compression");
            try {
              frame.payload = gunzipSync(frame.payload, { maxOutputLength: LIMITS.inflatedBytes });
            } catch {
              throw new CursorProtocolError("LIMIT_EXCEEDED", "Invalid or oversized Cursor compressed frame");
            }
          }
          if ((frame.flags & 2) !== 0) {
            terminalError(frame.payload);
            ended2 = true;
          }
          yield frame;
        }
      }
      decoder.finish();
      this.checkTrailers();
      if (this.#failure) throw this.#failure;
    } catch (error) {
      throw this.#failure ?? safeError(error, `Invalid Cursor HTTP/2 response (${nodeCode(error)}, rst ${this.stream.rstCode ?? "none"})`);
    }
  }
  get failure() {
    return this.#failure;
  }
  checkTrailers() {
    if (this.#failure) throw this.#failure;
    const status = this.#trailers["grpc-status"];
    if (status !== void 0 && status !== "0") throw statusError(String(status), this.#trailers["grpc-message"]);
  }
  end() {
    if (!this.stream.writableEnded && !this.#closed) this.stream.end();
  }
  close(failure) {
    if (this.#closed) return;
    this.#closed = true;
    this.#failure = failure;
    this.#signal?.removeEventListener("abort", this.#onAbort);
    this.#settleReady(failure ?? new CursorProtocolError("CLOSED", "Cursor connection is closed"));
    clearTimeout(this.#handshake);
    if (!this.stream.closed && !this.stream.destroyed) this.stream.close(constants.NGHTTP2_CANCEL);
    this.stream.destroy();
    this.session.destroy();
  }
};
async function requestUnary(path, body, options) {
  if (options.unaryTransport === "http2") return requestUnaryHttp2(path, body, options);
  const origin = validateTransport(options);
  if (!path.startsWith("/") || path.startsWith("//") || path.includes("://")) throw new CursorProtocolError("INVALID_REQUEST", "Invalid Cursor RPC path");
  if (body.byteLength > LIMITS.frameBytes) throw limitError();
  const timeout = AbortSignal.timeout(LIMITS.unaryMs);
  const signal = options.signal ? AbortSignal.any([options.signal, timeout]) : timeout;
  let reader;
  let response;
  const abort = () => {
    void reader?.cancel().catch(() => {
    });
  };
  try {
    response = await globalThis.fetch(`${origin}${path}`, {
      method: "POST",
      redirect: "error",
      signal,
      headers: { ...protocolHeaders(options), "content-type": "application/proto", "connect-protocol-version": "1" },
      body: Buffer.from(body)
    });
    signal.throwIfAborted();
    if (!response.ok) {
      void response.body?.cancel().catch(() => {
      });
      throw httpError(response.status);
    }
    if (!response.body) throw new CursorProtocolError("INVALID_RESPONSE", "Cursor metadata response has no body");
    reader = response.body.getReader();
    signal.addEventListener("abort", abort, { once: true });
    const chunks = [];
    let length = 0;
    while (true) {
      signal.throwIfAborted();
      const item = await reader.read();
      signal.throwIfAborted();
      if (item.done) break;
      length += item.value.byteLength;
      if (length > LIMITS.frameBytes) {
        await reader.cancel();
        throw limitError();
      }
      chunks.push(item.value);
    }
    return new Uint8Array(Buffer.concat(chunks, length));
  } catch (error) {
    if (options.signal?.aborted) throw new CursorProtocolError("ABORTED", "Cursor metadata request was aborted");
    if (timeout.aborted) throw new CursorProtocolError("TIMEOUT", "Cursor metadata request timed out");
    throw safeError(error, "Cursor metadata transport failed");
  } finally {
    signal.removeEventListener("abort", abort);
    if (!reader && signal.aborted) void response?.body?.cancel().catch(() => {
    });
    reader?.releaseLock();
  }
}
async function requestUnaryHttp2(path, body, options) {
  validateTransport(options);
  if (body.byteLength > LIMITS.frameBytes) throw limitError();
  const channel = new H2Channel(path, options, false);
  const timer = setTimeout(() => channel.close(new CursorProtocolError("TIMEOUT", "Cursor unary request timeout")), LIMITS.unaryMs);
  timer.unref();
  try {
    await channel.write(body);
    channel.end();
    await channel.ready;
    const chunks = [];
    let length = 0;
    for await (const chunk of channel.stream) {
      length += chunk.length;
      if (length > LIMITS.frameBytes) throw limitError();
      chunks.push(chunk);
    }
    channel.checkTrailers();
    return new Uint8Array(Buffer.concat(chunks, length));
  } catch (error) {
    throw channel.failure ?? safeError(error);
  } finally {
    clearTimeout(timer);
    channel.close();
  }
}

// src/protocol/endpoint.ts
var resolved = /* @__PURE__ */ new Map();
async function cursorRunEndpoint(options) {
  if (options.endpoint !== void 0) return options.endpoint;
  const key = createHash2("sha256").update(options.accessToken).digest("hex");
  const known = resolved.get(key);
  if (known) return known;
  const timeout = AbortSignal.timeout(15e3);
  const signal = options.signal ? AbortSignal.any([options.signal, timeout]) : timeout;
  let response;
  try {
    response = await fetch(`${DEFAULT_ENDPOINT}/aiserver.v1.ServerConfigService/GetServerConfig`, {
      method: "POST",
      signal,
      headers: { ...protocolHeaders(options), "content-type": "application/json", accept: "application/json" },
      body: JSON.stringify({ telem_enabled: false })
    });
  } catch (error) {
    if (options.signal?.aborted) throw new CursorProtocolError("ABORTED", "Cursor routing request was aborted");
    if (timeout.aborted) throw new CursorProtocolError("TIMEOUT", "Cursor routing request timed out");
    throw safeError(error, "Cursor routing request failed");
  }
  if (!response.ok) throw httpError(response.status);
  let raw;
  try {
    const body = await response.json();
    raw = body.agentUrlConfig?.agentnUrl || body.agentUrlConfig?.agentUrl;
  } catch {
    throw new CursorProtocolError("INVALID_RESPONSE", "Invalid Cursor routing response");
  }
  let url;
  try {
    url = new URL(typeof raw === "string" && !raw.includes("://") ? `https://${raw}` : String(raw));
  } catch {
    throw new CursorProtocolError("INVALID_RESPONSE", "Cursor routing response has no valid agent origin");
  }
  if (url.protocol !== "https:" || !/^([a-z0-9-]+\.)+cursor\.sh$/iu.test(url.hostname) || url.username || url.password) {
    throw new CursorProtocolError("INVALID_RESPONSE", "Cursor routing response has an invalid agent origin");
  }
  resolved.set(key, url.origin);
  if (resolved.size > 64) resolved.delete(resolved.keys().next().value);
  return url.origin;
}

// src/protocol/content.ts
import { create, fromBinary, fromJson, toBinary, toJson } from "@bufbuild/protobuf";
import { ValueSchema } from "@bufbuild/protobuf/wkt";

// src/protocol/vendor/agent_pb.ts
import { enumDesc, fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
var file_agent = /* @__PURE__ */ fileDesc("CgthZ2VudC5wcm90bxIIYWdlbnQudjEicgoOR2xvYlRvb2xSZXN1bHQSLAoHc3VjY2VzcxgBIAEoCzIZLmFnZW50LnYxLkdsb2JUb29sU3VjY2Vzc0gAEigKBWVycm9yGAIgASgLMhcuYWdlbnQudjEuR2xvYlRvb2xFcnJvckgAQggKBnJlc3VsdCIeCg1HbG9iVG9vbEVycm9yEg0KBWVycm9yGAEgASgJIokBCg9HbG9iVG9vbFN1Y2Nlc3MSDwoHcGF0dGVybhgBIAEoCRIMCgRwYXRoGAIgASgJEg0KBWZpbGVzGAMgAygJEhMKC3RvdGFsX2ZpbGVzGAQgASgFEhgKEGNsaWVudF90cnVuY2F0ZWQYBSABKAgSGQoRcmlwZ3JlcF90cnVuY2F0ZWQYBiABKAgiRgoMR2xvYlRvb2xDYWxsEgwKBGFyZ3MYASABKAwSKAoGcmVzdWx0GAIgASgLMhguYWdlbnQudjEuR2xvYlRvb2xSZXN1bHQibQoRUmVhZExpbnRzVG9vbENhbGwSKQoEYXJncxgBIAEoCzIbLmFnZW50LnYxLlJlYWRMaW50c1Rvb2xBcmdzEi0KBnJlc3VsdBgCIAEoCzIdLmFnZW50LnYxLlJlYWRMaW50c1Rvb2xSZXN1bHQiIgoRUmVhZExpbnRzVG9vbEFyZ3MSDQoFcGF0aHMYASADKAkigQEKE1JlYWRMaW50c1Rvb2xSZXN1bHQSMQoHc3VjY2VzcxgBIAEoCzIeLmFnZW50LnYxLlJlYWRMaW50c1Rvb2xTdWNjZXNzSAASLQoFZXJyb3IYAiABKAsyHC5hZ2VudC52MS5SZWFkTGludHNUb29sRXJyb3JIAEIICgZyZXN1bHQiewoUUmVhZExpbnRzVG9vbFN1Y2Nlc3MSMwoQZmlsZV9kaWFnbm9zdGljcxgBIAMoCzIZLmFnZW50LnYxLkZpbGVEaWFnbm9zdGljcxITCgt0b3RhbF9maWxlcxgCIAEoBRIZChF0b3RhbF9kaWFnbm9zdGljcxgDIAEoBSJpCg9GaWxlRGlhZ25vc3RpY3MSDAoEcGF0aBgBIAEoCRItCgtkaWFnbm9zdGljcxgCIAMoCzIYLmFnZW50LnYxLkRpYWdub3N0aWNJdGVtEhkKEWRpYWdub3N0aWNzX2NvdW50GAMgASgFIqsBCg5EaWFnbm9zdGljSXRlbRIuCghzZXZlcml0eRgBIAEoDjIcLmFnZW50LnYxLkRpYWdub3N0aWNTZXZlcml0eRIoCgVyYW5nZRgCIAEoCzIZLmFnZW50LnYxLkRpYWdub3N0aWNSYW5nZRIPCgdtZXNzYWdlGAMgASgJEg4KBnNvdXJjZRgEIAEoCRIMCgRjb2RlGAUgASgJEhAKCGlzX3N0YWxlGAYgASgIIlUKD0RpYWdub3N0aWNSYW5nZRIhCgVzdGFydBgBIAEoCzISLmFnZW50LnYxLlBvc2l0aW9uEh8KA2VuZBgCIAEoCzISLmFnZW50LnYxLlBvc2l0aW9uIisKElJlYWRMaW50c1Rvb2xFcnJvchIVCg1lcnJvcl9tZXNzYWdlGAEgASgJIj0KDE1jcFRvb2xFcnJvchINCgVlcnJvchgBIAEoCRIeChZyZWFkX3Rvb2xfZGVmX3JlbWluZGVyGAIgASgJItIBCg1NY3BUb29sUmVzdWx0EicKB3N1Y2Nlc3MYASABKAsyFC5hZ2VudC52MS5NY3BTdWNjZXNzSAASJwoFZXJyb3IYAiABKAsyFi5hZ2VudC52MS5NY3BUb29sRXJyb3JIABIpCghyZWplY3RlZBgDIAEoCzIVLmFnZW50LnYxLk1jcFJlamVjdGVkSAASOgoRcGVybWlzc2lvbl9kZW5pZWQYBCABKAsyHS5hZ2VudC52MS5NY3BQZXJtaXNzaW9uRGVuaWVkSABCCAoGcmVzdWx0IoEBCgtNY3BUb29sQ2FsbBIfCgRhcmdzGAEgASgLMhEuYWdlbnQudjEuTWNwQXJncxInCgZyZXN1bHQYAiABKAsyFy5hZ2VudC52MS5NY3BUb29sUmVzdWx0EhgKC2Rlc2NyaXB0aW9uGAMgASgJSACIAQFCDgoMX2Rlc2NyaXB0aW9uIm0KEVNlbVNlYXJjaFRvb2xDYWxsEikKBGFyZ3MYASABKAsyGy5hZ2VudC52MS5TZW1TZWFyY2hUb29sQXJncxItCgZyZXN1bHQYAiABKAsyHS5hZ2VudC52MS5TZW1TZWFyY2hUb29sUmVzdWx0IlMKEVNlbVNlYXJjaFRvb2xBcmdzEg0KBXF1ZXJ5GAEgASgJEhoKEnRhcmdldF9kaXJlY3RvcmllcxgCIAMoCRITCgtleHBsYW5hdGlvbhgDIAEoCSKBAQoTU2VtU2VhcmNoVG9vbFJlc3VsdBIxCgdzdWNjZXNzGAEgASgLMh4uYWdlbnQudjEuU2VtU2VhcmNoVG9vbFN1Y2Nlc3NIABItCgVlcnJvchgCIAEoCzIcLmFnZW50LnYxLlNlbVNlYXJjaFRvb2xFcnJvckgAQggKBnJlc3VsdCI9ChRTZW1TZWFyY2hUb29sU3VjY2VzcxIPCgdyZXN1bHRzGAEgASgJEhQKDGNvZGVfcmVzdWx0cxgCIAMoDCIrChJTZW1TZWFyY2hUb29sRXJyb3ISFQoNZXJyb3JfbWVzc2FnZRgBIAEoCSKCAQoYTGlzdE1jcFJlc291cmNlc1Rvb2xDYWxsEjAKBGFyZ3MYASABKAsyIi5hZ2VudC52MS5MaXN0TWNwUmVzb3VyY2VzRXhlY0FyZ3MSNAoGcmVzdWx0GAIgASgLMiQuYWdlbnQudjEuTGlzdE1jcFJlc291cmNlc0V4ZWNSZXN1bHQifwoXUmVhZE1jcFJlc291cmNlVG9vbENhbGwSLwoEYXJncxgBIAEoCzIhLmFnZW50LnYxLlJlYWRNY3BSZXNvdXJjZUV4ZWNBcmdzEjMKBnJlc3VsdBgCIAEoCzIjLmFnZW50LnYxLlJlYWRNY3BSZXNvdXJjZUV4ZWNSZXN1bHQiWQoNRmV0Y2hUb29sQ2FsbBIhCgRhcmdzGAEgASgLMhMuYWdlbnQudjEuRmV0Y2hBcmdzEiUKBnJlc3VsdBgCIAEoCzIVLmFnZW50LnYxLkZldGNoUmVzdWx0Im4KFFJlY29yZFNjcmVlblRvb2xDYWxsEigKBGFyZ3MYASABKAsyGi5hZ2VudC52MS5SZWNvcmRTY3JlZW5BcmdzEiwKBnJlc3VsdBgCIAEoCzIcLmFnZW50LnYxLlJlY29yZFNjcmVlblJlc3VsdCJ3ChdXcml0ZVNoZWxsU3RkaW5Ub29sQ2FsbBIrCgRhcmdzGAEgASgLMh0uYWdlbnQudjEuV3JpdGVTaGVsbFN0ZGluQXJncxIvCgZyZXN1bHQYAiABKAsyHy5hZ2VudC52MS5Xcml0ZVNoZWxsU3RkaW5SZXN1bHQisQEKC1JlZmxlY3RBcmdzEiIKGnVuZXhwZWN0ZWRfYWN0aW9uX291dGNvbWVzGAEgASgJEh0KFXJlbGV2YW50X2luc3RydWN0aW9ucxgCIAEoCRIZChFzY2VuYXJpb19hbmFseXNpcxgDIAEoCRIaChJjcml0aWNhbF9zeW50aGVzaXMYBCABKAkSEgoKbmV4dF9zdGVwcxgFIAEoCRIUCgx0b29sX2NhbGxfaWQYBiABKAkibwoNUmVmbGVjdFJlc3VsdBIrCgdzdWNjZXNzGAEgASgLMhguYWdlbnQudjEuUmVmbGVjdFN1Y2Nlc3NIABInCgVlcnJvchgCIAEoCzIWLmFnZW50LnYxLlJlZmxlY3RFcnJvckgAQggKBnJlc3VsdCIQCg5SZWZsZWN0U3VjY2VzcyIdCgxSZWZsZWN0RXJyb3ISDQoFZXJyb3IYASABKAkiXwoPUmVmbGVjdFRvb2xDYWxsEiMKBGFyZ3MYASABKAsyFS5hZ2VudC52MS5SZWZsZWN0QXJncxInCgZyZXN1bHQYAiABKAsyFy5hZ2VudC52MS5SZWZsZWN0UmVzdWx0IlkKF1N0YXJ0R3JpbmRFeGVjdXRpb25BcmdzEhgKC2V4cGxhbmF0aW9uGAEgASgJSACIAQESFAoMdG9vbF9jYWxsX2lkGAIgASgJQg4KDF9leHBsYW5hdGlvbiKTAQoZU3RhcnRHcmluZEV4ZWN1dGlvblJlc3VsdBI3CgdzdWNjZXNzGAEgASgLMiQuYWdlbnQudjEuU3RhcnRHcmluZEV4ZWN1dGlvblN1Y2Nlc3NIABIzCgVlcnJvchgCIAEoCzIiLmFnZW50LnYxLlN0YXJ0R3JpbmRFeGVjdXRpb25FcnJvckgAQggKBnJlc3VsdCIcChpTdGFydEdyaW5kRXhlY3V0aW9uU3VjY2VzcyIpChhTdGFydEdyaW5kRXhlY3V0aW9uRXJyb3ISDQoFZXJyb3IYASABKAkigwEKG1N0YXJ0R3JpbmRFeGVjdXRpb25Ub29sQ2FsbBIvCgRhcmdzGAEgASgLMiEuYWdlbnQudjEuU3RhcnRHcmluZEV4ZWN1dGlvbkFyZ3MSMwoGcmVzdWx0GAIgASgLMiMuYWdlbnQudjEuU3RhcnRHcmluZEV4ZWN1dGlvblJlc3VsdCJYChZTdGFydEdyaW5kUGxhbm5pbmdBcmdzEhgKC2V4cGxhbmF0aW9uGAEgASgJSACIAQESFAoMdG9vbF9jYWxsX2lkGAIgASgJQg4KDF9leHBsYW5hdGlvbiKQAQoYU3RhcnRHcmluZFBsYW5uaW5nUmVzdWx0EjYKB3N1Y2Nlc3MYASABKAsyIy5hZ2VudC52MS5TdGFydEdyaW5kUGxhbm5pbmdTdWNjZXNzSAASMgoFZXJyb3IYAiABKAsyIS5hZ2VudC52MS5TdGFydEdyaW5kUGxhbm5pbmdFcnJvckgAQggKBnJlc3VsdCIbChlTdGFydEdyaW5kUGxhbm5pbmdTdWNjZXNzIigKF1N0YXJ0R3JpbmRQbGFubmluZ0Vycm9yEg0KBWVycm9yGAEgASgJIoABChpTdGFydEdyaW5kUGxhbm5pbmdUb29sQ2FsbBIuCgRhcmdzGAEgASgLMiAuYWdlbnQudjEuU3RhcnRHcmluZFBsYW5uaW5nQXJncxIyCgZyZXN1bHQYAiABKAsyIi5hZ2VudC52MS5TdGFydEdyaW5kUGxhbm5pbmdSZXN1bHQinAEKCFRhc2tBcmdzEhMKC2Rlc2NyaXB0aW9uGAEgASgJEg4KBnByb21wdBgCIAEoCRItCg1zdWJhZ2VudF90eXBlGAMgASgLMhYuYWdlbnQudjEuU3ViYWdlbnRUeXBlEhIKBW1vZGVsGAQgASgJSACIAQESEwoGcmVzdW1lGAUgASgJSAGIAQFCCAoGX21vZGVsQgkKB19yZXN1bWUiqgEKC1Rhc2tTdWNjZXNzEjYKEmNvbnZlcnNhdGlvbl9zdGVwcxgBIAMoCzIaLmFnZW50LnYxLkNvbnZlcnNhdGlvblN0ZXASFQoIYWdlbnRfaWQYAiABKAlIAIgBARIVCg1pc19iYWNrZ3JvdW5kGAMgASgIEhgKC2R1cmF0aW9uX21zGAQgASgESAGIAQFCCwoJX2FnZW50X2lkQg4KDF9kdXJhdGlvbl9tcyIaCglUYXNrRXJyb3ISDQoFZXJyb3IYASABKAkiZgoKVGFza1Jlc3VsdBIoCgdzdWNjZXNzGAEgASgLMhUuYWdlbnQudjEuVGFza1N1Y2Nlc3NIABIkCgVlcnJvchgCIAEoCzITLmFnZW50LnYxLlRhc2tFcnJvckgAQggKBnJlc3VsdCJWCgxUYXNrVG9vbENhbGwSIAoEYXJncxgBIAEoCzISLmFnZW50LnYxLlRhc2tBcmdzEiQKBnJlc3VsdBgCIAEoCzIULmFnZW50LnYxLlRhc2tSZXN1bHQiTAoRVGFza1Rvb2xDYWxsRGVsdGESNwoSaW50ZXJhY3Rpb25fdXBkYXRlGAEgASgLMhsuYWdlbnQudjEuSW50ZXJhY3Rpb25VcGRhdGUihhQKCFRvb2xDYWxsEjIKD3NoZWxsX3Rvb2xfY2FsbBgBIAEoCzIXLmFnZW50LnYxLlNoZWxsVG9vbENhbGxIABI0ChBkZWxldGVfdG9vbF9jYWxsGAMgASgLMhguYWdlbnQudjEuRGVsZXRlVG9vbENhbGxIABIwCg5nbG9iX3Rvb2xfY2FsbBgEIAEoCzIWLmFnZW50LnYxLkdsb2JUb29sQ2FsbEgAEjAKDmdyZXBfdG9vbF9jYWxsGAUgASgLMhYuYWdlbnQudjEuR3JlcFRvb2xDYWxsSAASMAoOcmVhZF90b29sX2NhbGwYCCABKAsyFi5hZ2VudC52MS5SZWFkVG9vbENhbGxIABI/ChZ1cGRhdGVfdG9kb3NfdG9vbF9jYWxsGAkgASgLMh0uYWdlbnQudjEuVXBkYXRlVG9kb3NUb29sQ2FsbEgAEjsKFHJlYWRfdG9kb3NfdG9vbF9jYWxsGAogASgLMhsuYWdlbnQudjEuUmVhZFRvZG9zVG9vbENhbGxIABIwCg5lZGl0X3Rvb2xfY2FsbBgMIAEoCzIWLmFnZW50LnYxLkVkaXRUb29sQ2FsbEgAEiwKDGxzX3Rvb2xfY2FsbBgNIAEoCzIULmFnZW50LnYxLkxzVG9vbENhbGxIABI7ChRyZWFkX2xpbnRzX3Rvb2xfY2FsbBgOIAEoCzIbLmFnZW50LnYxLlJlYWRMaW50c1Rvb2xDYWxsSAASLgoNbWNwX3Rvb2xfY2FsbBgPIAEoCzIVLmFnZW50LnYxLk1jcFRvb2xDYWxsSAASOwoUc2VtX3NlYXJjaF90b29sX2NhbGwYECABKAsyGy5hZ2VudC52MS5TZW1TZWFyY2hUb29sQ2FsbEgAEj0KFWNyZWF0ZV9wbGFuX3Rvb2xfY2FsbBgRIAEoCzIcLmFnZW50LnYxLkNyZWF0ZVBsYW5Ub29sQ2FsbEgAEjsKFHdlYl9zZWFyY2hfdG9vbF9jYWxsGBIgASgLMhsuYWdlbnQudjEuV2ViU2VhcmNoVG9vbENhbGxIABIwCg50YXNrX3Rvb2xfY2FsbBgTIAEoCzIWLmFnZW50LnYxLlRhc2tUb29sQ2FsbEgAEkoKHGxpc3RfbWNwX3Jlc291cmNlc190b29sX2NhbGwYFCABKAsyIi5hZ2VudC52MS5MaXN0TWNwUmVzb3VyY2VzVG9vbENhbGxIABJIChtyZWFkX21jcF9yZXNvdXJjZV90b29sX2NhbGwYFSABKAsyIS5hZ2VudC52MS5SZWFkTWNwUmVzb3VyY2VUb29sQ2FsbEgAEkYKGmFwcGx5X2FnZW50X2RpZmZfdG9vbF9jYWxsGBYgASgLMiAuYWdlbnQudjEuQXBwbHlBZ2VudERpZmZUb29sQ2FsbEgAEj8KFmFza19xdWVzdGlvbl90b29sX2NhbGwYFyABKAsyHS5hZ2VudC52MS5Bc2tRdWVzdGlvblRvb2xDYWxsSAASMgoPZmV0Y2hfdG9vbF9jYWxsGBggASgLMhcuYWdlbnQudjEuRmV0Y2hUb29sQ2FsbEgAEj0KFXN3aXRjaF9tb2RlX3Rvb2xfY2FsbBgZIAEoCzIcLmFnZW50LnYxLlN3aXRjaE1vZGVUb29sQ2FsbEgAEjsKFGV4YV9zZWFyY2hfdG9vbF9jYWxsGBogASgLMhsuYWdlbnQudjEuRXhhU2VhcmNoVG9vbENhbGxIABI5ChNleGFfZmV0Y2hfdG9vbF9jYWxsGBsgASgLMhouYWdlbnQudjEuRXhhRmV0Y2hUb29sQ2FsbEgAEkMKGGdlbmVyYXRlX2ltYWdlX3Rvb2xfY2FsbBgcIAEoCzIfLmFnZW50LnYxLkdlbmVyYXRlSW1hZ2VUb29sQ2FsbEgAEkEKF3JlY29yZF9zY3JlZW5fdG9vbF9jYWxsGB0gASgLMh4uYWdlbnQudjEuUmVjb3JkU2NyZWVuVG9vbENhbGxIABI/ChZjb21wdXRlcl91c2VfdG9vbF9jYWxsGB4gASgLMh0uYWdlbnQudjEuQ29tcHV0ZXJVc2VUb29sQ2FsbEgAEkgKG3dyaXRlX3NoZWxsX3N0ZGluX3Rvb2xfY2FsbBgfIAEoCzIhLmFnZW50LnYxLldyaXRlU2hlbGxTdGRpblRvb2xDYWxsSAASNgoRcmVmbGVjdF90b29sX2NhbGwYICABKAsyGS5hZ2VudC52MS5SZWZsZWN0VG9vbENhbGxIABJOCh5zZXR1cF92bV9lbnZpcm9ubWVudF90b29sX2NhbGwYISABKAsyJC5hZ2VudC52MS5TZXR1cFZtRW52aXJvbm1lbnRUb29sQ2FsbEgAEjoKE3RydW5jYXRlZF90b29sX2NhbGwYIiABKAsyGy5hZ2VudC52MS5UcnVuY2F0ZWRUb29sQ2FsbEgAElAKH3N0YXJ0X2dyaW5kX2V4ZWN1dGlvbl90b29sX2NhbGwYIyABKAsyJS5hZ2VudC52MS5TdGFydEdyaW5kRXhlY3V0aW9uVG9vbENhbGxIABJOCh5zdGFydF9ncmluZF9wbGFubmluZ190b29sX2NhbGwYJCABKAsyJC5hZ2VudC52MS5TdGFydEdyaW5kUGxhbm5pbmdUb29sQ2FsbEgAEjUKEXBpX3JlYWRfdG9vbF9jYWxsGD0gASgLMhguYWdlbnQudjEuUGlSZWFkVG9vbENhbGxIABI1ChFwaV9iYXNoX3Rvb2xfY2FsbBg+IAEoCzIYLmFnZW50LnYxLlBpQmFzaFRvb2xDYWxsSAASNQoRcGlfZWRpdF90b29sX2NhbGwYPyABKAsyGC5hZ2VudC52MS5QaUVkaXRUb29sQ2FsbEgAEjcKEnBpX3dyaXRlX3Rvb2xfY2FsbBhAIAEoCzIZLmFnZW50LnYxLlBpV3JpdGVUb29sQ2FsbEgAEjUKEXBpX2dyZXBfdG9vbF9jYWxsGEEgASgLMhguYWdlbnQudjEuUGlHcmVwVG9vbENhbGxIABI1ChFwaV9maW5kX3Rvb2xfY2FsbBhCIAEoCzIYLmFnZW50LnYxLlBpRmluZFRvb2xDYWxsSAASMQoPcGlfbHNfdG9vbF9jYWxsGEMgASgLMhYuYWdlbnQudjEuUGlMc1Rvb2xDYWxsSAASPQoVY29ubmVjdF9zY21fdG9vbF9jYWxsGEQgASgLMhwuYWdlbnQudjEuQ29ubmVjdFNjbVRvb2xDYWxsSAASTwoec2VhcmNoX2NvbnZlcnNhdGlvbnNfdG9vbF9jYWxsGEUgASgLMiUuYWdlbnQudjEuU2VhcmNoQ29udmVyc2F0aW9uc1Rvb2xDYWxsSAASGQoMdG9vbF9jYWxsX2lkGDkgASgJSAGIAQFCBgoEdG9vbEIPCg1fdG9vbF9jYWxsX2lkIhcKFVRydW5jYXRlZFRvb2xDYWxsQXJncyIaChhUcnVuY2F0ZWRUb29sQ2FsbFN1Y2Nlc3MiJwoWVHJ1bmNhdGVkVG9vbENhbGxFcnJvchINCgVlcnJvchgBIAEoCSKNAQoXVHJ1bmNhdGVkVG9vbENhbGxSZXN1bHQSNQoHc3VjY2VzcxgBIAEoCzIiLmFnZW50LnYxLlRydW5jYXRlZFRvb2xDYWxsU3VjY2Vzc0gAEjEKBWVycm9yGAIgASgLMiAuYWdlbnQudjEuVHJ1bmNhdGVkVG9vbENhbGxFcnJvckgAQggKBnJlc3VsdCKUAQoRVHJ1bmNhdGVkVG9vbENhbGwSHQoVb3JpZ2luYWxfc3RlcF9ibG9iX2lkGAEgASgMEi0KBGFyZ3MYAiABKAsyHy5hZ2VudC52MS5UcnVuY2F0ZWRUb29sQ2FsbEFyZ3MSMQoGcmVzdWx0GAMgASgLMiEuYWdlbnQudjEuVHJ1bmNhdGVkVG9vbENhbGxSZXN1bHQi0QEKDVRvb2xDYWxsRGVsdGESPQoVc2hlbGxfdG9vbF9jYWxsX2RlbHRhGAEgASgLMhwuYWdlbnQudjEuU2hlbGxUb29sQ2FsbERlbHRhSAASOwoUdGFza190b29sX2NhbGxfZGVsdGEYAiABKAsyGy5hZ2VudC52MS5UYXNrVG9vbENhbGxEZWx0YUgAEjsKFGVkaXRfdG9vbF9jYWxsX2RlbHRhGAMgASgLMhsuYWdlbnQudjEuRWRpdFRvb2xDYWxsRGVsdGFIAEIHCgVkZWx0YSK2AQoQQ29udmVyc2F0aW9uU3RlcBI3ChFhc3Npc3RhbnRfbWVzc2FnZRgBIAEoCzIaLmFnZW50LnYxLkFzc2lzdGFudE1lc3NhZ2VIABInCgl0b29sX2NhbGwYAiABKAsyEi5hZ2VudC52MS5Ub29sQ2FsbEgAEjUKEHRoaW5raW5nX21lc3NhZ2UYAyABKAsyGS5hZ2VudC52MS5UaGlua2luZ01lc3NhZ2VIAEIJCgdtZXNzYWdlIoEEChJDb252ZXJzYXRpb25BY3Rpb24SOgoTdXNlcl9tZXNzYWdlX2FjdGlvbhgBIAEoCzIbLmFnZW50LnYxLlVzZXJNZXNzYWdlQWN0aW9uSAASLwoNcmVzdW1lX2FjdGlvbhgCIAEoCzIWLmFnZW50LnYxLlJlc3VtZUFjdGlvbkgAEi8KDWNhbmNlbF9hY3Rpb24YAyABKAsyFi5hZ2VudC52MS5DYW5jZWxBY3Rpb25IABI1ChBzdW1tYXJpemVfYWN0aW9uGAQgASgLMhkuYWdlbnQudjEuU3VtbWFyaXplQWN0aW9uSAASPAoUc2hlbGxfY29tbWFuZF9hY3Rpb24YBSABKAsyHC5hZ2VudC52MS5TaGVsbENvbW1hbmRBY3Rpb25IABI2ChFzdGFydF9wbGFuX2FjdGlvbhgGIAEoCzIZLmFnZW50LnYxLlN0YXJ0UGxhbkFjdGlvbkgAEjoKE2V4ZWN1dGVfcGxhbl9hY3Rpb24YByABKAsyGy5hZ2VudC52MS5FeGVjdXRlUGxhbkFjdGlvbkgAEloKJGFzeW5jX2Fza19xdWVzdGlvbl9jb21wbGV0aW9uX2FjdGlvbhgIIAEoCzIqLmFnZW50LnYxLkFzeW5jQXNrUXVlc3Rpb25Db21wbGV0aW9uQWN0aW9uSABCCAoGYWN0aW9uIr8BChFVc2VyTWVzc2FnZUFjdGlvbhIrCgx1c2VyX21lc3NhZ2UYASABKAsyFS5hZ2VudC52MS5Vc2VyTWVzc2FnZRIxCg9yZXF1ZXN0X2NvbnRleHQYAiABKAsyGC5hZ2VudC52MS5SZXF1ZXN0Q29udGV4dBIpChxzZW5kX3RvX2ludGVyYWN0aW9uX2xpc3RlbmVyGAMgASgISACIAQFCHwodX3NlbmRfdG9faW50ZXJhY3Rpb25fbGlzdGVuZXIiDgoMQ2FuY2VsQWN0aW9uIkEKDFJlc3VtZUFjdGlvbhIxCg9yZXF1ZXN0X2NvbnRleHQYAiABKAsyGC5hZ2VudC52MS5SZXF1ZXN0Q29udGV4dCKgAQogQXN5bmNBc2tRdWVzdGlvbkNvbXBsZXRpb25BY3Rpb24SHQoVb3JpZ2luYWxfdG9vbF9jYWxsX2lkGAEgASgJEjAKDW9yaWdpbmFsX2FyZ3MYAiABKAsyGS5hZ2VudC52MS5Bc2tRdWVzdGlvbkFyZ3MSKwoGcmVzdWx0GAMgASgLMhsuYWdlbnQudjEuQXNrUXVlc3Rpb25SZXN1bHQiEQoPU3VtbWFyaXplQWN0aW9uIlQKElNoZWxsQ29tbWFuZEFjdGlvbhItCg1zaGVsbF9jb21tYW5kGAEgASgLMhYuYWdlbnQudjEuU2hlbGxDb21tYW5kEg8KB2V4ZWNfaWQYAiABKAkiggEKD1N0YXJ0UGxhbkFjdGlvbhIrCgx1c2VyX21lc3NhZ2UYASABKAsyFS5hZ2VudC52MS5Vc2VyTWVzc2FnZRIxCg9yZXF1ZXN0X2NvbnRleHQYAiABKAsyGC5hZ2VudC52MS5SZXF1ZXN0Q29udGV4dBIPCgdpc19zcGVjGAMgASgIIuIBChFFeGVjdXRlUGxhbkFjdGlvbhIxCg9yZXF1ZXN0X2NvbnRleHQYASABKAsyGC5hZ2VudC52MS5SZXF1ZXN0Q29udGV4dBItCgRwbGFuGAIgASgLMhouYWdlbnQudjEuQ29udmVyc2F0aW9uUGxhbkgAiAEBEhoKDXBsYW5fZmlsZV91cmkYAyABKAlIAYgBARIeChFwbGFuX2ZpbGVfY29udGVudBgEIAEoCUgCiAEBQgcKBV9wbGFuQhAKDl9wbGFuX2ZpbGVfdXJpQhQKEl9wbGFuX2ZpbGVfY29udGVudCLoAgoLVXNlck1lc3NhZ2USDAoEdGV4dBgBIAEoCRISCgptZXNzYWdlX2lkGAIgASgJEjgKEHNlbGVjdGVkX2NvbnRleHQYAyABKAsyGS5hZ2VudC52MS5TZWxlY3RlZENvbnRleHRIAIgBARIMCgRtb2RlGAQgASgFEh0KEGlzX3NpbXVsYXRlZF9tc2cYBSABKAhIAYgBARIfChJiZXN0X29mX25fZ3JvdXBfaWQYBiABKAlIAogBARIoCht0cnlfdXNlX2Jlc3Rfb2Zfbl9wcm9tb3Rpb24YByABKAhIA4gBARIWCglyaWNoX3RleHQYCCABKAlIBIgBAUITChFfc2VsZWN0ZWRfY29udGV4dEITChFfaXNfc2ltdWxhdGVkX21zZ0IVChNfYmVzdF9vZl9uX2dyb3VwX2lkQh4KHF90cnlfdXNlX2Jlc3Rfb2Zfbl9wcm9tb3Rpb25CDAoKX3JpY2hfdGV4dCIgChBBc3Npc3RhbnRNZXNzYWdlEgwKBHRleHQYASABKAkiNAoPVGhpbmtpbmdNZXNzYWdlEgwKBHRleHQYASABKAkSEwoLZHVyYXRpb25fbXMYAiABKA0iHwoMU2hlbGxDb21tYW5kEg8KB2NvbW1hbmQYASABKAkiQAoLU2hlbGxPdXRwdXQSDgoGc3Rkb3V0GAEgASgJEg4KBnN0ZGVychgCIAEoCRIRCglleGl0X2NvZGUYAyABKAUiogEKEENvbnZlcnNhdGlvblR1cm4SQgoXYWdlbnRfY29udmVyc2F0aW9uX3R1cm4YASABKAsyHy5hZ2VudC52MS5BZ2VudENvbnZlcnNhdGlvblR1cm5IABJCChdzaGVsbF9jb252ZXJzYXRpb25fdHVybhgCIAEoCzIfLmFnZW50LnYxLlNoZWxsQ29udmVyc2F0aW9uVHVybkgAQgYKBHR1cm4iIAoQQ29udmVyc2F0aW9uUGxhbhIMCgRwbGFuGAEgASgJIr0BChlDb252ZXJzYXRpb25UdXJuU3RydWN0dXJlEksKF2FnZW50X2NvbnZlcnNhdGlvbl90dXJuGAEgASgLMiguYWdlbnQudjEuQWdlbnRDb252ZXJzYXRpb25UdXJuU3RydWN0dXJlSAASSwoXc2hlbGxfY29udmVyc2F0aW9uX3R1cm4YAiABKAsyKC5hZ2VudC52MS5TaGVsbENvbnZlcnNhdGlvblR1cm5TdHJ1Y3R1cmVIAEIGCgR0dXJuIpcBChVBZ2VudENvbnZlcnNhdGlvblR1cm4SKwoMdXNlcl9tZXNzYWdlGAEgASgLMhUuYWdlbnQudjEuVXNlck1lc3NhZ2USKQoFc3RlcHMYAiADKAsyGi5hZ2VudC52MS5Db252ZXJzYXRpb25TdGVwEhcKCnJlcXVlc3RfaWQYAyABKAlIAIgBAUINCgtfcmVxdWVzdF9pZCJtCh5BZ2VudENvbnZlcnNhdGlvblR1cm5TdHJ1Y3R1cmUSFAoMdXNlcl9tZXNzYWdlGAEgASgMEg0KBXN0ZXBzGAIgAygMEhcKCnJlcXVlc3RfaWQYAyABKAlIAIgBAUINCgtfcmVxdWVzdF9pZCJzChVTaGVsbENvbnZlcnNhdGlvblR1cm4SLQoNc2hlbGxfY29tbWFuZBgBIAEoCzIWLmFnZW50LnYxLlNoZWxsQ29tbWFuZBIrCgxzaGVsbF9vdXRwdXQYAiABKAsyFS5hZ2VudC52MS5TaGVsbE91dHB1dCJNCh5TaGVsbENvbnZlcnNhdGlvblR1cm5TdHJ1Y3R1cmUSFQoNc2hlbGxfY29tbWFuZBgBIAEoDBIUCgxzaGVsbF9vdXRwdXQYAiABKAwiJgoTQ29udmVyc2F0aW9uU3VtbWFyeRIPCgdzdW1tYXJ5GAEgASgJIngKGkNvbnZlcnNhdGlvblN1bW1hcnlBcmNoaXZlEhsKE3N1bW1hcml6ZWRfbWVzc2FnZXMYASADKAwSDwoHc3VtbWFyeRgCIAEoCRITCgt3aW5kb3dfdGFpbBgDIAEoDRIXCg9zdW1tYXJ5X21lc3NhZ2UYBCABKAwiQwoYQ29udmVyc2F0aW9uVG9rZW5EZXRhaWxzEhMKC3VzZWRfdG9rZW5zGAEgASgNEhIKCm1heF90b2tlbnMYAiABKA0iXwoJRmlsZVN0YXRlEhQKB2NvbnRlbnQYASABKAlIAIgBARIcCg9pbml0aWFsX2NvbnRlbnQYAiABKAlIAYgBAUIKCghfY29udGVudEISChBfaW5pdGlhbF9jb250ZW50ImgKEkZpbGVTdGF0ZVN0cnVjdHVyZRIUCgdjb250ZW50GAEgASgMSACIAQESHAoPaW5pdGlhbF9jb250ZW50GAIgASgMSAGIAQFCCgoIX2NvbnRlbnRCEgoQX2luaXRpYWxfY29udGVudCI3CgpTdGVwVGltaW5nEhMKC2R1cmF0aW9uX21zGAEgASgEEhQKDHRpbWVzdGFtcF9tcxgCIAEoBCL2BAoRQ29udmVyc2F0aW9uU3RhdGUSIQoZcm9vdF9wcm9tcHRfbWVzc2FnZXNfanNvbhgBIAMoCRIpCgV0dXJucxgIIAMoCzIaLmFnZW50LnYxLkNvbnZlcnNhdGlvblR1cm4SIQoFdG9kb3MYAyADKAsyEi5hZ2VudC52MS5Ub2RvSXRlbRIaChJwZW5kaW5nX3Rvb2xfY2FsbHMYBCADKAkSOQoNdG9rZW5fZGV0YWlscxgFIAEoCzIiLmFnZW50LnYxLkNvbnZlcnNhdGlvblRva2VuRGV0YWlscxIzCgdzdW1tYXJ5GAYgASgLMh0uYWdlbnQudjEuQ29udmVyc2F0aW9uU3VtbWFyeUgAiAEBEi0KBHBsYW4YByABKAsyGi5hZ2VudC52MS5Db252ZXJzYXRpb25QbGFuSAGIAQESQgoPc3VtbWFyeV9hcmNoaXZlGAkgASgLMiQuYWdlbnQudjEuQ29udmVyc2F0aW9uU3VtbWFyeUFyY2hpdmVIAogBARJACgtmaWxlX3N0YXRlcxgKIAMoCzIrLmFnZW50LnYxLkNvbnZlcnNhdGlvblN0YXRlLkZpbGVTdGF0ZXNFbnRyeRI+ChBzdW1tYXJ5X2FyY2hpdmVzGAsgAygLMiQuYWdlbnQudjEuQ29udmVyc2F0aW9uU3VtbWFyeUFyY2hpdmUaRgoPRmlsZVN0YXRlc0VudHJ5EgsKA2tleRgBIAEoCRIiCgV2YWx1ZRgCIAEoCzITLmFnZW50LnYxLkZpbGVTdGF0ZToCOAFCCgoIX3N1bW1hcnlCBwoFX3BsYW5CEgoQX3N1bW1hcnlfYXJjaGl2ZSLHAQoWU3ViYWdlbnRQZXJzaXN0ZWRTdGF0ZRJAChJjb252ZXJzYXRpb25fc3RhdGUYASABKAsyJC5hZ2VudC52MS5Db252ZXJzYXRpb25TdGF0ZVN0cnVjdHVyZRIcChRjcmVhdGVkX3RpbWVzdGFtcF9tcxgCIAEoBBIeChZsYXN0X3VzZWRfdGltZXN0YW1wX21zGAMgASgEEi0KDXN1YmFnZW50X3R5cGUYBCABKAsyFi5hZ2VudC52MS5TdWJhZ2VudFR5cGUitwcKGkNvbnZlcnNhdGlvblN0YXRlU3RydWN0dXJlEhEKCXR1cm5zX29sZBgCIAMoDBIhChlyb290X3Byb21wdF9tZXNzYWdlc19qc29uGAEgAygMEg0KBXR1cm5zGAggAygMEg0KBXRvZG9zGAMgAygMEhoKEnBlbmRpbmdfdG9vbF9jYWxscxgEIAMoCRI5Cg10b2tlbl9kZXRhaWxzGAUgASgLMiIuYWdlbnQudjEuQ29udmVyc2F0aW9uVG9rZW5EZXRhaWxzEhQKB3N1bW1hcnkYBiABKAxIAIgBARIRCgRwbGFuGAcgASgMSAGIAQESHwoXcHJldmlvdXNfd29ya3NwYWNlX3VyaXMYCSADKAkSEQoEbW9kZRgKIAEoBUgCiAEBEhwKD3N1bW1hcnlfYXJjaGl2ZRgLIAEoDEgDiAEBEkkKC2ZpbGVfc3RhdGVzGAwgAygLMjQuYWdlbnQudjEuQ29udmVyc2F0aW9uU3RhdGVTdHJ1Y3R1cmUuRmlsZVN0YXRlc0VudHJ5Ek4KDmZpbGVfc3RhdGVzX3YyGA8gAygLMjYuYWdlbnQudjEuQ29udmVyc2F0aW9uU3RhdGVTdHJ1Y3R1cmUuRmlsZVN0YXRlc1YyRW50cnkSGAoQc3VtbWFyeV9hcmNoaXZlcxgNIAMoDBIqCgx0dXJuX3RpbWluZ3MYDiADKAsyFC5hZ2VudC52MS5TdGVwVGltaW5nElEKD3N1YmFnZW50X3N0YXRlcxgQIAMoCzI4LmFnZW50LnYxLkNvbnZlcnNhdGlvblN0YXRlU3RydWN0dXJlLlN1YmFnZW50U3RhdGVzRW50cnkSGgoSc2VsZl9zdW1tYXJ5X2NvdW50GBEgASgNEhIKCnJlYWRfcGF0aHMYEiADKAkaMQoPRmlsZVN0YXRlc0VudHJ5EgsKA2tleRgBIAEoCRINCgV2YWx1ZRgCIAEoDDoCOAEaUQoRRmlsZVN0YXRlc1YyRW50cnkSCwoDa2V5GAEgASgJEisKBXZhbHVlGAIgASgLMhwuYWdlbnQudjEuRmlsZVN0YXRlU3RydWN0dXJlOgI4ARpXChNTdWJhZ2VudFN0YXRlc0VudHJ5EgsKA2tleRgBIAEoCRIvCgV2YWx1ZRgCIAEoCzIgLmFnZW50LnYxLlN1YmFnZW50UGVyc2lzdGVkU3RhdGU6AjgBQgoKCF9zdW1tYXJ5QgcKBV9wbGFuQgcKBV9tb2RlQhIKEF9zdW1tYXJ5X2FyY2hpdmUiEQoPVGhpbmtpbmdEZXRhaWxzIkgKEUFwaUtleUNyZWRlbnRpYWxzEg8KB2FwaV9rZXkYASABKAkSFQoIYmFzZV91cmwYAiABKAlIAIgBAUILCglfYmFzZV91cmwiSQoQQXp1cmVDcmVkZW50aWFscxIPCgdhcGlfa2V5GAEgASgJEhAKCGJhc2VfdXJsGAIgASgJEhIKCmRlcGxveW1lbnQYAyABKAkiegoSQmVkcm9ja0NyZWRlbnRpYWxzEhIKCmFjY2Vzc19rZXkYASABKAkSEgoKc2VjcmV0X2tleRgCIAEoCRIOCgZyZWdpb24YAyABKAkSGgoNc2Vzc2lvbl90b2tlbhgEIAEoCUgAiAEBQhAKDl9zZXNzaW9uX3Rva2VuIrEDCgxNb2RlbERldGFpbHMSEAoIbW9kZWxfaWQYASABKAkSGAoQZGlzcGxheV9tb2RlbF9pZBgDIAEoCRIUCgxkaXNwbGF5X25hbWUYBCABKAkSGgoSZGlzcGxheV9uYW1lX3Nob3J0GAUgASgJEg8KB2FsaWFzZXMYBiADKAkSOAoQdGhpbmtpbmdfZGV0YWlscxgCIAEoCzIZLmFnZW50LnYxLlRoaW5raW5nRGV0YWlsc0gBiAEBEhUKCG1heF9tb2RlGAcgASgISAKIAQESOgoTYXBpX2tleV9jcmVkZW50aWFscxgIIAEoCzIbLmFnZW50LnYxLkFwaUtleUNyZWRlbnRpYWxzSAASNwoRYXp1cmVfY3JlZGVudGlhbHMYCSABKAsyGi5hZ2VudC52MS5BenVyZUNyZWRlbnRpYWxzSAASOwoTYmVkcm9ja19jcmVkZW50aWFscxgKIAEoCzIcLmFnZW50LnYxLkJlZHJvY2tDcmVkZW50aWFsc0gAQg0KC2NyZWRlbnRpYWxzQhMKEV90aGlua2luZ19kZXRhaWxzQgsKCV9tYXhfbW9kZSK3AgoOUmVxdWVzdGVkTW9kZWwSEAoIbW9kZWxfaWQYASABKAkSEAoIbWF4X21vZGUYAiABKAgSQAoKcGFyYW1ldGVycxgDIAMoCzIsLmFnZW50LnYxLlJlcXVlc3RlZE1vZGVsX01vZGVsUGFyYW1ldGVyYnl0ZXMSOgoTYXBpX2tleV9jcmVkZW50aWFscxgEIAEoCzIbLmFnZW50LnYxLkFwaUtleUNyZWRlbnRpYWxzSAASNwoRYXp1cmVfY3JlZGVudGlhbHMYBSABKAsyGi5hZ2VudC52MS5BenVyZUNyZWRlbnRpYWxzSAASOwoTYmVkcm9ja19jcmVkZW50aWFscxgGIAEoCzIcLmFnZW50LnYxLkJlZHJvY2tDcmVkZW50aWFsc0gAQg0KC2NyZWRlbnRpYWxzIj8KIlJlcXVlc3RlZE1vZGVsX01vZGVsUGFyYW1ldGVyYnl0ZXMSCgoCaWQYASABKAkSDQoFdmFsdWUYAiABKAkiuQQKD0FnZW50UnVuUmVxdWVzdBJAChJjb252ZXJzYXRpb25fc3RhdGUYASABKAsyJC5hZ2VudC52MS5Db252ZXJzYXRpb25TdGF0ZVN0cnVjdHVyZRIsCgZhY3Rpb24YAiABKAsyHC5hZ2VudC52MS5Db252ZXJzYXRpb25BY3Rpb24SLQoNbW9kZWxfZGV0YWlscxgDIAEoCzIWLmFnZW50LnYxLk1vZGVsRGV0YWlscxI2Cg9yZXF1ZXN0ZWRfbW9kZWwYCSABKAsyGC5hZ2VudC52MS5SZXF1ZXN0ZWRNb2RlbEgAiAEBEiUKCW1jcF90b29scxgEIAEoCzISLmFnZW50LnYxLk1jcFRvb2xzEhwKD2NvbnZlcnNhdGlvbl9pZBgFIAEoCUgBiAEBEkQKF21jcF9maWxlX3N5c3RlbV9vcHRpb25zGAYgASgLMh4uYWdlbnQudjEuTWNwRmlsZVN5c3RlbU9wdGlvbnNIAogBARIyCg1za2lsbF9vcHRpb25zGAcgASgLMhYuYWdlbnQudjEuU2tpbGxPcHRpb25zSAOIAQESIQoUY3VzdG9tX3N5c3RlbV9wcm9tcHQYCCABKAlIBIgBAUISChBfcmVxdWVzdGVkX21vZGVsQhIKEF9jb252ZXJzYXRpb25faWRCGgoYX21jcF9maWxlX3N5c3RlbV9vcHRpb25zQhAKDl9za2lsbF9vcHRpb25zQhcKFV9jdXN0b21fc3lzdGVtX3Byb21wdCIfCg9UZXh0RGVsdGFVcGRhdGUSDAoEdGV4dBgBIAEoCSJmChVUb29sQ2FsbFN0YXJ0ZWRVcGRhdGUSDwoHY2FsbF9pZBgBIAEoCRIlCgl0b29sX2NhbGwYAiABKAsyEi5hZ2VudC52MS5Ub29sQ2FsbBIVCg1tb2RlbF9jYWxsX2lkGAMgASgJImgKF1Rvb2xDYWxsQ29tcGxldGVkVXBkYXRlEg8KB2NhbGxfaWQYASABKAkSJQoJdG9vbF9jYWxsGAIgASgLMhIuYWdlbnQudjEuVG9vbENhbGwSFQoNbW9kZWxfY2FsbF9pZBgDIAEoCSJvChNUb29sQ2FsbERlbHRhVXBkYXRlEg8KB2NhbGxfaWQYASABKAkSMAoPdG9vbF9jYWxsX2RlbHRhGAIgASgLMhcuYWdlbnQudjEuVG9vbENhbGxEZWx0YRIVCg1tb2RlbF9jYWxsX2lkGAMgASgJIn8KFVBhcnRpYWxUb29sQ2FsbFVwZGF0ZRIPCgdjYWxsX2lkGAEgASgJEiUKCXRvb2xfY2FsbBgCIAEoCzISLmFnZW50LnYxLlRvb2xDYWxsEhcKD2FyZ3NfdGV4dF9kZWx0YRgDIAEoCRIVCg1tb2RlbF9jYWxsX2lkGAQgASgJIiMKE1RoaW5raW5nRGVsdGFVcGRhdGUSDAoEdGV4dBgBIAEoCSI3ChdUaGlua2luZ0NvbXBsZXRlZFVwZGF0ZRIcChR0aGlua2luZ19kdXJhdGlvbl9tcxgBIAEoBSIiChBUb2tlbkRlbHRhVXBkYXRlEg4KBnRva2VucxgBIAEoBSIgCg1TdW1tYXJ5VXBkYXRlEg8KB3N1bW1hcnkYASABKAkiFgoUU3VtbWFyeVN0YXJ0ZWRVcGRhdGUiEQoPSGVhcnRiZWF0VXBkYXRlIhgKFlN1bW1hcnlDb21wbGV0ZWRVcGRhdGUi1wEKFlNoZWxsT3V0cHV0RGVsdGFVcGRhdGUSLQoGc3Rkb3V0GAEgASgLMhsuYWdlbnQudjEuU2hlbGxTdHJlYW1TdGRvdXRIABItCgZzdGRlcnIYAiABKAsyGy5hZ2VudC52MS5TaGVsbFN0cmVhbVN0ZGVyckgAEikKBGV4aXQYAyABKAsyGS5hZ2VudC52MS5TaGVsbFN0cmVhbUV4aXRIABIrCgVzdGFydBgEIAEoCzIaLmFnZW50LnYxLlNoZWxsU3RyZWFtU3RhcnRIAEIHCgVldmVudCIRCg9UdXJuRW5kZWRVcGRhdGUiSAoZVXNlck1lc3NhZ2VBcHBlbmRlZFVwZGF0ZRIrCgx1c2VyX21lc3NhZ2UYASABKAsyFS5hZ2VudC52MS5Vc2VyTWVzc2FnZSIkChFTdGVwU3RhcnRlZFVwZGF0ZRIPCgdzdGVwX2lkGAEgASgEIkAKE1N0ZXBDb21wbGV0ZWRVcGRhdGUSDwoHc3RlcF9pZBgBIAEoBBIYChBzdGVwX2R1cmF0aW9uX21zGAIgASgDIu8HChFJbnRlcmFjdGlvblVwZGF0ZRIvCgp0ZXh0X2RlbHRhGAEgASgLMhkuYWdlbnQudjEuVGV4dERlbHRhVXBkYXRlSAASPAoRcGFydGlhbF90b29sX2NhbGwYByABKAsyHy5hZ2VudC52MS5QYXJ0aWFsVG9vbENhbGxVcGRhdGVIABI4Cg90b29sX2NhbGxfZGVsdGEYDyABKAsyHS5hZ2VudC52MS5Ub29sQ2FsbERlbHRhVXBkYXRlSAASPAoRdG9vbF9jYWxsX3N0YXJ0ZWQYAiABKAsyHy5hZ2VudC52MS5Ub29sQ2FsbFN0YXJ0ZWRVcGRhdGVIABJAChN0b29sX2NhbGxfY29tcGxldGVkGAMgASgLMiEuYWdlbnQudjEuVG9vbENhbGxDb21wbGV0ZWRVcGRhdGVIABI3Cg50aGlua2luZ19kZWx0YRgEIAEoCzIdLmFnZW50LnYxLlRoaW5raW5nRGVsdGFVcGRhdGVIABI/ChJ0aGlua2luZ19jb21wbGV0ZWQYBSABKAsyIS5hZ2VudC52MS5UaGlua2luZ0NvbXBsZXRlZFVwZGF0ZUgAEkQKFXVzZXJfbWVzc2FnZV9hcHBlbmRlZBgGIAEoCzIjLmFnZW50LnYxLlVzZXJNZXNzYWdlQXBwZW5kZWRVcGRhdGVIABIxCgt0b2tlbl9kZWx0YRgIIAEoCzIaLmFnZW50LnYxLlRva2VuRGVsdGFVcGRhdGVIABIqCgdzdW1tYXJ5GAkgASgLMhcuYWdlbnQudjEuU3VtbWFyeVVwZGF0ZUgAEjkKD3N1bW1hcnlfc3RhcnRlZBgKIAEoCzIeLmFnZW50LnYxLlN1bW1hcnlTdGFydGVkVXBkYXRlSAASPQoRc3VtbWFyeV9jb21wbGV0ZWQYCyABKAsyIC5hZ2VudC52MS5TdW1tYXJ5Q29tcGxldGVkVXBkYXRlSAASPgoSc2hlbGxfb3V0cHV0X2RlbHRhGAwgASgLMiAuYWdlbnQudjEuU2hlbGxPdXRwdXREZWx0YVVwZGF0ZUgAEi4KCWhlYXJ0YmVhdBgNIAEoCzIZLmFnZW50LnYxLkhlYXJ0YmVhdFVwZGF0ZUgAEi8KCnR1cm5fZW5kZWQYDiABKAsyGS5hZ2VudC52MS5UdXJuRW5kZWRVcGRhdGVIABIzCgxzdGVwX3N0YXJ0ZWQYECABKAsyGy5hZ2VudC52MS5TdGVwU3RhcnRlZFVwZGF0ZUgAEjcKDnN0ZXBfY29tcGxldGVkGBEgASgLMh0uYWdlbnQudjEuU3RlcENvbXBsZXRlZFVwZGF0ZUgAQgkKB21lc3NhZ2UimgQKEEludGVyYWN0aW9uUXVlcnkSCgoCaWQYASABKA0SQwoYd2ViX3NlYXJjaF9yZXF1ZXN0X3F1ZXJ5GAIgASgLMh8uYWdlbnQudjEuV2ViU2VhcmNoUmVxdWVzdFF1ZXJ5SAASTwoeYXNrX3F1ZXN0aW9uX2ludGVyYWN0aW9uX3F1ZXJ5GAMgASgLMiUuYWdlbnQudjEuQXNrUXVlc3Rpb25JbnRlcmFjdGlvblF1ZXJ5SAASRQoZc3dpdGNoX21vZGVfcmVxdWVzdF9xdWVyeRgEIAEoCzIgLmFnZW50LnYxLlN3aXRjaE1vZGVSZXF1ZXN0UXVlcnlIABJDChhleGFfc2VhcmNoX3JlcXVlc3RfcXVlcnkYBSABKAsyHy5hZ2VudC52MS5FeGFTZWFyY2hSZXF1ZXN0UXVlcnlIABJBChdleGFfZmV0Y2hfcmVxdWVzdF9xdWVyeRgGIAEoCzIeLmFnZW50LnYxLkV4YUZldGNoUmVxdWVzdFF1ZXJ5SAASRQoZY3JlYXRlX3BsYW5fcmVxdWVzdF9xdWVyeRgHIAEoCzIgLmFnZW50LnYxLkNyZWF0ZVBsYW5SZXF1ZXN0UXVlcnlIABJFChlzZXR1cF92bV9lbnZpcm9ubWVudF9hcmdzGAggASgLMiAuYWdlbnQudjEuU2V0dXBWbUVudmlyb25tZW50QXJnc0gAQgcKBXF1ZXJ5IsYEChNJbnRlcmFjdGlvblJlc3BvbnNlEgoKAmlkGAEgASgNEkkKG3dlYl9zZWFyY2hfcmVxdWVzdF9yZXNwb25zZRgCIAEoCzIiLmFnZW50LnYxLldlYlNlYXJjaFJlcXVlc3RSZXNwb25zZUgAElUKIWFza19xdWVzdGlvbl9pbnRlcmFjdGlvbl9yZXNwb25zZRgDIAEoCzIoLmFnZW50LnYxLkFza1F1ZXN0aW9uSW50ZXJhY3Rpb25SZXNwb25zZUgAEksKHHN3aXRjaF9tb2RlX3JlcXVlc3RfcmVzcG9uc2UYBCABKAsyIy5hZ2VudC52MS5Td2l0Y2hNb2RlUmVxdWVzdFJlc3BvbnNlSAASSQobZXhhX3NlYXJjaF9yZXF1ZXN0X3Jlc3BvbnNlGAUgASgLMiIuYWdlbnQudjEuRXhhU2VhcmNoUmVxdWVzdFJlc3BvbnNlSAASRwoaZXhhX2ZldGNoX3JlcXVlc3RfcmVzcG9uc2UYBiABKAsyIS5hZ2VudC52MS5FeGFGZXRjaFJlcXVlc3RSZXNwb25zZUgAEksKHGNyZWF0ZV9wbGFuX3JlcXVlc3RfcmVzcG9uc2UYByABKAsyIy5hZ2VudC52MS5DcmVhdGVQbGFuUmVxdWVzdFJlc3BvbnNlSAASSQobc2V0dXBfdm1fZW52aXJvbm1lbnRfcmVzdWx0GAggASgLMiIuYWdlbnQudjEuU2V0dXBWbUVudmlyb25tZW50UmVzdWx0SABCCAoGcmVzdWx0IlwKG0Fza1F1ZXN0aW9uSW50ZXJhY3Rpb25RdWVyeRInCgRhcmdzGAEgASgLMhkuYWdlbnQudjEuQXNrUXVlc3Rpb25BcmdzEhQKDHRvb2xfY2FsbF9pZBgCIAEoCSJNCh5Bc2tRdWVzdGlvbkludGVyYWN0aW9uUmVzcG9uc2USKwoGcmVzdWx0GAEgASgLMhsuYWdlbnQudjEuQXNrUXVlc3Rpb25SZXN1bHQiEQoPQ2xpZW50SGVhcnRiZWF0IsYECg5QcmV3YXJtUmVxdWVzdBItCg1tb2RlbF9kZXRhaWxzGAEgASgLMhYuYWdlbnQudjEuTW9kZWxEZXRhaWxzEjYKD3JlcXVlc3RlZF9tb2RlbBgJIAEoCzIYLmFnZW50LnYxLlJlcXVlc3RlZE1vZGVsSACIAQESHAoPY29udmVyc2F0aW9uX2lkGAIgASgJSAGIAQESQAoSY29udmVyc2F0aW9uX3N0YXRlGAMgASgLMiQuYWdlbnQudjEuQ29udmVyc2F0aW9uU3RhdGVTdHJ1Y3R1cmUSJQoJbWNwX3Rvb2xzGAQgASgLMhIuYWdlbnQudjEuTWNwVG9vbHMSRAoXbWNwX2ZpbGVfc3lzdGVtX29wdGlvbnMYBSABKAsyHi5hZ2VudC52MS5NY3BGaWxlU3lzdGVtT3B0aW9uc0gCiAEBEh8KEmJlc3Rfb2Zfbl9ncm91cF9pZBgGIAEoCUgDiAEBEigKG3RyeV91c2VfYmVzdF9vZl9uX3Byb21vdGlvbhgHIAEoCEgEiAEBEiEKFGN1c3RvbV9zeXN0ZW1fcHJvbXB0GAggASgJSAWIAQFCEgoQX3JlcXVlc3RlZF9tb2RlbEISChBfY29udmVyc2F0aW9uX2lkQhoKGF9tY3BfZmlsZV9zeXN0ZW1fb3B0aW9uc0IVChNfYmVzdF9vZl9uX2dyb3VwX2lkQh4KHF90cnlfdXNlX2Jlc3Rfb2Zfbl9wcm9tb3Rpb25CFwoVX2N1c3RvbV9zeXN0ZW1fcHJvbXB0Ih0KD0V4ZWNTZXJ2ZXJBYm9ydBIKCgJpZBgBIAEoDSJRChhFeGVjU2VydmVyQ29udHJvbE1lc3NhZ2USKgoFYWJvcnQYASABKAsyGS5hZ2VudC52MS5FeGVjU2VydmVyQWJvcnRIAEIJCgdtZXNzYWdlIvgDChJBZ2VudENsaWVudE1lc3NhZ2USMAoLcnVuX3JlcXVlc3QYASABKAsyGS5hZ2VudC52MS5BZ2VudFJ1blJlcXVlc3RIABI6ChNleGVjX2NsaWVudF9tZXNzYWdlGAIgASgLMhsuYWdlbnQudjEuRXhlY0NsaWVudE1lc3NhZ2VIABJJChtleGVjX2NsaWVudF9jb250cm9sX21lc3NhZ2UYBSABKAsyIi5hZ2VudC52MS5FeGVjQ2xpZW50Q29udHJvbE1lc3NhZ2VIABI2ChFrdl9jbGllbnRfbWVzc2FnZRgDIAEoCzIZLmFnZW50LnYxLkt2Q2xpZW50TWVzc2FnZUgAEjsKE2NvbnZlcnNhdGlvbl9hY3Rpb24YBCABKAsyHC5hZ2VudC52MS5Db252ZXJzYXRpb25BY3Rpb25IABI9ChRpbnRlcmFjdGlvbl9yZXNwb25zZRgGIAEoCzIdLmFnZW50LnYxLkludGVyYWN0aW9uUmVzcG9uc2VIABI1ChBjbGllbnRfaGVhcnRiZWF0GAcgASgLMhkuYWdlbnQudjEuQ2xpZW50SGVhcnRiZWF0SAASMwoPcHJld2FybV9yZXF1ZXN0GAggASgLMhguYWdlbnQudjEuUHJld2FybVJlcXVlc3RIAEIJCgdtZXNzYWdlIqIDChJBZ2VudFNlcnZlck1lc3NhZ2USOQoSaW50ZXJhY3Rpb25fdXBkYXRlGAEgASgLMhsuYWdlbnQudjEuSW50ZXJhY3Rpb25VcGRhdGVIABI6ChNleGVjX3NlcnZlcl9tZXNzYWdlGAIgASgLMhsuYWdlbnQudjEuRXhlY1NlcnZlck1lc3NhZ2VIABJJChtleGVjX3NlcnZlcl9jb250cm9sX21lc3NhZ2UYBSABKAsyIi5hZ2VudC52MS5FeGVjU2VydmVyQ29udHJvbE1lc3NhZ2VIABJOCh5jb252ZXJzYXRpb25fY2hlY2twb2ludF91cGRhdGUYAyABKAsyJC5hZ2VudC52MS5Db252ZXJzYXRpb25TdGF0ZVN0cnVjdHVyZUgAEjYKEWt2X3NlcnZlcl9tZXNzYWdlGAQgASgLMhkuYWdlbnQudjEuS3ZTZXJ2ZXJNZXNzYWdlSAASNwoRaW50ZXJhY3Rpb25fcXVlcnkYByABKAsyGi5hZ2VudC52MS5JbnRlcmFjdGlvblF1ZXJ5SABCCQoHbWVzc2FnZSIoChBOYW1lQWdlbnRSZXF1ZXN0EhQKDHVzZXJfbWVzc2FnZRgBIAEoCSIhChFOYW1lQWdlbnRSZXNwb25zZRIMCgRuYW1lGAEgASgJIjIKFkdldFVzYWJsZU1vZGVsc1JlcXVlc3QSGAoQY3VzdG9tX21vZGVsX2lkcxgBIAMoCSJBChdHZXRVc2FibGVNb2RlbHNSZXNwb25zZRImCgZtb2RlbHMYASADKAsyFi5hZ2VudC52MS5Nb2RlbERldGFpbHMiHgocR2V0RGVmYXVsdE1vZGVsRm9yQ2xpUmVxdWVzdCJGCh1HZXREZWZhdWx0TW9kZWxGb3JDbGlSZXNwb25zZRIlCgVtb2RlbBgBIAEoCzIWLmFnZW50LnYxLk1vZGVsRGV0YWlscyIfCh1HZXRBbGxvd2VkTW9kZWxJbnRlbnRzUmVxdWVzdCI3Ch5HZXRBbGxvd2VkTW9kZWxJbnRlbnRzUmVzcG9uc2USFQoNbW9kZWxfaW50ZW50cxgBIAMoCSKXAgoTSWRlRWRpdG9yc1N0YXRlRmlsZRIVCg1yZWxhdGl2ZV9wYXRoGAEgASgJEhUKDWFic29sdXRlX3BhdGgYAiABKAkSIQoUaXNfY3VycmVudGx5X2ZvY3VzZWQYAyABKAhIAIgBARIgChNjdXJyZW50X2xpbmVfbnVtYmVyGAQgASgFSAGIAQESHgoRY3VycmVudF9saW5lX3RleHQYBSABKAlIAogBARIXCgpsaW5lX2NvdW50GAYgASgFSAOIAQFCFwoVX2lzX2N1cnJlbnRseV9mb2N1c2VkQhYKFF9jdXJyZW50X2xpbmVfbnVtYmVyQhQKEl9jdXJyZW50X2xpbmVfdGV4dEINCgtfbGluZV9jb3VudCJTChNJZGVFZGl0b3JzU3RhdGVMaXRlEjwKFXJlY2VudGx5X3ZpZXdlZF9maWxlcxgBIAMoCzIdLmFnZW50LnYxLklkZUVkaXRvcnNTdGF0ZUZpbGUidAoWQXBwbHlBZ2VudERpZmZUb29sQ2FsbBIqCgRhcmdzGAEgASgLMhwuYWdlbnQudjEuQXBwbHlBZ2VudERpZmZBcmdzEi4KBnJlc3VsdBgCIAEoCzIeLmFnZW50LnYxLkFwcGx5QWdlbnREaWZmUmVzdWx0IiYKEkFwcGx5QWdlbnREaWZmQXJncxIQCghhZ2VudF9pZBgBIAEoCSKEAQoUQXBwbHlBZ2VudERpZmZSZXN1bHQSMgoHc3VjY2VzcxgBIAEoCzIfLmFnZW50LnYxLkFwcGx5QWdlbnREaWZmU3VjY2Vzc0gAEi4KBWVycm9yGAIgASgLMh0uYWdlbnQudjEuQXBwbHlBZ2VudERpZmZFcnJvckgAQggKBnJlc3VsdCJOChVBcHBseUFnZW50RGlmZlN1Y2Nlc3MSNQoPYXBwbGllZF9jaGFuZ2VzGAEgAygLMhwuYWdlbnQudjEuQXBwbGllZEFnZW50Q2hhbmdlIukBChJBcHBsaWVkQWdlbnRDaGFuZ2USDAoEcGF0aBgBIAEoCRITCgtjaGFuZ2VfdHlwZRgCIAEoBRIbCg5iZWZvcmVfY29udGVudBgDIAEoCUgAiAEBEhoKDWFmdGVyX2NvbnRlbnQYBCABKAlIAYgBARISCgVlcnJvchgFIAEoCUgCiAEBEh4KEW1lc3NhZ2VfZm9yX21vZGVsGAYgASgJSAOIAQFCEQoPX2JlZm9yZV9jb250ZW50QhAKDl9hZnRlcl9jb250ZW50QggKBl9lcnJvckIUChJfbWVzc2FnZV9mb3JfbW9kZWwiWwoTQXBwbHlBZ2VudERpZmZFcnJvchINCgVlcnJvchgBIAEoCRI1Cg9hcHBsaWVkX2NoYW5nZXMYAiADKAsyHC5hZ2VudC52MS5BcHBsaWVkQWdlbnRDaGFuZ2UiawoTQXNrUXVlc3Rpb25Ub29sQ2FsbBInCgRhcmdzGAEgASgLMhkuYWdlbnQudjEuQXNrUXVlc3Rpb25BcmdzEisKBnJlc3VsdBgCIAEoCzIbLmFnZW50LnYxLkFza1F1ZXN0aW9uUmVzdWx0Io8BCg9Bc2tRdWVzdGlvbkFyZ3MSDQoFdGl0bGUYASABKAkSNQoJcXVlc3Rpb25zGAIgAygLMiIuYWdlbnQudjEuQXNrUXVlc3Rpb25BcmdzX1F1ZXN0aW9uEhEKCXJ1bl9hc3luYxgFIAEoCBIjChthc3luY19vcmlnaW5hbF90b29sX2NhbGxfaWQYBiABKAkigQEKGEFza1F1ZXN0aW9uQXJnc19RdWVzdGlvbhIKCgJpZBgBIAEoCRIOCgZwcm9tcHQYAiABKAkSMQoHb3B0aW9ucxgDIAMoCzIgLmFnZW50LnYxLkFza1F1ZXN0aW9uQXJnc19PcHRpb24SFgoOYWxsb3dfbXVsdGlwbGUYBCABKAgiMwoWQXNrUXVlc3Rpb25BcmdzX09wdGlvbhIKCgJpZBgBIAEoCRINCgVsYWJlbBgCIAEoCSISChBBc2tRdWVzdGlvbkFzeW5jItsBChFBc2tRdWVzdGlvblJlc3VsdBIvCgdzdWNjZXNzGAEgASgLMhwuYWdlbnQudjEuQXNrUXVlc3Rpb25TdWNjZXNzSAASKwoFZXJyb3IYAiABKAsyGi5hZ2VudC52MS5Bc2tRdWVzdGlvbkVycm9ySAASMQoIcmVqZWN0ZWQYAyABKAsyHS5hZ2VudC52MS5Bc2tRdWVzdGlvblJlamVjdGVkSAASKwoFYXN5bmMYBCABKAsyGi5hZ2VudC52MS5Bc2tRdWVzdGlvbkFzeW5jSABCCAoGcmVzdWx0IkoKEkFza1F1ZXN0aW9uU3VjY2VzcxI0CgdhbnN3ZXJzGAEgAygLMiMuYWdlbnQudjEuQXNrUXVlc3Rpb25TdWNjZXNzX0Fuc3dlciJNChlBc2tRdWVzdGlvblN1Y2Nlc3NfQW5zd2VyEhMKC3F1ZXN0aW9uX2lkGAEgASgJEhsKE3NlbGVjdGVkX29wdGlvbl9pZHMYAiADKAkiKQoQQXNrUXVlc3Rpb25FcnJvchIVCg1lcnJvcl9tZXNzYWdlGAEgASgJIiUKE0Fza1F1ZXN0aW9uUmVqZWN0ZWQSDgoGcmVhc29uGAEgASgJIv0FChhCYWNrZ3JvdW5kU2hlbGxTcGF3bkFyZ3MSDwoHY29tbWFuZBgBIAEoCRIZChF3b3JraW5nX2RpcmVjdG9yeRgCIAEoCRIUCgx0b29sX2NhbGxfaWQYAyABKAkSOwoOcGFyc2luZ19yZXN1bHQYBCABKAsyIy5hZ2VudC52MS5TaGVsbENvbW1hbmRQYXJzaW5nUmVzdWx0EjQKDnNhbmRib3hfcG9saWN5GAUgASgLMhcuYWdlbnQudjEuU2FuZGJveFBvbGljeUgAiAEBEiUKHWVuYWJsZV93cml0ZV9zaGVsbF9zdGRpbl90b29sGAYgASgIEhgKC2Rlc2NyaXB0aW9uGAcgASgJSAGIAQESQQoRY2xhc3NpZmllcl9yZXN1bHQYCCABKAsyIS5hZ2VudC52MS5Db21tYW5kQ2xhc3NpZmllclJlc3VsdEgCiAEBEkkKE291dHB1dF9ub3RpZmljYXRpb24YCSABKAsyJy5hZ2VudC52MS5TaGVsbE91dHB1dE5vdGlmaWNhdGlvbkNvbmZpZ0gDiAEBEj0KE3NtYXJ0X21vZGVfYXBwcm92YWwYCiABKAsyGy5hZ2VudC52MS5TbWFydE1vZGVBcHByb3ZhbEgEiAEBEk4KGWhvb2tfYXBwcm92YWxfcmVxdWlyZW1lbnQYCyABKAsyJi5hZ2VudC52MS5TaGVsbEhvb2tBcHByb3ZhbFJlcXVpcmVtZW50SAWIAQESFQoNc2tpcF9hcHByb3ZhbBgMIAEoCBIcCg9jb252ZXJzYXRpb25faWQYDSABKAlIBogBAUIRCg9fc2FuZGJveF9wb2xpY3lCDgoMX2Rlc2NyaXB0aW9uQhQKEl9jbGFzc2lmaWVyX3Jlc3VsdEIWChRfb3V0cHV0X25vdGlmaWNhdGlvbkIWChRfc21hcnRfbW9kZV9hcHByb3ZhbEIcChpfaG9va19hcHByb3ZhbF9yZXF1aXJlbWVudEISChBfY29udmVyc2F0aW9uX2lkIoECChpCYWNrZ3JvdW5kU2hlbGxTcGF3blJlc3VsdBI4CgdzdWNjZXNzGAEgASgLMiUuYWdlbnQudjEuQmFja2dyb3VuZFNoZWxsU3Bhd25TdWNjZXNzSAASNAoFZXJyb3IYAiABKAsyIy5hZ2VudC52MS5CYWNrZ3JvdW5kU2hlbGxTcGF3bkVycm9ySAASKwoIcmVqZWN0ZWQYAyABKAsyFy5hZ2VudC52MS5TaGVsbFJlamVjdGVkSAASPAoRcGVybWlzc2lvbl9kZW5pZWQYBCABKAsyHy5hZ2VudC52MS5TaGVsbFBlcm1pc3Npb25EZW5pZWRIAEIICgZyZXN1bHQidQobQmFja2dyb3VuZFNoZWxsU3Bhd25TdWNjZXNzEhAKCHNoZWxsX2lkGAEgASgNEg8KB2NvbW1hbmQYAiABKAkSGQoRd29ya2luZ19kaXJlY3RvcnkYAyABKAkSEAoDcGlkGAQgASgNSACIAQFCBgoEX3BpZCJWChlCYWNrZ3JvdW5kU2hlbGxTcGF3bkVycm9yEg8KB2NvbW1hbmQYASABKAkSGQoRd29ya2luZ19kaXJlY3RvcnkYAiABKAkSDQoFZXJyb3IYAyABKAkiNgoTV3JpdGVTaGVsbFN0ZGluQXJncxIQCghzaGVsbF9pZBgBIAEoDRINCgVjaGFycxgCIAEoCSKHAQoVV3JpdGVTaGVsbFN0ZGluUmVzdWx0EjMKB3N1Y2Nlc3MYASABKAsyIC5hZ2VudC52MS5Xcml0ZVNoZWxsU3RkaW5TdWNjZXNzSAASLwoFZXJyb3IYAiABKAsyHi5hZ2VudC52MS5Xcml0ZVNoZWxsU3RkaW5FcnJvckgAQggKBnJlc3VsdCJdChZXcml0ZVNoZWxsU3RkaW5TdWNjZXNzEhAKCHNoZWxsX2lkGAEgASgNEjEKKXRlcm1pbmFsX2ZpbGVfbGVuZ3RoX2JlZm9yZV9pbnB1dF93cml0dGVuGAIgASgNIiUKFFdyaXRlU2hlbGxTdGRpbkVycm9yEg0KBWVycm9yGAEgASgJIiIKCkNvb3JkaW5hdGUSCQoBeBgBIAEoBRIJCgF5GAIgASgFIlUKD0NvbXB1dGVyVXNlQXJncxIUCgx0b29sX2NhbGxfaWQYASABKAkSLAoHYWN0aW9ucxgCIAMoCzIbLmFnZW50LnYxLkNvbXB1dGVyVXNlQWN0aW9uIoEEChFDb21wdXRlclVzZUFjdGlvbhIvCgptb3VzZV9tb3ZlGAEgASgLMhkuYWdlbnQudjEuTW91c2VNb3ZlQWN0aW9uSAASJgoFY2xpY2sYAiABKAsyFS5hZ2VudC52MS5DbGlja0FjdGlvbkgAEi8KCm1vdXNlX2Rvd24YAyABKAsyGS5hZ2VudC52MS5Nb3VzZURvd25BY3Rpb25IABIrCghtb3VzZV91cBgEIAEoCzIXLmFnZW50LnYxLk1vdXNlVXBBY3Rpb25IABIkCgRkcmFnGAUgASgLMhQuYWdlbnQudjEuRHJhZ0FjdGlvbkgAEigKBnNjcm9sbBgGIAEoCzIWLmFnZW50LnYxLlNjcm9sbEFjdGlvbkgAEiQKBHR5cGUYByABKAsyFC5hZ2VudC52MS5UeXBlQWN0aW9uSAASIgoDa2V5GAggASgLMhMuYWdlbnQudjEuS2V5QWN0aW9uSAASJAoEd2FpdBgJIAEoCzIULmFnZW50LnYxLldhaXRBY3Rpb25IABIwCgpzY3JlZW5zaG90GAogASgLMhouYWdlbnQudjEuU2NyZWVuc2hvdEFjdGlvbkgAEjkKD2N1cnNvcl9wb3NpdGlvbhgLIAEoCzIeLmFnZW50LnYxLkN1cnNvclBvc2l0aW9uQWN0aW9uSABCCAoGYWN0aW9uIjsKD01vdXNlTW92ZUFjdGlvbhIoCgpjb29yZGluYXRlGAEgASgLMhQuYWdlbnQudjEuQ29vcmRpbmF0ZSKYAQoLQ2xpY2tBY3Rpb24SLQoKY29vcmRpbmF0ZRgBIAEoCzIULmFnZW50LnYxLkNvb3JkaW5hdGVIAIgBARIOCgZidXR0b24YAiABKAUSDQoFY291bnQYAyABKAUSGgoNbW9kaWZpZXJfa2V5cxgEIAEoCUgBiAEBQg0KC19jb29yZGluYXRlQhAKDl9tb2RpZmllcl9rZXlzIiEKD01vdXNlRG93bkFjdGlvbhIOCgZidXR0b24YASABKAUiHwoNTW91c2VVcEFjdGlvbhIOCgZidXR0b24YASABKAUiQAoKRHJhZ0FjdGlvbhIiCgRwYXRoGAEgAygLMhQuYWdlbnQudjEuQ29vcmRpbmF0ZRIOCgZidXR0b24YAiABKAUinQEKDFNjcm9sbEFjdGlvbhItCgpjb29yZGluYXRlGAEgASgLMhQuYWdlbnQudjEuQ29vcmRpbmF0ZUgAiAEBEhEKCWRpcmVjdGlvbhgCIAEoBRIOCgZhbW91bnQYAyABKAUSGgoNbW9kaWZpZXJfa2V5cxgEIAEoCUgBiAEBQg0KC19jb29yZGluYXRlQhAKDl9tb2RpZmllcl9rZXlzIhoKClR5cGVBY3Rpb24SDAoEdGV4dBgBIAEoCSJMCglLZXlBY3Rpb24SCwoDa2V5GAEgASgJEh0KEGhvbGRfZHVyYXRpb25fbXMYAiABKAVIAIgBAUITChFfaG9sZF9kdXJhdGlvbl9tcyIhCgpXYWl0QWN0aW9uEhMKC2R1cmF0aW9uX21zGAEgASgFIhIKEFNjcmVlbnNob3RBY3Rpb24iFgoUQ3Vyc29yUG9zaXRpb25BY3Rpb24iewoRQ29tcHV0ZXJVc2VSZXN1bHQSLwoHc3VjY2VzcxgBIAEoCzIcLmFnZW50LnYxLkNvbXB1dGVyVXNlU3VjY2Vzc0gAEisKBWVycm9yGAIgASgLMhouYWdlbnQudjEuQ29tcHV0ZXJVc2VFcnJvckgAQggKBnJlc3VsdCL7AQoSQ29tcHV0ZXJVc2VTdWNjZXNzEhQKDGFjdGlvbl9jb3VudBgBIAEoBRITCgtkdXJhdGlvbl9tcxgCIAEoBRIXCgpzY3JlZW5zaG90GAMgASgJSACIAQESEAoDbG9nGAQgASgJSAGIAQESHAoPc2NyZWVuc2hvdF9wYXRoGAUgASgJSAKIAQESMgoPY3Vyc29yX3Bvc2l0aW9uGAYgASgLMhQuYWdlbnQudjEuQ29vcmRpbmF0ZUgDiAEBQg0KC19zY3JlZW5zaG90QgYKBF9sb2dCEgoQX3NjcmVlbnNob3RfcGF0aEISChBfY3Vyc29yX3Bvc2l0aW9uIsABChBDb21wdXRlclVzZUVycm9yEg0KBWVycm9yGAEgASgJEhQKDGFjdGlvbl9jb3VudBgCIAEoBRITCgtkdXJhdGlvbl9tcxgDIAEoBRIQCgNsb2cYBCABKAlIAIgBARIXCgpzY3JlZW5zaG90GAUgASgJSAGIAQESHAoPc2NyZWVuc2hvdF9wYXRoGAYgASgJSAKIAQFCBgoEX2xvZ0INCgtfc2NyZWVuc2hvdEISChBfc2NyZWVuc2hvdF9wYXRoImsKE0NvbXB1dGVyVXNlVG9vbENhbGwSJwoEYXJncxgBIAEoCzIZLmFnZW50LnYxLkNvbXB1dGVyVXNlQXJncxIrCgZyZXN1bHQYAiABKAsyGy5hZ2VudC52MS5Db21wdXRlclVzZVJlc3VsdCJoChJDcmVhdGVQbGFuVG9vbENhbGwSJgoEYXJncxgBIAEoCzIYLmFnZW50LnYxLkNyZWF0ZVBsYW5BcmdzEioKBnJlc3VsdBgCIAEoCzIaLmFnZW50LnYxLkNyZWF0ZVBsYW5SZXN1bHQiOAoFUGhhc2USDAoEbmFtZRgBIAEoCRIhCgV0b2RvcxgCIAMoCzISLmFnZW50LnYxLlRvZG9JdGVtIpYBCg5DcmVhdGVQbGFuQXJncxIMCgRwbGFuGAEgASgJEiEKBXRvZG9zGAIgAygLMhIuYWdlbnQudjEuVG9kb0l0ZW0SEAoIb3ZlcnZpZXcYAyABKAkSDAoEbmFtZRgEIAEoCRISCgppc19wcm9qZWN0GAUgASgIEh8KBnBoYXNlcxgGIAMoCzIPLmFnZW50LnYxLlBoYXNlIooBChBDcmVhdGVQbGFuUmVzdWx0EhAKCHBsYW5fdXJpGAMgASgJEi4KB3N1Y2Nlc3MYASABKAsyGy5hZ2VudC52MS5DcmVhdGVQbGFuU3VjY2Vzc0gAEioKBWVycm9yGAIgASgLMhkuYWdlbnQudjEuQ3JlYXRlUGxhbkVycm9ySABCCAoGcmVzdWx0IhMKEUNyZWF0ZVBsYW5TdWNjZXNzIiAKD0NyZWF0ZVBsYW5FcnJvchINCgVlcnJvchgBIAEoCSJWChZDcmVhdGVQbGFuUmVxdWVzdFF1ZXJ5EiYKBGFyZ3MYASABKAsyGC5hZ2VudC52MS5DcmVhdGVQbGFuQXJncxIUCgx0b29sX2NhbGxfaWQYAiABKAkiRwoZQ3JlYXRlUGxhblJlcXVlc3RSZXNwb25zZRIqCgZyZXN1bHQYASABKAsyGi5hZ2VudC52MS5DcmVhdGVQbGFuUmVzdWx0IhYKFEN1cnNvclJ1bGVUeXBlR2xvYmFsIigKF0N1cnNvclJ1bGVUeXBlRmlsZUdsb2JzEg0KBWdsb2JzGAEgAygJIjEKGkN1cnNvclJ1bGVUeXBlQWdlbnRGZXRjaGVkEhMKC2Rlc2NyaXB0aW9uGAEgASgJIiAKHkN1cnNvclJ1bGVUeXBlTWFudWFsbHlBdHRhY2hlZCKLAgoOQ3Vyc29yUnVsZVR5cGUSMAoGZ2xvYmFsGAEgASgLMh4uYWdlbnQudjEuQ3Vyc29yUnVsZVR5cGVHbG9iYWxIABI5CgxmaWxlX2dsb2JiZWQYAiABKAsyIS5hZ2VudC52MS5DdXJzb3JSdWxlVHlwZUZpbGVHbG9ic0gAEj0KDWFnZW50X2ZldGNoZWQYAyABKAsyJC5hZ2VudC52MS5DdXJzb3JSdWxlVHlwZUFnZW50RmV0Y2hlZEgAEkUKEW1hbnVhbGx5X2F0dGFjaGVkGAQgASgLMiguYWdlbnQudjEuQ3Vyc29yUnVsZVR5cGVNYW51YWxseUF0dGFjaGVkSABCBgoEdHlwZSLIAQoKQ3Vyc29yUnVsZRIRCglmdWxsX3BhdGgYASABKAkSDwoHY29udGVudBgCIAEoCRImCgR0eXBlGAMgASgLMhguYWdlbnQudjEuQ3Vyc29yUnVsZVR5cGUSDgoGc291cmNlGAQgASgFEh4KEWdpdF9yZW1vdGVfb3JpZ2luGAUgASgJSACIAQESGAoLcGFyc2VfZXJyb3IYBiABKAlIAYgBAUIUChJfZ2l0X3JlbW90ZV9vcmlnaW5CDgoMX3BhcnNlX2Vycm9yIjAKCkRlbGV0ZUFyZ3MSDAoEcGF0aBgBIAEoCRIUCgx0b29sX2NhbGxfaWQYAiABKAki7QIKDERlbGV0ZVJlc3VsdBIqCgdzdWNjZXNzGAEgASgLMhcuYWdlbnQudjEuRGVsZXRlU3VjY2Vzc0gAEjYKDmZpbGVfbm90X2ZvdW5kGAIgASgLMhwuYWdlbnQudjEuRGVsZXRlRmlsZU5vdEZvdW5kSAASKwoIbm90X2ZpbGUYAyABKAsyFy5hZ2VudC52MS5EZWxldGVOb3RGaWxlSAASPQoRcGVybWlzc2lvbl9kZW5pZWQYBCABKAsyIC5hZ2VudC52MS5EZWxldGVQZXJtaXNzaW9uRGVuaWVkSAASLQoJZmlsZV9idXN5GAUgASgLMhguYWdlbnQudjEuRGVsZXRlRmlsZUJ1c3lIABIsCghyZWplY3RlZBgGIAEoCzIYLmFnZW50LnYxLkRlbGV0ZVJlamVjdGVkSAASJgoFZXJyb3IYByABKAsyFS5hZ2VudC52MS5EZWxldGVFcnJvckgAQggKBnJlc3VsdCJcCg1EZWxldGVTdWNjZXNzEgwKBHBhdGgYASABKAkSFAoMZGVsZXRlZF9maWxlGAIgASgJEhEKCWZpbGVfc2l6ZRgDIAEoAxIUCgxwcmV2X2NvbnRlbnQYBCABKAkiIgoSRGVsZXRlRmlsZU5vdEZvdW5kEgwKBHBhdGgYASABKAkiMgoNRGVsZXRlTm90RmlsZRIMCgRwYXRoGAEgASgJEhMKC2FjdHVhbF90eXBlGAIgASgJIlkKFkRlbGV0ZVBlcm1pc3Npb25EZW5pZWQSDAoEcGF0aBgBIAEoCRIcChRjbGllbnRfdmlzaWJsZV9lcnJvchgCIAEoCRITCgtpc19yZWFkb25seRgDIAEoCCIeCg5EZWxldGVGaWxlQnVzeRIMCgRwYXRoGAEgASgJIi4KDkRlbGV0ZVJlamVjdGVkEgwKBHBhdGgYASABKAkSDgoGcmVhc29uGAIgASgJIioKC0RlbGV0ZUVycm9yEgwKBHBhdGgYASABKAkSDQoFZXJyb3IYAiABKAkiXAoORGVsZXRlVG9vbENhbGwSIgoEYXJncxgBIAEoCzIULmFnZW50LnYxLkRlbGV0ZUFyZ3MSJgoGcmVzdWx0GAIgASgLMhYuYWdlbnQudjEuRGVsZXRlUmVzdWx0IjUKD0RpYWdub3N0aWNzQXJncxIMCgRwYXRoGAEgASgJEhQKDHRvb2xfY2FsbF9pZBgCIAEoCSKvAgoRRGlhZ25vc3RpY3NSZXN1bHQSLwoHc3VjY2VzcxgBIAEoCzIcLmFnZW50LnYxLkRpYWdub3N0aWNzU3VjY2Vzc0gAEisKBWVycm9yGAIgASgLMhouYWdlbnQudjEuRGlhZ25vc3RpY3NFcnJvckgAEjEKCHJlamVjdGVkGAMgASgLMh0uYWdlbnQudjEuRGlhZ25vc3RpY3NSZWplY3RlZEgAEjsKDmZpbGVfbm90X2ZvdW5kGAQgASgLMiEuYWdlbnQudjEuRGlhZ25vc3RpY3NGaWxlTm90Rm91bmRIABJCChFwZXJtaXNzaW9uX2RlbmllZBgFIAEoCzIlLmFnZW50LnYxLkRpYWdub3N0aWNzUGVybWlzc2lvbkRlbmllZEgAQggKBnJlc3VsdCJoChJEaWFnbm9zdGljc1N1Y2Nlc3MSDAoEcGF0aBgBIAEoCRIpCgtkaWFnbm9zdGljcxgCIAMoCzIULmFnZW50LnYxLkRpYWdub3N0aWMSGQoRdG90YWxfZGlhZ25vc3RpY3MYAyABKAUifwoKRGlhZ25vc3RpYxIQCghzZXZlcml0eRgBIAEoBRIeCgVyYW5nZRgCIAEoCzIPLmFnZW50LnYxLlJhbmdlEg8KB21lc3NhZ2UYAyABKAkSDgoGc291cmNlGAQgASgJEgwKBGNvZGUYBSABKAkSEAoIaXNfc3RhbGUYBiABKAgiLwoQRGlhZ25vc3RpY3NFcnJvchIMCgRwYXRoGAEgASgJEg0KBWVycm9yGAIgASgJIjMKE0RpYWdub3N0aWNzUmVqZWN0ZWQSDAoEcGF0aBgBIAEoCRIOCgZyZWFzb24YAiABKAkiJwoXRGlhZ25vc3RpY3NGaWxlTm90Rm91bmQSDAoEcGF0aBgBIAEoCSIrChtEaWFnbm9zdGljc1Blcm1pc3Npb25EZW5pZWQSDAoEcGF0aBgBIAEoCSJICghFZGl0QXJncxIMCgRwYXRoGAEgASgJEhsKDnN0cmVhbV9jb250ZW50GAYgASgJSACIAQFCEQoPX3N0cmVhbV9jb250ZW50ItYCCgpFZGl0UmVzdWx0EigKB3N1Y2Nlc3MYASABKAsyFS5hZ2VudC52MS5FZGl0U3VjY2Vzc0gAEjQKDmZpbGVfbm90X2ZvdW5kGAIgASgLMhouYWdlbnQudjEuRWRpdEZpbGVOb3RGb3VuZEgAEkQKFnJlYWRfcGVybWlzc2lvbl9kZW5pZWQYAyABKAsyIi5hZ2VudC52MS5FZGl0UmVhZFBlcm1pc3Npb25EZW5pZWRIABJGChd3cml0ZV9wZXJtaXNzaW9uX2RlbmllZBgEIAEoCzIjLmFnZW50LnYxLkVkaXRXcml0ZVBlcm1pc3Npb25EZW5pZWRIABIqCghyZWplY3RlZBgGIAEoCzIWLmFnZW50LnYxLkVkaXRSZWplY3RlZEgAEiQKBWVycm9yGAcgASgLMhMuYWdlbnQudjEuRWRpdEVycm9ySABCCAoGcmVzdWx0IqQCCgtFZGl0U3VjY2VzcxIMCgRwYXRoGAEgASgJEhgKC2xpbmVzX2FkZGVkGAMgASgFSACIAQESGgoNbGluZXNfcmVtb3ZlZBgEIAEoBUgBiAEBEhgKC2RpZmZfc3RyaW5nGAUgASgJSAKIAQESJQoYYmVmb3JlX2Z1bGxfZmlsZV9jb250ZW50GAYgASgJSAOIAQESHwoXYWZ0ZXJfZnVsbF9maWxlX2NvbnRlbnQYByABKAkSFAoHbWVzc2FnZRgIIAEoCUgEiAEBQg4KDF9saW5lc19hZGRlZEIQCg5fbGluZXNfcmVtb3ZlZEIOCgxfZGlmZl9zdHJpbmdCGwoZX2JlZm9yZV9mdWxsX2ZpbGVfY29udGVudEIKCghfbWVzc2FnZSIgChBFZGl0RmlsZU5vdEZvdW5kEgwKBHBhdGgYASABKAkiKAoYRWRpdFJlYWRQZXJtaXNzaW9uRGVuaWVkEgwKBHBhdGgYASABKAkiTQoZRWRpdFdyaXRlUGVybWlzc2lvbkRlbmllZBIMCgRwYXRoGAEgASgJEg0KBWVycm9yGAIgASgJEhMKC2lzX3JlYWRvbmx5GAMgASgIIiwKDEVkaXRSZWplY3RlZBIMCgRwYXRoGAEgASgJEg4KBnJlYXNvbhgCIAEoCSJiCglFZGl0RXJyb3ISDAoEcGF0aBgBIAEoCRINCgVlcnJvchgCIAEoCRIgChNtb2RlbF92aXNpYmxlX2Vycm9yGAUgASgJSACIAQFCFgoUX21vZGVsX3Zpc2libGVfZXJyb3IiVgoMRWRpdFRvb2xDYWxsEiAKBGFyZ3MYASABKAsyEi5hZ2VudC52MS5FZGl0QXJncxIkCgZyZXN1bHQYAiABKAsyFC5hZ2VudC52MS5FZGl0UmVzdWx0IjEKEUVkaXRUb29sQ2FsbERlbHRhEhwKFHN0cmVhbV9jb250ZW50X2RlbHRhGAEgASgJIjEKDEV4YUZldGNoQXJncxILCgNpZHMYASADKAkSFAoMdG9vbF9jYWxsX2lkGAIgASgJIqIBCg5FeGFGZXRjaFJlc3VsdBIsCgdzdWNjZXNzGAEgASgLMhkuYWdlbnQudjEuRXhhRmV0Y2hTdWNjZXNzSAASKAoFZXJyb3IYAiABKAsyFy5hZ2VudC52MS5FeGFGZXRjaEVycm9ySAASLgoIcmVqZWN0ZWQYAyABKAsyGi5hZ2VudC52MS5FeGFGZXRjaFJlamVjdGVkSABCCAoGcmVzdWx0Ij4KD0V4YUZldGNoU3VjY2VzcxIrCghjb250ZW50cxgBIAMoCzIZLmFnZW50LnYxLkV4YUZldGNoQ29udGVudCIeCg1FeGFGZXRjaEVycm9yEg0KBWVycm9yGAEgASgJIiIKEEV4YUZldGNoUmVqZWN0ZWQSDgoGcmVhc29uGAEgASgJIlMKD0V4YUZldGNoQ29udGVudBINCgV0aXRsZRgBIAEoCRILCgN1cmwYAiABKAkSDAoEdGV4dBgDIAEoCRIWCg5wdWJsaXNoZWRfZGF0ZRgEIAEoCSJiChBFeGFGZXRjaFRvb2xDYWxsEiQKBGFyZ3MYASABKAsyFi5hZ2VudC52MS5FeGFGZXRjaEFyZ3MSKAoGcmVzdWx0GAIgASgLMhguYWdlbnQudjEuRXhhRmV0Y2hSZXN1bHQiPAoURXhhRmV0Y2hSZXF1ZXN0UXVlcnkSJAoEYXJncxgBIAEoCzIWLmFnZW50LnYxLkV4YUZldGNoQXJncyKjAQoXRXhhRmV0Y2hSZXF1ZXN0UmVzcG9uc2USPgoIYXBwcm92ZWQYASABKAsyKi5hZ2VudC52MS5FeGFGZXRjaFJlcXVlc3RSZXNwb25zZV9BcHByb3ZlZEgAEj4KCHJlamVjdGVkGAIgASgLMiouYWdlbnQudjEuRXhhRmV0Y2hSZXF1ZXN0UmVzcG9uc2VfUmVqZWN0ZWRIAEIICgZyZXN1bHQiIgogRXhhRmV0Y2hSZXF1ZXN0UmVzcG9uc2VfQXBwcm92ZWQiMgogRXhhRmV0Y2hSZXF1ZXN0UmVzcG9uc2VfUmVqZWN0ZWQSDgoGcmVhc29uGAEgASgJIlcKDUV4YVNlYXJjaEFyZ3MSDQoFcXVlcnkYASABKAkSDAoEdHlwZRgCIAEoCRITCgtudW1fcmVzdWx0cxgDIAEoBRIUCgx0b29sX2NhbGxfaWQYBCABKAkipgEKD0V4YVNlYXJjaFJlc3VsdBItCgdzdWNjZXNzGAEgASgLMhouYWdlbnQudjEuRXhhU2VhcmNoU3VjY2Vzc0gAEikKBWVycm9yGAIgASgLMhguYWdlbnQudjEuRXhhU2VhcmNoRXJyb3JIABIvCghyZWplY3RlZBgDIAEoCzIbLmFnZW50LnYxLkV4YVNlYXJjaFJlamVjdGVkSABCCAoGcmVzdWx0IkQKEEV4YVNlYXJjaFN1Y2Nlc3MSMAoKcmVmZXJlbmNlcxgBIAMoCzIcLmFnZW50LnYxLkV4YVNlYXJjaFJlZmVyZW5jZSIfCg5FeGFTZWFyY2hFcnJvchINCgVlcnJvchgBIAEoCSIjChFFeGFTZWFyY2hSZWplY3RlZBIOCgZyZWFzb24YASABKAkiVgoSRXhhU2VhcmNoUmVmZXJlbmNlEg0KBXRpdGxlGAEgASgJEgsKA3VybBgCIAEoCRIMCgR0ZXh0GAMgASgJEhYKDnB1Ymxpc2hlZF9kYXRlGAQgASgJImUKEUV4YVNlYXJjaFRvb2xDYWxsEiUKBGFyZ3MYASABKAsyFy5hZ2VudC52MS5FeGFTZWFyY2hBcmdzEikKBnJlc3VsdBgCIAEoCzIZLmFnZW50LnYxLkV4YVNlYXJjaFJlc3VsdCI+ChVFeGFTZWFyY2hSZXF1ZXN0UXVlcnkSJQoEYXJncxgBIAEoCzIXLmFnZW50LnYxLkV4YVNlYXJjaEFyZ3MipgEKGEV4YVNlYXJjaFJlcXVlc3RSZXNwb25zZRI/CghhcHByb3ZlZBgBIAEoCzIrLmFnZW50LnYxLkV4YVNlYXJjaFJlcXVlc3RSZXNwb25zZV9BcHByb3ZlZEgAEj8KCHJlamVjdGVkGAIgASgLMisuYWdlbnQudjEuRXhhU2VhcmNoUmVxdWVzdFJlc3BvbnNlX1JlamVjdGVkSABCCAoGcmVzdWx0IiMKIUV4YVNlYXJjaFJlcXVlc3RSZXNwb25zZV9BcHByb3ZlZCIzCiFFeGFTZWFyY2hSZXF1ZXN0UmVzcG9uc2VfUmVqZWN0ZWQSDgoGcmVhc29uGAEgASgJIiMKFUV4ZWNDbGllbnRTdHJlYW1DbG9zZRIKCgJpZBgBIAEoDSJ+Cg9FeGVjQ2xpZW50VGhyb3cSCgoCaWQYASABKA0SDQoFZXJyb3IYAiABKAkSGAoLc3RhY2tfdHJhY2UYAyABKAlIAIgBARIXCgplcnJvcl9jb2RlGAQgASgJSAGIAQFCDgoMX3N0YWNrX3RyYWNlQg0KC19lcnJvcl9jb2RlIiEKE0V4ZWNDbGllbnRIZWFydGJlYXQSCgoCaWQYASABKA0ivgEKGEV4ZWNDbGllbnRDb250cm9sTWVzc2FnZRI3CgxzdHJlYW1fY2xvc2UYASABKAsyHy5hZ2VudC52MS5FeGVjQ2xpZW50U3RyZWFtQ2xvc2VIABIqCgV0aHJvdxgCIAEoCzIZLmFnZW50LnYxLkV4ZWNDbGllbnRUaHJvd0gAEjIKCWhlYXJ0YmVhdBgDIAEoCzIdLmFnZW50LnYxLkV4ZWNDbGllbnRIZWFydGJlYXRIAEIJCgdtZXNzYWdlIoQBCgtTcGFuQ29udGV4dBIQCgh0cmFjZV9pZBgBIAEoCRIPCgdzcGFuX2lkGAIgASgJEhgKC3RyYWNlX2ZsYWdzGAMgASgNSACIAQESGAoLdHJhY2Vfc3RhdGUYBCABKAlIAYgBAUIOCgxfdHJhY2VfZmxhZ3NCDgoMX3RyYWNlX3N0YXRlIgsKCUFib3J0QXJncyINCgtBYm9ydFJlc3VsdCLaEwoRRXhlY1NlcnZlck1lc3NhZ2USCgoCaWQYASABKA0SDwoHZXhlY19pZBgPIAEoCRIwCgxzcGFuX2NvbnRleHQYEyABKAsyFS5hZ2VudC52MS5TcGFuQ29udGV4dEgBiAEBEikKCnNoZWxsX2FyZ3MYAiABKAsyEy5hZ2VudC52MS5TaGVsbEFyZ3NIABIpCgp3cml0ZV9hcmdzGAMgASgLMhMuYWdlbnQudjEuV3JpdGVBcmdzSAASKwoLZGVsZXRlX2FyZ3MYBCABKAsyFC5hZ2VudC52MS5EZWxldGVBcmdzSAASJwoJZ3JlcF9hcmdzGAUgASgLMhIuYWdlbnQudjEuR3JlcEFyZ3NIABInCglyZWFkX2FyZ3MYByABKAsyEi5hZ2VudC52MS5SZWFkQXJnc0gAEiMKB2xzX2FyZ3MYCCABKAsyEC5hZ2VudC52MS5Mc0FyZ3NIABI1ChBkaWFnbm9zdGljc19hcmdzGAkgASgLMhkuYWdlbnQudjEuRGlhZ25vc3RpY3NBcmdzSAASPAoUcmVxdWVzdF9jb250ZXh0X2FyZ3MYCiABKAsyHC5hZ2VudC52MS5SZXF1ZXN0Q29udGV4dEFyZ3NIABIlCghtY3BfYXJncxgLIAEoCzIRLmFnZW50LnYxLk1jcEFyZ3NIABIwChFzaGVsbF9zdHJlYW1fYXJncxgOIAEoCzITLmFnZW50LnYxLlNoZWxsQXJnc0gAEkkKG2JhY2tncm91bmRfc2hlbGxfc3Bhd25fYXJncxgQIAEoCzIiLmFnZW50LnYxLkJhY2tncm91bmRTaGVsbFNwYXduQXJnc0gAEkoKHGxpc3RfbWNwX3Jlc291cmNlc19leGVjX2FyZ3MYESABKAsyIi5hZ2VudC52MS5MaXN0TWNwUmVzb3VyY2VzRXhlY0FyZ3NIABJIChtyZWFkX21jcF9yZXNvdXJjZV9leGVjX2FyZ3MYEiABKAsyIS5hZ2VudC52MS5SZWFkTWNwUmVzb3VyY2VFeGVjQXJnc0gAEikKCmZldGNoX2FyZ3MYFCABKAsyEy5hZ2VudC52MS5GZXRjaEFyZ3NIABI4ChJyZWNvcmRfc2NyZWVuX2FyZ3MYFSABKAsyGi5hZ2VudC52MS5SZWNvcmRTY3JlZW5BcmdzSAASNgoRY29tcHV0ZXJfdXNlX2FyZ3MYFiABKAsyGS5hZ2VudC52MS5Db21wdXRlclVzZUFyZ3NIABI/ChZ3cml0ZV9zaGVsbF9zdGRpbl9hcmdzGBcgASgLMh0uYWdlbnQudjEuV3JpdGVTaGVsbFN0ZGluQXJnc0gAEjAKEnJlZGFjdGVkX3JlYWRfYXJncxgdIAEoCzISLmFnZW50LnYxLlJlYWRBcmdzSAASOQoTbWNwX3N0YXRlX2V4ZWNfYXJncxgkIAEoCzIaLmFnZW50LnYxLk1jcFN0YXRlRXhlY0FyZ3NIABI2ChFleGVjdXRlX2hvb2tfYXJncxgbIAEoCzIZLmFnZW50LnYxLkV4ZWN1dGVIb29rQXJnc0gAEi8KDXN1YmFnZW50X2FyZ3MYHCABKAsyFi5hZ2VudC52MS5TdWJhZ2VudEFyZ3NIABJJChtmb3JjZV9iYWNrZ3JvdW5kX3NoZWxsX2FyZ3MYHiABKAsyIi5hZ2VudC52MS5Gb3JjZUJhY2tncm91bmRTaGVsbEFyZ3NIABJPCh5mb3JjZV9iYWNrZ3JvdW5kX3N1YmFnZW50X2FyZ3MYHyABKAsyJS5hZ2VudC52MS5Gb3JjZUJhY2tncm91bmRTdWJhZ2VudEFyZ3NIABI6ChNzdWJhZ2VudF9hd2FpdF9hcmdzGCUgASgLMhsuYWdlbnQudjEuU3ViYWdlbnRBd2FpdEFyZ3NIABJHChpzbWFydF9tb2RlX2NsYXNzaWZpZXJfYXJncxgmIAEoCzIhLmFnZW50LnYxLlNtYXJ0TW9kZUNsYXNzaWZpZXJBcmdzSAASQgoXY2FudmFzX2RpYWdub3N0aWNzX2FyZ3MYKCABKAsyHy5hZ2VudC52MS5DYW52YXNEaWFnbm9zdGljc0FyZ3NIABJNCh1zaGVsbF9hbGxvd2xpc3RfcHJlY2hlY2tfYXJncxgpIAEoCzIkLmFnZW50LnYxLlNoZWxsQWxsb3dsaXN0UHJlY2hlY2tBcmdzSAASSQobbWNwX2FsbG93bGlzdF9wcmVjaGVja19hcmdzGCogASgLMiIuYWdlbnQudjEuTWNwQWxsb3dsaXN0UHJlY2hlY2tBcmdzSAASVAohd2ViX2ZldGNoX2FsbG93bGlzdF9wcmVjaGVja19hcmdzGCsgASgLMicuYWdlbnQudjEuV2ViRmV0Y2hBbGxvd2xpc3RQcmVjaGVja0FyZ3NIABI0ChBnaXRfZGlmZl9yZXF1ZXN0GCwgASgLMhguYWdlbnQudjEuR2V0RGlmZlJlcXVlc3RIABIwCgxwaV9yZWFkX2FyZ3MYLSABKAsyGC5hZ2VudC52MS5QaVJlYWRFeGVjQXJnc0gAEjAKDHBpX2Jhc2hfYXJncxguIAEoCzIYLmFnZW50LnYxLlBpQmFzaEV4ZWNBcmdzSAASMAoMcGlfZWRpdF9hcmdzGC8gASgLMhguYWdlbnQudjEuUGlFZGl0RXhlY0FyZ3NIABIyCg1waV93cml0ZV9hcmdzGDAgASgLMhkuYWdlbnQudjEuUGlXcml0ZUV4ZWNBcmdzSAASMAoMcGlfZ3JlcF9hcmdzGDEgASgLMhguYWdlbnQudjEuUGlHcmVwRXhlY0FyZ3NIABIwCgxwaV9maW5kX2FyZ3MYMiABKAsyGC5hZ2VudC52MS5QaUZpbmRFeGVjQXJnc0gAEiwKCnBpX2xzX2FyZ3MYMyABKAsyFi5hZ2VudC52MS5QaUxzRXhlY0FyZ3NIABI3ChhtaW5pX3N3ZV9hZ2VudF9iYXNoX2FyZ3MYNCABKAsyEy5hZ2VudC52MS5TaGVsbEFyZ3NIABJEChhjb252ZXJzYXRpb25fc2VhcmNoX2FyZ3MYNSABKAsyIC5hZ2VudC52MS5Db252ZXJzYXRpb25TZWFyY2hBcmdzSAASRQoZYWdlbnRfc3RvcmVfY29uZmxpY3RfYXJncxg2IAEoCzIgLmFnZW50LnYxLkFnZW50U3RvcmVDb25mbGljdEFyZ3NIABIsCh9hY2NlcHRfaG9va19hZGRpdGlvbmFsX2NvbnRleHRzGDcgASgISAKIAQFCCQoHbWVzc2FnZUIPCg1fc3Bhbl9jb250ZXh0QiIKIF9hY2NlcHRfaG9va19hZGRpdGlvbmFsX2NvbnRleHRzIuEUChFFeGVjQ2xpZW50TWVzc2FnZRIKCgJpZBgBIAEoDRIPCgdleGVjX2lkGA8gASgJEi0KDHNoZWxsX3Jlc3VsdBgCIAEoCzIVLmFnZW50LnYxLlNoZWxsUmVzdWx0SAASLQoMd3JpdGVfcmVzdWx0GAMgASgLMhUuYWdlbnQudjEuV3JpdGVSZXN1bHRIABIvCg1kZWxldGVfcmVzdWx0GAQgASgLMhYuYWdlbnQudjEuRGVsZXRlUmVzdWx0SAASKwoLZ3JlcF9yZXN1bHQYBSABKAsyFC5hZ2VudC52MS5HcmVwUmVzdWx0SAASKwoLcmVhZF9yZXN1bHQYByABKAsyFC5hZ2VudC52MS5SZWFkUmVzdWx0SAASJwoJbHNfcmVzdWx0GAggASgLMhIuYWdlbnQudjEuTHNSZXN1bHRIABI5ChJkaWFnbm9zdGljc19yZXN1bHQYCSABKAsyGy5hZ2VudC52MS5EaWFnbm9zdGljc1Jlc3VsdEgAEkAKFnJlcXVlc3RfY29udGV4dF9yZXN1bHQYCiABKAsyHi5hZ2VudC52MS5SZXF1ZXN0Q29udGV4dFJlc3VsdEgAEikKCm1jcF9yZXN1bHQYCyABKAsyEy5hZ2VudC52MS5NY3BSZXN1bHRIABItCgxzaGVsbF9zdHJlYW0YDiABKAsyFS5hZ2VudC52MS5TaGVsbFN0cmVhbUgAEk0KHWJhY2tncm91bmRfc2hlbGxfc3Bhd25fcmVzdWx0GBAgASgLMiQuYWdlbnQudjEuQmFja2dyb3VuZFNoZWxsU3Bhd25SZXN1bHRIABJOCh5saXN0X21jcF9yZXNvdXJjZXNfZXhlY19yZXN1bHQYESABKAsyJC5hZ2VudC52MS5MaXN0TWNwUmVzb3VyY2VzRXhlY1Jlc3VsdEgAEkwKHXJlYWRfbWNwX3Jlc291cmNlX2V4ZWNfcmVzdWx0GBIgASgLMiMuYWdlbnQudjEuUmVhZE1jcFJlc291cmNlRXhlY1Jlc3VsdEgAEi0KDGZldGNoX3Jlc3VsdBgUIAEoCzIVLmFnZW50LnYxLkZldGNoUmVzdWx0SAASPAoUcmVjb3JkX3NjcmVlbl9yZXN1bHQYFSABKAsyHC5hZ2VudC52MS5SZWNvcmRTY3JlZW5SZXN1bHRIABI6ChNjb21wdXRlcl91c2VfcmVzdWx0GBYgASgLMhsuYWdlbnQudjEuQ29tcHV0ZXJVc2VSZXN1bHRIABJDChh3cml0ZV9zaGVsbF9zdGRpbl9yZXN1bHQYFyABKAsyHy5hZ2VudC52MS5Xcml0ZVNoZWxsU3RkaW5SZXN1bHRIABI0ChRyZWRhY3RlZF9yZWFkX3Jlc3VsdBgdIAEoCzIULmFnZW50LnYxLlJlYWRSZXN1bHRIABI9ChVtY3Bfc3RhdGVfZXhlY19yZXN1bHQYJCABKAsyHC5hZ2VudC52MS5NY3BTdGF0ZUV4ZWNSZXN1bHRIABI6ChNleGVjdXRlX2hvb2tfcmVzdWx0GBsgASgLMhsuYWdlbnQudjEuRXhlY3V0ZUhvb2tSZXN1bHRIABIzCg9zdWJhZ2VudF9yZXN1bHQYHCABKAsyGC5hZ2VudC52MS5TdWJhZ2VudFJlc3VsdEgAEk0KHWZvcmNlX2JhY2tncm91bmRfc2hlbGxfcmVzdWx0GB4gASgLMiQuYWdlbnQudjEuRm9yY2VCYWNrZ3JvdW5kU2hlbGxSZXN1bHRIABJTCiBmb3JjZV9iYWNrZ3JvdW5kX3N1YmFnZW50X3Jlc3VsdBgfIAEoCzInLmFnZW50LnYxLkZvcmNlQmFja2dyb3VuZFN1YmFnZW50UmVzdWx0SAASPgoVc3ViYWdlbnRfYXdhaXRfcmVzdWx0GCUgASgLMh0uYWdlbnQudjEuU3ViYWdlbnRBd2FpdFJlc3VsdEgAEksKHHNtYXJ0X21vZGVfY2xhc3NpZmllcl9yZXN1bHQYJiABKAsyIy5hZ2VudC52MS5TbWFydE1vZGVDbGFzc2lmaWVyUmVzdWx0SAASRgoZY2FudmFzX2RpYWdub3N0aWNzX3Jlc3VsdBgoIAEoCzIhLmFnZW50LnYxLkNhbnZhc0RpYWdub3N0aWNzUmVzdWx0SAASUQofc2hlbGxfYWxsb3dsaXN0X3ByZWNoZWNrX3Jlc3VsdBgpIAEoCzImLmFnZW50LnYxLlNoZWxsQWxsb3dsaXN0UHJlY2hlY2tSZXN1bHRIABJNCh1tY3BfYWxsb3dsaXN0X3ByZWNoZWNrX3Jlc3VsdBgqIAEoCzIkLmFnZW50LnYxLk1jcEFsbG93bGlzdFByZWNoZWNrUmVzdWx0SAASWAojd2ViX2ZldGNoX2FsbG93bGlzdF9wcmVjaGVja19yZXN1bHQYKyABKAsyKS5hZ2VudC52MS5XZWJGZXRjaEFsbG93bGlzdFByZWNoZWNrUmVzdWx0SAASNgoRZ2l0X2RpZmZfcmVzcG9uc2UYLCABKAsyGS5hZ2VudC52MS5HZXREaWZmUmVzcG9uc2VIABI0Cg5waV9yZWFkX3Jlc3VsdBguIAEoCzIaLmFnZW50LnYxLlBpUmVhZEV4ZWNSZXN1bHRIABI0Cg5waV9iYXNoX3Jlc3VsdBgvIAEoCzIaLmFnZW50LnYxLlBpQmFzaEV4ZWNSZXN1bHRIABI0Cg5waV9lZGl0X3Jlc3VsdBgwIAEoCzIaLmFnZW50LnYxLlBpRWRpdEV4ZWNSZXN1bHRIABI2Cg9waV93cml0ZV9yZXN1bHQYMSABKAsyGy5hZ2VudC52MS5QaVdyaXRlRXhlY1Jlc3VsdEgAEjQKDnBpX2dyZXBfcmVzdWx0GDIgASgLMhouYWdlbnQudjEuUGlHcmVwRXhlY1Jlc3VsdEgAEjQKDnBpX2ZpbmRfcmVzdWx0GDMgASgLMhouYWdlbnQudjEuUGlGaW5kRXhlY1Jlc3VsdEgAEjAKDHBpX2xzX3Jlc3VsdBg0IAEoCzIYLmFnZW50LnYxLlBpTHNFeGVjUmVzdWx0SAASSAoaY29udmVyc2F0aW9uX3NlYXJjaF9yZXN1bHQYNSABKAsyIi5hZ2VudC52MS5Db252ZXJzYXRpb25TZWFyY2hSZXN1bHRIABJJChthZ2VudF9zdG9yZV9jb25mbGljdF9yZXN1bHQYNiABKAsyIi5hZ2VudC52MS5BZ2VudFN0b3JlQ29uZmxpY3RSZXN1bHRIABI7ChptaW5pX3N3ZV9hZ2VudF9iYXNoX3Jlc3VsdBg3IAEoCzIVLmFnZW50LnYxLlNoZWxsUmVzdWx0SAASJAoXbG9jYWxfZXhlY3V0aW9uX3RpbWVfbXMYJyABKAVIAYgBARJBChhob29rX2FkZGl0aW9uYWxfY29udGV4dHMYLSADKAsyHy5hZ2VudC52MS5Ib29rQWRkaXRpb25hbENvbnRleHRCCQoHbWVzc2FnZUIaChhfbG9jYWxfZXhlY3V0aW9uX3RpbWVfbXMiLgoJRmV0Y2hBcmdzEgsKA3VybBgBIAEoCRIUCgx0b29sX2NhbGxfaWQYAiABKAkiaQoLRmV0Y2hSZXN1bHQSKQoHc3VjY2VzcxgBIAEoCzIWLmFnZW50LnYxLkZldGNoU3VjY2Vzc0gAEiUKBWVycm9yGAIgASgLMhQuYWdlbnQudjEuRmV0Y2hFcnJvckgAQggKBnJlc3VsdCJXCgxGZXRjaFN1Y2Nlc3MSCwoDdXJsGAEgASgJEg8KB2NvbnRlbnQYAiABKAkSEwoLc3RhdHVzX2NvZGUYAyABKAUSFAoMY29udGVudF90eXBlGAQgASgJIigKCkZldGNoRXJyb3ISCwoDdXJsGAEgASgJEg0KBWVycm9yGAIgASgJIm0KEUdlbmVyYXRlSW1hZ2VBcmdzEhMKC2Rlc2NyaXB0aW9uGAEgASgJEhYKCWZpbGVfcGF0aBgCIAEoCUgAiAEBEh0KFXJlZmVyZW5jZV9pbWFnZV9wYXRocxgFIAMoCUIMCgpfZmlsZV9wYXRoIoEBChNHZW5lcmF0ZUltYWdlUmVzdWx0EjEKB3N1Y2Nlc3MYASABKAsyHi5hZ2VudC52MS5HZW5lcmF0ZUltYWdlU3VjY2Vzc0gAEi0KBWVycm9yGAIgASgLMhwuYWdlbnQudjEuR2VuZXJhdGVJbWFnZUVycm9ySABCCAoGcmVzdWx0Ij0KFEdlbmVyYXRlSW1hZ2VTdWNjZXNzEhEKCWZpbGVfcGF0aBgBIAEoCRISCgppbWFnZV9kYXRhGAIgASgJIiMKEkdlbmVyYXRlSW1hZ2VFcnJvchINCgVlcnJvchgBIAEoCSJxChVHZW5lcmF0ZUltYWdlVG9vbENhbGwSKQoEYXJncxgBIAEoCzIbLmFnZW50LnYxLkdlbmVyYXRlSW1hZ2VBcmdzEi0KBnJlc3VsdBgCIAEoCzIdLmFnZW50LnYxLkdlbmVyYXRlSW1hZ2VSZXN1bHQi5gQKCEdyZXBBcmdzEg8KB3BhdHRlcm4YASABKAkSEQoEcGF0aBgCIAEoCUgAiAEBEhEKBGdsb2IYAyABKAlIAYgBARIYCgtvdXRwdXRfbW9kZRgEIAEoCUgCiAEBEhsKDmNvbnRleHRfYmVmb3JlGAUgASgFSAOIAQESGgoNY29udGV4dF9hZnRlchgGIAEoBUgEiAEBEhQKB2NvbnRleHQYByABKAVIBYgBARIdChBjYXNlX2luc2Vuc2l0aXZlGAggASgISAaIAQESEQoEdHlwZRgJIAEoCUgHiAEBEhcKCmhlYWRfbGltaXQYCiABKAVICIgBARIWCgltdWx0aWxpbmUYCyABKAhICYgBARIRCgRzb3J0GAwgASgJSAqIAQESGwoOc29ydF9hc2NlbmRpbmcYDSABKAhIC4gBARIUCgx0b29sX2NhbGxfaWQYDiABKAkSNAoOc2FuZGJveF9wb2xpY3kYDyABKAsyFy5hZ2VudC52MS5TYW5kYm94UG9saWN5SAyIAQESEwoGb2Zmc2V0GBAgASgFSA2IAQFCBwoFX3BhdGhCBwoFX2dsb2JCDgoMX291dHB1dF9tb2RlQhEKD19jb250ZXh0X2JlZm9yZUIQCg5fY29udGV4dF9hZnRlckIKCghfY29udGV4dEITChFfY2FzZV9pbnNlbnNpdGl2ZUIHCgVfdHlwZUINCgtfaGVhZF9saW1pdEIMCgpfbXVsdGlsaW5lQgcKBV9zb3J0QhEKD19zb3J0X2FzY2VuZGluZ0IRCg9fc2FuZGJveF9wb2xpY3lCCQoHX29mZnNldCJmCgpHcmVwUmVzdWx0EigKB3N1Y2Nlc3MYASABKAsyFS5hZ2VudC52MS5HcmVwU3VjY2Vzc0gAEiQKBWVycm9yGAIgASgLMhMuYWdlbnQudjEuR3JlcEVycm9ySABCCAoGcmVzdWx0IhoKCUdyZXBFcnJvchINCgVlcnJvchgBIAEoCSK0AgoLR3JlcFN1Y2Nlc3MSDwoHcGF0dGVybhgBIAEoCRIMCgRwYXRoGAIgASgJEhMKC291dHB1dF9tb2RlGAMgASgJEkYKEXdvcmtzcGFjZV9yZXN1bHRzGAQgAygLMisuYWdlbnQudjEuR3JlcFN1Y2Nlc3MuV29ya3NwYWNlUmVzdWx0c0VudHJ5EjwKFGFjdGl2ZV9lZGl0b3JfcmVzdWx0GAUgASgLMhkuYWdlbnQudjEuR3JlcFVuaW9uUmVzdWx0SACIAQEaUgoVV29ya3NwYWNlUmVzdWx0c0VudHJ5EgsKA2tleRgBIAEoCRIoCgV2YWx1ZRgCIAEoCzIZLmFnZW50LnYxLkdyZXBVbmlvblJlc3VsdDoCOAFCFwoVX2FjdGl2ZV9lZGl0b3JfcmVzdWx0IqMBCg9HcmVwVW5pb25SZXN1bHQSKgoFY291bnQYASABKAsyGS5hZ2VudC52MS5HcmVwQ291bnRSZXN1bHRIABIqCgVmaWxlcxgCIAEoCzIZLmFnZW50LnYxLkdyZXBGaWxlc1Jlc3VsdEgAEi4KB2NvbnRlbnQYAyABKAsyGy5hZ2VudC52MS5HcmVwQ29udGVudFJlc3VsdEgAQggKBnJlc3VsdCKDAgoPR3JlcENvdW50UmVzdWx0EicKBmNvdW50cxgBIAMoCzIXLmFnZW50LnYxLkdyZXBGaWxlQ291bnQSEwoLdG90YWxfZmlsZXMYAiABKAUSFQoNdG90YWxfbWF0Y2hlcxgDIAEoBRIYChBjbGllbnRfdHJ1bmNhdGVkGAQgASgIEhkKEXJpcGdyZXBfdHJ1bmNhdGVkGAUgASgIEh8KEmhlYWRfbGltaXRfYXBwbGllZBgGIAEoBUgAiAEBEhsKDm9mZnNldF9hcHBsaWVkGAcgASgFSAGIAQFCFQoTX2hlYWRfbGltaXRfYXBwbGllZEIRCg9fb2Zmc2V0X2FwcGxpZWQiLAoNR3JlcEZpbGVDb3VudBIMCgRmaWxlGAEgASgJEg0KBWNvdW50GAIgASgFItIBCg9HcmVwRmlsZXNSZXN1bHQSDQoFZmlsZXMYASADKAkSEwoLdG90YWxfZmlsZXMYAiABKAUSGAoQY2xpZW50X3RydW5jYXRlZBgDIAEoCBIZChFyaXBncmVwX3RydW5jYXRlZBgEIAEoCBIfChJoZWFkX2xpbWl0X2FwcGxpZWQYBSABKAVIAIgBARIbCg5vZmZzZXRfYXBwbGllZBgGIAEoBUgBiAEBQhUKE19oZWFkX2xpbWl0X2FwcGxpZWRCEQoPX29mZnNldF9hcHBsaWVkIowCChFHcmVwQ29udGVudFJlc3VsdBIoCgdtYXRjaGVzGAEgAygLMhcuYWdlbnQudjEuR3JlcEZpbGVNYXRjaBITCgt0b3RhbF9saW5lcxgCIAEoBRIbChN0b3RhbF9tYXRjaGVkX2xpbmVzGAMgASgFEhgKEGNsaWVudF90cnVuY2F0ZWQYBCABKAgSGQoRcmlwZ3JlcF90cnVuY2F0ZWQYBSABKAgSHwoSaGVhZF9saW1pdF9hcHBsaWVkGAYgASgFSACIAQESGwoOb2Zmc2V0X2FwcGxpZWQYByABKAVIAYgBAUIVChNfaGVhZF9saW1pdF9hcHBsaWVkQhEKD19vZmZzZXRfYXBwbGllZCJKCg1HcmVwRmlsZU1hdGNoEgwKBGZpbGUYASABKAkSKwoHbWF0Y2hlcxgCIAMoCzIaLmFnZW50LnYxLkdyZXBDb250ZW50TWF0Y2gibAoQR3JlcENvbnRlbnRNYXRjaBITCgtsaW5lX251bWJlchgBIAEoBRIPCgdjb250ZW50GAIgASgJEhkKEWNvbnRlbnRfdHJ1bmNhdGVkGAMgASgIEhcKD2lzX2NvbnRleHRfbGluZRgEIAEoCCIdCgpHcmVwU3RyZWFtEg8KB3BhdHRlcm4YASABKAkiVgoMR3JlcFRvb2xDYWxsEiAKBGFyZ3MYASABKAsyEi5hZ2VudC52MS5HcmVwQXJncxIkCgZyZXN1bHQYAiABKAsyFC5hZ2VudC52MS5HcmVwUmVzdWx0Ih4KC0dldEJsb2JBcmdzEg8KB2Jsb2JfaWQYASABKAwiNQoNR2V0QmxvYlJlc3VsdBIWCglibG9iX2RhdGEYASABKAxIAIgBAUIMCgpfYmxvYl9kYXRhIjEKC1NldEJsb2JBcmdzEg8KB2Jsb2JfaWQYASABKAwSEQoJYmxvYl9kYXRhGAIgASgMIj4KDVNldEJsb2JSZXN1bHQSIwoFZXJyb3IYASABKAsyDy5hZ2VudC52MS5FcnJvckgAiAEBQggKBl9lcnJvciLLAQoPS3ZTZXJ2ZXJNZXNzYWdlEgoKAmlkGAEgASgNEjAKDHNwYW5fY29udGV4dBgEIAEoCzIVLmFnZW50LnYxLlNwYW5Db250ZXh0SAGIAQESLgoNZ2V0X2Jsb2JfYXJncxgCIAEoCzIVLmFnZW50LnYxLkdldEJsb2JBcmdzSAASLgoNc2V0X2Jsb2JfYXJncxgDIAEoCzIVLmFnZW50LnYxLlNldEJsb2JBcmdzSABCCQoHbWVzc2FnZUIPCg1fc3Bhbl9jb250ZXh0IpABCg9LdkNsaWVudE1lc3NhZ2USCgoCaWQYASABKA0SMgoPZ2V0X2Jsb2JfcmVzdWx0GAIgASgLMhcuYWdlbnQudjEuR2V0QmxvYlJlc3VsdEgAEjIKD3NldF9ibG9iX3Jlc3VsdBgDIAEoCzIXLmFnZW50LnYxLlNldEJsb2JSZXN1bHRIAEIJCgdtZXNzYWdlIq0BCgZMc0FyZ3MSDAoEcGF0aBgBIAEoCRIOCgZpZ25vcmUYAiADKAkSFAoMdG9vbF9jYWxsX2lkGAMgASgJEjQKDnNhbmRib3hfcG9saWN5GAQgASgLMhcuYWdlbnQudjEuU2FuZGJveFBvbGljeUgAiAEBEhcKCnRpbWVvdXRfbXMYBSABKA1IAYgBAUIRCg9fc2FuZGJveF9wb2xpY3lCDQoLX3RpbWVvdXRfbXMisgEKCExzUmVzdWx0EiYKB3N1Y2Nlc3MYASABKAsyEy5hZ2VudC52MS5Mc1N1Y2Nlc3NIABIiCgVlcnJvchgCIAEoCzIRLmFnZW50LnYxLkxzRXJyb3JIABIoCghyZWplY3RlZBgDIAEoCzIULmFnZW50LnYxLkxzUmVqZWN0ZWRIABImCgd0aW1lb3V0GAQgASgLMhMuYWdlbnQudjEuTHNUaW1lb3V0SABCCAoGcmVzdWx0IkcKCUxzU3VjY2VzcxI6ChNkaXJlY3RvcnlfdHJlZV9yb290GAEgASgLMh0uYWdlbnQudjEuTHNEaXJlY3RvcnlUcmVlTm9kZSL2AgoTTHNEaXJlY3RvcnlUcmVlTm9kZRIQCghhYnNfcGF0aBgBIAEoCRI0Cg1jaGlsZHJlbl9kaXJzGAIgAygLMh0uYWdlbnQudjEuTHNEaXJlY3RvcnlUcmVlTm9kZRI6Cg5jaGlsZHJlbl9maWxlcxgDIAMoCzIiLmFnZW50LnYxLkxzRGlyZWN0b3J5VHJlZU5vZGVfRmlsZRIfChdjaGlsZHJlbl93ZXJlX3Byb2Nlc3NlZBgEIAEoCBJkCh1mdWxsX3N1YnRyZWVfZXh0ZW5zaW9uX2NvdW50cxgFIAMoCzI9LmFnZW50LnYxLkxzRGlyZWN0b3J5VHJlZU5vZGUuRnVsbFN1YnRyZWVFeHRlbnNpb25Db3VudHNFbnRyeRIRCgludW1fZmlsZXMYBiABKAUaQQofRnVsbFN1YnRyZWVFeHRlbnNpb25Db3VudHNFbnRyeRILCgNrZXkYASABKAkSDQoFdmFsdWUYAiABKAU6AjgBInoKGExzRGlyZWN0b3J5VHJlZU5vZGVfRmlsZRIMCgRuYW1lGAEgASgJEjoKEXRlcm1pbmFsX21ldGFkYXRhGAIgASgLMhouYWdlbnQudjEuVGVybWluYWxNZXRhZGF0YUgAiAEBQhQKEl90ZXJtaW5hbF9tZXRhZGF0YSImCgdMc0Vycm9yEgwKBHBhdGgYASABKAkSDQoFZXJyb3IYAiABKAkiKgoKTHNSZWplY3RlZBIMCgRwYXRoGAEgASgJEg4KBnJlYXNvbhgCIAEoCSJHCglMc1RpbWVvdXQSOgoTZGlyZWN0b3J5X3RyZWVfcm9vdBgBIAEoCzIdLmFnZW50LnYxLkxzRGlyZWN0b3J5VHJlZU5vZGUi8QEKEFRlcm1pbmFsTWV0YWRhdGESEAoDY3dkGAEgASgJSACIAQESOQoNbGFzdF9jb21tYW5kcxgCIAMoCzIiLmFnZW50LnYxLlRlcm1pbmFsTWV0YWRhdGFfQ29tbWFuZBIdChBsYXN0X21vZGlmaWVkX21zGAMgASgDSAGIAQESQAoPY3VycmVudF9jb21tYW5kGAQgASgLMiIuYWdlbnQudjEuVGVybWluYWxNZXRhZGF0YV9Db21tYW5kSAKIAQFCBgoEX2N3ZEITChFfbGFzdF9tb2RpZmllZF9tc0ISChBfY3VycmVudF9jb21tYW5kIqcBChhUZXJtaW5hbE1ldGFkYXRhX0NvbW1hbmQSDwoHY29tbWFuZBgBIAEoCRIWCglleGl0X2NvZGUYAiABKAVIAIgBARIZCgx0aW1lc3RhbXBfbXMYAyABKANIAYgBARIYCgtkdXJhdGlvbl9tcxgEIAEoA0gCiAEBQgwKCl9leGl0X2NvZGVCDwoNX3RpbWVzdGFtcF9tc0IOCgxfZHVyYXRpb25fbXMiUAoKTHNUb29sQ2FsbBIeCgRhcmdzGAEgASgLMhAuYWdlbnQudjEuTHNBcmdzEiIKBnJlc3VsdBgCIAEoCzISLmFnZW50LnYxLkxzUmVzdWx0IuACCgdNY3BBcmdzEgwKBG5hbWUYASABKAkSKQoEYXJncxgCIAMoCzIbLmFnZW50LnYxLk1jcEFyZ3MuQXJnc0VudHJ5EhQKDHRvb2xfY2FsbF9pZBgDIAEoCRIbChNwcm92aWRlcl9pZGVudGlmaWVyGAQgASgJEhEKCXRvb2xfbmFtZRgFIAEoCRI9ChNzbWFydF9tb2RlX2FwcHJvdmFsGAYgASgLMhsuYWdlbnQudjEuU21hcnRNb2RlQXBwcm92YWxIAIgBARIgChhzbWFydF9tb2RlX2FwcHJvdmFsX29ubHkYByABKAgSFQoNc2tpcF9hcHByb3ZhbBgIIAEoCBIZChFzZXJ2ZXJfaWRlbnRpZmllchgJIAEoCRorCglBcmdzRW50cnkSCwoDa2V5GAEgASgJEg0KBXZhbHVlGAIgASgMOgI4AUIWChRfc21hcnRfbW9kZV9hcHByb3ZhbCLjAgoJTWNwUmVzdWx0EicKB3N1Y2Nlc3MYASABKAsyFC5hZ2VudC52MS5NY3BTdWNjZXNzSAASIwoFZXJyb3IYAiABKAsyEi5hZ2VudC52MS5NY3BFcnJvckgAEikKCHJlamVjdGVkGAMgASgLMhUuYWdlbnQudjEuTWNwUmVqZWN0ZWRIABI6ChFwZXJtaXNzaW9uX2RlbmllZBgEIAEoCzIdLmFnZW50LnYxLk1jcFBlcm1pc3Npb25EZW5pZWRIABIzCg50b29sX25vdF9mb3VuZBgFIAEoCzIZLmFnZW50LnYxLk1jcFRvb2xOb3RGb3VuZEgAEjcKEHNlcnZlcl9ub3RfZm91bmQYBiABKAsyGy5hZ2VudC52MS5NY3BTZXJ2ZXJOb3RGb3VuZEgAEikKCGFwcHJvdmVkGAcgASgLMhUuYWdlbnQudjEuTWNwQXBwcm92ZWRIAEIICgZyZXN1bHQiOAoPTWNwVG9vbE5vdEZvdW5kEgwKBG5hbWUYASABKAkSFwoPYXZhaWxhYmxlX3Rvb2xzGAIgAygJImoKDk1jcFRleHRDb250ZW50EgwKBHRleHQYASABKAkSNgoPb3V0cHV0X2xvY2F0aW9uGAIgASgLMhguYWdlbnQudjEuT3V0cHV0TG9jYXRpb25IAIgBAUISChBfb3V0cHV0X2xvY2F0aW9uIjIKD01jcEltYWdlQ29udGVudBIMCgRkYXRhGAEgASgMEhEKCW1pbWVfdHlwZRgCIAEoCSJ7ChhNY3BUb29sUmVzdWx0Q29udGVudEl0ZW0SKAoEdGV4dBgBIAEoCzIYLmFnZW50LnYxLk1jcFRleHRDb250ZW50SAASKgoFaW1hZ2UYAiABKAsyGS5hZ2VudC52MS5NY3BJbWFnZUNvbnRlbnRIAEIJCgdjb250ZW50IlMKCk1jcFN1Y2Nlc3MSMwoHY29udGVudBgBIAMoCzIiLmFnZW50LnYxLk1jcFRvb2xSZXN1bHRDb250ZW50SXRlbRIQCghpc19lcnJvchgCIAEoCCIZCghNY3BFcnJvchINCgVlcnJvchgBIAEoCSIyCgtNY3BSZWplY3RlZBIOCgZyZWFzb24YASABKAkSEwoLaXNfcmVhZG9ubHkYAiABKAgiOQoTTWNwUGVybWlzc2lvbkRlbmllZBINCgVlcnJvchgBIAEoCRITCgtpc19yZWFkb25seRgCIAEoCCI6ChhMaXN0TWNwUmVzb3VyY2VzRXhlY0FyZ3MSEwoGc2VydmVyGAEgASgJSACIAQFCCQoHX3NlcnZlciLGAQoaTGlzdE1jcFJlc291cmNlc0V4ZWNSZXN1bHQSNAoHc3VjY2VzcxgBIAEoCzIhLmFnZW50LnYxLkxpc3RNY3BSZXNvdXJjZXNTdWNjZXNzSAASMAoFZXJyb3IYAiABKAsyHy5hZ2VudC52MS5MaXN0TWNwUmVzb3VyY2VzRXJyb3JIABI2CghyZWplY3RlZBgDIAEoCzIiLmFnZW50LnYxLkxpc3RNY3BSZXNvdXJjZXNSZWplY3RlZEgAQggKBnJlc3VsdCK9AgomTGlzdE1jcFJlc291cmNlc0V4ZWNSZXN1bHRfTWNwUmVzb3VyY2USCwoDdXJpGAEgASgJEhEKBG5hbWUYAiABKAlIAIgBARIYCgtkZXNjcmlwdGlvbhgDIAEoCUgBiAEBEhYKCW1pbWVfdHlwZRgEIAEoCUgCiAEBEg4KBnNlcnZlchgFIAEoCRJWCgthbm5vdGF0aW9ucxgGIAMoCzJBLmFnZW50LnYxLkxpc3RNY3BSZXNvdXJjZXNFeGVjUmVzdWx0X01jcFJlc291cmNlLkFubm90YXRpb25zRW50cnkaMgoQQW5ub3RhdGlvbnNFbnRyeRILCgNrZXkYASABKAkSDQoFdmFsdWUYAiABKAk6AjgBQgcKBV9uYW1lQg4KDF9kZXNjcmlwdGlvbkIMCgpfbWltZV90eXBlIl4KF0xpc3RNY3BSZXNvdXJjZXNTdWNjZXNzEkMKCXJlc291cmNlcxgBIAMoCzIwLmFnZW50LnYxLkxpc3RNY3BSZXNvdXJjZXNFeGVjUmVzdWx0X01jcFJlc291cmNlIiYKFUxpc3RNY3BSZXNvdXJjZXNFcnJvchINCgVlcnJvchgBIAEoCSIqChhMaXN0TWNwUmVzb3VyY2VzUmVqZWN0ZWQSDgoGcmVhc29uGAEgASgJItEBChdSZWFkTWNwUmVzb3VyY2VFeGVjQXJncxIOCgZzZXJ2ZXIYASABKAkSCwoDdXJpGAIgASgJEhoKDWRvd25sb2FkX3BhdGgYAyABKAlIAIgBARIUCgx0b29sX2NhbGxfaWQYBCABKAkSPQoTc21hcnRfbW9kZV9hcHByb3ZhbBgFIAEoCzIbLmFnZW50LnYxLlNtYXJ0TW9kZUFwcHJvdmFsSAGIAQFCEAoOX2Rvd25sb2FkX3BhdGhCFgoUX3NtYXJ0X21vZGVfYXBwcm92YWwi+gEKGVJlYWRNY3BSZXNvdXJjZUV4ZWNSZXN1bHQSMwoHc3VjY2VzcxgBIAEoCzIgLmFnZW50LnYxLlJlYWRNY3BSZXNvdXJjZVN1Y2Nlc3NIABIvCgVlcnJvchgCIAEoCzIeLmFnZW50LnYxLlJlYWRNY3BSZXNvdXJjZUVycm9ySAASNQoIcmVqZWN0ZWQYAyABKAsyIS5hZ2VudC52MS5SZWFkTWNwUmVzb3VyY2VSZWplY3RlZEgAEjYKCW5vdF9mb3VuZBgEIAEoCzIhLmFnZW50LnYxLlJlYWRNY3BSZXNvdXJjZU5vdEZvdW5kSABCCAoGcmVzdWx0IrIDChZSZWFkTWNwUmVzb3VyY2VTdWNjZXNzEgsKA3VyaRgBIAEoCRIRCgRuYW1lGAIgASgJSAGIAQESGAoLZGVzY3JpcHRpb24YAyABKAlIAogBARIWCgltaW1lX3R5cGUYBCABKAlIA4gBARJGCgthbm5vdGF0aW9ucxgHIAMoCzIxLmFnZW50LnYxLlJlYWRNY3BSZXNvdXJjZVN1Y2Nlc3MuQW5ub3RhdGlvbnNFbnRyeRIaCg1kb3dubG9hZF9wYXRoGAggASgJSASIAQESDgoEdGV4dBgFIAEoCUgAEg4KBGJsb2IYBiABKAxIABI2Cg9vdXRwdXRfbG9jYXRpb24YCSABKAsyGC5hZ2VudC52MS5PdXRwdXRMb2NhdGlvbkgFiAEBGjIKEEFubm90YXRpb25zRW50cnkSCwoDa2V5GAEgASgJEg0KBXZhbHVlGAIgASgJOgI4AUIJCgdjb250ZW50QgcKBV9uYW1lQg4KDF9kZXNjcmlwdGlvbkIMCgpfbWltZV90eXBlQhAKDl9kb3dubG9hZF9wYXRoQhIKEF9vdXRwdXRfbG9jYXRpb24iMgoUUmVhZE1jcFJlc291cmNlRXJyb3ISCwoDdXJpGAEgASgJEg0KBWVycm9yGAIgASgJIjYKF1JlYWRNY3BSZXNvdXJjZVJlamVjdGVkEgsKA3VyaRgBIAEoCRIOCgZyZWFzb24YAiABKAkiJgoXUmVhZE1jcFJlc291cmNlTm90Rm91bmQSCwoDdXJpGAEgASgJIrIBChFNY3BUb29sRGVmaW5pdGlvbhIMCgRuYW1lGAEgASgJEhsKE3Byb3ZpZGVyX2lkZW50aWZpZXIYBCABKAkSEQoJdG9vbF9uYW1lGAUgASgJEhMKC2Rlc2NyaXB0aW9uGAIgASgJEhQKDGlucHV0X3NjaGVtYRgDIAEoDBIeChFpbnB1dF9zY2hlbWFfanNvbhgGIAEoCUgAiAEBQhQKEl9pbnB1dF9zY2hlbWFfanNvbiI6CghNY3BUb29scxIuCgltY3BfdG9vbHMYASADKAsyGy5hZ2VudC52MS5NY3BUb29sRGVmaW5pdGlvbiJXCg9NY3BJbnN0cnVjdGlvbnMSEwoLc2VydmVyX25hbWUYASABKAkSFAoMaW5zdHJ1Y3Rpb25zGAIgASgJEhkKEXNlcnZlcl9pZGVudGlmaWVyGAMgASgJIv0CCg1NY3BEZXNjcmlwdG9yEhMKC3NlcnZlcl9uYW1lGAEgASgJEhkKEXNlcnZlcl9pZGVudGlmaWVyGAIgASgJEhgKC2ZvbGRlcl9wYXRoGAMgASgJSACIAQESJAoXc2VydmVyX3VzZV9pbnN0cnVjdGlvbnMYBCABKAlIAYgBARIqCgV0b29scxgFIAMoCzIbLmFnZW50LnYxLk1jcFRvb2xEZXNjcmlwdG9yEhMKBnBsdWdpbhgHIAEoCUgCiAEBEhgKC21hcmtldHBsYWNlGAggASgJSAOIAQESGQoMcGx1Z2luX2RiX2lkGAkgASgJSASIAQESGwoObWFya2V0cGxhY2VfaWQYCiABKAlIBYgBAUIOCgxfZm9sZGVyX3BhdGhCGgoYX3NlcnZlcl91c2VfaW5zdHJ1Y3Rpb25zQgkKB19wbHVnaW5CDgoMX21hcmtldHBsYWNlQg8KDV9wbHVnaW5fZGJfaWRCEQoPX21hcmtldHBsYWNlX2lkIrgBChFNY3BUb29sRGVzY3JpcHRvchIRCgl0b29sX25hbWUYASABKAkSHAoPZGVmaW5pdGlvbl9wYXRoGAIgASgJSACIAQESGAoLZGVzY3JpcHRpb24YAyABKAlIAYgBARIeChFpbnB1dF9zY2hlbWFfanNvbhgFIAEoCUgCiAEBQhIKEF9kZWZpbml0aW9uX3BhdGhCDgoMX2Rlc2NyaXB0aW9uQhQKEl9pbnB1dF9zY2hlbWFfanNvbiJ4ChRNY3BGaWxlU3lzdGVtT3B0aW9ucxIPCgdlbmFibGVkGAEgASgIEh0KFXdvcmtzcGFjZV9wcm9qZWN0X2RpchgCIAEoCRIwCg9tY3BfZGVzY3JpcHRvcnMYAyADKAsyFy5hZ2VudC52MS5NY3BEZXNjcmlwdG9yIpoBCghSZWFkQXJncxIMCgRwYXRoGAEgASgJEhQKDHRvb2xfY2FsbF9pZBgCIAEoCRITCgZvZmZzZXQYBCABKAVIAIgBARISCgVsaW1pdBgFIAEoDUgBiAEBEhoKDWVuY29kaW5nX2hpbnQYBiABKAlIAogBAUIJCgdfb2Zmc2V0QggKBl9saW1pdEIQCg5fZW5jb2RpbmdfaGludCK4AgoKUmVhZFJlc3VsdBIoCgdzdWNjZXNzGAEgASgLMhUuYWdlbnQudjEuUmVhZFN1Y2Nlc3NIABIkCgVlcnJvchgCIAEoCzITLmFnZW50LnYxLlJlYWRFcnJvckgAEioKCHJlamVjdGVkGAMgASgLMhYuYWdlbnQudjEuUmVhZFJlamVjdGVkSAASNAoOZmlsZV9ub3RfZm91bmQYBCABKAsyGi5hZ2VudC52MS5SZWFkRmlsZU5vdEZvdW5kSAASOwoRcGVybWlzc2lvbl9kZW5pZWQYBSABKAsyHi5hZ2VudC52MS5SZWFkUGVybWlzc2lvbkRlbmllZEgAEjEKDGludmFsaWRfZmlsZRgGIAEoCzIZLmFnZW50LnYxLlJlYWRJbnZhbGlkRmlsZUgAQggKBnJlc3VsdCLKAQoLUmVhZFN1Y2Nlc3MSDAoEcGF0aBgBIAEoCRITCgt0b3RhbF9saW5lcxgDIAEoBRIRCglmaWxlX3NpemUYBCABKAMSEQoJdHJ1bmNhdGVkGAYgASgIEhsKDm91dHB1dF9ibG9iX2lkGAcgASgMSAGIAQESEQoHY29udGVudBgCIAEoCUgAEg4KBGRhdGEYBSABKAxIABIVCg1yYW5nZV9hcHBsaWVkGAggASgIQggKBm91dHB1dEIRCg9fb3V0cHV0X2Jsb2JfaWQiKAoJUmVhZEVycm9yEgwKBHBhdGgYASABKAkSDQoFZXJyb3IYAiABKAkiLAoMUmVhZFJlamVjdGVkEgwKBHBhdGgYASABKAkSDgoGcmVhc29uGAIgASgJIiAKEFJlYWRGaWxlTm90Rm91bmQSDAoEcGF0aBgBIAEoCSIkChRSZWFkUGVybWlzc2lvbkRlbmllZBIMCgRwYXRoGAEgASgJIi8KD1JlYWRJbnZhbGlkRmlsZRIMCgRwYXRoGAEgASgJEg4KBnJlYXNvbhgCIAEoCSJeCgxSZWFkVG9vbENhbGwSJAoEYXJncxgBIAEoCzIWLmFnZW50LnYxLlJlYWRUb29sQXJncxIoCgZyZXN1bHQYAiABKAsyGC5hZ2VudC52MS5SZWFkVG9vbFJlc3VsdCJaCgxSZWFkVG9vbEFyZ3MSDAoEcGF0aBgBIAEoCRITCgZvZmZzZXQYAiABKAVIAIgBARISCgVsaW1pdBgDIAEoBUgBiAEBQgkKB19vZmZzZXRCCAoGX2xpbWl0InIKDlJlYWRUb29sUmVzdWx0EiwKB3N1Y2Nlc3MYASABKAsyGS5hZ2VudC52MS5SZWFkVG9vbFN1Y2Nlc3NIABIoCgVlcnJvchgCIAEoCzIXLmFnZW50LnYxLlJlYWRUb29sRXJyb3JIAEIICgZyZXN1bHQiMQoJUmVhZFJhbmdlEhIKCnN0YXJ0X2xpbmUYASABKA0SEAoIZW5kX2xpbmUYAiABKA0ijgIKD1JlYWRUb29sU3VjY2VzcxIQCghpc19lbXB0eRgCIAEoCBIWCg5leGNlZWRlZF9saW1pdBgDIAEoCBITCgt0b3RhbF9saW5lcxgEIAEoDRIRCglmaWxlX3NpemUYBSABKA0SDAoEcGF0aBgHIAEoCRIsCgpyZWFkX3JhbmdlGAggASgLMhMuYWdlbnQudjEuUmVhZFJhbmdlSAGIAQESEQoHY29udGVudBgBIAEoCUgAEg4KBGRhdGEYBiABKAxIABIWCgxkYXRhX2Jsb2JfaWQYCSABKAxIABIZCg9jb250ZW50X2Jsb2JfaWQYCiABKAxIAEIICgZvdXRwdXRCDQoLX3JlYWRfcmFuZ2UiJgoNUmVhZFRvb2xFcnJvchIVCg1lcnJvcl9tZXNzYWdlGAEgASgJImoKEFJlY29yZFNjcmVlbkFyZ3MSDAoEbW9kZRgBIAEoBRIUCgx0b29sX2NhbGxfaWQYAiABKAkSHQoQc2F2ZV9hc19maWxlbmFtZRgDIAEoCUgAiAEBQhMKEV9zYXZlX2FzX2ZpbGVuYW1lIokCChJSZWNvcmRTY3JlZW5SZXN1bHQSOwoNc3RhcnRfc3VjY2VzcxgBIAEoCzIiLmFnZW50LnYxLlJlY29yZFNjcmVlblN0YXJ0U3VjY2Vzc0gAEjkKDHNhdmVfc3VjY2VzcxgCIAEoCzIhLmFnZW50LnYxLlJlY29yZFNjcmVlblNhdmVTdWNjZXNzSAASPwoPZGlzY2FyZF9zdWNjZXNzGAMgASgLMiQuYWdlbnQudjEuUmVjb3JkU2NyZWVuRGlzY2FyZFN1Y2Nlc3NIABIwCgdmYWlsdXJlGAQgASgLMh0uYWdlbnQudjEuUmVjb3JkU2NyZWVuRmFpbHVyZUgAQggKBnJlc3VsdCJnChhSZWNvcmRTY3JlZW5TdGFydFN1Y2Nlc3MSJQodd2FzX3ByaW9yX3JlY29yZGluZ19jYW5jZWxsZWQYASABKAgSJAocd2FzX3NhdmVfYXNfZmlsZW5hbWVfaWdub3JlZBgCIAEoCCKgAQoXUmVjb3JkU2NyZWVuU2F2ZVN1Y2Nlc3MSDAoEcGF0aBgBIAEoCRIdChVyZWNvcmRpbmdfZHVyYXRpb25fbXMYAiABKAMSMAojcmVxdWVzdGVkX2ZpbGVfcGF0aF9yZWplY3RlZF9yZWFzb24YAyABKAVIAIgBAUImCiRfcmVxdWVzdGVkX2ZpbGVfcGF0aF9yZWplY3RlZF9yZWFzb24iHAoaUmVjb3JkU2NyZWVuRGlzY2FyZFN1Y2Nlc3MiJAoTUmVjb3JkU2NyZWVuRmFpbHVyZRINCgVlcnJvchgBIAEoCSI2ChNDdXJzb3JQYWNrYWdlUHJvbXB0EgwKBG5hbWUYASABKAkSEQoJZmlsZV9wYXRoGAIgASgJIuIBCg1DdXJzb3JQYWNrYWdlEgwKBG5hbWUYASABKAkSEwoLZGVzY3JpcHRpb24YAiABKAkSEwoLZm9sZGVyX3BhdGgYAyABKAkSDwoHZW5hYmxlZBgEIAEoCBIYCgtwYXJzZV9lcnJvchgFIAEoCUgAiAEBEi4KB3Byb21wdHMYBiADKAsyHS5hZ2VudC52MS5DdXJzb3JQYWNrYWdlUHJvbXB0EhgKEHJlYWRtZV9maWxlX3BhdGgYByABKAkSFAoMcGFja2FnZV90eXBlGAggASgFQg4KDF9wYXJzZV9lcnJvciKrAgoWUmVwb3NpdG9yeUluZGV4aW5nSW5mbxIfChdyZWxhdGl2ZV93b3Jrc3BhY2VfcGF0aBgBIAEoCRITCgtyZW1vdGVfdXJscxgCIAMoCRIUCgxyZW1vdGVfbmFtZXMYAyADKAkSEQoJcmVwb19uYW1lGAQgASgJEhIKCnJlcG9fb3duZXIYBSABKAkSEgoKaXNfdHJhY2tlZBgGIAEoCBIQCghpc19sb2NhbBgHIAEoCBImChlvcnRob2dvbmFsX3RyYW5zZm9ybV9zZWVkGAggASgBSACIAQESFQoNd29ya3NwYWNlX3VyaRgJIAEoCRIbChNwYXRoX2VuY3J5cHRpb25fa2V5GAogASgJQhwKGl9vcnRob2dvbmFsX3RyYW5zZm9ybV9zZWVkIqwCChJSZXF1ZXN0Q29udGV4dEFyZ3MSHQoQbm90ZXNfc2Vzc2lvbl9pZBgCIAEoCUgAiAEBEhkKDHdvcmtzcGFjZV9pZBgDIAEoCUgBiAEBEiYKGXJlYWRfb25seV9waW5uZWRfdHJlZV9zaGEYBCABKAlIAogBARIoChtyZWFkX29ubHlfcGx1Z2luX2NhY2hlX3Jvb3QYBSABKAlIA4gBARIXCgp1c2VfY2FjaGVkGAcgASgISASIAQFCEwoRX25vdGVzX3Nlc3Npb25faWRCDwoNX3dvcmtzcGFjZV9pZEIcChpfcmVhZF9vbmx5X3Bpbm5lZF90cmVlX3NoYUIeChxfcmVhZF9vbmx5X3BsdWdpbl9jYWNoZV9yb290Qg0KC191c2VfY2FjaGVkIroBChRSZXF1ZXN0Q29udGV4dFJlc3VsdBIyCgdzdWNjZXNzGAEgASgLMh8uYWdlbnQudjEuUmVxdWVzdENvbnRleHRTdWNjZXNzSAASLgoFZXJyb3IYAiABKAsyHS5hZ2VudC52MS5SZXF1ZXN0Q29udGV4dEVycm9ySAASNAoIcmVqZWN0ZWQYAyABKAsyIC5hZ2VudC52MS5SZXF1ZXN0Q29udGV4dFJlamVjdGVkSABCCAoGcmVzdWx0IooBChVSZXF1ZXN0Q29udGV4dFN1Y2Nlc3MSMQoPcmVxdWVzdF9jb250ZXh0GAEgASgLMhguYWdlbnQudjEuUmVxdWVzdENvbnRleHQSIwoWc2VydmVkX2Zyb21fZGlza19jYWNoZRgCIAEoCEgAiAEBQhkKF19zZXJ2ZWRfZnJvbV9kaXNrX2NhY2hlIiQKE1JlcXVlc3RDb250ZXh0RXJyb3ISDQoFZXJyb3IYASABKAkiKAoWUmVxdWVzdENvbnRleHRSZWplY3RlZBIOCgZyZWFzb24YASABKAkiwgEKCkltYWdlUHJvdG8SDAoEZGF0YRgBIAEoDBIMCgR1dWlkGAIgASgJEgwKBHBhdGgYAyABKAkSMQoJZGltZW5zaW9uGAQgASgLMh4uYWdlbnQudjEuSW1hZ2VQcm90b19EaW1lbnNpb24SJgoZdGFza19zcGVjaWZpY19kZXNjcmlwdGlvbhgGIAEoCUgAiAEBEhEKCW1pbWVfdHlwZRgHIAEoCUIcChpfdGFza19zcGVjaWZpY19kZXNjcmlwdGlvbiI1ChRJbWFnZVByb3RvX0RpbWVuc2lvbhINCgV3aWR0aBgBIAEoBRIOCgZoZWlnaHQYAiABKAUiaAoLR2l0UmVwb0luZm8SDAoEcGF0aBgBIAEoCRIOCgZzdGF0dXMYAiABKAkSEwoLYnJhbmNoX25hbWUYAyABKAkSFwoKcmVtb3RlX3VybBgEIAEoCUgAiAEBQg0KC19yZW1vdGVfdXJsIpsCChFSZXF1ZXN0Q29udGV4dEVudhISCgpvc192ZXJzaW9uGAEgASgJEhcKD3dvcmtzcGFjZV9wYXRocxgCIAMoCRINCgVzaGVsbBgDIAEoCRIXCg9zYW5kYm94X2VuYWJsZWQYBSABKAgSGAoQdGVybWluYWxzX2ZvbGRlchgHIAEoCRIhChlhZ2VudF9zaGFyZWRfbm90ZXNfZm9sZGVyGAggASgJEicKH2FnZW50X2NvbnZlcnNhdGlvbl9ub3Rlc19mb2xkZXIYCSABKAkSEQoJdGltZV96b25lGAogASgJEhYKDnByb2plY3RfZm9sZGVyGAsgASgJEiAKGGFnZW50X3RyYW5zY3JpcHRzX2ZvbGRlchgMIAEoCSI8Cg9EZWJ1Z01vZGVDb25maWcSEAoIbG9nX3BhdGgYASABKAkSFwoPc2VydmVyX2VuZHBvaW50GAIgASgJIrQBCg9Ta2lsbERlc2NyaXB0b3ISDAoEbmFtZRgBIAEoCRITCgtkZXNjcmlwdGlvbhgCIAEoCRITCgtmb2xkZXJfcGF0aBgDIAEoCRIPCgdlbmFibGVkGAQgASgIEhgKC3BhcnNlX2Vycm9yGAUgASgJSACIAQESGAoQcmVhZG1lX2ZpbGVfcGF0aBgGIAEoCRIUCgxwYWNrYWdlX3R5cGUYByABKAVCDgoMX3BhcnNlX2Vycm9yIkQKDFNraWxsT3B0aW9ucxI0ChFza2lsbF9kZXNjcmlwdG9ycxgBIAMoCzIZLmFnZW50LnYxLlNraWxsRGVzY3JpcHRvciL2CAoOUmVxdWVzdENvbnRleHQSIwoFcnVsZXMYAiADKAsyFC5hZ2VudC52MS5DdXJzb3JSdWxlEigKA2VudhgEIAEoCzIbLmFnZW50LnYxLlJlcXVlc3RDb250ZXh0RW52EjkKD3JlcG9zaXRvcnlfaW5mbxgGIAMoCzIgLmFnZW50LnYxLlJlcG9zaXRvcnlJbmRleGluZ0luZm8SKgoFdG9vbHMYByADKAsyGy5hZ2VudC52MS5NY3BUb29sRGVmaW5pdGlvbhInChpjb252ZXJzYXRpb25fbm90ZXNfbGlzdGluZxgIIAEoCUgAiAEBEiEKFHNoYXJlZF9ub3Rlc19saXN0aW5nGAkgASgJSAGIAQESKAoJZ2l0X3JlcG9zGAsgAygLMhUuYWdlbnQudjEuR2l0UmVwb0luZm8SNgoPcHJvamVjdF9sYXlvdXRzGA0gAygLMh0uYWdlbnQudjEuTHNEaXJlY3RvcnlUcmVlTm9kZRIzChBtY3BfaW5zdHJ1Y3Rpb25zGA4gAygLMhkuYWdlbnQudjEuTWNwSW5zdHJ1Y3Rpb25zEjkKEWRlYnVnX21vZGVfY29uZmlnGA8gASgLMhkuYWdlbnQudjEuRGVidWdNb2RlQ29uZmlnSAKIAQESFwoKY2xvdWRfcnVsZRgQIAEoCUgDiAEBEh8KEndlYl9zZWFyY2hfZW5hYmxlZBgRIAEoCEgEiAEBEjIKDXNraWxsX29wdGlvbnMYEiABKAsyFi5hZ2VudC52MS5Ta2lsbE9wdGlvbnNIBYgBARIuCiFyZXBvc2l0b3J5X2luZm9fc2hvdWxkX3F1ZXJ5X3Byb2QYEyABKAhIBogBARJBCg1maWxlX2NvbnRlbnRzGBQgAygLMiouYWdlbnQudjEuUmVxdWVzdENvbnRleHQuRmlsZUNvbnRlbnRzRW50cnkSIAoTdXNlcl9pbnRlbnRfc3VtbWFyeRgVIAEoCUgHiAEBEjIKEGN1c3RvbV9zdWJhZ2VudHMYFiADKAsyGC5hZ2VudC52MS5DdXN0b21TdWJhZ2VudBJEChdtY3BfZmlsZV9zeXN0ZW1fb3B0aW9ucxgXIAEoCzIeLmFnZW50LnYxLk1jcEZpbGVTeXN0ZW1PcHRpb25zSAiIAQEaMwoRRmlsZUNvbnRlbnRzRW50cnkSCwoDa2V5GAEgASgJEg0KBXZhbHVlGAIgASgJOgI4AUIdChtfY29udmVyc2F0aW9uX25vdGVzX2xpc3RpbmdCFwoVX3NoYXJlZF9ub3Rlc19saXN0aW5nQhQKEl9kZWJ1Z19tb2RlX2NvbmZpZ0INCgtfY2xvdWRfcnVsZUIVChNfd2ViX3NlYXJjaF9lbmFibGVkQhAKDl9za2lsbF9vcHRpb25zQiQKIl9yZXBvc2l0b3J5X2luZm9fc2hvdWxkX3F1ZXJ5X3Byb2RCFgoUX3VzZXJfaW50ZW50X3N1bW1hcnlCGgoYX21jcF9maWxlX3N5c3RlbV9vcHRpb25zIrICCg1TYW5kYm94UG9saWN5EgwKBHR5cGUYASABKAUSGwoObmV0d29ya19hY2Nlc3MYAiABKAhIAIgBARIiChphZGRpdGlvbmFsX3JlYWR3cml0ZV9wYXRocxgDIAMoCRIhChlhZGRpdGlvbmFsX3JlYWRvbmx5X3BhdGhzGAQgAygJEh0KEGRlYnVnX291dHB1dF9kaXIYBSABKAlIAYgBARIdChBibG9ja19naXRfd3JpdGVzGAYgASgISAKIAQESHgoRZGlzYWJsZV90bXBfd3JpdGUYByABKAhIA4gBAUIRCg9fbmV0d29ya19hY2Nlc3NCEwoRX2RlYnVnX291dHB1dF9kaXJCEwoRX2Jsb2NrX2dpdF93cml0ZXNCFAoSX2Rpc2FibGVfdG1wX3dyaXRlIu8BCg1TZWxlY3RlZEltYWdlEgwKBHV1aWQYAiABKAkSDAoEcGF0aBgDIAEoCRI0CglkaW1lbnNpb24YBCABKAsyIS5hZ2VudC52MS5TZWxlY3RlZEltYWdlX0RpbWVuc2lvbhIRCgltaW1lX3R5cGUYByABKAkSEQoHYmxvYl9pZBgBIAEoDEgAEg4KBGRhdGEYCCABKAxIABJDChFibG9iX2lkX3dpdGhfZGF0YRgJIAEoCzImLmFnZW50LnYxLlNlbGVjdGVkSW1hZ2VfQmxvYklkV2l0aERhdGFIAEIRCg9kYXRhX29yX2Jsb2JfaWQiPQocU2VsZWN0ZWRJbWFnZV9CbG9iSWRXaXRoRGF0YRIPCgdibG9iX2lkGAEgASgMEgwKBGRhdGEYAiABKAwiOAoXU2VsZWN0ZWRJbWFnZV9EaW1lbnNpb24SDQoFd2lkdGgYASABKAUSDgoGaGVpZ2h0GAIgASgFIkkKEUV4dHJhQ29udGV4dEVudHJ5Eg4KBGRhdGEYASABKAlIABIRCgdibG9iX2lkGAIgASgMSABCEQoPZGF0YV9vcl9ibG9iX2lkIlsKDFNlbGVjdGVkRmlsZRIPCgdjb250ZW50GAEgASgJEgwKBHBhdGgYAiABKAkSGgoNcmVsYXRpdmVfcGF0aBgDIAEoCUgAiAEBQhAKDl9yZWxhdGl2ZV9wYXRoIoQBChVTZWxlY3RlZENvZGVTZWxlY3Rpb24SDwoHY29udGVudBgBIAEoCRIMCgRwYXRoGAIgASgJEhoKDXJlbGF0aXZlX3BhdGgYAyABKAlIAIgBARIeCgVyYW5nZRgEIAEoCzIPLmFnZW50LnYxLlJhbmdlQhAKDl9yZWxhdGl2ZV9wYXRoIl0KEFNlbGVjdGVkVGVybWluYWwSDwoHY29udGVudBgBIAEoCRISCgV0aXRsZRgCIAEoCUgAiAEBEhEKBHBhdGgYAyABKAlIAYgBAUIICgZfdGl0bGVCBwoFX3BhdGgihgEKGVNlbGVjdGVkVGVybWluYWxTZWxlY3Rpb24SDwoHY29udGVudBgBIAEoCRISCgV0aXRsZRgCIAEoCUgAiAEBEhEKBHBhdGgYAyABKAlIAYgBARIeCgVyYW5nZRgEIAEoCzIPLmFnZW50LnYxLlJhbmdlQggKBl90aXRsZUIHCgVfcGF0aCKDAQoOU2VsZWN0ZWRGb2xkZXISDAoEcGF0aBgBIAEoCRIaCg1yZWxhdGl2ZV9wYXRoGAIgASgJSACIAQESNQoOZGlyZWN0b3J5X3RyZWUYAyABKAsyHS5hZ2VudC52MS5Mc0RpcmVjdG9yeVRyZWVOb2RlQhAKDl9yZWxhdGl2ZV9wYXRoIp8BChRTZWxlY3RlZEV4dGVybmFsTGluaxILCgN1cmwYASABKAkSDAoEdXVpZBgCIAEoCRIYCgtwZGZfY29udGVudBgDIAEoCUgAiAEBEhMKBmlzX3BkZhgEIAEoCEgBiAEBEhUKCGZpbGVuYW1lGAUgASgJSAKIAQFCDgoMX3BkZl9jb250ZW50QgkKB19pc19wZGZCCwoJX2ZpbGVuYW1lIjgKElNlbGVjdGVkQ3Vyc29yUnVsZRIiCgRydWxlGAEgASgLMhQuYWdlbnQudjEuQ3Vyc29yUnVsZSIiCg9TZWxlY3RlZEdpdERpZmYSDwoHY29udGVudBgBIAEoCSIyCh9TZWxlY3RlZEdpdERpZmZGcm9tQnJhbmNoVG9NYWluEg8KB2NvbnRlbnQYASABKAkiaQoRU2VsZWN0ZWRHaXRDb21taXQSCwoDc2hhGAEgASgJEg8KB21lc3NhZ2UYAiABKAkSGAoLZGVzY3JpcHRpb24YAyABKAlIAIgBARIMCgRkaWZmGAQgASgJQg4KDF9kZXNjcmlwdGlvbiLdAQoTU2VsZWN0ZWRQdWxsUmVxdWVzdBIOCgZudW1iZXIYASABKAUSCwoDdXJsGAIgASgJEhIKBXRpdGxlGAMgASgJSACIAQESEwoLZm9sZGVyX3BhdGgYBCABKAkSGQoMc3VtbWFyeV9qc29uGAUgASgJSAGIAQESGAoLZGVzY3JpcHRpb24YBiABKAlIAogBARIUCgdibG9iX2lkGAcgASgMSAOIAQFCCAoGX3RpdGxlQg8KDV9zdW1tYXJ5X2pzb25CDgoMX2Rlc2NyaXB0aW9uQgoKCF9ibG9iX2lkIrMBChpTZWxlY3RlZEdpdFBSRGlmZlNlbGVjdGlvbhIOCgZwcl91cmwYASABKAkSEQoJZmlsZV9wYXRoGAIgASgJEhIKCnN0YXJ0X2xpbmUYAyABKAUSEAoIZW5kX2xpbmUYBCABKAUSGQoMZGlmZl9jb250ZW50GAUgASgJSACIAQESFAoHYmxvYl9pZBgGIAEoDEgBiAEBQg8KDV9kaWZmX2NvbnRlbnRCCgoIX2Jsb2JfaWQiNgoVU2VsZWN0ZWRDdXJzb3JDb21tYW5kEgwKBG5hbWUYASABKAkSDwoHY29udGVudBgCIAEoCSI1ChVTZWxlY3RlZERvY3VtZW50YXRpb24SDgoGZG9jX2lkGAEgASgJEgwKBG5hbWUYAiABKAkiMgoQU2VsZWN0ZWRQYXN0Q2hhdBIQCghhZ2VudF9pZBgBIAEoCRIMCgRuYW1lGAIgASgJIqsBCglDYWxsRnJhbWUSGgoNZnVuY3Rpb25fbmFtZRgBIAEoCUgAiAEBEhAKA3VybBgCIAEoCUgBiAEBEhgKC2xpbmVfbnVtYmVyGAMgASgFSAKIAQESGgoNY29sdW1uX251bWJlchgEIAEoBUgDiAEBQhAKDl9mdW5jdGlvbl9uYW1lQgYKBF91cmxCDgoMX2xpbmVfbnVtYmVyQhAKDl9jb2x1bW5fbnVtYmVyImgKClN0YWNrVHJhY2USKAoLY2FsbF9mcmFtZXMYASADKAsyEy5hZ2VudC52MS5DYWxsRnJhbWUSHAoPcmF3X3N0YWNrX3RyYWNlGAIgASgJSACIAQFCEgoQX3Jhd19zdGFja190cmFjZSLkAQoSU2VsZWN0ZWRDb25zb2xlTG9nEg8KB21lc3NhZ2UYASABKAkSEQoJdGltZXN0YW1wGAIgASgBEg0KBWxldmVsGAMgASgJEhMKC2NsaWVudF9uYW1lGAQgASgJEhIKCnNlc3Npb25faWQYBSABKAkSLgoLc3RhY2tfdHJhY2UYBiABKAsyFC5hZ2VudC52MS5TdGFja1RyYWNlSACIAQESHQoQb2JqZWN0X2RhdGFfanNvbhgHIAEoCUgBiAEBQg4KDF9zdGFja190cmFjZUITChFfb2JqZWN0X2RhdGFfanNvbiK6AQoRU2VsZWN0ZWRVSUVsZW1lbnQSDwoHZWxlbWVudBgBIAEoCRINCgV4cGF0aBgCIAEoCRIUCgx0ZXh0X2NvbnRlbnQYAyABKAkSDQoFZXh0cmEYBCABKAkSFgoJY29tcG9uZW50GAUgASgJSACIAQESIQoUY29tcG9uZW50X3Byb3BzX2pzb24YBiABKAlIAYgBAUIMCgpfY29tcG9uZW50QhcKFV9jb21wb25lbnRfcHJvcHNfanNvbiIgChBTZWxlY3RlZFN1YmFnZW50EgwKBG5hbWUYASABKAkiggoKD1NlbGVjdGVkQ29udGV4dBIwCg9zZWxlY3RlZF9pbWFnZXMYASADKAsyFy5hZ2VudC52MS5TZWxlY3RlZEltYWdlEjwKEmludm9jYXRpb25fY29udGV4dBgCIAEoCzIbLmFnZW50LnYxLkludm9jYXRpb25Db250ZXh0SACIAQESFQoNZXh0cmFfY29udGV4dBgDIAMoCRI6ChVleHRyYV9jb250ZXh0X2VudHJpZXMYECADKAsyGy5hZ2VudC52MS5FeHRyYUNvbnRleHRFbnRyeRIlCgVmaWxlcxgEIAMoCzIWLmFnZW50LnYxLlNlbGVjdGVkRmlsZRI4Cg9jb2RlX3NlbGVjdGlvbnMYBSADKAsyHy5hZ2VudC52MS5TZWxlY3RlZENvZGVTZWxlY3Rpb24SLQoJdGVybWluYWxzGAYgAygLMhouYWdlbnQudjEuU2VsZWN0ZWRUZXJtaW5hbBJAChN0ZXJtaW5hbF9zZWxlY3Rpb25zGAcgAygLMiMuYWdlbnQudjEuU2VsZWN0ZWRUZXJtaW5hbFNlbGVjdGlvbhIpCgdmb2xkZXJzGAggAygLMhguYWdlbnQudjEuU2VsZWN0ZWRGb2xkZXISNgoOZXh0ZXJuYWxfbGlua3MYCSADKAsyHi5hZ2VudC52MS5TZWxlY3RlZEV4dGVybmFsTGluaxIyCgxjdXJzb3JfcnVsZXMYCiADKAsyHC5hZ2VudC52MS5TZWxlY3RlZEN1cnNvclJ1bGUSMAoIZ2l0X2RpZmYYEiABKAsyGS5hZ2VudC52MS5TZWxlY3RlZEdpdERpZmZIAYgBARJUChxnaXRfZGlmZl9mcm9tX2JyYW5jaF90b19tYWluGAsgASgLMikuYWdlbnQudjEuU2VsZWN0ZWRHaXREaWZmRnJvbUJyYW5jaFRvTWFpbkgCiAEBEjgKD2N1cnNvcl9jb21tYW5kcxgMIAMoCzIfLmFnZW50LnYxLlNlbGVjdGVkQ3Vyc29yQ29tbWFuZBI3Cg5kb2N1bWVudGF0aW9ucxgNIAMoCzIfLmFnZW50LnYxLlNlbGVjdGVkRG9jdW1lbnRhdGlvbhIwCgt1aV9lbGVtZW50cxgOIAMoCzIbLmFnZW50LnYxLlNlbGVjdGVkVUlFbGVtZW50EjIKDGNvbnNvbGVfbG9ncxgPIAMoCzIcLmFnZW50LnYxLlNlbGVjdGVkQ29uc29sZUxvZxIwCgtnaXRfY29tbWl0cxgRIAMoCzIbLmFnZW50LnYxLlNlbGVjdGVkR2l0Q29tbWl0Ei4KCnBhc3RfY2hhdHMYEyADKAsyGi5hZ2VudC52MS5TZWxlY3RlZFBhc3RDaGF0EkQKFmdpdF9wcl9kaWZmX3NlbGVjdGlvbnMYFCADKAsyJC5hZ2VudC52MS5TZWxlY3RlZEdpdFBSRGlmZlNlbGVjdGlvbhI9ChZzZWxlY3RlZF9wdWxsX3JlcXVlc3RzGBUgAygLMh0uYWdlbnQudjEuU2VsZWN0ZWRQdWxsUmVxdWVzdBI2ChJzZWxlY3RlZF9zdWJhZ2VudHMYFiADKAsyGi5hZ2VudC52MS5TZWxlY3RlZFN1YmFnZW50QhUKE19pbnZvY2F0aW9uX2NvbnRleHRCCwoJX2dpdF9kaWZmQh8KHV9naXRfZGlmZl9mcm9tX2JyYW5jaF90b19tYWluIuUBChFJbnZvY2F0aW9uQ29udGV4dBI/CgxzbGFja190aHJlYWQYASABKAsyJy5hZ2VudC52MS5JbnZvY2F0aW9uQ29udGV4dF9TbGFja1RocmVhZEgAEjkKCWdpdGh1Yl9wchgCIAEoCzIkLmFnZW50LnYxLkludm9jYXRpb25Db250ZXh0X0dpdGh1YlBSSAASOQoJaWRlX3N0YXRlGAMgASgLMiQuYWdlbnQudjEuSW52b2NhdGlvbkNvbnRleHRfSWRlU3RhdGVIABIRCgdibG9iX2lkGAogASgMSABCBgoEZGF0YSK7AQodSW52b2NhdGlvbkNvbnRleHRfU2xhY2tUaHJlYWQSDgoGdGhyZWFkGAEgASgJEhkKDGNoYW5uZWxfbmFtZRgCIAEoCUgAiAEBEhwKD2NoYW5uZWxfcHVycG9zZRgDIAEoCUgBiAEBEhoKDWNoYW5uZWxfdG9waWMYBCABKAlIAogBAUIPCg1fY2hhbm5lbF9uYW1lQhIKEF9jaGFubmVsX3B1cnBvc2VCEAoOX2NoYW5uZWxfdG9waWMifAoaSW52b2NhdGlvbkNvbnRleHRfR2l0aHViUFISDQoFdGl0bGUYASABKAkSEwoLZGVzY3JpcHRpb24YAiABKAkSEAoIY29tbWVudHMYAyABKAkSGAoLY2lfZmFpbHVyZXMYBCABKAlIAIgBAUIOCgxfY2lfZmFpbHVyZXMi/gEKGkludm9jYXRpb25Db250ZXh0X0lkZVN0YXRlEkAKDXZpc2libGVfZmlsZXMYASADKAsyKS5hZ2VudC52MS5JbnZvY2F0aW9uQ29udGV4dF9JZGVTdGF0ZV9GaWxlEkgKFXJlY2VudGx5X3ZpZXdlZF9maWxlcxgCIAMoCzIpLmFnZW50LnYxLkludm9jYXRpb25Db250ZXh0X0lkZVN0YXRlX0ZpbGUSVAoUY3VycmVudGx5X3ZpZXdlZF9wcnMYAyADKAsyNi5hZ2VudC52MS5JbnZvY2F0aW9uQ29udGV4dF9JZGVTdGF0ZV9WaWV3ZWRQdWxsUmVxdWVzdCKOAgofSW52b2NhdGlvbkNvbnRleHRfSWRlU3RhdGVfRmlsZRIMCgRwYXRoGAEgASgJEhoKDXJlbGF0aXZlX3BhdGgYAiABKAlIAIgBARJWCg9jdXJzb3JfcG9zaXRpb24YAyABKAsyOC5hZ2VudC52MS5JbnZvY2F0aW9uQ29udGV4dF9JZGVTdGF0ZV9GaWxlX0N1cnNvclBvc2l0aW9uSAGIAQESEwoLdG90YWxfbGluZXMYBCABKAUSGwoOYWN0aXZlX2NvbW1hbmQYBSABKAlIAogBAUIQCg5fcmVsYXRpdmVfcGF0aEISChBfY3Vyc29yX3Bvc2l0aW9uQhEKD19hY3RpdmVfY29tbWFuZCJMCi5JbnZvY2F0aW9uQ29udGV4dF9JZGVTdGF0ZV9GaWxlX0N1cnNvclBvc2l0aW9uEgwKBGxpbmUYASABKAUSDAoEdGV4dBgCIAEoCSLpAQosSW52b2NhdGlvbkNvbnRleHRfSWRlU3RhdGVfVmlld2VkUHVsbFJlcXVlc3QSDgoGbnVtYmVyGAEgASgFEgsKA3VybBgCIAEoCRISCgV0aXRsZRgDIAEoCUgAiAEBEhgKC2ZvbGRlcl9wYXRoGAQgASgJSAGIAQESGQoMc3VtbWFyeV9qc29uGAUgASgJSAKIAQESGAoLZGVzY3JpcHRpb24YBiABKAlIA4gBAUIICgZfdGl0bGVCDgoMX2ZvbGRlcl9wYXRoQg8KDV9zdW1tYXJ5X2pzb25CDgoMX2Rlc2NyaXB0aW9uIkgKFlNldHVwVm1FbnZpcm9ubWVudEFyZ3MSFwoPaW5zdGFsbF9jb21tYW5kGAIgASgJEhUKDXN0YXJ0X2NvbW1hbmQYAyABKAkiXAoYU2V0dXBWbUVudmlyb25tZW50UmVzdWx0EjYKB3N1Y2Nlc3MYASABKAsyIy5hZ2VudC52MS5TZXR1cFZtRW52aXJvbm1lbnRTdWNjZXNzSABCCAoGcmVzdWx0IhsKGVNldHVwVm1FbnZpcm9ubWVudFN1Y2Nlc3MigAEKGlNldHVwVm1FbnZpcm9ubWVudFRvb2xDYWxsEi4KBGFyZ3MYASABKAsyIC5hZ2VudC52MS5TZXR1cFZtRW52aXJvbm1lbnRBcmdzEjIKBnJlc3VsdBgCIAEoCzIiLmFnZW50LnYxLlNldHVwVm1FbnZpcm9ubWVudFJlc3VsdCLJAgoZU2hlbGxDb21tYW5kUGFyc2luZ1Jlc3VsdBIWCg5wYXJzaW5nX2ZhaWxlZBgBIAEoCBJSChNleGVjdXRhYmxlX2NvbW1hbmRzGAIgAygLMjUuYWdlbnQudjEuU2hlbGxDb21tYW5kUGFyc2luZ1Jlc3VsdF9FeGVjdXRhYmxlQ29tbWFuZBIVCg1oYXNfcmVkaXJlY3RzGAMgASgIEiAKGGhhc19jb21tYW5kX3N1YnN0aXR1dGlvbhgEIAEoCBInChphbGxfcmVkaXJlY3RzX2FyZV9kZXZfbnVsbBgFIAEoCEgAiAEBEj8KCXJlZGlyZWN0cxgGIAMoCzIsLmFnZW50LnYxLlNoZWxsQ29tbWFuZFBhcnNpbmdSZXN1bHRfUmVkaXJlY3RCHQobX2FsbF9yZWRpcmVjdHNfYXJlX2Rldl9udWxsIk0KLlNoZWxsQ29tbWFuZFBhcnNpbmdSZXN1bHRfRXhlY3V0YWJsZUNvbW1hbmRBcmcSDAoEdHlwZRgBIAEoCRINCgV2YWx1ZRgCIAEoCSKWAQorU2hlbGxDb21tYW5kUGFyc2luZ1Jlc3VsdF9FeGVjdXRhYmxlQ29tbWFuZBIMCgRuYW1lGAEgASgJEkYKBGFyZ3MYAiADKAsyOC5hZ2VudC52MS5TaGVsbENvbW1hbmRQYXJzaW5nUmVzdWx0X0V4ZWN1dGFibGVDb21tYW5kQXJnEhEKCWZ1bGxfdGV4dBgDIAEoCSL6BwoJU2hlbGxBcmdzEg8KB2NvbW1hbmQYASABKAkSGQoRd29ya2luZ19kaXJlY3RvcnkYAiABKAkSDwoHdGltZW91dBgDIAEoBRIUCgx0b29sX2NhbGxfaWQYBCABKAkSFwoPc2ltcGxlX2NvbW1hbmRzGAUgAygJEhoKEmhhc19pbnB1dF9yZWRpcmVjdBgGIAEoCBIbChNoYXNfb3V0cHV0X3JlZGlyZWN0GAcgASgIEjsKDnBhcnNpbmdfcmVzdWx0GAggASgLMiMuYWdlbnQudjEuU2hlbGxDb21tYW5kUGFyc2luZ1Jlc3VsdBI+ChhyZXF1ZXN0ZWRfc2FuZGJveF9wb2xpY3kYCSABKAsyFy5hZ2VudC52MS5TYW5kYm94UG9saWN5SACIAQESKAobZmlsZV9vdXRwdXRfdGhyZXNob2xkX2J5dGVzGAogASgESAGIAQESFQoNaXNfYmFja2dyb3VuZBgLIAEoCBIVCg1za2lwX2FwcHJvdmFsGAwgASgIEhgKEHRpbWVvdXRfYmVoYXZpb3IYDSABKAUSGQoMaGFyZF90aW1lb3V0GA4gASgFSAKIAQESGAoLZGVzY3JpcHRpb24YDyABKAlIA4gBARJBChFjbGFzc2lmaWVyX3Jlc3VsdBgQIAEoCzIhLmFnZW50LnYxLkNvbW1hbmRDbGFzc2lmaWVyUmVzdWx0SASIAQESEwoLY2xvc2Vfc3RkaW4YESABKAgSSQoTb3V0cHV0X25vdGlmaWNhdGlvbhgSIAEoCzInLmFnZW50LnYxLlNoZWxsT3V0cHV0Tm90aWZpY2F0aW9uQ29uZmlnSAWIAQESPQoTc21hcnRfbW9kZV9hcHByb3ZhbBgTIAEoCzIbLmFnZW50LnYxLlNtYXJ0TW9kZUFwcHJvdmFsSAaIAQESTgoZaG9va19hcHByb3ZhbF9yZXF1aXJlbWVudBgUIAEoCzImLmFnZW50LnYxLlNoZWxsSG9va0FwcHJvdmFsUmVxdWlyZW1lbnRIB4gBARIcCg9jb252ZXJzYXRpb25faWQYFSABKAlICIgBAUIbChlfcmVxdWVzdGVkX3NhbmRib3hfcG9saWN5Qh4KHF9maWxlX291dHB1dF90aHJlc2hvbGRfYnl0ZXNCDwoNX2hhcmRfdGltZW91dEIOCgxfZGVzY3JpcHRpb25CFAoSX2NsYXNzaWZpZXJfcmVzdWx0QhYKFF9vdXRwdXRfbm90aWZpY2F0aW9uQhYKFF9zbWFydF9tb2RlX2FwcHJvdmFsQhwKGl9ob29rX2FwcHJvdmFsX3JlcXVpcmVtZW50QhIKEF9jb252ZXJzYXRpb25faWQi+gMKC1NoZWxsUmVzdWx0EjQKDnNhbmRib3hfcG9saWN5GGUgASgLMhcuYWdlbnQudjEuU2FuZGJveFBvbGljeUgBiAEBEhoKDWlzX2JhY2tncm91bmQYZiABKAhIAogBARIdChB0ZXJtaW5hbHNfZm9sZGVyGGcgASgJSAOIAQESEAoDcGlkGGggASgNSASIAQESKQoHc3VjY2VzcxgBIAEoCzIWLmFnZW50LnYxLlNoZWxsU3VjY2Vzc0gAEikKB2ZhaWx1cmUYAiABKAsyFi5hZ2VudC52MS5TaGVsbEZhaWx1cmVIABIpCgd0aW1lb3V0GAMgASgLMhYuYWdlbnQudjEuU2hlbGxUaW1lb3V0SAASKwoIcmVqZWN0ZWQYBCABKAsyFy5hZ2VudC52MS5TaGVsbFJlamVjdGVkSAASMAoLc3Bhd25fZXJyb3IYBSABKAsyGS5hZ2VudC52MS5TaGVsbFNwYXduRXJyb3JIABI8ChFwZXJtaXNzaW9uX2RlbmllZBgHIAEoCzIfLmFnZW50LnYxLlNoZWxsUGVybWlzc2lvbkRlbmllZEgAQggKBnJlc3VsdEIRCg9fc2FuZGJveF9wb2xpY3lCEAoOX2lzX2JhY2tncm91bmRCEwoRX3Rlcm1pbmFsc19mb2xkZXJCBgoEX3BpZCIhChFTaGVsbFN0cmVhbVN0ZG91dBIMCgRkYXRhGAEgASgJIiEKEVNoZWxsU3RyZWFtU3RkZXJyEgwKBGRhdGEYASABKAki9wEKD1NoZWxsU3RyZWFtRXhpdBIMCgRjb2RlGAEgASgNEgsKA2N3ZBgCIAEoCRI2Cg9vdXRwdXRfbG9jYXRpb24YAyABKAsyGC5hZ2VudC52MS5PdXRwdXRMb2NhdGlvbkgAiAEBEg8KB2Fib3J0ZWQYBCABKAgSGQoMYWJvcnRfcmVhc29uGAUgASgFSAGIAQESJAoXbG9jYWxfZXhlY3V0aW9uX3RpbWVfbXMYBiABKAVIAogBAUISChBfb3V0cHV0X2xvY2F0aW9uQg8KDV9hYm9ydF9yZWFzb25CGgoYX2xvY2FsX2V4ZWN1dGlvbl90aW1lX21zIlsKEFNoZWxsU3RyZWFtU3RhcnQSNAoOc2FuZGJveF9wb2xpY3kYASABKAsyFy5hZ2VudC52MS5TYW5kYm94UG9saWN5SACIAQFCEQoPX3NhbmRib3hfcG9saWN5ItoBChdTaGVsbFN0cmVhbUJhY2tncm91bmRlZBIQCghzaGVsbF9pZBgBIAEoDRIPCgdjb21tYW5kGAIgASgJEhkKEXdvcmtpbmdfZGlyZWN0b3J5GAMgASgJEhAKA3BpZBgEIAEoDUgAiAEBEhcKCm1zX3RvX3dhaXQYBSABKAVIAYgBARI0CgZyZWFzb24YBiABKA4yHy5hZ2VudC52MS5TaGVsbEJhY2tncm91bmRSZWFzb25IAogBAUIGCgRfcGlkQg0KC19tc190b193YWl0QgkKB19yZWFzb24irAMKC1NoZWxsU3RyZWFtEi0KBnN0ZG91dBgBIAEoCzIbLmFnZW50LnYxLlNoZWxsU3RyZWFtU3Rkb3V0SAASLQoGc3RkZXJyGAIgASgLMhsuYWdlbnQudjEuU2hlbGxTdHJlYW1TdGRlcnJIABIpCgRleGl0GAMgASgLMhkuYWdlbnQudjEuU2hlbGxTdHJlYW1FeGl0SAASKwoFc3RhcnQYBCABKAsyGi5hZ2VudC52MS5TaGVsbFN0cmVhbVN0YXJ0SAASKwoIcmVqZWN0ZWQYBSABKAsyFy5hZ2VudC52MS5TaGVsbFJlamVjdGVkSAASPAoRcGVybWlzc2lvbl9kZW5pZWQYBiABKAsyHy5hZ2VudC52MS5TaGVsbFBlcm1pc3Npb25EZW5pZWRIABI5CgxiYWNrZ3JvdW5kZWQYByABKAsyIS5hZ2VudC52MS5TaGVsbFN0cmVhbUJhY2tncm91bmRlZEgAEjgKDGhvb2tfY29udGV4dBgIIAEoCzIgLmFnZW50LnYxLlNoZWxsU3RyZWFtSG9va0NvbnRleHRIAEIHCgVldmVudCJLCg5PdXRwdXRMb2NhdGlvbhIRCglmaWxlX3BhdGgYASABKAkSEgoKc2l6ZV9ieXRlcxgCIAEoAxISCgpsaW5lX2NvdW50GAMgASgDIpgFCgxTaGVsbFN1Y2Nlc3MSDwoHY29tbWFuZBgBIAEoCRIZChF3b3JraW5nX2RpcmVjdG9yeRgCIAEoCRIRCglleGl0X2NvZGUYAyABKAUSDgoGc2lnbmFsGAQgASgJEg4KBnN0ZG91dBgFIAEoCRIOCgZzdGRlcnIYBiABKAkSFgoOZXhlY3V0aW9uX3RpbWUYByABKAUSNgoPb3V0cHV0X2xvY2F0aW9uGAggASgLMhguYWdlbnQudjEuT3V0cHV0TG9jYXRpb25IAIgBARIVCghzaGVsbF9pZBgJIAEoDUgBiAEBEh8KEmludGVybGVhdmVkX291dHB1dBgKIAEoCUgCiAEBEhAKA3BpZBgLIAEoDUgDiAEBEhcKCm1zX3RvX3dhaXQYDCABKAVIBIgBARIkChdsb2NhbF9leGVjdXRpb25fdGltZV9tcxgNIAEoBUgFiAEBEj8KEWJhY2tncm91bmRfcmVhc29uGA4gASgOMh8uYWdlbnQudjEuU2hlbGxCYWNrZ3JvdW5kUmVhc29uSAaIAQESGAoLb3V0cHV0X2hlYWQYDyABKAlIB4gBARIYCgtvdXRwdXRfdGFpbBgQIAEoCUgIiAEBEhkKDGVsaWRlZF9jaGFycxgRIAEoDUgJiAEBQhIKEF9vdXRwdXRfbG9jYXRpb25CCwoJX3NoZWxsX2lkQhUKE19pbnRlcmxlYXZlZF9vdXRwdXRCBgoEX3BpZEINCgtfbXNfdG9fd2FpdEIaChhfbG9jYWxfZXhlY3V0aW9uX3RpbWVfbXNCFAoSX2JhY2tncm91bmRfcmVhc29uQg4KDF9vdXRwdXRfaGVhZEIOCgxfb3V0cHV0X3RhaWxCDwoNX2VsaWRlZF9jaGFycyKYBAoMU2hlbGxGYWlsdXJlEg8KB2NvbW1hbmQYASABKAkSGQoRd29ya2luZ19kaXJlY3RvcnkYAiABKAkSEQoJZXhpdF9jb2RlGAMgASgFEg4KBnNpZ25hbBgEIAEoCRIOCgZzdGRvdXQYBSABKAkSDgoGc3RkZXJyGAYgASgJEhYKDmV4ZWN1dGlvbl90aW1lGAcgASgFEjYKD291dHB1dF9sb2NhdGlvbhgIIAEoCzIYLmFnZW50LnYxLk91dHB1dExvY2F0aW9uSACIAQESHwoSaW50ZXJsZWF2ZWRfb3V0cHV0GAkgASgJSAGIAQESGQoMYWJvcnRfcmVhc29uGAogASgFSAKIAQESDwoHYWJvcnRlZBgLIAEoCBIkChdsb2NhbF9leGVjdXRpb25fdGltZV9tcxgMIAEoBUgDiAEBEhgKC291dHB1dF9oZWFkGA0gASgJSASIAQESGAoLb3V0cHV0X3RhaWwYDiABKAlIBYgBARIZCgxlbGlkZWRfY2hhcnMYDyABKA1IBogBAUISChBfb3V0cHV0X2xvY2F0aW9uQhUKE19pbnRlcmxlYXZlZF9vdXRwdXRCDwoNX2Fib3J0X3JlYXNvbkIaChhfbG9jYWxfZXhlY3V0aW9uX3RpbWVfbXNCDgoMX291dHB1dF9oZWFkQg4KDF9vdXRwdXRfdGFpbEIPCg1fZWxpZGVkX2NoYXJzIk4KDFNoZWxsVGltZW91dBIPCgdjb21tYW5kGAEgASgJEhkKEXdvcmtpbmdfZGlyZWN0b3J5GAIgASgJEhIKCnRpbWVvdXRfbXMYAyABKAUiYAoNU2hlbGxSZWplY3RlZBIPCgdjb21tYW5kGAEgASgJEhkKEXdvcmtpbmdfZGlyZWN0b3J5GAIgASgJEg4KBnJlYXNvbhgDIAEoCRITCgtpc19yZWFkb25seRgEIAEoCCJnChVTaGVsbFBlcm1pc3Npb25EZW5pZWQSDwoHY29tbWFuZBgBIAEoCRIZChF3b3JraW5nX2RpcmVjdG9yeRgCIAEoCRINCgVlcnJvchgDIAEoCRITCgtpc19yZWFkb25seRgEIAEoCCJMCg9TaGVsbFNwYXduRXJyb3ISDwoHY29tbWFuZBgBIAEoCRIZChF3b3JraW5nX2RpcmVjdG9yeRgCIAEoCRINCgVlcnJvchgDIAEoCSJAChJTaGVsbFBhcnRpYWxSZXN1bHQSFAoMc3Rkb3V0X2RlbHRhGAEgASgJEhQKDHN0ZGVycl9kZWx0YRgCIAEoCSJZCg1TaGVsbFRvb2xDYWxsEiEKBGFyZ3MYASABKAsyEy5hZ2VudC52MS5TaGVsbEFyZ3MSJQoGcmVzdWx0GAIgASgLMhUuYWdlbnQudjEuU2hlbGxSZXN1bHQiKwoYU2hlbGxUb29sQ2FsbFN0ZG91dERlbHRhEg8KB2NvbnRlbnQYASABKAkiKwoYU2hlbGxUb29sQ2FsbFN0ZGVyckRlbHRhEg8KB2NvbnRlbnQYASABKAkiiQEKElNoZWxsVG9vbENhbGxEZWx0YRI0CgZzdGRvdXQYASABKAsyIi5hZ2VudC52MS5TaGVsbFRvb2xDYWxsU3Rkb3V0RGVsdGFIABI0CgZzdGRlcnIYAiABKAsyIi5hZ2VudC52MS5TaGVsbFRvb2xDYWxsU3RkZXJyRGVsdGFIAEIHCgVkZWx0YSLtAQoMU3ViYWdlbnRUeXBlEjgKC3Vuc3BlY2lmaWVkGAEgASgLMiEuYWdlbnQudjEuU3ViYWdlbnRUeXBlVW5zcGVjaWZpZWRIABI5Cgxjb21wdXRlcl91c2UYAiABKAsyIS5hZ2VudC52MS5TdWJhZ2VudFR5cGVDb21wdXRlclVzZUgAEi4KBmN1c3RvbRgDIAEoCzIcLmFnZW50LnYxLlN1YmFnZW50VHlwZUN1c3RvbUgAEjAKB2V4cGxvcmUYBCABKAsyHS5hZ2VudC52MS5TdWJhZ2VudFR5cGVFeHBsb3JlSABCBgoEdHlwZSIZChdTdWJhZ2VudFR5cGVVbnNwZWNpZmllZCIZChdTdWJhZ2VudFR5cGVDb21wdXRlclVzZSIVChNTdWJhZ2VudFR5cGVFeHBsb3JlIiIKElN1YmFnZW50VHlwZUN1c3RvbRIMCgRuYW1lGAEgASgJIo0BCg5DdXN0b21TdWJhZ2VudBIRCglmdWxsX3BhdGgYASABKAkSDAoEbmFtZRgCIAEoCRITCgtkZXNjcmlwdGlvbhgDIAEoCRINCgV0b29scxgEIAMoCRINCgVtb2RlbBgFIAEoCRIOCgZwcm9tcHQYBiABKAkSFwoPcGVybWlzc2lvbl9tb2RlGAcgASgFImgKDlN3aXRjaE1vZGVBcmdzEhYKDnRhcmdldF9tb2RlX2lkGAEgASgJEhgKC2V4cGxhbmF0aW9uGAIgASgJSACIAQESFAoMdG9vbF9jYWxsX2lkGAMgASgJQg4KDF9leHBsYW5hdGlvbiKqAQoQU3dpdGNoTW9kZVJlc3VsdBIuCgdzdWNjZXNzGAEgASgLMhsuYWdlbnQudjEuU3dpdGNoTW9kZVN1Y2Nlc3NIABIqCgVlcnJvchgCIAEoCzIZLmFnZW50LnYxLlN3aXRjaE1vZGVFcnJvckgAEjAKCHJlamVjdGVkGAMgASgLMhwuYWdlbnQudjEuU3dpdGNoTW9kZVJlamVjdGVkSABCCAoGcmVzdWx0Ij0KEVN3aXRjaE1vZGVTdWNjZXNzEhQKDGZyb21fbW9kZV9pZBgBIAEoCRISCgp0b19tb2RlX2lkGAIgASgJIiAKD1N3aXRjaE1vZGVFcnJvchINCgVlcnJvchgBIAEoCSIkChJTd2l0Y2hNb2RlUmVqZWN0ZWQSDgoGcmVhc29uGAEgASgJImgKElN3aXRjaE1vZGVUb29sQ2FsbBImCgRhcmdzGAEgASgLMhguYWdlbnQudjEuU3dpdGNoTW9kZUFyZ3MSKgoGcmVzdWx0GAIgASgLMhouYWdlbnQudjEuU3dpdGNoTW9kZVJlc3VsdCJAChZTd2l0Y2hNb2RlUmVxdWVzdFF1ZXJ5EiYKBGFyZ3MYASABKAsyGC5hZ2VudC52MS5Td2l0Y2hNb2RlQXJncyKpAQoZU3dpdGNoTW9kZVJlcXVlc3RSZXNwb25zZRJACghhcHByb3ZlZBgBIAEoCzIsLmFnZW50LnYxLlN3aXRjaE1vZGVSZXF1ZXN0UmVzcG9uc2VfQXBwcm92ZWRIABJACghyZWplY3RlZBgCIAEoCzIsLmFnZW50LnYxLlN3aXRjaE1vZGVSZXF1ZXN0UmVzcG9uc2VfUmVqZWN0ZWRIAEIICgZyZXN1bHQiJAoiU3dpdGNoTW9kZVJlcXVlc3RSZXNwb25zZV9BcHByb3ZlZCI0CiJTd2l0Y2hNb2RlUmVxdWVzdFJlc3BvbnNlX1JlamVjdGVkEg4KBnJlYXNvbhgBIAEoCSJ1CghUb2RvSXRlbRIKCgJpZBgBIAEoCRIPCgdjb250ZW50GAIgASgJEg4KBnN0YXR1cxgDIAEoBRISCgpjcmVhdGVkX2F0GAQgASgDEhIKCnVwZGF0ZWRfYXQYBSABKAMSFAoMZGVwZW5kZW5jaWVzGAYgAygJImsKE1VwZGF0ZVRvZG9zVG9vbENhbGwSJwoEYXJncxgBIAEoCzIZLmFnZW50LnYxLlVwZGF0ZVRvZG9zQXJncxIrCgZyZXN1bHQYAiABKAsyGy5hZ2VudC52MS5VcGRhdGVUb2Rvc1Jlc3VsdCJDCg9VcGRhdGVUb2Rvc0FyZ3MSIQoFdG9kb3MYASADKAsyEi5hZ2VudC52MS5Ub2RvSXRlbRINCgVtZXJnZRgCIAEoCCJ7ChFVcGRhdGVUb2Rvc1Jlc3VsdBIvCgdzdWNjZXNzGAEgASgLMhwuYWdlbnQudjEuVXBkYXRlVG9kb3NTdWNjZXNzSAASKwoFZXJyb3IYAiABKAsyGi5hZ2VudC52MS5VcGRhdGVUb2Rvc0Vycm9ySABCCAoGcmVzdWx0Il8KElVwZGF0ZVRvZG9zU3VjY2VzcxIhCgV0b2RvcxgBIAMoCzISLmFnZW50LnYxLlRvZG9JdGVtEhMKC3RvdGFsX2NvdW50GAIgASgFEhEKCXdhc19tZXJnZRgDIAEoCCIhChBVcGRhdGVUb2Rvc0Vycm9yEg0KBWVycm9yGAEgASgJImUKEVJlYWRUb2Rvc1Rvb2xDYWxsEiUKBGFyZ3MYASABKAsyFy5hZ2VudC52MS5SZWFkVG9kb3NBcmdzEikKBnJlc3VsdBgCIAEoCzIZLmFnZW50LnYxLlJlYWRUb2Rvc1Jlc3VsdCI5Cg1SZWFkVG9kb3NBcmdzEhUKDXN0YXR1c19maWx0ZXIYASADKAUSEQoJaWRfZmlsdGVyGAIgAygJInUKD1JlYWRUb2Rvc1Jlc3VsdBItCgdzdWNjZXNzGAEgASgLMhouYWdlbnQudjEuUmVhZFRvZG9zU3VjY2Vzc0gAEikKBWVycm9yGAIgASgLMhguYWdlbnQudjEuUmVhZFRvZG9zRXJyb3JIAEIICgZyZXN1bHQiSgoQUmVhZFRvZG9zU3VjY2VzcxIhCgV0b2RvcxgBIAMoCzISLmFnZW50LnYxLlRvZG9JdGVtEhMKC3RvdGFsX2NvdW50GAIgASgFIh8KDlJlYWRUb2Rvc0Vycm9yEg0KBWVycm9yGAEgASgJIksKBVJhbmdlEiEKBXN0YXJ0GAEgASgLMhIuYWdlbnQudjEuUG9zaXRpb24SHwoDZW5kGAIgASgLMhIuYWdlbnQudjEuUG9zaXRpb24iKAoIUG9zaXRpb24SDAoEbGluZRgBIAEoDRIOCgZjb2x1bW4YAiABKA0iGAoFRXJyb3ISDwoHbWVzc2FnZRgBIAEoCSI6Cg1XZWJTZWFyY2hBcmdzEhMKC3NlYXJjaF90ZXJtGAEgASgJEhQKDHRvb2xfY2FsbF9pZBgCIAEoCSKmAQoPV2ViU2VhcmNoUmVzdWx0Ei0KB3N1Y2Nlc3MYASABKAsyGi5hZ2VudC52MS5XZWJTZWFyY2hTdWNjZXNzSAASKQoFZXJyb3IYAiABKAsyGC5hZ2VudC52MS5XZWJTZWFyY2hFcnJvckgAEi8KCHJlamVjdGVkGAMgASgLMhsuYWdlbnQudjEuV2ViU2VhcmNoUmVqZWN0ZWRIAEIICgZyZXN1bHQiRAoQV2ViU2VhcmNoU3VjY2VzcxIwCgpyZWZlcmVuY2VzGAEgAygLMhwuYWdlbnQudjEuV2ViU2VhcmNoUmVmZXJlbmNlIh8KDldlYlNlYXJjaEVycm9yEg0KBWVycm9yGAEgASgJIiMKEVdlYlNlYXJjaFJlamVjdGVkEg4KBnJlYXNvbhgBIAEoCSI/ChJXZWJTZWFyY2hSZWZlcmVuY2USDQoFdGl0bGUYASABKAkSCwoDdXJsGAIgASgJEg0KBWNodW5rGAMgASgJImUKEVdlYlNlYXJjaFRvb2xDYWxsEiUKBGFyZ3MYASABKAsyFy5hZ2VudC52MS5XZWJTZWFyY2hBcmdzEikKBnJlc3VsdBgCIAEoCzIZLmFnZW50LnYxLldlYlNlYXJjaFJlc3VsdCI+ChVXZWJTZWFyY2hSZXF1ZXN0UXVlcnkSJQoEYXJncxgBIAEoCzIXLmFnZW50LnYxLldlYlNlYXJjaEFyZ3MipgEKGFdlYlNlYXJjaFJlcXVlc3RSZXNwb25zZRI/CghhcHByb3ZlZBgBIAEoCzIrLmFnZW50LnYxLldlYlNlYXJjaFJlcXVlc3RSZXNwb25zZV9BcHByb3ZlZEgAEj8KCHJlamVjdGVkGAIgASgLMisuYWdlbnQudjEuV2ViU2VhcmNoUmVxdWVzdFJlc3BvbnNlX1JlamVjdGVkSABCCAoGcmVzdWx0IiMKIVdlYlNlYXJjaFJlcXVlc3RSZXNwb25zZV9BcHByb3ZlZCIzCiFXZWJTZWFyY2hSZXF1ZXN0UmVzcG9uc2VfUmVqZWN0ZWQSDgoGcmVhc29uGAEgASgJIq0BCglXcml0ZUFyZ3MSDAoEcGF0aBgBIAEoCRIRCglmaWxlX3RleHQYAiABKAkSFAoMdG9vbF9jYWxsX2lkGAMgASgJEicKH3JldHVybl9maWxlX2NvbnRlbnRfYWZ0ZXJfd3JpdGUYBCABKAgSEgoKZmlsZV9ieXRlcxgFIAEoDBIaCg1lbmNvZGluZ19oaW50GAYgASgJSACIAQFCEAoOX2VuY29kaW5nX2hpbnQigAIKC1dyaXRlUmVzdWx0EikKB3N1Y2Nlc3MYASABKAsyFi5hZ2VudC52MS5Xcml0ZVN1Y2Nlc3NIABI8ChFwZXJtaXNzaW9uX2RlbmllZBgDIAEoCzIfLmFnZW50LnYxLldyaXRlUGVybWlzc2lvbkRlbmllZEgAEioKCG5vX3NwYWNlGAQgASgLMhYuYWdlbnQudjEuV3JpdGVOb1NwYWNlSAASJQoFZXJyb3IYBSABKAsyFC5hZ2VudC52MS5Xcml0ZUVycm9ySAASKwoIcmVqZWN0ZWQYBiABKAsyFy5hZ2VudC52MS5Xcml0ZVJlamVjdGVkSABCCAoGcmVzdWx0IooBCgxXcml0ZVN1Y2Nlc3MSDAoEcGF0aBgBIAEoCRIVCg1saW5lc19jcmVhdGVkGAIgASgFEhEKCWZpbGVfc2l6ZRgDIAEoBRIlChhmaWxlX2NvbnRlbnRfYWZ0ZXJfd3JpdGUYBCABKAlIAIgBAUIbChlfZmlsZV9jb250ZW50X2FmdGVyX3dyaXRlIm8KFVdyaXRlUGVybWlzc2lvbkRlbmllZBIMCgRwYXRoGAEgASgJEhEKCWRpcmVjdG9yeRgCIAEoCRIRCglvcGVyYXRpb24YAyABKAkSDQoFZXJyb3IYBCABKAkSEwoLaXNfcmVhZG9ubHkYBSABKAgiHAoMV3JpdGVOb1NwYWNlEgwKBHBhdGgYASABKAkiKQoKV3JpdGVFcnJvchIMCgRwYXRoGAEgASgJEg0KBWVycm9yGAIgASgJIi0KDVdyaXRlUmVqZWN0ZWQSDAoEcGF0aBgBIAEoCRIOCgZyZWFzb24YAiABKAkigwEKF0Jvb3RzdHJhcFN0YXRzaWdSZXF1ZXN0Eh4KEWlnbm9yZV9kZXZfc3RhdHVzGAEgASgISACIAQESHQoQb3BlcmF0aW5nX3N5c3RlbRgCIAEoBUgBiAEBQhQKEl9pZ25vcmVfZGV2X3N0YXR1c0ITChFfb3BlcmF0aW5nX3N5c3RlbSIOCgxQaW5nUmVzcG9uc2UitwEKC0V4ZWNSZXF1ZXN0Eg8KB2NvbW1hbmQYASABKAkSEAoDY3dkGAIgASgJSACIAQESDAoEYXJncxgDIAMoCRI7CgtlbnZpcm9ubWVudBgEIAMoCzImLmFnZW50LnYxLkV4ZWNSZXF1ZXN0LkVudmlyb25tZW50RW50cnkaMgoQRW52aXJvbm1lbnRFbnRyeRILCgNrZXkYASABKAkSDQoFdmFsdWUYAiABKAk6AjgBQgYKBF9jd2QioAEKDEV4ZWNSZXNwb25zZRItCgxzdGRvdXRfZXZlbnQYASABKAsyFS5hZ2VudC52MS5TdGRvdXRFdmVudEgAEi0KDHN0ZGVycl9ldmVudBgCIAEoCzIVLmFnZW50LnYxLlN0ZGVyckV2ZW50SAASKQoKZXhpdF9ldmVudBgDIAEoCzITLmFnZW50LnYxLkV4aXRFdmVudEgAQgcKBWV2ZW50IhsKC1N0ZG91dEV2ZW50EgwKBGRhdGEYASABKAkiGwoLU3RkZXJyRXZlbnQSDAoEZGF0YRgBIAEoCSIeCglFeGl0RXZlbnQSEQoJZXhpdF9jb2RlGAEgASgFIiMKE1JlYWRUZXh0RmlsZVJlcXVlc3QSDAoEcGF0aBgBIAEoCSInChRSZWFkVGV4dEZpbGVSZXNwb25zZRIPCgdjb250ZW50GAEgASgJIjUKFFdyaXRlVGV4dEZpbGVSZXF1ZXN0EgwKBHBhdGgYASABKAkSDwoHY29udGVudBgCIAEoCSIXChVXcml0ZVRleHRGaWxlUmVzcG9uc2UiJQoVUmVhZEJpbmFyeUZpbGVSZXF1ZXN0EgwKBHBhdGgYASABKAkiKQoWUmVhZEJpbmFyeUZpbGVSZXNwb25zZRIPCgdjb250ZW50GAEgASgMIjcKFldyaXRlQmluYXJ5RmlsZVJlcXVlc3QSDAoEcGF0aBgBIAEoCRIPCgdjb250ZW50GAIgASgMIhkKF1dyaXRlQmluYXJ5RmlsZVJlc3BvbnNlIkUKHkdldFdvcmtzcGFjZUNoYW5nZXNIYXNoUmVxdWVzdBIRCglyb290X3BhdGgYASABKAkSEAoIYmFzZV9yZWYYAiABKAkiLwofR2V0V29ya3NwYWNlQ2hhbmdlc0hhc2hSZXNwb25zZRIMCgRoYXNoGAEgASgJIlAKH1JlZnJlc2hHaXRodWJBY2Nlc3NUb2tlblJlcXVlc3QSGwoTZ2l0aHViX2FjY2Vzc190b2tlbhgBIAEoCRIQCghob3N0bmFtZRgCIAEoCSIiCiBSZWZyZXNoR2l0aHViQWNjZXNzVG9rZW5SZXNwb25zZSJXCh1XYXJtUmVtb3RlQWNjZXNzU2VydmVyUmVxdWVzdBIOCgZjb21taXQYASABKAkSDAoEcG9ydBgCIAEoBRIYChBjb25uZWN0aW9uX3Rva2VuGAMgASgJIiAKHldhcm1SZW1vdGVBY2Nlc3NTZXJ2ZXJSZXNwb25zZSIWChRMaXN0QXJ0aWZhY3RzUmVxdWVzdCKKAgoWQXJ0aWZhY3RVcGxvYWRNZXRhZGF0YRIVCg1hYnNvbHV0ZV9wYXRoGAEgASgJEhIKCnNpemVfYnl0ZXMYAiABKAQSGgoSdXBkYXRlZF9hdF91bml4X21zGAMgASgDEg4KBnN0YXR1cxgEIAEoBRIWCg5ieXRlc191cGxvYWRlZBgFIAEoBBISCgpsYXN0X2Vycm9yGAYgASgJEhcKD3VwbG9hZF9hdHRlbXB0cxgHIAEoDRIfChdsYXN0X3N0YXJ0ZWRfYXRfdW5peF9tcxgIIAEoAxIgChhsYXN0X2ZpbmlzaGVkX2F0X3VuaXhfbXMYCSABKAMSEQoJdXBsb2FkX2lkGAogASgJIkwKFUxpc3RBcnRpZmFjdHNSZXNwb25zZRIzCglhcnRpZmFjdHMYASADKAsyIC5hZ2VudC52MS5BcnRpZmFjdFVwbG9hZE1ldGFkYXRhIk4KFlVwbG9hZEFydGlmYWN0c1JlcXVlc3QSNAoHdXBsb2FkcxgBIAMoCzIjLmFnZW50LnYxLkFydGlmYWN0VXBsb2FkSW5zdHJ1Y3Rpb24i1wIKGUFydGlmYWN0VXBsb2FkSW5zdHJ1Y3Rpb24SFQoNYWJzb2x1dGVfcGF0aBgBIAEoCRISCgp1cGxvYWRfdXJsGAIgASgJEg4KBm1ldGhvZBgDIAEoCRJBCgdoZWFkZXJzGAQgAygLMjAuYWdlbnQudjEuQXJ0aWZhY3RVcGxvYWRJbnN0cnVjdGlvbi5IZWFkZXJzRW50cnkSGQoMY29udGVudF90eXBlGAUgASgJSACIAQESHQoQc2xhY2tfdXBsb2FkX3VybBgGIAEoCUgBiAEBEhoKDXNsYWNrX2ZpbGVfaWQYByABKAlIAogBARouCgxIZWFkZXJzRW50cnkSCwoDa2V5GAEgASgJEg0KBXZhbHVlGAIgASgJOgI4AUIPCg1fY29udGVudF90eXBlQhMKEV9zbGFja191cGxvYWRfdXJsQhAKDl9zbGFja19maWxlX2lkIoQBChxBcnRpZmFjdFVwbG9hZERpc3BhdGNoUmVzdWx0EhUKDWFic29sdXRlX3BhdGgYASABKAkSDgoGc3RhdHVzGAIgASgFEg8KB21lc3NhZ2UYAyABKAkSGgoNc2xhY2tfZmlsZV9pZBgEIAEoCUgAiAEBQhAKDl9zbGFja19maWxlX2lkIlIKF1VwbG9hZEFydGlmYWN0c1Jlc3BvbnNlEjcKB3Jlc3VsdHMYASADKAsyJi5hZ2VudC52MS5BcnRpZmFjdFVwbG9hZERpc3BhdGNoUmVzdWx0IhwKGkdldE1jcFJlZnJlc2hUb2tlbnNSZXF1ZXN0IqUBChtHZXRNY3BSZWZyZXNoVG9rZW5zUmVzcG9uc2USUAoOcmVmcmVzaF90b2tlbnMYASADKAsyOC5hZ2VudC52MS5HZXRNY3BSZWZyZXNoVG9rZW5zUmVzcG9uc2UuUmVmcmVzaFRva2Vuc0VudHJ5GjQKElJlZnJlc2hUb2tlbnNFbnRyeRILCgNrZXkYASABKAkSDQoFdmFsdWUYAiABKAk6AjgBIqMBCiFVcGRhdGVFbnZpcm9ubWVudFZhcmlhYmxlc1JlcXVlc3QSQQoDZW52GAEgAygLMjQuYWdlbnQudjEuVXBkYXRlRW52aXJvbm1lbnRWYXJpYWJsZXNSZXF1ZXN0LkVudkVudHJ5Eg8KB3JlcGxhY2UYAiABKAgaKgoIRW52RW50cnkSCwoDa2V5GAEgASgJEg0KBXZhbHVlGAIgASgJOgI4ASJGCiJVcGRhdGVFbnZpcm9ubWVudFZhcmlhYmxlc1Jlc3BvbnNlEg8KB2FwcGxpZWQYASABKA0SDwoHcmVtb3ZlZBgCIAEoDSKDAQoSTWNwT0F1dGhTdG9yZWREYXRhEhUKDXJlZnJlc2hfdG9rZW4YASABKAkSEQoJY2xpZW50X2lkGAIgASgJEhoKDWNsaWVudF9zZWNyZXQYAyABKAlIAIgBARIVCg1yZWRpcmVjdF91cmlzGAQgAygJQhAKDl9jbGllbnRfc2VjcmV0Ik4KBUZyYW1lEgoKAmlkGAEgASgJEg4KBm1ldGhvZBgCIAEoCRIMCgRkYXRhGAMgASgMEgwKBGtpbmQYBCABKAUSDQoFZXJyb3IYBSABKAkiBwoFRW1wdHkiIwoNQmlkaVJlcXVlc3RJZBISCgpyZXF1ZXN0X2lkGAEgASgJIoECCgxQaVRydW5jYXRpb24SEQoJdHJ1bmNhdGVkGAEgASgIEhQKDHRydW5jYXRlZF9ieRgCIAEoCRITCgt0b3RhbF9saW5lcxgDIAEoDRIUCgxvdXRwdXRfbGluZXMYBCABKA0SFAoMb3V0cHV0X2J5dGVzGAUgASgNEhYKCW1heF9saW5lcxgGIAEoDUgAiAEBEhYKCW1heF9ieXRlcxgHIAEoDUgBiAEBEiAKGGZpcnN0X2xpbmVfZXhjZWVkc19saW1pdBgIIAEoCBIZChFsYXN0X2xpbmVfcGFydGlhbBgJIAEoCEIMCgpfbWF4X2xpbmVzQgwKCl9tYXhfYnl0ZXMiNwoRUGlFZGl0UmVwbGFjZW1lbnQSEAoIb2xkX3RleHQYASABKAkSEAoIbmV3X3RleHQYAiABKAkiXAoOUGlSZWFkRXhlY0FyZ3MSDAoEcGF0aBgBIAEoCRITCgZvZmZzZXQYAiABKAVIAIgBARISCgVsaW1pdBgDIAEoBUgBiAEBQgkKB19vZmZzZXRCCAoGX2xpbWl0ImMKEVBpUmVhZEV4ZWNTdWNjZXNzEg4KBm91dHB1dBgBIAEoCRIvCgp0cnVuY2F0aW9uGAIgASgLMhYuYWdlbnQudjEuUGlUcnVuY2F0aW9uSACIAQFCDQoLX3RydW5jYXRpb24iIAoPUGlSZWFkRXhlY0Vycm9yEg0KBWVycm9yGAEgASgJIngKEFBpUmVhZEV4ZWNSZXN1bHQSLgoHc3VjY2VzcxgBIAEoCzIbLmFnZW50LnYxLlBpUmVhZEV4ZWNTdWNjZXNzSAASKgoFZXJyb3IYAiABKAsyGS5hZ2VudC52MS5QaVJlYWRFeGVjRXJyb3JIAEIICgZyZXN1bHQiQwoOUGlCYXNoRXhlY0FyZ3MSDwoHY29tbWFuZBgBIAEoCRIUCgd0aW1lb3V0GAIgASgBSACIAQFCCgoIX3RpbWVvdXQilwEKEVBpQmFzaEV4ZWNTdWNjZXNzEg4KBm91dHB1dBgBIAEoCRIvCgp0cnVuY2F0aW9uGAIgASgLMhYuYWdlbnQudjEuUGlUcnVuY2F0aW9uSACIAQESHQoQZnVsbF9vdXRwdXRfcGF0aBgDIAEoCUgBiAEBQg0KC190cnVuY2F0aW9uQhMKEV9mdWxsX291dHB1dF9wYXRoIpQBCg9QaUJhc2hFeGVjRXJyb3ISDQoFZXJyb3IYASABKAkSLwoKdHJ1bmNhdGlvbhgCIAEoCzIWLmFnZW50LnYxLlBpVHJ1bmNhdGlvbkgAiAEBEh0KEGZ1bGxfb3V0cHV0X3BhdGgYAyABKAlIAYgBAUINCgtfdHJ1bmNhdGlvbkITChFfZnVsbF9vdXRwdXRfcGF0aCJ4ChBQaUJhc2hFeGVjUmVzdWx0Ei4KB3N1Y2Nlc3MYASABKAsyGy5hZ2VudC52MS5QaUJhc2hFeGVjU3VjY2Vzc0gAEioKBWVycm9yGAIgASgLMhkuYWdlbnQudjEuUGlCYXNoRXhlY0Vycm9ySABCCAoGcmVzdWx0IkoKDlBpRWRpdEV4ZWNBcmdzEgwKBHBhdGgYASABKAkSKgoFZWRpdHMYAiADKAsyGy5hZ2VudC52MS5QaUVkaXRSZXBsYWNlbWVudCJ4ChFQaUVkaXRFeGVjU3VjY2VzcxIOCgZvdXRwdXQYASABKAkSDAoEZGlmZhgCIAEoCRINCgVwYXRjaBgDIAEoCRIfChJmaXJzdF9jaGFuZ2VkX2xpbmUYBCABKA1IAIgBAUIVChNfZmlyc3RfY2hhbmdlZF9saW5lIiAKD1BpRWRpdEV4ZWNFcnJvchINCgVlcnJvchgBIAEoCSIkChJQaUVkaXRFeGVjUmVqZWN0ZWQSDgoGcmVhc29uGAEgASgJIqoBChBQaUVkaXRFeGVjUmVzdWx0Ei4KB3N1Y2Nlc3MYASABKAsyGy5hZ2VudC52MS5QaUVkaXRFeGVjU3VjY2Vzc0gAEioKBWVycm9yGAIgASgLMhkuYWdlbnQudjEuUGlFZGl0RXhlY0Vycm9ySAASMAoIcmVqZWN0ZWQYAyABKAsyHC5hZ2VudC52MS5QaUVkaXRFeGVjUmVqZWN0ZWRIAEIICgZyZXN1bHQiMAoPUGlXcml0ZUV4ZWNBcmdzEgwKBHBhdGgYASABKAkSDwoHY29udGVudBgCIAEoCSIkChJQaVdyaXRlRXhlY1N1Y2Nlc3MSDgoGb3V0cHV0GAEgASgJIiEKEFBpV3JpdGVFeGVjRXJyb3ISDQoFZXJyb3IYASABKAkiJQoTUGlXcml0ZUV4ZWNSZWplY3RlZBIOCgZyZWFzb24YASABKAkirgEKEVBpV3JpdGVFeGVjUmVzdWx0Ei8KB3N1Y2Nlc3MYASABKAsyHC5hZ2VudC52MS5QaVdyaXRlRXhlY1N1Y2Nlc3NIABIrCgVlcnJvchgCIAEoCzIaLmFnZW50LnYxLlBpV3JpdGVFeGVjRXJyb3JIABIxCghyZWplY3RlZBgDIAEoCzIdLmFnZW50LnYxLlBpV3JpdGVFeGVjUmVqZWN0ZWRIAEIICgZyZXN1bHQi5QEKDlBpR3JlcEV4ZWNBcmdzEg8KB3BhdHRlcm4YASABKAkSEQoEcGF0aBgCIAEoCUgAiAEBEhEKBGdsb2IYAyABKAlIAYgBARIYCgtpZ25vcmVfY2FzZRgEIAEoCEgCiAEBEhQKB2xpdGVyYWwYBSABKAhIA4gBARIUCgdjb250ZXh0GAYgASgFSASIAQESEgoFbGltaXQYByABKAVIBYgBAUIHCgVfcGF0aEIHCgVfZ2xvYkIOCgxfaWdub3JlX2Nhc2VCCgoIX2xpdGVyYWxCCgoIX2NvbnRleHRCCAoGX2xpbWl0IrYBChFQaUdyZXBFeGVjU3VjY2VzcxIOCgZvdXRwdXQYASABKAkSLwoKdHJ1bmNhdGlvbhgCIAEoCzIWLmFnZW50LnYxLlBpVHJ1bmNhdGlvbkgAiAEBEiAKE21hdGNoX2xpbWl0X3JlYWNoZWQYAyABKA1IAYgBARIXCg9saW5lc190cnVuY2F0ZWQYBCABKAhCDQoLX3RydW5jYXRpb25CFgoUX21hdGNoX2xpbWl0X3JlYWNoZWQiIAoPUGlHcmVwRXhlY0Vycm9yEg0KBWVycm9yGAEgASgJIngKEFBpR3JlcEV4ZWNSZXN1bHQSLgoHc3VjY2VzcxgBIAEoCzIbLmFnZW50LnYxLlBpR3JlcEV4ZWNTdWNjZXNzSAASKgoFZXJyb3IYAiABKAsyGS5hZ2VudC52MS5QaUdyZXBFeGVjRXJyb3JIAEIICgZyZXN1bHQiWwoOUGlGaW5kRXhlY0FyZ3MSDwoHcGF0dGVybhgBIAEoCRIRCgRwYXRoGAIgASgJSACIAQESEgoFbGltaXQYAyABKAVIAYgBAUIHCgVfcGF0aEIICgZfbGltaXQinwEKEVBpRmluZEV4ZWNTdWNjZXNzEg4KBm91dHB1dBgBIAEoCRIvCgp0cnVuY2F0aW9uGAIgASgLMhYuYWdlbnQudjEuUGlUcnVuY2F0aW9uSACIAQESIQoUcmVzdWx0X2xpbWl0X3JlYWNoZWQYAyABKA1IAYgBAUINCgtfdHJ1bmNhdGlvbkIXChVfcmVzdWx0X2xpbWl0X3JlYWNoZWQiIAoPUGlGaW5kRXhlY0Vycm9yEg0KBWVycm9yGAEgASgJIngKEFBpRmluZEV4ZWNSZXN1bHQSLgoHc3VjY2VzcxgBIAEoCzIbLmFnZW50LnYxLlBpRmluZEV4ZWNTdWNjZXNzSAASKgoFZXJyb3IYAiABKAsyGS5hZ2VudC52MS5QaUZpbmRFeGVjRXJyb3JIAEIICgZyZXN1bHQiSAoMUGlMc0V4ZWNBcmdzEhEKBHBhdGgYASABKAlIAIgBARISCgVsaW1pdBgCIAEoBUgBiAEBQgcKBV9wYXRoQggKBl9saW1pdCKbAQoPUGlMc0V4ZWNTdWNjZXNzEg4KBm91dHB1dBgBIAEoCRIvCgp0cnVuY2F0aW9uGAIgASgLMhYuYWdlbnQudjEuUGlUcnVuY2F0aW9uSACIAQESIAoTZW50cnlfbGltaXRfcmVhY2hlZBgDIAEoDUgBiAEBQg0KC190cnVuY2F0aW9uQhYKFF9lbnRyeV9saW1pdF9yZWFjaGVkIh4KDVBpTHNFeGVjRXJyb3ISDQoFZXJyb3IYASABKAkicgoOUGlMc0V4ZWNSZXN1bHQSLAoHc3VjY2VzcxgBIAEoCzIZLmFnZW50LnYxLlBpTHNFeGVjU3VjY2Vzc0gAEigKBWVycm9yGAIgASgLMhcuYWdlbnQudjEuUGlMc0V4ZWNFcnJvckgAQggKBnJlc3VsdCI8ChFNY3BTZXJ2ZXJOb3RGb3VuZBIMCgRuYW1lGAEgASgJEhkKEWF2YWlsYWJsZV9zZXJ2ZXJzGAIgAygJIg0KC01jcEFwcHJvdmVkIkEKEE1jcFN0YXRlRXhlY0FyZ3MSGgoSc2VydmVyX2lkZW50aWZpZXJzGAEgAygJEhEKCWtpY2tfb25seRgCIAEoCCKHAgoOTWNwU3RhdGVTZXJ2ZXISEwoLc2VydmVyX25hbWUYASABKAkSGQoRc2VydmVyX2lkZW50aWZpZXIYAiABKAkSEwoGcGx1Z2luGAMgASgJSACIAQESGAoLbWFya2V0cGxhY2UYBCABKAlIAYgBARIqCgV0b29scxgFIAMoCzIbLmFnZW50LnYxLk1jcFRvb2xEZWZpbml0aW9uEi8KDGluc3RydWN0aW9ucxgGIAMoCzIZLmFnZW50LnYxLk1jcEluc3RydWN0aW9ucxITCgZzdGF0dXMYByABKAlIAogBAUIJCgdfcGx1Z2luQg4KDF9tYXJrZXRwbGFjZUIJCgdfc3RhdHVzIjwKD01jcFN0YXRlU3VjY2VzcxIpCgdzZXJ2ZXJzGAEgAygLMhguYWdlbnQudjEuTWNwU3RhdGVTZXJ2ZXIiHgoNTWNwU3RhdGVFcnJvchINCgVlcnJvchgBIAEoCSIiChBNY3BTdGF0ZVJlamVjdGVkEg4KBnJlYXNvbhgBIAEoCSKmAQoSTWNwU3RhdGVFeGVjUmVzdWx0EiwKB3N1Y2Nlc3MYASABKAsyGS5hZ2VudC52MS5NY3BTdGF0ZVN1Y2Nlc3NIABIoCgVlcnJvchgCIAEoCzIXLmFnZW50LnYxLk1jcFN0YXRlRXJyb3JIABIuCghyZWplY3RlZBgDIAEoCzIaLmFnZW50LnYxLk1jcFN0YXRlUmVqZWN0ZWRIAEIICgZyZXN1bHQirQEKKUNvbW1hbmRDbGFzc2lmaWVyUmVzdWx0X0NsYXNzaWZpZWRDb21tYW5kEgwKBG5hbWUYASABKAkSEQoJYXJndW1lbnRzGAIgAygJEiYKGXN1Z2dlc3RlZF9hbGxvd2xpc3RfZW50cnkYAyABKAlIAIgBARIZChFzdWJjb21tYW5kX3Rva2VucxgEIAMoCUIcChpfc3VnZ2VzdGVkX2FsbG93bGlzdF9lbnRyeSLXAQoXQ29tbWFuZENsYXNzaWZpZXJSZXN1bHQSRQoIY29tbWFuZHMYASADKAsyMy5hZ2VudC52MS5Db21tYW5kQ2xhc3NpZmllclJlc3VsdF9DbGFzc2lmaWVkQ29tbWFuZBJWChZzdWdnZXN0ZWRfc2FuZGJveF9tb2RlGAIgASgOMjYuYWdlbnQudjEuQ29tbWFuZENsYXNzaWZpZXJSZXN1bHRfU3VnZ2VzdGVkU2FuZGJveE1vZGUSHQoVY2xhc3NpZmljYXRpb25fZmFpbGVkGAMgASgIIpMBCiJTaGVsbENvbW1hbmRQYXJzaW5nUmVzdWx0X1JlZGlyZWN0EhAKCG9wZXJhdG9yGAEgASgJEhcKD2Rlc3RpbmF0aW9uX2ZkcxgCIAMoDRIYChB0YXJnZXRfbm9kZV90eXBlGAMgASgJEhgKC3RhcmdldF90ZXh0GAQgASgJSACIAQFCDgoMX3RhcmdldF90ZXh0IooCChpTaGVsbEFsbG93bGlzdFByZWNoZWNrQXJncxIPCgdjb21tYW5kGAEgASgJEhkKEXdvcmtpbmdfZGlyZWN0b3J5GAIgASgJEjsKDnBhcnNpbmdfcmVzdWx0GAMgASgLMiMuYWdlbnQudjEuU2hlbGxDb21tYW5kUGFyc2luZ1Jlc3VsdBJBChFjbGFzc2lmaWVyX3Jlc3VsdBgEIAEoCzIhLmFnZW50LnYxLkNvbW1hbmRDbGFzc2lmaWVyUmVzdWx0SACIAQESGQoMdG9vbF9jYWxsX2lkGAUgASgJSAGIAQFCFAoSX2NsYXNzaWZpZXJfcmVzdWx0Qg8KDV90b29sX2NhbGxfaWQiMwocU2hlbGxBbGxvd2xpc3RQcmVjaGVja1Jlc3VsdBITCgthbGxvd2xpc3RlZBgBIAEoCCJ2ChhNY3BBbGxvd2xpc3RQcmVjaGVja0FyZ3MSGwoTcHJvdmlkZXJfaWRlbnRpZmllchgBIAEoCRIRCgl0b29sX25hbWUYAiABKAkSGQoMdG9vbF9jYWxsX2lkGAMgASgJSACIAQFCDwoNX3Rvb2xfY2FsbF9pZCIxChpNY3BBbGxvd2xpc3RQcmVjaGVja1Jlc3VsdBITCgthbGxvd2xpc3RlZBgBIAEoCCJYCh1XZWJGZXRjaEFsbG93bGlzdFByZWNoZWNrQXJncxILCgN1cmwYASABKAkSGQoMdG9vbF9jYWxsX2lkGAIgASgJSACIAQFCDwoNX3Rvb2xfY2FsbF9pZCI2Ch9XZWJGZXRjaEFsbG93bGlzdFByZWNoZWNrUmVzdWx0EhMKC2FsbG93bGlzdGVkGAEgASgIIjcKEVNtYXJ0TW9kZUFwcHJvdmFsEhIKCnJlcXVlc3RfaWQYASABKAkSDgoGcmVhc29uGAIgASgJIpwBCh1TaGVsbE91dHB1dE5vdGlmaWNhdGlvbkNvbmZpZxIPCgdwYXR0ZXJuGAEgASgJEg4KBnJlYXNvbhgCIAEoCRIVCghkZWJvdW5jZRgDIAEoAUgAiAEBEh8KEm5vdGlmaWNhdGlvbl9saW1pdBgEIAEoBUgBiAEBQgsKCV9kZWJvdW5jZUIVChNfbm90aWZpY2F0aW9uX2xpbWl0InkKHFNoZWxsSG9va0FwcHJvdmFsUmVxdWlyZW1lbnQSOQoEa2luZBgBIAEoDjIrLmFnZW50LnYxLlNoZWxsSG9va0FwcHJvdmFsUmVxdWlyZW1lbnRfS2luZBITCgZyZWFzb24YAiABKAlIAIgBAUIJCgdfcmVhc29uIjAKGEZvcmNlQmFja2dyb3VuZFNoZWxsQXJncxIUCgx0b29sX2NhbGxfaWQYASABKAkilQEKGkZvcmNlQmFja2dyb3VuZFNoZWxsUmVzdWx0EjQKBnN0YXR1cxgBIAEoDjIkLmFnZW50LnYxLkZvcmNlQmFja2dyb3VuZFNoZWxsU3RhdHVzEjAKDHNoZWxsX3Jlc3VsdBgCIAEoCzIVLmFnZW50LnYxLlNoZWxsUmVzdWx0SACIAQFCDwoNX3NoZWxsX3Jlc3VsdCJBChVIb29rQWRkaXRpb25hbENvbnRleHQSFwoPaG9va19ldmVudF9uYW1lGAEgASgJEg8KB2NvbnRlbnQYAiABKAkiWwoWU2hlbGxTdHJlYW1Ib29rQ29udGV4dBJBChhob29rX2FkZGl0aW9uYWxfY29udGV4dHMYASADKAsyHy5hZ2VudC52MS5Ib29rQWRkaXRpb25hbENvbnRleHQiSwoMU3ViYWdlbnRBcmdzEhQKDHRvb2xfY2FsbF9pZBgBIAEoCRIVCg1zdWJhZ2VudF90eXBlGAIgASgJEg4KBnByb21wdBgEIAEoCSLbAQoPU3ViYWdlbnRTdWNjZXNzEhAKCGFnZW50X2lkGAEgASgJEhoKDWZpbmFsX21lc3NhZ2UYAiABKAlIAIgBARIXCg90b29sX2NhbGxfY291bnQYAyABKAUSPQoRYmFja2dyb3VuZF9yZWFzb24YBCABKA4yIi5hZ2VudC52MS5TdWJhZ2VudEJhY2tncm91bmRSZWFzb24SHAoPdHJhbnNjcmlwdF9wYXRoGAUgASgJSAGIAQFCEAoOX2ZpbmFsX21lc3NhZ2VCEgoQX3RyYW5zY3JpcHRfcGF0aCJCCg1TdWJhZ2VudEVycm9yEhUKCGFnZW50X2lkGAEgASgJSACIAQESDQoFZXJyb3IYAiABKAlCCwoJX2FnZW50X2lkInIKDlN1YmFnZW50UmVzdWx0EiwKB3N1Y2Nlc3MYASABKAsyGS5hZ2VudC52MS5TdWJhZ2VudFN1Y2Nlc3NIABIoCgVlcnJvchgCIAEoCzIXLmFnZW50LnYxLlN1YmFnZW50RXJyb3JIAEIICgZyZXN1bHQiOQoRU3ViYWdlbnRBd2FpdEFyZ3MSEAoIYWdlbnRfaWQYASABKAkSEgoKdGltZW91dF9tcxgCIAEoDSKiAQoVU3ViYWdlbnRBd2FpdENvbXBsZXRlEhAKCGFnZW50X2lkGAEgASgJEhwKD3RyYW5zY3JpcHRfcGF0aBgCIAEoCUgAiAEBEhcKD3Rvb2xfY2FsbF9jb3VudBgDIAEoBRIaCg1maW5hbF9tZXNzYWdlGAQgASgJSAGIAQFCEgoQX3RyYW5zY3JpcHRfcGF0aEIQCg5fZmluYWxfbWVzc2FnZSJfChlTdWJhZ2VudEF3YWl0U3RpbGxSdW5uaW5nEhAKCGFnZW50X2lkGAEgASgJEhwKD3RyYW5zY3JpcHRfcGF0aBgCIAEoCUgAiAEBQhIKEF90cmFuc2NyaXB0X3BhdGgiKQoVU3ViYWdlbnRBd2FpdE5vdEZvdW5kEhAKCGFnZW50X2lkGAEgASgJIkcKElN1YmFnZW50QXdhaXRFcnJvchIVCghhZ2VudF9pZBgBIAEoCUgAiAEBEg0KBWVycm9yGAIgASgJQgsKCV9hZ2VudF9pZCL3AQoTU3ViYWdlbnRBd2FpdFJlc3VsdBIzCghjb21wbGV0ZRgBIAEoCzIfLmFnZW50LnYxLlN1YmFnZW50QXdhaXRDb21wbGV0ZUgAEjwKDXN0aWxsX3J1bm5pbmcYAiABKAsyIy5hZ2VudC52MS5TdWJhZ2VudEF3YWl0U3RpbGxSdW5uaW5nSAASNAoJbm90X2ZvdW5kGAMgASgLMh8uYWdlbnQudjEuU3ViYWdlbnRBd2FpdE5vdEZvdW5kSAASLQoFZXJyb3IYBCABKAsyHC5hZ2VudC52MS5TdWJhZ2VudEF3YWl0RXJyb3JIAEIICgZyZXN1bHQiMwobRm9yY2VCYWNrZ3JvdW5kU3ViYWdlbnRBcmdzEhQKDHRvb2xfY2FsbF9pZBgBIAEoCSJYCh1Gb3JjZUJhY2tncm91bmRTdWJhZ2VudFJlc3VsdBI3CgZzdGF0dXMYASABKA4yJy5hZ2VudC52MS5Gb3JjZUJhY2tncm91bmRTdWJhZ2VudFN0YXR1cyIYChZQcmVDb21wYWN0UmVxdWVzdFF1ZXJ5IhsKGVN1YmFnZW50U3RhcnRSZXF1ZXN0UXVlcnkiGgoYU3ViYWdlbnRTdG9wUmVxdWVzdFF1ZXJ5IhgKFlByZVRvb2xVc2VSZXF1ZXN0UXVlcnkiGQoXUG9zdFRvb2xVc2VSZXF1ZXN0UXVlcnkiIAoeUG9zdFRvb2xVc2VGYWlsdXJlUmVxdWVzdFF1ZXJ5IiAKHkJlZm9yZVN1Ym1pdFByb21wdFJlcXVlc3RRdWVyeSIgCh5BZnRlckFnZW50UmVzcG9uc2VSZXF1ZXN0UXVlcnkiHwodQWZ0ZXJBZ2VudFRob3VnaHRSZXF1ZXN0UXVlcnkiEgoQU3RvcFJlcXVlc3RRdWVyeSJHChlQcmVDb21wYWN0UmVxdWVzdFJlc3BvbnNlEhkKDHVzZXJfbWVzc2FnZRgBIAEoCUgAiAEBQg8KDV91c2VyX21lc3NhZ2UiqgEKHFN1YmFnZW50U3RhcnRSZXF1ZXN0UmVzcG9uc2USFwoKcGVybWlzc2lvbhgBIAEoCUgAiAEBEhkKDHVzZXJfbWVzc2FnZRgCIAEoCUgBiAEBEh8KEmFkZGl0aW9uYWxfY29udGV4dBgDIAEoCUgCiAEBQg0KC19wZXJtaXNzaW9uQg8KDV91c2VyX21lc3NhZ2VCFQoTX2FkZGl0aW9uYWxfY29udGV4dCKJAQobU3ViYWdlbnRTdG9wUmVxdWVzdFJlc3BvbnNlEh0KEGZvbGxvd3VwX21lc3NhZ2UYASABKAlIAIgBARIfChJhZGRpdGlvbmFsX2NvbnRleHQYAiABKAlIAYgBAUITChFfZm9sbG93dXBfbWVzc2FnZUIVChNfYWRkaXRpb25hbF9jb250ZXh0IoMCChlQcmVUb29sVXNlUmVxdWVzdFJlc3BvbnNlEhcKCnBlcm1pc3Npb24YASABKAlIAIgBARIZCgx1c2VyX21lc3NhZ2UYAiABKAlIAYgBARIaCg1hZ2VudF9tZXNzYWdlGAMgASgJSAKIAQESGgoNdXBkYXRlZF9pbnB1dBgEIAEoCUgDiAEBEh8KEmFkZGl0aW9uYWxfY29udGV4dBgFIAEoCUgEiAEBQg0KC19wZXJtaXNzaW9uQg8KDV91c2VyX21lc3NhZ2VCEAoOX2FnZW50X21lc3NhZ2VCEAoOX3VwZGF0ZWRfaW5wdXRCFQoTX2FkZGl0aW9uYWxfY29udGV4dCJUChpQb3N0VG9vbFVzZVJlcXVlc3RSZXNwb25zZRIfChJhZGRpdGlvbmFsX2NvbnRleHQYASABKAlIAIgBAUIVChNfYWRkaXRpb25hbF9jb250ZXh0IlsKIVBvc3RUb29sVXNlRmFpbHVyZVJlcXVlc3RSZXNwb25zZRIfChJhZGRpdGlvbmFsX2NvbnRleHQYASABKAlIAIgBAUIVChNfYWRkaXRpb25hbF9jb250ZXh0IqsBCiFCZWZvcmVTdWJtaXRQcm9tcHRSZXF1ZXN0UmVzcG9uc2USFQoIY29udGludWUYASABKAhIAIgBARIZCgx1c2VyX21lc3NhZ2UYAiABKAlIAYgBARIfChJhZGRpdGlvbmFsX2NvbnRleHQYAyABKAlIAogBAUILCglfY29udGludWVCDwoNX3VzZXJfbWVzc2FnZUIVChNfYWRkaXRpb25hbF9jb250ZXh0IiMKIUFmdGVyQWdlbnRSZXNwb25zZVJlcXVlc3RSZXNwb25zZSIiCiBBZnRlckFnZW50VGhvdWdodFJlcXVlc3RSZXNwb25zZSJJChNTdG9wUmVxdWVzdFJlc3BvbnNlEh0KEGZvbGxvd3VwX21lc3NhZ2UYASABKAlIAIgBAUITChFfZm9sbG93dXBfbWVzc2FnZSKdBQoSRXhlY3V0ZUhvb2tSZXF1ZXN0EjcKC3ByZV9jb21wYWN0GAEgASgLMiAuYWdlbnQudjEuUHJlQ29tcGFjdFJlcXVlc3RRdWVyeUgAEj0KDnN1YmFnZW50X3N0YXJ0GAIgASgLMiMuYWdlbnQudjEuU3ViYWdlbnRTdGFydFJlcXVlc3RRdWVyeUgAEjsKDXN1YmFnZW50X3N0b3AYAyABKAsyIi5hZ2VudC52MS5TdWJhZ2VudFN0b3BSZXF1ZXN0UXVlcnlIABI4CgxwcmVfdG9vbF91c2UYBCABKAsyIC5hZ2VudC52MS5QcmVUb29sVXNlUmVxdWVzdFF1ZXJ5SAASOgoNcG9zdF90b29sX3VzZRgFIAEoCzIhLmFnZW50LnYxLlBvc3RUb29sVXNlUmVxdWVzdFF1ZXJ5SAASSQoVcG9zdF90b29sX3VzZV9mYWlsdXJlGAYgASgLMiguYWdlbnQudjEuUG9zdFRvb2xVc2VGYWlsdXJlUmVxdWVzdFF1ZXJ5SAASSAoUYmVmb3JlX3N1Ym1pdF9wcm9tcHQYByABKAsyKC5hZ2VudC52MS5CZWZvcmVTdWJtaXRQcm9tcHRSZXF1ZXN0UXVlcnlIABJIChRhZnRlcl9hZ2VudF9yZXNwb25zZRgIIAEoCzIoLmFnZW50LnYxLkFmdGVyQWdlbnRSZXNwb25zZVJlcXVlc3RRdWVyeUgAEkYKE2FmdGVyX2FnZW50X3Rob3VnaHQYCSABKAsyJy5hZ2VudC52MS5BZnRlckFnZW50VGhvdWdodFJlcXVlc3RRdWVyeUgAEioKBHN0b3AYCyABKAsyGi5hZ2VudC52MS5TdG9wUmVxdWVzdFF1ZXJ5SABCCQoHcmVxdWVzdCK9BQoTRXhlY3V0ZUhvb2tSZXNwb25zZRI6CgtwcmVfY29tcGFjdBgBIAEoCzIjLmFnZW50LnYxLlByZUNvbXBhY3RSZXF1ZXN0UmVzcG9uc2VIABJACg5zdWJhZ2VudF9zdGFydBgCIAEoCzImLmFnZW50LnYxLlN1YmFnZW50U3RhcnRSZXF1ZXN0UmVzcG9uc2VIABI+Cg1zdWJhZ2VudF9zdG9wGAMgASgLMiUuYWdlbnQudjEuU3ViYWdlbnRTdG9wUmVxdWVzdFJlc3BvbnNlSAASOwoMcHJlX3Rvb2xfdXNlGAQgASgLMiMuYWdlbnQudjEuUHJlVG9vbFVzZVJlcXVlc3RSZXNwb25zZUgAEj0KDXBvc3RfdG9vbF91c2UYBSABKAsyJC5hZ2VudC52MS5Qb3N0VG9vbFVzZVJlcXVlc3RSZXNwb25zZUgAEkwKFXBvc3RfdG9vbF91c2VfZmFpbHVyZRgGIAEoCzIrLmFnZW50LnYxLlBvc3RUb29sVXNlRmFpbHVyZVJlcXVlc3RSZXNwb25zZUgAEksKFGJlZm9yZV9zdWJtaXRfcHJvbXB0GAcgASgLMisuYWdlbnQudjEuQmVmb3JlU3VibWl0UHJvbXB0UmVxdWVzdFJlc3BvbnNlSAASSwoUYWZ0ZXJfYWdlbnRfcmVzcG9uc2UYCCABKAsyKy5hZ2VudC52MS5BZnRlckFnZW50UmVzcG9uc2VSZXF1ZXN0UmVzcG9uc2VIABJJChNhZnRlcl9hZ2VudF90aG91Z2h0GAkgASgLMiouYWdlbnQudjEuQWZ0ZXJBZ2VudFRob3VnaHRSZXF1ZXN0UmVzcG9uc2VIABItCgRzdG9wGAsgASgLMh0uYWdlbnQudjEuU3RvcFJlcXVlc3RSZXNwb25zZUgAQgoKCHJlc3BvbnNlIkAKD0V4ZWN1dGVIb29rQXJncxItCgdyZXF1ZXN0GAEgASgLMhwuYWdlbnQudjEuRXhlY3V0ZUhvb2tSZXF1ZXN0IkQKEUV4ZWN1dGVIb29rUmVzdWx0Ei8KCHJlc3BvbnNlGAEgASgLMh0uYWdlbnQudjEuRXhlY3V0ZUhvb2tSZXNwb25zZSIlChNTbWFydE1vZGVSaXNrVGFyZ2V0Eg4KBmFjdGlvbhgBIAEoCSJHCiZTbWFydE1vZGVDbGFzc2lmaWVyQ29udmVyc2F0aW9uTWVzc2FnZRIMCgRyb2xlGAEgASgJEg8KB2NvbnRlbnQYAiABKAki7gEKF1NtYXJ0TW9kZUNsYXNzaWZpZXJBcmdzEhQKDHRvb2xfY2FsbF9pZBgBIAEoCRIjChZwYXJlbnRfY29udmVyc2F0aW9uX2lkGAIgASgJSACIAQESLQoGdGFyZ2V0GAMgASgLMh0uYWdlbnQudjEuU21hcnRNb2RlUmlza1RhcmdldBJOChRjb252ZXJzYXRpb25fY29udGV4dBgEIAMoCzIwLmFnZW50LnYxLlNtYXJ0TW9kZUNsYXNzaWZpZXJDb252ZXJzYXRpb25NZXNzYWdlQhkKF19wYXJlbnRfY29udmVyc2F0aW9uX2lkIoEBChpTbWFydE1vZGVDbGFzc2lmaWVyU3VjY2VzcxI3CghkZWNpc2lvbhgBIAEoDjIlLmFnZW50LnYxLlNtYXJ0TW9kZUNsYXNzaWZpZXJEZWNpc2lvbhIZCgxibG9ja19yZWFzb24YAiABKAlIAIgBAUIPCg1fYmxvY2tfcmVhc29uIikKGFNtYXJ0TW9kZUNsYXNzaWZpZXJFcnJvchINCgVlcnJvchgBIAEoCSKTAQoZU21hcnRNb2RlQ2xhc3NpZmllclJlc3VsdBI3CgdzdWNjZXNzGAEgASgLMiQuYWdlbnQudjEuU21hcnRNb2RlQ2xhc3NpZmllclN1Y2Nlc3NIABIzCgVlcnJvchgCIAEoCzIiLmFnZW50LnYxLlNtYXJ0TW9kZUNsYXNzaWZpZXJFcnJvckgAQggKBnJlc3VsdCI7ChVDYW52YXNEaWFnbm9zdGljc0FyZ3MSDAoEcGF0aBgBIAEoCRIUCgx0b29sX2NhbGxfaWQYAiABKAkiUwoYQ2FudmFzRGlhZ25vc3RpY3NTdWNjZXNzEgwKBHBhdGgYASABKAkSKQoLZGlhZ25vc3RpY3MYAiADKAsyFC5hZ2VudC52MS5EaWFnbm9zdGljIjUKFkNhbnZhc0RpYWdub3N0aWNzRXJyb3ISDAoEcGF0aBgBIAEoCRINCgVlcnJvchgCIAEoCSKNAQoXQ2FudmFzRGlhZ25vc3RpY3NSZXN1bHQSNQoHc3VjY2VzcxgBIAEoCzIiLmFnZW50LnYxLkNhbnZhc0RpYWdub3N0aWNzU3VjY2Vzc0gAEjEKBWVycm9yGAIgASgLMiAuYWdlbnQudjEuQ2FudmFzRGlhZ25vc3RpY3NFcnJvckgAQggKBnJlc3VsdCKsAQoVQ29udmVyc2F0aW9uU2VhcmNoSGl0EhcKD2NvbnZlcnNhdGlvbl9pZBgBIAEoCRINCgV0aXRsZRgCIAEoCRIyCgZzb3VyY2UYAyABKA4yIi5hZ2VudC52MS5Db252ZXJzYXRpb25TZWFyY2hTb3VyY2USFQoNdXBkYXRlZF9hdF9tcxgEIAEoAxIUCgdzbmlwcGV0GAUgASgJSACIAQFCCgoIX3NuaXBwZXQiggEKGUNvbnZlcnNhdGlvblNlYXJjaFN1Y2Nlc3MSLQoEaGl0cxgBIAMoCzIfLmFnZW50LnYxLkNvbnZlcnNhdGlvblNlYXJjaEhpdBIRCgl0cnVuY2F0ZWQYAiABKAgSDwoHcGFydGlhbBgDIAEoCBISCgpyZWJ1aWxkaW5nGAQgASgIIigKF0NvbnZlcnNhdGlvblNlYXJjaEVycm9yEg0KBWVycm9yGAEgASgJIlsKFkNvbnZlcnNhdGlvblNlYXJjaEFyZ3MSDQoFcXVlcnkYASABKAkSFAoMdG9vbF9jYWxsX2lkGAIgASgJEhIKBWxpbWl0GAMgASgFSACIAQFCCAoGX2xpbWl0IpABChhDb252ZXJzYXRpb25TZWFyY2hSZXN1bHQSNgoHc3VjY2VzcxgBIAEoCzIjLmFnZW50LnYxLkNvbnZlcnNhdGlvblNlYXJjaFN1Y2Nlc3NIABIyCgVlcnJvchgCIAEoCzIhLmFnZW50LnYxLkNvbnZlcnNhdGlvblNlYXJjaEVycm9ySABCCAoGcmVzdWx0IlUKGEFnZW50U3RvcmVDb25mbGljdEN1cnNvchIVCg1qb3VybmFsX2Vwb2NoGAEgASgJEgsKA3NlcRgCIAEoBBIVCg1sYXN0X2V2ZW50X2lkGAMgASgJIqUDChdBZ2VudFN0b3JlQ29uZmxpY3RFdmVudBIJCgF2GAEgASgNEhAKCGV2ZW50X2lkGAIgASgJEhUKDWpvdXJuYWxfZXBvY2gYAyABKAkSCwoDc2VxGAQgASgEEg0KBXRzX21zGAUgASgEEgwKBGtpbmQYBiABKAkSFQoIc3RvcmVfaWQYByABKAlIAIgBARIeChFvcmlnaW5hbF9yZWxfcGF0aBgIIAEoCUgBiAEBEh4KEWNvbmZsaWN0X3JlbF9wYXRoGAkgASgJSAKIAQESHgoRb3JpZ2luYWxfYWJzX3BhdGgYCiABKAlIA4gBARIeChFjb25mbGljdF9hYnNfcGF0aBgLIAEoCUgEiAEBEhwKD3ByZXNlcnZlZF9ieXRlcxgMIAEoBEgFiAEBQgsKCV9zdG9yZV9pZEIUChJfb3JpZ2luYWxfcmVsX3BhdGhCFAoSX2NvbmZsaWN0X3JlbF9wYXRoQhQKEl9vcmlnaW5hbF9hYnNfcGF0aEIUChJfY29uZmxpY3RfYWJzX3BhdGhCEgoQX3ByZXNlcnZlZF9ieXRlcyKUAQoZQWdlbnRTdG9yZUNvbmZsaWN0U3VjY2VzcxIxCgZldmVudHMYASADKAsyIS5hZ2VudC52MS5BZ2VudFN0b3JlQ29uZmxpY3RFdmVudBI3CgtuZXh0X2N1cnNvchgCIAEoCzIiLmFnZW50LnYxLkFnZW50U3RvcmVDb25mbGljdEN1cnNvchILCgNnYXAYAyABKAgiKAoXQWdlbnRTdG9yZUNvbmZsaWN0RXJyb3ISDQoFZXJyb3IYASABKAkifgoWQWdlbnRTdG9yZUNvbmZsaWN0QXJncxI3CgZjdXJzb3IYASABKAsyIi5hZ2VudC52MS5BZ2VudFN0b3JlQ29uZmxpY3RDdXJzb3JIAIgBARIUCgdhZHZhbmNlGAIgASgISAGIAQFCCQoHX2N1cnNvckIKCghfYWR2YW5jZSKQAQoYQWdlbnRTdG9yZUNvbmZsaWN0UmVzdWx0EjYKB3N1Y2Nlc3MYASABKAsyIy5hZ2VudC52MS5BZ2VudFN0b3JlQ29uZmxpY3RTdWNjZXNzSAASMgoFZXJyb3IYAiABKAsyIS5hZ2VudC52MS5BZ2VudFN0b3JlQ29uZmxpY3RFcnJvckgAQggKBnJlc3VsdCJ8Cg5GaWxlRGlmZl9DaHVuaxIPCgdjb250ZW50GAEgASgJEg0KBWxpbmVzGAIgAygJEhEKCW9sZF9zdGFydBgDIAEoBRIRCglvbGRfbGluZXMYBCABKAUSEQoJbmV3X3N0YXJ0GAUgASgFEhEKCW5ld19saW5lcxgGIAEoBSKQAgoIRmlsZURpZmYSDQoFYWRkZWQYBCABKAUSDwoHcmVtb3ZlZBgFIAEoBRIMCgRmcm9tGAEgASgJEgoKAnRvGAIgASgJEigKBmNodW5rcxgDIAMoCzIYLmFnZW50LnYxLkZpbGVEaWZmX0NodW5rEiEKFGJlZm9yZV9maWxlX2NvbnRlbnRzGAYgASgJSACIAQESIAoTYWZ0ZXJfZmlsZV9jb250ZW50cxgHIAEoCUgBiAEBEhkKDGlzX2dlbmVyYXRlZBgIIAEoCEgCiAEBQhcKFV9iZWZvcmVfZmlsZV9jb250ZW50c0IWChRfYWZ0ZXJfZmlsZV9jb250ZW50c0IPCg1faXNfZ2VuZXJhdGVkIlsKB0dpdERpZmYSIQoFZGlmZnMYASADKAsyEi5hZ2VudC52MS5GaWxlRGlmZhItCglkaWZmX3R5cGUYAiABKA4yGi5hZ2VudC52MS5HaXREaWZmX0RpZmZUeXBlImgKHUdldERpZmZSZXNwb25zZV9TdWJtb2R1bGVEaWZmEhUKDXJlbGF0aXZlX3BhdGgYASABKAkSHwoEZGlmZhgCIAEoCzIRLmFnZW50LnYxLkdpdERpZmYSDwoHZXJyb3JlZBgDIAEoCCLyAwoOR2V0RGlmZlJlcXVlc3QSCwoDY3dkGAEgASgJEgsKA3JlZhgCIAEoCRIQCghiYXNlX3JlZhgDIAEoCRISCgptZXJnZV9iYXNlGAQgASgIEhQKDHRhcmdldF9wYXRocxgFIAMoCRIiChV1bmlmaWVkX2NvbnRleHRfbGluZXMYBiABKAVIAIgBARIbChNtYXhfdW50cmFja2VkX2ZpbGVzGAcgASgFEh8KF3N1Ym1vZHVsZV9yZWN1cnNlX2RlcHRoGAkgASgFEh0KFWluY2x1ZGVfc3BhY2VfY2hhbmdlcxgKIAEoCBIWCg5jb21taXR0ZWRfb25seRgLIAEoCBIYChBjb21wdXRlX3BhdGNoX2lkGAwgASgIEhwKD3JldHVybl9oZWFkX3NoYRgNIAEoCEgBiAEBEh8KEm1heF9yZXNwb25zZV9ieXRlcxgOIAEoBUgCiAEBEkEKDW91dHB1dF9mb3JtYXQYCCABKA4yJS5hZ2VudC52MS5HZXREaWZmUmVxdWVzdF9PdXRwdXRGb3JtYXRIA4gBAUIYChZfdW5pZmllZF9jb250ZXh0X2xpbmVzQhIKEF9yZXR1cm5faGVhZF9zaGFCFQoTX21heF9yZXNwb25zZV9ieXRlc0IQCg5fb3V0cHV0X2Zvcm1hdCL+AQoPR2V0RGlmZlJlc3BvbnNlEh8KBGRpZmYYASABKAsyES5hZ2VudC52MS5HaXREaWZmEkAKD3N1Ym1vZHVsZV9kaWZmcxgCIAMoCzInLmFnZW50LnYxLkdldERpZmZSZXNwb25zZV9TdWJtb2R1bGVEaWZmEhUKCHBhdGNoX2lkGAMgASgJSACIAQESFQoIaGVhZF9zaGEYBCABKAlIAYgBARIkChdoYXNfdW5jb21taXR0ZWRfY2hhbmdlcxgFIAEoCEgCiAEBQgsKCV9wYXRjaF9pZEILCglfaGVhZF9zaGFCGgoYX2hhc191bmNvbW1pdHRlZF9jaGFuZ2VzImQKDlBpUmVhZFRvb2xDYWxsEiYKBGFyZ3MYASABKAsyGC5hZ2VudC52MS5QaVJlYWRFeGVjQXJncxIqCgZyZXN1bHQYAiABKAsyGi5hZ2VudC52MS5QaVJlYWRFeGVjUmVzdWx0ImQKDlBpQmFzaFRvb2xDYWxsEiYKBGFyZ3MYASABKAsyGC5hZ2VudC52MS5QaUJhc2hFeGVjQXJncxIqCgZyZXN1bHQYAiABKAsyGi5hZ2VudC52MS5QaUJhc2hFeGVjUmVzdWx0ImQKDlBpRWRpdFRvb2xDYWxsEiYKBGFyZ3MYASABKAsyGC5hZ2VudC52MS5QaUVkaXRFeGVjQXJncxIqCgZyZXN1bHQYAiABKAsyGi5hZ2VudC52MS5QaUVkaXRFeGVjUmVzdWx0ImcKD1BpV3JpdGVUb29sQ2FsbBInCgRhcmdzGAEgASgLMhkuYWdlbnQudjEuUGlXcml0ZUV4ZWNBcmdzEisKBnJlc3VsdBgCIAEoCzIbLmFnZW50LnYxLlBpV3JpdGVFeGVjUmVzdWx0ImQKDlBpR3JlcFRvb2xDYWxsEiYKBGFyZ3MYASABKAsyGC5hZ2VudC52MS5QaUdyZXBFeGVjQXJncxIqCgZyZXN1bHQYAiABKAsyGi5hZ2VudC52MS5QaUdyZXBFeGVjUmVzdWx0ImQKDlBpRmluZFRvb2xDYWxsEiYKBGFyZ3MYASABKAsyGC5hZ2VudC52MS5QaUZpbmRFeGVjQXJncxIqCgZyZXN1bHQYAiABKAsyGi5hZ2VudC52MS5QaUZpbmRFeGVjUmVzdWx0Il4KDFBpTHNUb29sQ2FsbBIkCgRhcmdzGAEgASgLMhYuYWdlbnQudjEuUGlMc0V4ZWNBcmdzEigKBnJlc3VsdBgCIAEoCzIYLmFnZW50LnYxLlBpTHNFeGVjUmVzdWx0IoEBChtTZWFyY2hDb252ZXJzYXRpb25zVG9vbENhbGwSLgoEYXJncxgBIAEoCzIgLmFnZW50LnYxLkNvbnZlcnNhdGlvblNlYXJjaEFyZ3MSMgoGcmVzdWx0GAIgASgLMiIuYWdlbnQudjEuQ29udmVyc2F0aW9uU2VhcmNoUmVzdWx0IjkKGkNvbm5lY3RTY21HaXRodWJSZXBvc2l0b3J5Eg0KBW93bmVyGAEgASgJEgwKBHJlcG8YAiABKAkifgoQQ29ubmVjdFNjbUdpdGh1YhI4CgpyZXBvc2l0b3J5GAEgASgLMiQuYWdlbnQudjEuQ29ubmVjdFNjbUdpdGh1YlJlcG9zaXRvcnkSHAoPZ2hlX2FwcGxpY2F0aW9uGAIgASgJSACIAQFCEgoQX2doZV9hcHBsaWNhdGlvbiJeCg5Db25uZWN0U2NtQXJncxIUCgx0b29sX2NhbGxfaWQYASABKAkSLAoGZ2l0aHViGAIgASgLMhouYWdlbnQudjEuQ29ubmVjdFNjbUdpdGh1YkgAQggKBnRhcmdldCITChFDb25uZWN0U2NtU3VjY2VzcyIgCg9Db25uZWN0U2NtRXJyb3ISDQoFZXJyb3IYASABKAkiJAoSQ29ubmVjdFNjbVJlamVjdGVkEg4KBnJlYXNvbhgBIAEoCSKqAQoQQ29ubmVjdFNjbVJlc3VsdBIuCgdzdWNjZXNzGAEgASgLMhsuYWdlbnQudjEuQ29ubmVjdFNjbVN1Y2Nlc3NIABIqCgVlcnJvchgCIAEoCzIZLmFnZW50LnYxLkNvbm5lY3RTY21FcnJvckgAEjAKCHJlamVjdGVkGAMgASgLMhwuYWdlbnQudjEuQ29ubmVjdFNjbVJlamVjdGVkSABCCAoGcmVzdWx0ImgKEkNvbm5lY3RTY21Ub29sQ2FsbBImCgRhcmdzGAEgASgLMhguYWdlbnQudjEuQ29ubmVjdFNjbUFyZ3MSKgoGcmVzdWx0GAIgASgLMhouYWdlbnQudjEuQ29ubmVjdFNjbVJlc3VsdCqIAQodQXBwbGllZEFnZW50Q2hhbmdlX0NoYW5nZVR5cGUSGwoXQ0hBTkdFX1RZUEVfVU5TUEVDSUZJRUQQABIXChNDSEFOR0VfVFlQRV9DUkVBVEVEEAESGAoUQ0hBTkdFX1RZUEVfTU9ESUZJRUQQAhIXChNDSEFOR0VfVFlQRV9ERUxFVEVEEAMqpAEKC01vdXNlQnV0dG9uEhwKGE1PVVNFX0JVVFRPTl9VTlNQRUNJRklFRBAAEhUKEU1PVVNFX0JVVFRPTl9MRUZUEAESFgoSTU9VU0VfQlVUVE9OX1JJR0hUEAISFwoTTU9VU0VfQlVUVE9OX01JRERMRRADEhUKEU1PVVNFX0JVVFRPTl9CQUNLEAQSGAoUTU9VU0VfQlVUVE9OX0ZPUldBUkQQBSqeAQoPU2Nyb2xsRGlyZWN0aW9uEiAKHFNDUk9MTF9ESVJFQ1RJT05fVU5TUEVDSUZJRUQQABIXChNTQ1JPTExfRElSRUNUSU9OX1VQEAESGQoVU0NST0xMX0RJUkVDVElPTl9ET1dOEAISGQoVU0NST0xMX0RJUkVDVElPTl9MRUZUEAMSGgoWU0NST0xMX0RJUkVDVElPTl9SSUdIVBAEKnAKEEN1cnNvclJ1bGVTb3VyY2USIgoeQ1VSU09SX1JVTEVfU09VUkNFX1VOU1BFQ0lGSUVEEAASGwoXQ1VSU09SX1JVTEVfU09VUkNFX1RFQU0QARIbChdDVVJTT1JfUlVMRV9TT1VSQ0VfVVNFUhACKrwBChJEaWFnbm9zdGljU2V2ZXJpdHkSIwofRElBR05PU1RJQ19TRVZFUklUWV9VTlNQRUNJRklFRBAAEh0KGURJQUdOT1NUSUNfU0VWRVJJVFlfRVJST1IQARIfChtESUFHTk9TVElDX1NFVkVSSVRZX1dBUk5JTkcQAhIjCh9ESUFHTk9TVElDX1NFVkVSSVRZX0lORk9STUFUSU9OEAMSHAoYRElBR05PU1RJQ19TRVZFUklUWV9ISU5UEAQqnAEKDVJlY29yZGluZ01vZGUSHgoaUkVDT1JESU5HX01PREVfVU5TUEVDSUZJRUQQABIiCh5SRUNPUkRJTkdfTU9ERV9TVEFSVF9SRUNPUkRJTkcQARIhCh1SRUNPUkRJTkdfTU9ERV9TQVZFX1JFQ09SRElORxACEiQKIFJFQ09SRElOR19NT0RFX0RJU0NBUkRfUkVDT1JESU5HEAMqkwEKH1JlcXVlc3RlZEZpbGVQYXRoUmVqZWN0ZWRSZWFzb24SMwovUkVRVUVTVEVEX0ZJTEVfUEFUSF9SRUpFQ1RFRF9SRUFTT05fVU5TUEVDSUZJRUQQABI7CjdSRVFVRVNURURfRklMRV9QQVRIX1JFSkVDVEVEX1JFQVNPTl9TTEFTSEVTX05PVF9BTExPV0VEEAEqrQEKC1BhY2thZ2VUeXBlEhwKGFBBQ0tBR0VfVFlQRV9VTlNQRUNJRklFRBAAEh8KG1BBQ0tBR0VfVFlQRV9DVVJTT1JfUFJPSkVDVBABEiAKHFBBQ0tBR0VfVFlQRV9DVVJTT1JfUEVSU09OQUwQAhIdChlQQUNLQUdFX1RZUEVfQ0xBVURFX1NLSUxMEAMSHgoaUEFDS0FHRV9UWVBFX0NMQVVERV9QTFVHSU4QBCp9ChJTYW5kYm94UG9saWN5X1R5cGUSFAoQVFlQRV9VTlNQRUNJRklFRBAAEhYKElRZUEVfSU5TRUNVUkVfTk9ORRABEhwKGFRZUEVfV09SS1NQQUNFX1JFQURXUklURRACEhsKF1RZUEVfV09SS1NQQUNFX1JFQURPTkxZEAMqcQoPVGltZW91dEJlaGF2aW9yEiAKHFRJTUVPVVRfQkVIQVZJT1JfVU5TUEVDSUZJRUQQABIbChdUSU1FT1VUX0JFSEFWSU9SX0NBTkNFTBABEh8KG1RJTUVPVVRfQkVIQVZJT1JfQkFDS0dST1VORBACKnkKEFNoZWxsQWJvcnRSZWFzb24SIgoeU0hFTExfQUJPUlRfUkVBU09OX1VOU1BFQ0lGSUVEEAASIQodU0hFTExfQUJPUlRfUkVBU09OX1VTRVJfQUJPUlQQARIeChpTSEVMTF9BQk9SVF9SRUFTT05fVElNRU9VVBACKqoBChxDdXN0b21TdWJhZ2VudFBlcm1pc3Npb25Nb2RlEi8KK0NVU1RPTV9TVUJBR0VOVF9QRVJNSVNTSU9OX01PREVfVU5TUEVDSUZJRUQQABIrCidDVVNUT01fU1VCQUdFTlRfUEVSTUlTU0lPTl9NT0RFX0RFRkFVTFQQARIsCihDVVNUT01fU1VCQUdFTlRfUEVSTUlTU0lPTl9NT0RFX1JFQURPTkxZEAIqlQEKClRvZG9TdGF0dXMSGwoXVE9ET19TVEFUVVNfVU5TUEVDSUZJRUQQABIXChNUT0RPX1NUQVRVU19QRU5ESU5HEAESGwoXVE9ET19TVEFUVVNfSU5fUFJPR1JFU1MQAhIZChVUT0RPX1NUQVRVU19DT01QTEVURUQQAxIZChVUT0RPX1NUQVRVU19DQU5DRUxMRUQQBCpmCghDbGllbnRPUxIZChVDTElFTlRfT1NfVU5TUEVDSUZJRUQQABIVChFDTElFTlRfT1NfV0lORE9XUxABEhMKD0NMSUVOVF9PU19NQUNPUxACEhMKD0NMSUVOVF9PU19MSU5VWBADKuwBChxBcnRpZmFjdFVwbG9hZERpc3BhdGNoU3RhdHVzEi8KK0FSVElGQUNUX1VQTE9BRF9ESVNQQVRDSF9TVEFUVVNfVU5TUEVDSUZJRUQQABIsCihBUlRJRkFDVF9VUExPQURfRElTUEFUQ0hfU1RBVFVTX0FDQ0VQVEVEEAESLAooQVJUSUZBQ1RfVVBMT0FEX0RJU1BBVENIX1NUQVRVU19SRUpFQ1RFRBACEj8KO0FSVElGQUNUX1VQTE9BRF9ESVNQQVRDSF9TVEFUVVNfU0tJUFBFRF9BTFJFQURZX0lOX1BST0dSRVNTEAMqVwoKRnJhbWVfS2luZBIUChBLSU5EX1VOU1BFQ0lGSUVEEAASEAoMS0lORF9SRVFVRVNUEAESEQoNS0lORF9SRVNQT05TRRACEg4KCktJTkRfRVJST1IQAyqwAgoXQnVnYm90RGVlcGxpbmtFdmVudEtpbmQSKgomQlVHQk9UX0RFRVBMSU5LX0VWRU5UX0tJTkRfVU5TUEVDSUZJRUQQABImCiJCVUdCT1RfREVFUExJTktfRVZFTlRfS0lORF9DTElDS0VEEAESMwovQlVHQk9UX0RFRVBMSU5LX0VWRU5UX0tJTkRfSEFORExFRF9ESUFMT0dfU0hPV04QAhIzCi9CVUdCT1RfREVFUExJTktfRVZFTlRfS0lORF9IQU5ETEVEX0NIQVRfQ1JFQVRFRBADEiQKIEJVR0JPVF9ERUVQTElOS19FVkVOVF9LSU5EX0VSUk9SEAQSMQotQlVHQk9UX0RFRVBMSU5LX0VWRU5UX0tJTkRfSEFORExFRF9GSVhfSU5fV0VCEAUqygEKLENvbW1hbmRDbGFzc2lmaWVyUmVzdWx0X1N1Z2dlc3RlZFNhbmRib3hNb2RlEiYKIlNVR0dFU1RFRF9TQU5EQk9YX01PREVfVU5TUEVDSUZJRUQQABIiCh5TVUdHRVNURURfU0FOREJPWF9NT0RFX1NBTkRCT1gQARIlCiFTVUdHRVNURURfU0FOREJPWF9NT0RFX05PX1NBTkRCT1gQAhInCiNTVUdHRVNURURfU0FOREJPWF9NT0RFX1VOREVURVJNSU5FRBADKpABCiFTaGVsbEhvb2tBcHByb3ZhbFJlcXVpcmVtZW50X0tpbmQSNAowU0hFTExfSE9PS19BUFBST1ZBTF9SRVFVSVJFTUVOVF9LSU5EX1VOU1BFQ0lGSUVEEAASNQoxU0hFTExfSE9PS19BUFBST1ZBTF9SRVFVSVJFTUVOVF9LSU5EX0ZPUkNFX1BST01QVBABKo8BChVTaGVsbEJhY2tncm91bmRSZWFzb24SJwojU0hFTExfQkFDS0dST1VORF9SRUFTT05fVU5TUEVDSUZJRUQQABIjCh9TSEVMTF9CQUNLR1JPVU5EX1JFQVNPTl9USU1FT1VUEAESKAokU0hFTExfQkFDS0dST1VORF9SRUFTT05fVVNFUl9SRVFVRVNUEAIqpAEKGkZvcmNlQmFja2dyb3VuZFNoZWxsU3RhdHVzEi0KKUZPUkNFX0JBQ0tHUk9VTkRfU0hFTExfU1RBVFVTX1VOU1BFQ0lGSUVEEAASKgomRk9SQ0VfQkFDS0dST1VORF9TSEVMTF9TVEFUVVNfQUNDRVBURUQQARIrCidGT1JDRV9CQUNLR1JPVU5EX1NIRUxMX1NUQVRVU19OT1RfRk9VTkQQAirSAQoYU3ViYWdlbnRCYWNrZ3JvdW5kUmVhc29uEioKJlNVQkFHRU5UX0JBQ0tHUk9VTkRfUkVBU09OX1VOU1BFQ0lGSUVEEAASLAooU1VCQUdFTlRfQkFDS0dST1VORF9SRUFTT05fQUdFTlRfUkVRVUVTVBABEisKJ1NVQkFHRU5UX0JBQ0tHUk9VTkRfUkVBU09OX1VTRVJfUkVRVUVTVBACEi8KK1NVQkFHRU5UX0JBQ0tHUk9VTkRfUkVBU09OX1FVRVVFRF9GT0xMT1dfVVAQAyqwAQodRm9yY2VCYWNrZ3JvdW5kU3ViYWdlbnRTdGF0dXMSMAosRk9SQ0VfQkFDS0dST1VORF9TVUJBR0VOVF9TVEFUVVNfVU5TUEVDSUZJRUQQABItCilGT1JDRV9CQUNLR1JPVU5EX1NVQkFHRU5UX1NUQVRVU19BQ0NFUFRFRBABEi4KKkZPUkNFX0JBQ0tHUk9VTkRfU1VCQUdFTlRfU1RBVFVTX05PVF9GT1VORBACKqEBChtTbWFydE1vZGVDbGFzc2lmaWVyRGVjaXNpb24SLgoqU01BUlRfTU9ERV9DTEFTU0lGSUVSX0RFQ0lTSU9OX1VOU1BFQ0lGSUVEEAASKAokU01BUlRfTU9ERV9DTEFTU0lGSUVSX0RFQ0lTSU9OX0FMTE9XEAESKAokU01BUlRfTU9ERV9DTEFTU0lGSUVSX0RFQ0lTSU9OX0JMT0NLEAIqmAEKGENvbnZlcnNhdGlvblNlYXJjaFNvdXJjZRIqCiZDT05WRVJTQVRJT05fU0VBUkNIX1NPVVJDRV9VTlNQRUNJRklFRBAAEiQKIENPTlZFUlNBVElPTl9TRUFSQ0hfU09VUkNFX0xPQ0FMEAESKgomQ09OVkVSU0FUSU9OX1NFQVJDSF9TT1VSQ0VfQ0xPVURfQ0FDSEUQAirTAQobR2V0RGlmZlJlcXVlc3RfT3V0cHV0Rm9ybWF0Eh0KGU9VVFBVVF9GT1JNQVRfVU5TUEVDSUZJRUQQABIdChlPVVRQVVRfRk9STUFUX05BTUVfU1RBVFVTEAESKQolT1VUUFVUX0ZPUk1BVF9OQU1FX1NUQVRVU19BTkRfTlVNU1RBVBACEhwKGE9VVFBVVF9GT1JNQVRfRklMRV9ESUZGUxADEi0KKU9VVFBVVF9GT1JNQVRfRElGRlNfV0lUSF9CRUZPUkVfQU5EX0FGVEVSEAQqcQoQR2l0RGlmZl9EaWZmVHlwZRIZChVESUZGX1RZUEVfVU5TUEVDSUZJRUQQABIaChZESUZGX1RZUEVfRElGRl9UT19IRUFEEAESJgoiRElGRl9UWVBFX0RJRkZfRlJPTV9CUkFOQ0hfVE9fTUFJThACMocECgxBZ2VudFNlcnZpY2USQQoDUnVuEhwuYWdlbnQudjEuQWdlbnRDbGllbnRNZXNzYWdlGhwuYWdlbnQudjEuQWdlbnRTZXJ2ZXJNZXNzYWdlEj8KBlJ1blNTRRIXLmFnZW50LnYxLkJpZGlSZXF1ZXN0SWQaHC5hZ2VudC52MS5BZ2VudFNlcnZlck1lc3NhZ2USRAoJTmFtZUFnZW50EhouYWdlbnQudjEuTmFtZUFnZW50UmVxdWVzdBobLmFnZW50LnYxLk5hbWVBZ2VudFJlc3BvbnNlElYKD0dldFVzYWJsZU1vZGVscxIgLmFnZW50LnYxLkdldFVzYWJsZU1vZGVsc1JlcXVlc3QaIS5hZ2VudC52MS5HZXRVc2FibGVNb2RlbHNSZXNwb25zZRJoChVHZXREZWZhdWx0TW9kZWxGb3JDbGkSJi5hZ2VudC52MS5HZXREZWZhdWx0TW9kZWxGb3JDbGlSZXF1ZXN0GicuYWdlbnQudjEuR2V0RGVmYXVsdE1vZGVsRm9yQ2xpUmVzcG9uc2USawoWR2V0QWxsb3dlZE1vZGVsSW50ZW50cxInLmFnZW50LnYxLkdldEFsbG93ZWRNb2RlbEludGVudHNSZXF1ZXN0GiguYWdlbnQudjEuR2V0QWxsb3dlZE1vZGVsSW50ZW50c1Jlc3BvbnNlMrUICg5Db250cm9sU2VydmljZRJNCgxSZWFkVGV4dEZpbGUSHS5hZ2VudC52MS5SZWFkVGV4dEZpbGVSZXF1ZXN0Gh4uYWdlbnQudjEuUmVhZFRleHRGaWxlUmVzcG9uc2USUAoNV3JpdGVUZXh0RmlsZRIeLmFnZW50LnYxLldyaXRlVGV4dEZpbGVSZXF1ZXN0Gh8uYWdlbnQudjEuV3JpdGVUZXh0RmlsZVJlc3BvbnNlElMKDlJlYWRCaW5hcnlGaWxlEh8uYWdlbnQudjEuUmVhZEJpbmFyeUZpbGVSZXF1ZXN0GiAuYWdlbnQudjEuUmVhZEJpbmFyeUZpbGVSZXNwb25zZRJWCg9Xcml0ZUJpbmFyeUZpbGUSIC5hZ2VudC52MS5Xcml0ZUJpbmFyeUZpbGVSZXF1ZXN0GiEuYWdlbnQudjEuV3JpdGVCaW5hcnlGaWxlUmVzcG9uc2USbgoXR2V0V29ya3NwYWNlQ2hhbmdlc0hhc2gSKC5hZ2VudC52MS5HZXRXb3Jrc3BhY2VDaGFuZ2VzSGFzaFJlcXVlc3QaKS5hZ2VudC52MS5HZXRXb3Jrc3BhY2VDaGFuZ2VzSGFzaFJlc3BvbnNlEnEKGFJlZnJlc2hHaXRodWJBY2Nlc3NUb2tlbhIpLmFnZW50LnYxLlJlZnJlc2hHaXRodWJBY2Nlc3NUb2tlblJlcXVlc3QaKi5hZ2VudC52MS5SZWZyZXNoR2l0aHViQWNjZXNzVG9rZW5SZXNwb25zZRJrChZXYXJtUmVtb3RlQWNjZXNzU2VydmVyEicuYWdlbnQudjEuV2FybVJlbW90ZUFjY2Vzc1NlcnZlclJlcXVlc3QaKC5hZ2VudC52MS5XYXJtUmVtb3RlQWNjZXNzU2VydmVyUmVzcG9uc2USUAoNTGlzdEFydGlmYWN0cxIeLmFnZW50LnYxLkxpc3RBcnRpZmFjdHNSZXF1ZXN0Gh8uYWdlbnQudjEuTGlzdEFydGlmYWN0c1Jlc3BvbnNlElYKD1VwbG9hZEFydGlmYWN0cxIgLmFnZW50LnYxLlVwbG9hZEFydGlmYWN0c1JlcXVlc3QaIS5hZ2VudC52MS5VcGxvYWRBcnRpZmFjdHNSZXNwb25zZRJiChNHZXRNY3BSZWZyZXNoVG9rZW5zEiQuYWdlbnQudjEuR2V0TWNwUmVmcmVzaFRva2Vuc1JlcXVlc3QaJS5hZ2VudC52MS5HZXRNY3BSZWZyZXNoVG9rZW5zUmVzcG9uc2USdwoaVXBkYXRlRW52aXJvbm1lbnRWYXJpYWJsZXMSKy5hZ2VudC52MS5VcGRhdGVFbnZpcm9ubWVudFZhcmlhYmxlc1JlcXVlc3QaLC5hZ2VudC52MS5VcGRhdGVFbnZpcm9ubWVudFZhcmlhYmxlc1Jlc3BvbnNlMg0KC0V4ZWNTZXJ2aWNlMlEKIlByaXZhdGVXb3JrZXJCcmlkZ2VFeHRlcm5hbFNlcnZpY2USKwoHQ29ubmVjdBIPLmFnZW50LnYxLkZyYW1lGg8uYWdlbnQudjEuRnJhbWUyeAoQTGlmZWN5Y2xlU2VydmljZRIxCg1SZXNldEluc3RhbmNlEg8uYWdlbnQudjEuRW1wdHkaDy5hZ2VudC52MS5FbXB0eRIxCg1SZW5ld0luc3RhbmNlEg8uYWdlbnQudjEuRW1wdHkaDy5hZ2VudC52MS5FbXB0eWIGcHJvdG8z");
var McpToolResultSchema = /* @__PURE__ */ messageDesc(file_agent, 13);
var McpToolCallSchema = /* @__PURE__ */ messageDesc(file_agent, 14);
var ToolCallSchema = /* @__PURE__ */ messageDesc(file_agent, 46);
var ConversationStepSchema = /* @__PURE__ */ messageDesc(file_agent, 53);
var ConversationActionSchema = /* @__PURE__ */ messageDesc(file_agent, 54);
var UserMessageActionSchema = /* @__PURE__ */ messageDesc(file_agent, 55);
var ResumeActionSchema = /* @__PURE__ */ messageDesc(file_agent, 57);
var UserMessageSchema = /* @__PURE__ */ messageDesc(file_agent, 63);
var AssistantMessageSchema = /* @__PURE__ */ messageDesc(file_agent, 64);
var ThinkingMessageSchema = /* @__PURE__ */ messageDesc(file_agent, 65);
var ConversationTurnStructureSchema = /* @__PURE__ */ messageDesc(file_agent, 70);
var AgentConversationTurnStructureSchema = /* @__PURE__ */ messageDesc(file_agent, 72);
var ConversationStateStructureSchema = /* @__PURE__ */ messageDesc(file_agent, 83);
var ThinkingDetailsSchema = /* @__PURE__ */ messageDesc(file_agent, 84);
var ModelDetailsSchema = /* @__PURE__ */ messageDesc(file_agent, 88);
var RequestedModelSchema = /* @__PURE__ */ messageDesc(file_agent, 89);
var RequestedModel_ModelParameterbytesSchema = /* @__PURE__ */ messageDesc(file_agent, 90);
var AgentRunRequestSchema = /* @__PURE__ */ messageDesc(file_agent, 91);
var InteractionResponseSchema = /* @__PURE__ */ messageDesc(file_agent, 111);
var ClientHeartbeatSchema = /* @__PURE__ */ messageDesc(file_agent, 114);
var AgentClientMessageSchema = /* @__PURE__ */ messageDesc(file_agent, 118);
var GetUsableModelsRequestSchema = /* @__PURE__ */ messageDesc(file_agent, 122);
var GetUsableModelsResponseSchema = /* @__PURE__ */ messageDesc(file_agent, 123);
var CursorRuleTypeGlobalSchema = /* @__PURE__ */ messageDesc(file_agent, 180);
var CursorRuleTypeSchema = /* @__PURE__ */ messageDesc(file_agent, 184);
var CursorRuleSchema = /* @__PURE__ */ messageDesc(file_agent, 185);
var ExecClientStreamCloseSchema = /* @__PURE__ */ messageDesc(file_agent, 236);
var ExecClientHeartbeatSchema = /* @__PURE__ */ messageDesc(file_agent, 238);
var ExecClientControlMessageSchema = /* @__PURE__ */ messageDesc(file_agent, 239);
var ExecServerMessageSchema = /* @__PURE__ */ messageDesc(file_agent, 243);
var ExecClientMessageSchema = /* @__PURE__ */ messageDesc(file_agent, 244);
var GetBlobResultSchema = /* @__PURE__ */ messageDesc(file_agent, 268);
var SetBlobResultSchema = /* @__PURE__ */ messageDesc(file_agent, 270);
var KvClientMessageSchema = /* @__PURE__ */ messageDesc(file_agent, 272);
var McpArgsSchema = /* @__PURE__ */ messageDesc(file_agent, 284);
var McpResultSchema = /* @__PURE__ */ messageDesc(file_agent, 285);
var McpToolNotFoundSchema = /* @__PURE__ */ messageDesc(file_agent, 286);
var McpTextContentSchema = /* @__PURE__ */ messageDesc(file_agent, 287);
var McpImageContentSchema = /* @__PURE__ */ messageDesc(file_agent, 288);
var McpToolResultContentItemSchema = /* @__PURE__ */ messageDesc(file_agent, 289);
var McpSuccessSchema = /* @__PURE__ */ messageDesc(file_agent, 290);
var McpRejectedSchema = /* @__PURE__ */ messageDesc(file_agent, 292);
var McpToolDefinitionSchema = /* @__PURE__ */ messageDesc(file_agent, 306);
var McpDescriptorSchema = /* @__PURE__ */ messageDesc(file_agent, 309);
var McpToolDescriptorSchema = /* @__PURE__ */ messageDesc(file_agent, 310);
var RequestContextResultSchema = /* @__PURE__ */ messageDesc(file_agent, 336);
var RequestContextSuccessSchema = /* @__PURE__ */ messageDesc(file_agent, 337);
var RequestContextEnvSchema = /* @__PURE__ */ messageDesc(file_agent, 343);
var SelectedImageSchema = /* @__PURE__ */ messageDesc(file_agent, 349);
var SelectedImage_BlobIdWithDataSchema = /* @__PURE__ */ messageDesc(file_agent, 350);
var SelectedImage_DimensionSchema = /* @__PURE__ */ messageDesc(file_agent, 351);
var SelectedContextSchema = /* @__PURE__ */ messageDesc(file_agent, 373);
var McpApprovedSchema = /* @__PURE__ */ messageDesc(file_agent, 525);
var McpStateServerSchema = /* @__PURE__ */ messageDesc(file_agent, 527);
var McpStateSuccessSchema = /* @__PURE__ */ messageDesc(file_agent, 528);
var McpStateExecResultSchema = /* @__PURE__ */ messageDesc(file_agent, 531);
var ShellAllowlistPrecheckResultSchema = /* @__PURE__ */ messageDesc(file_agent, 536);
var McpAllowlistPrecheckResultSchema = /* @__PURE__ */ messageDesc(file_agent, 538);
var WebFetchAllowlistPrecheckResultSchema = /* @__PURE__ */ messageDesc(file_agent, 540);
var ForceBackgroundShellResultSchema = /* @__PURE__ */ messageDesc(file_agent, 545);
var ForceBackgroundSubagentResultSchema = /* @__PURE__ */ messageDesc(file_agent, 559);

// src/protocol/content.ts
function parseArguments(raw) {
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new CursorProtocolError("INVALID_REQUEST", "Cursor tool arguments must be a JSON object");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new CursorProtocolError("INVALID_REQUEST", "Cursor tool arguments must be a JSON object");
  return parsed;
}
function encodeArguments(raw) {
  return Object.fromEntries(Object.entries(parseArguments(raw)).map(([name, value]) => [name, toBinary(ValueSchema, fromJson(ValueSchema, value))]));
}
function decodeArguments(args) {
  try {
    return JSON.stringify(Object.fromEntries(Object.entries(args).map(([name, value]) => [name, toJson(ValueSchema, fromBinary(ValueSchema, value))])));
  } catch {
    throw new CursorProtocolError("INVALID_RESPONSE", "Invalid Cursor MCP argument encoding");
  }
}
function textOf(content) {
  return content.filter((item) => item.type === "text").map((item) => item.text).join("\n");
}
function mcpContent(result) {
  return create(McpSuccessSchema, {
    isError: result.isError === true,
    content: result.content.map((item) => {
      if (item.type === "image") return create(McpToolResultContentItemSchema, {
        content: { case: "image", value: create(McpImageContentSchema, { data: item.data, mimeType: item.mediaType }) }
      });
      if (item.type === "text") return create(McpToolResultContentItemSchema, {
        content: { case: "text", value: create(McpTextContentSchema, { text: item.text }) }
      });
      throw new CursorProtocolError("INVALID_REQUEST", "Cursor tool results support text and image content only");
    })
  });
}

// src/protocol/control.ts
import { create as create4, fromJson as fromJson2, toBinary as toBinary3 } from "@bufbuild/protobuf";
import { ValueSchema as ValueSchema2 } from "@bufbuild/protobuf/wkt";

// src/protocol/history.ts
import { createHash as createHash3 } from "node:crypto";
import { clone, create as create2, toBinary as toBinary2 } from "@bufbuild/protobuf";
var MCP_PROVIDER = "dsh";
function uuid(seed) {
  const bytes = createHash3("sha256").update(seed).digest().subarray(0, 16);
  bytes[6] = bytes[6] & 15 | 64;
  bytes[8] = bytes[8] & 63 | 128;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
function wireCallId(id) {
  if (!id) throw new CursorProtocolError("INVALID_REQUEST", "Cursor history tool-call IDs must not be empty");
  return /^[a-zA-Z0-9_-]+$/u.test(id) ? id : `dsh_${createHash3("sha256").update(id).digest("base64url")}`;
}
function asStructuredSelectedImages(content, blobs) {
  return content.flatMap((item) => {
    if (item.type !== "image") return [];
    const blobId = blobs.put(item.data);
    return [create2(SelectedImageSchema, {
      uuid: uuid(`image:${Buffer.from(blobId).toString("hex")}`),
      mimeType: item.mediaType,
      dimension: create2(SelectedImage_DimensionSchema, { width: item.width, height: item.height }),
      dataOrBlobId: { case: "blobIdWithData", value: create2(SelectedImage_BlobIdWithDataSchema, { blobId, data: item.data }) }
    })];
  });
}
function userMessage(messages, blobs, seed) {
  const content = messages.flatMap((message2) => message2.content);
  const selectedImages = asStructuredSelectedImages(content, blobs);
  return create2(UserMessageSchema, {
    text: textOf(content),
    messageId: uuid(seed),
    ...selectedImages.length ? { selectedContext: create2(SelectedContextSchema, { selectedImages }) } : {}
  });
}
function rootContent(content) {
  return content.map((item) => {
    switch (item.type) {
      case "text":
        return { type: "text", text: item.text };
      case "reasoning":
        return { type: "reasoning", text: item.text };
      case "image":
        return { type: "image", image: `data:${item.mediaType};base64,${Buffer.from(item.data).toString("base64")}`, mediaType: item.mediaType };
      case "tool-call":
        return { type: "tool-call", toolCallId: wireCallId(item.id), toolName: item.name, args: parseArguments(item.arguments) };
    }
  });
}
function rootResult(message2) {
  return message2.content.map((item) => item.type === "image" ? `[${item.mediaType} image, attached below]` : item.type === "text" ? item.text : "").filter(Boolean).join("\n");
}
function activeIndex(messages) {
  if (messages.at(-1)?.role !== "user") return -1;
  let index = messages.length - 1;
  while (index > 0 && messages[index - 1]?.role === "user") index--;
  return index;
}
function sameHead(previous, head) {
  return previous.length >= head.length && head.every((id, index) => Buffer.from(previous[index]).equals(id));
}
function systemPrompts(messages) {
  return messages.filter((message2) => message2.role === "system" || message2.role === "developer").map((message2) => textOf(message2.content).trim()).filter(Boolean);
}
function buildRunRequest(request, blobs, cached) {
  const active = activeIndex(request.messages);
  const historyEnd = active < 0 ? request.messages.length : active;
  const prompts = systemPrompts(request.messages);
  const systemIds = (prompts.length ? prompts : ["You are a helpful assistant."]).map((content) => blobs.put(new TextEncoder().encode(JSON.stringify({ role: "system", content }))));
  const roots = [];
  const pushRoot = (value) => {
    roots.push(blobs.put(new TextEncoder().encode(JSON.stringify(value))));
  };
  const results = /* @__PURE__ */ new Map();
  const calls = /* @__PURE__ */ new Set();
  for (let index = 0; index < historyEnd; index++) {
    const message2 = request.messages[index];
    if (message2.role === "tool") {
      if (!message2.toolCallId) throw new CursorProtocolError("INVALID_REQUEST", "Cursor history tool results require toolCallId");
      results.set(message2.toolCallId, message2);
    }
    if (message2.role === "assistant") for (const item of message2.content) {
      if (item.type !== "tool-call") continue;
      if (calls.has(item.id)) throw new CursorProtocolError("INVALID_REQUEST", "Duplicate Cursor history tool-call ID");
      calls.add(item.id);
    }
  }
  let toolImages = [];
  const flushToolImages = () => {
    if (!toolImages.length) return;
    pushRoot({ role: "user", content: rootContent([{ type: "text", text: "Images returned by the preceding tool results:" }, ...toolImages]) });
    toolImages = [];
  };
  for (let index = 0; index < historyEnd; index++) {
    const message2 = request.messages[index];
    if (message2.role === "system" || message2.role === "developer") continue;
    if (message2.role !== "tool") flushToolImages();
    if (message2.role === "tool") {
      if (!calls.has(message2.toolCallId)) throw new CursorProtocolError("INVALID_REQUEST", "Cursor history contains an unpaired tool result");
      pushRoot({ role: "tool", id: wireCallId(message2.toolCallId), content: [{
        type: "tool-result",
        toolCallId: wireCallId(message2.toolCallId),
        toolName: message2.toolName ?? "",
        result: rootResult(message2),
        ...message2.isError ? { isError: true } : {}
      }] });
      toolImages.push(...message2.content.filter((item) => item.type === "image"));
    } else {
      pushRoot({ role: message2.role, content: rootContent(message2.content) });
    }
  }
  flushToolImages();
  const head = [...systemIds];
  if (roots.length && prompts.length) {
    head.push(blobs.put(new TextEncoder().encode(JSON.stringify({ role: "user", content: `<rules>
${prompts.join("\n\n")}
</rules>` }))));
  }
  roots.unshift(...head);
  const turns = [];
  let current;
  const flushTurn = () => {
    if (!current) return;
    turns.push(blobs.put(toBinary2(ConversationTurnStructureSchema, create2(ConversationTurnStructureSchema, {
      turn: { case: "agentConversationTurn", value: create2(AgentConversationTurnStructureSchema, { userMessage: current.user, steps: current.steps }) }
    }))));
    current = void 0;
  };
  const pushStep = (step) => {
    current?.steps.push(blobs.put(toBinary2(ConversationStepSchema, step)));
  };
  for (let index = 0; index < historyEnd; index++) {
    const message2 = request.messages[index];
    if (message2.role === "user") {
      flushTurn();
      current = { user: blobs.put(toBinary2(UserMessageSchema, userMessage([message2], blobs, `${request.conversationId}:user:${index}`))), steps: [] };
    } else if (message2.role === "assistant") {
      for (const item of message2.content) {
        if (item.type === "text") pushStep(create2(ConversationStepSchema, { message: { case: "assistantMessage", value: create2(AssistantMessageSchema, { text: item.text }) } }));
        if (item.type === "reasoning") pushStep(create2(ConversationStepSchema, { message: { case: "thinkingMessage", value: create2(ThinkingMessageSchema, { text: item.text }) } }));
        if (item.type === "tool-call") {
          const result = results.get(item.id);
          pushStep(create2(ConversationStepSchema, { message: { case: "toolCall", value: create2(ToolCallSchema, {
            toolCallId: wireCallId(item.id),
            tool: { case: "mcpToolCall", value: create2(McpToolCallSchema, {
              args: create2(McpArgsSchema, { name: item.name, toolName: item.name, providerIdentifier: MCP_PROVIDER, toolCallId: wireCallId(item.id), args: encodeArguments(item.arguments) }),
              ...result ? { result: create2(McpToolResultSchema, { result: { case: "success", value: mcpContent(result) } }) } : {}
            }) }
          }) } }));
        }
      }
    }
  }
  flushTurn();
  const action = active < 0 ? create2(ConversationActionSchema, { action: { case: "resumeAction", value: create2(ResumeActionSchema) } }) : create2(ConversationActionSchema, { action: { case: "userMessageAction", value: create2(UserMessageActionSchema, {
    userMessage: userMessage(request.messages.slice(active), blobs, `${request.conversationId}:active:${active}`)
  }) } });
  const conversationState = cached && sameHead(cached.rootPromptMessagesJson, systemIds) ? clone(ConversationStateStructureSchema, cached) : create2(ConversationStateStructureSchema);
  conversationState.rootPromptMessagesJson = roots;
  conversationState.turns = turns;
  conversationState.pendingToolCalls = [];
  return create2(AgentRunRequestSchema, {
    conversationId: request.conversationId,
    conversationState,
    action,
    ...modelFields(request)
  });
}
function modelFields(request) {
  return {
    modelDetails: create2(ModelDetailsSchema, { modelId: request.selection.wireModelId, maxMode: request.selection.maxMode }),
    requestedModel: create2(RequestedModelSchema, {
      modelId: request.selection.requestedModelId ?? request.selection.wireModelId,
      maxMode: request.selection.maxMode === true,
      parameters: (request.selection.parameters ?? []).map((parameter) => create2(RequestedModel_ModelParameterbytesSchema, { id: parameter.key, value: parameter.value }))
    })
  };
}
function messagesDigest(messages) {
  const hash = createHash3("sha256");
  for (const message2 of messages) {
    hash.update(JSON.stringify([message2.role, message2.toolCallId ?? null, message2.isError === true]));
    for (const item of message2.content) {
      hash.update(item.type === "image" ? `image:${createHash3("sha256").update(item.data).digest("hex")}` : JSON.stringify(item));
    }
    hash.update("\0");
  }
  return hash.digest("hex");
}
function activeUserIndex(messages) {
  return activeIndex(messages);
}
function buildResumeRequest(request, blobs, checkpoint, active) {
  return create2(AgentRunRequestSchema, {
    conversationId: request.conversationId,
    conversationState: checkpoint,
    action: active < 0 ? create2(ConversationActionSchema, { action: { case: "resumeAction", value: create2(ResumeActionSchema) } }) : create2(ConversationActionSchema, { action: { case: "userMessageAction", value: create2(UserMessageActionSchema, {
      userMessage: userMessage(request.messages.slice(active), blobs, `${request.conversationId}:active:${request.messages.length}:${active}`)
    }) } }),
    ...modelFields(request)
  });
}

// src/protocol/schema.ts
import { clone as clone2, create as create3, createFileRegistry } from "@bufbuild/protobuf";
import {
  DescriptorProtoSchema,
  FieldDescriptorProtoSchema,
  FieldDescriptorProto_Label,
  FieldDescriptorProto_Type,
  OneofDescriptorProtoSchema,
  FileDescriptorProtoSchema
} from "@bufbuild/protobuf/wkt";
var descriptor = clone2(FileDescriptorProtoSchema, file_agent.proto);
var ended = descriptor.messageType.find((message2) => message2.name === "TurnEndedUpdate");
for (const [index, name] of ["input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "reasoning_tokens"].entries()) {
  ended.oneofDecl.push(create3(OneofDescriptorProtoSchema, { name: `_${name}` }));
  ended.field.push(create3(FieldDescriptorProtoSchema, {
    name,
    jsonName: name.replace(/_([a-z])/gu, (_, letter) => letter.toUpperCase()),
    number: index + 1,
    label: FieldDescriptorProto_Label.OPTIONAL,
    type: FieldDescriptorProto_Type.INT64,
    proto3Optional: true,
    oneofIndex: index
  }));
}
descriptor.messageType.push(create3(DescriptorProtoSchema, {
  name: "ToolRequestsListedUpdate",
  field: [create3(FieldDescriptorProtoSchema, {
    name: "call_count",
    jsonName: "callCount",
    number: 1,
    label: FieldDescriptorProto_Label.OPTIONAL,
    type: FieldDescriptorProto_Type.UINT32
  })]
}));
descriptor.messageType.find((message2) => message2.name === "InteractionUpdate").field.push(create3(FieldDescriptorProtoSchema, {
  name: "tool_requests_listed",
  jsonName: "toolRequestsListed",
  number: 27,
  label: FieldDescriptorProto_Label.OPTIONAL,
  type: FieldDescriptorProto_Type.MESSAGE,
  typeName: ".agent.v1.ToolRequestsListedUpdate",
  oneofIndex: 0
}));
var field = (name, jsonName, number, type, typeName, oneofIndex) => create3(FieldDescriptorProtoSchema, { name, jsonName, number, label: FieldDescriptorProto_Label.OPTIONAL, type, ...typeName ? { typeName } : {}, ...oneofIndex === void 0 ? {} : { oneofIndex } });
var { STRING, MESSAGE, INT32, INT64 } = FieldDescriptorProto_Type;
descriptor.messageType.push(
  create3(DescriptorProtoSchema, { name: "SystemContextInjection", field: [field("producer", "producer", 1, STRING), field("content", "content", 2, STRING)] }),
  create3(DescriptorProtoSchema, { name: "UserContextInjection", field: [
    field("user_message", "userMessage", 1, MESSAGE, ".agent.v1.UserMessage"),
    field("request_context", "requestContext", 2, MESSAGE, ".agent.v1.RequestContext")
  ] }),
  create3(DescriptorProtoSchema, { name: "InjectContextAction", field: [
    field("injection_id", "injectionId", 1, STRING),
    field("expected_run_id", "expectedRunId", 2, STRING),
    field("user_context", "userContext", 3, MESSAGE, ".agent.v1.UserContextInjection"),
    field("system_context", "systemContext", 4, MESSAGE, ".agent.v1.SystemContextInjection")
  ] }),
  create3(DescriptorProtoSchema, { name: "ContextInjectionDelivered", field: [field("step", "step", 1, INT32), field("delivery_batch_id", "deliveryBatchId", 2, STRING), field("delivered_at_ms", "deliveredAtMs", 3, INT64)] }),
  create3(DescriptorProtoSchema, { name: "ContextInjectionRejected", field: [field("reason", "reason", 1, STRING)] }),
  ...["ContextInjectionQueued", "ContextInjectionQueuedForNextTurn", "ContextInjectionCancelled"].map((name) => create3(DescriptorProtoSchema, { name })),
  create3(DescriptorProtoSchema, { name: "ContextInjectionState", oneofDecl: [create3(OneofDescriptorProtoSchema, { name: "state" })], field: [
    field("queued", "queued", 1, MESSAGE, ".agent.v1.ContextInjectionQueued", 0),
    field("delivered", "delivered", 2, MESSAGE, ".agent.v1.ContextInjectionDelivered", 0),
    field("queued_for_next_turn", "queuedForNextTurn", 3, MESSAGE, ".agent.v1.ContextInjectionQueuedForNextTurn", 0),
    field("cancelled", "cancelled", 4, MESSAGE, ".agent.v1.ContextInjectionCancelled", 0),
    field("rejected", "rejected", 5, MESSAGE, ".agent.v1.ContextInjectionRejected", 0)
  ] }),
  create3(DescriptorProtoSchema, { name: "ContextInjectionStateUpdate", field: [
    field("injection_id", "injectionId", 1, STRING),
    field("state", "state", 2, MESSAGE, ".agent.v1.ContextInjectionState")
  ] })
);
var message = (name) => descriptor.messageType.find((item) => item.name === name);
message("ConversationAction").field.push(field("inject_context_action", "injectContextAction", 19, MESSAGE, ".agent.v1.InjectContextAction", 0));
message("InteractionUpdate").field.push(field("context_injection_state", "contextInjectionState", 23, MESSAGE, ".agent.v1.ContextInjectionStateUpdate", 0));
message("AgentRunRequest").field.push(field("run_id", "runId", 25, STRING));
descriptor.messageType.push(create3(DescriptorProtoSchema, {
  name: "McpMetaToolOptions",
  field: [
    field("enabled", "enabled", 1, FieldDescriptorProto_Type.BOOL),
    create3(FieldDescriptorProtoSchema, {
      name: "mcp_descriptors",
      jsonName: "mcpDescriptors",
      number: 2,
      label: FieldDescriptorProto_Label.REPEATED,
      type: MESSAGE,
      typeName: ".agent.v1.McpDescriptor"
    })
  ]
}));
message("RequestContext").field.push(
  field("mcp_meta_tool_options", "mcpMetaToolOptions", 34, MESSAGE, ".agent.v1.McpMetaToolOptions"),
  field("mcp_info_complete", "mcpInfoComplete", 36, FieldDescriptorProto_Type.BOOL)
);
var registry = createFileRegistry(descriptor, () => void 0);
var WireRequestContextSchema = registry.getMessage("agent.v1.RequestContext");
var WireMcpMetaToolOptionsSchema = registry.getMessage("agent.v1.McpMetaToolOptions");
var WireAgentServerMessageSchema = registry.getMessage("agent.v1.AgentServerMessage");
var WireInteractionUpdateSchema = registry.getMessage("agent.v1.InteractionUpdate");
var WireTurnEndedSchema = registry.getMessage("agent.v1.TurnEndedUpdate");
var WireAgentClientMessageSchema = registry.getMessage("agent.v1.AgentClientMessage");

// src/protocol/control.ts
var DENIALS = {
  shellArgs: "shellResult",
  shellStreamArgs: "shellStream",
  miniSweAgentBashArgs: "miniSweAgentBashResult",
  readArgs: "readResult",
  redactedReadArgs: "redactedReadResult",
  writeArgs: "writeResult",
  deleteArgs: "deleteResult",
  lsArgs: "lsResult",
  grepArgs: "grepResult",
  diagnosticsArgs: "diagnosticsResult",
  backgroundShellSpawnArgs: "backgroundShellSpawnResult",
  writeShellStdinArgs: "writeShellStdinResult",
  fetchArgs: "fetchResult",
  computerUseArgs: "computerUseResult",
  recordScreenArgs: "recordScreenResult",
  subagentArgs: "subagentResult",
  subagentAwaitArgs: "subagentAwaitResult",
  piBashArgs: "piBashResult",
  piReadArgs: "piReadResult",
  piWriteArgs: "piWriteResult",
  piEditArgs: "piEditResult",
  piLsArgs: "piLsResult",
  piFindArgs: "piFindResult",
  piGrepArgs: "piGrepResult",
  canvasDiagnosticsArgs: "canvasDiagnosticsResult",
  conversationSearchArgs: "conversationSearchResult",
  agentStoreConflictArgs: "agentStoreConflictResult",
  smartModeClassifierArgs: "smartModeClassifierResult",
  listMcpResourcesExecArgs: "listMcpResourcesExecResult",
  readMcpResourceExecArgs: "readMcpResourceExecResult"
};
var REASON = "Native execution is not supported by this client. Tools are owned by DeepSeek Harness.";
function execResponse(exec, message2) {
  return create4(AgentClientMessageSchema, {
    message: { case: "execClientMessage", value: create4(ExecClientMessageSchema, { id: exec.id, execId: exec.execId, message: message2 }) }
  });
}
function toolDefinitions(tools) {
  if (tools.length > LIMITS.toolCount) throw limitError();
  const seen = /* @__PURE__ */ new Set();
  let bytes = 0;
  return tools.map((tool) => {
    if (!tool.name || seen.has(tool.name)) throw new CursorProtocolError("INVALID_REQUEST", "Cursor tool names must be unique and nonempty");
    seen.add(tool.name);
    const definition = create4(McpToolDefinitionSchema, {
      name: tool.name,
      toolName: tool.name,
      description: tool.description,
      providerIdentifier: MCP_PROVIDER,
      inputSchema: toBinary3(ValueSchema2, fromJson2(ValueSchema2, tool.parameters))
    });
    bytes += toBinary3(McpToolDefinitionSchema, definition).byteLength;
    if (bytes > LIMITS.toolBytes) throw limitError();
    return definition;
  });
}
function advertisedName(args, names) {
  if (args.providerIdentifier && args.providerIdentifier !== MCP_PROVIDER) return void 0;
  if (args.serverIdentifier && args.serverIdentifier !== MCP_PROVIDER) return void 0;
  const name = args.toolName || args.name;
  if (!names.has(name)) return void 0;
  return name;
}
function contextResponse(exec, definitions, cwd, instructions) {
  return execResponse(exec, { case: "requestContextResult", value: create4(RequestContextResultSchema, {
    result: { case: "success", value: create4(RequestContextSuccessSchema, {
      requestContext: create4(WireRequestContextSchema, {
        mcpInfoComplete: true,
        mcpMetaToolOptions: create4(WireMcpMetaToolOptionsSchema, {
          enabled: true,
          mcpDescriptors: definitions.length ? [create4(McpDescriptorSchema, {
            serverName: MCP_PROVIDER,
            serverIdentifier: MCP_PROVIDER,
            tools: definitions.map((tool) => create4(McpToolDescriptorSchema, { toolName: tool.toolName }))
          })] : []
        }),
        // Cursor's standard instruction carrier. Content is the exact host text,
        // not a provider-authored persona, tool manifest or filesystem rule.
        rules: instructions ? [create4(CursorRuleSchema, {
          fullPath: "DSH system instructions",
          content: instructions,
          type: create4(CursorRuleTypeSchema, { type: { case: "global", value: create4(CursorRuleTypeGlobalSchema) } })
        })] : [],
        ...cwd !== void 0 ? { env: create4(RequestContextEnvSchema, { workspacePaths: [cwd] }) } : {}
      })
    }) }
  }) });
}
function mcpStateResponse(exec, definitions) {
  if (exec.message.case !== "mcpStateExecArgs") throw new Error("Expected MCP state request");
  const requested = exec.message.value.serverIdentifiers;
  return execResponse(exec, { case: "mcpStateExecResult", value: create4(McpStateExecResultSchema, {
    result: { case: "success", value: create4(McpStateSuccessSchema, {
      servers: requested.length === 0 || requested.includes(MCP_PROVIDER) ? [create4(McpStateServerSchema, { serverName: MCP_PROVIDER, serverIdentifier: MCP_PROVIDER, tools: definitions })] : []
    }) }
  }) });
}
function kvResponse(message2, blobs) {
  let response;
  if (message2.message.case === "getBlobArgs") {
    const data = blobs.get(message2.message.value.blobId);
    response = { case: "getBlobResult", value: create4(GetBlobResultSchema, data === void 0 ? {} : { blobData: data }) };
  } else if (message2.message.case === "setBlobArgs") {
    blobs.set(message2.message.value.blobId, message2.message.value.blobData);
    response = { case: "setBlobResult", value: create4(SetBlobResultSchema) };
  } else throw new CursorProtocolError("UNSUPPORTED_CONTROL", "Unsupported Cursor key-value request");
  return create4(AgentClientMessageSchema, { message: { case: "kvClientMessage", value: create4(KvClientMessageSchema, { id: message2.id, message: response }) } });
}
function rejectNative(exec) {
  if (exec.message.case === "forceBackgroundShellArgs") return execResponse(exec, {
    case: "forceBackgroundShellResult",
    value: create4(ForceBackgroundShellResultSchema, { status: 2 /* NOT_FOUND */ })
  });
  if (exec.message.case === "forceBackgroundSubagentArgs") return execResponse(exec, {
    case: "forceBackgroundSubagentResult",
    value: create4(ForceBackgroundSubagentResultSchema, { status: 2 /* NOT_FOUND */ })
  });
  const responseCase = exec.message.case ? DENIALS[exec.message.case] : void 0;
  const result = ExecClientMessageSchema.fields.find((field2) => field2.localName === responseCase);
  if (result?.fieldKind === "message") {
    for (const denial of ["rejected", "error", "failure"]) {
      const field2 = result.message.fields.find((candidate) => candidate.localName === denial && candidate.oneof !== void 0);
      if (field2?.fieldKind !== "message") continue;
      const reason = field2.message.fields.find((candidate) => ["reason", "error", "errorMessage", "message"].includes(candidate.localName) && candidate.fieldKind === "scalar" && candidate.scalar === 9);
      if (!reason) continue;
      const response = fromJson2(ExecClientMessageSchema, {
        id: exec.id,
        execId: exec.execId,
        [result.jsonName]: { [field2.jsonName]: { [reason.jsonName]: REASON } }
      });
      return create4(AgentClientMessageSchema, { message: { case: "execClientMessage", value: response } });
    }
  }
  throw new CursorProtocolError("UNSUPPORTED_EXEC", "Unsupported Cursor execution request has no safe denial schema");
}
function rejectInteraction(query) {
  let response;
  if (query.query.case === "askQuestionInteractionQuery") {
    response = { id: query.id, askQuestionInteractionResponse: { result: { rejected: { reason: REASON } } } };
  } else if (query.query.case === "switchModeRequestQuery") {
    response = { id: query.id, switchModeRequestResponse: { rejected: { reason: REASON } } };
  } else if (query.query.case === "createPlanRequestQuery") {
    response = { id: query.id, createPlanRequestResponse: { result: { error: { error: REASON } } } };
  } else {
    throw new CursorProtocolError("UNSUPPORTED_INTERACTION", "Unsupported Cursor interaction query has no safe denial schema");
  }
  return create4(AgentClientMessageSchema, { message: { case: "interactionResponse", value: fromJson2(InteractionResponseSchema, response) } });
}

// src/protocol/events.ts
var RunEvents = class {
  #queue = [];
  #bytes = 0;
  #ended = false;
  #failure;
  #lease;
  #waiting;
  push(event) {
    if (this.#ended) return;
    const bytes = Buffer.byteLength(JSON.stringify(event));
    if (bytes > LIMITS.eventBytes) throw limitError();
    if (this.#waiting) {
      const waiting = this.#waiting;
      this.#waiting = void 0;
      waiting.resolve({ done: false, value: event });
      return;
    }
    if (this.#queue.length >= LIMITS.eventCount || this.#bytes + bytes > LIMITS.eventBytes) throw limitError();
    this.#queue.push({ event, bytes });
    this.#bytes += bytes;
  }
  finish(failure) {
    if (this.#ended) return;
    this.#ended = true;
    this.#failure = failure;
    if (failure) {
      this.#queue.length = 0;
      this.#bytes = 0;
    }
    const waiting = this.#waiting;
    this.#waiting = void 0;
    if (waiting) {
      if (failure) waiting.reject(failure);
      else waiting.resolve({ done: true, value: void 0 });
    }
  }
  [Symbol.asyncIterator]() {
    if (this.#lease) throw new CursorProtocolError("INVALID_STATE", "Cursor Run already has an active events reader");
    const lease = {};
    this.#lease = lease;
    let returned = false;
    const release = () => {
      returned = true;
      if (this.#lease === lease) this.#lease = void 0;
      if (this.#waiting?.lease === lease) {
        const waiting = this.#waiting;
        this.#waiting = void 0;
        waiting.resolve({ done: true, value: void 0 });
      }
    };
    return {
      next: async () => {
        if (returned) return { done: true, value: void 0 };
        if (this.#waiting) throw new CursorProtocolError("INVALID_STATE", "Concurrent Cursor event reads are not supported");
        const item = this.#queue.shift();
        if (item) {
          this.#bytes -= item.bytes;
          return { done: false, value: item.event };
        }
        if (this.#ended) {
          release();
          if (this.#failure) throw this.#failure;
          return { done: true, value: void 0 };
        }
        return new Promise((resolve, reject) => {
          this.#waiting = { lease, resolve, reject };
        });
      },
      return: async () => {
        release();
        return { done: true, value: void 0 };
      },
      throw: async (error) => {
        release();
        throw error;
      }
    };
  }
};

// src/protocol/split.ts
import { Agent as HttpsAgent, request as httpsRequest } from "node:https";
import { Agent as HttpAgent, request as httpRequest } from "node:http";
import { randomUUID as randomUUID2 } from "node:crypto";
import { promisify as promisify2 } from "node:util";
import { gzip as gzip2, gunzipSync as gunzipSync2 } from "node:zlib";
var compress2 = promisify2(gzip2);
var SPLIT_LIMITS = Object.freeze({ parallel: 16, sequenceWindow: 64, replyBytes: 256 * 1024 });
var RUN_STREAM_PATH = "/agent.v1.AgentService/RunSSE";
var APPEND_PATH = "/aiserver.v1.BidiService/BidiAppend";
function varint(value) {
  const bytes = [];
  do {
    const low = value % 128;
    value = Math.floor(value / 128);
    bytes.push(low | (value ? 128 : 0));
  } while (value);
  return Buffer.from(bytes);
}
function bytesField(field2, bytes) {
  return Buffer.concat([varint(field2 * 8 + 2), varint(bytes.length), bytes]);
}
function encodeRequestId(id) {
  return bytesField(1, Buffer.from(id));
}
function encodeAppend(id, sequence, payload) {
  return Buffer.concat([
    bytesField(2, encodeRequestId(id)),
    ...sequence ? [Buffer.concat([Buffer.from([24]), varint(sequence)])] : [],
    bytesField(4, payload)
  ]);
}
var SplitChannel = class {
  ready;
  #resolveReady;
  #rejectReady;
  #readySettled = false;
  #response;
  #origin;
  #id = randomUUID2();
  #headers;
  #agent;
  #request;
  #trace;
  #signal;
  #onAbort;
  #closed = false;
  #failure;
  #requests = /* @__PURE__ */ new Set();
  #timers = /* @__PURE__ */ new Set();
  #nextSequence = 0;
  #acknowledgedThrough = 0;
  #acknowledged = /* @__PURE__ */ new Set();
  #pending = /* @__PURE__ */ new Set();
  #waiters = /* @__PURE__ */ new Set();
  #admission = Promise.resolve();
  #writes = 0;
  #bytes = 0;
  constructor(options) {
    this.#origin = validateTransport(options);
    this.#trace = options.trace;
    this.#signal = options.signal;
    const secure = this.#origin.startsWith("https:");
    this.#request = secure ? httpsRequest : httpRequest;
    this.#agent = secure ? new HttpsAgent({ keepAlive: true, maxSockets: SPLIT_LIMITS.parallel + 1 }) : new HttpAgent({ keepAlive: true, maxSockets: SPLIT_LIMITS.parallel + 1 });
    this.#headers = {
      ...protocolHeaders(options),
      "x-request-id": this.#id,
      "x-cursor-streaming": "true",
      "connect-protocol-version": "1"
    };
    this.ready = new Promise((resolve, reject) => {
      this.#resolveReady = resolve;
      this.#rejectReady = reject;
    });
    void this.ready.catch(() => {
    });
    this.#onAbort = () => this.close(new CursorProtocolError("ABORTED", "Cursor Run was aborted"));
    this.#signal?.addEventListener("abort", this.#onAbort, { once: true });
    if (this.#signal?.aborted) {
      this.#onAbort();
      return;
    }
    this.#open();
  }
  #timer(action, ms) {
    const timer = setTimeout(() => {
      this.#timers.delete(timer);
      action();
    }, ms);
    timer.unref();
    this.#timers.add(timer);
    return timer;
  }
  #clear(timer) {
    clearTimeout(timer);
    this.#timers.delete(timer);
  }
  #settleReady(error) {
    if (this.#readySettled) return;
    this.#readySettled = true;
    if (error) this.#rejectReady(error);
    else this.#resolveReady();
  }
  #track(request) {
    this.#requests.add(request);
    request.once("close", () => this.#requests.delete(request));
  }
  #open() {
    const body = frameMessage(encodeRequestId(this.#id));
    const req = this.#request(new URL(RUN_STREAM_PATH, this.#origin), {
      method: "POST",
      agent: this.#agent,
      headers: {
        ...this.#headers,
        "content-type": "application/connect+proto",
        "connect-accept-encoding": "gzip",
        "content-length": String(body.length)
      }
    }, (response) => {
      this.#clear(timer);
      this.#response = response;
      response.on("error", () => this.close(new CursorProtocolError("TRANSPORT", "Cursor split response failed")));
      if (this.#closed) {
        response.destroy();
        return;
      }
      if (response.statusCode !== 200) {
        this.close(httpError(response.statusCode ?? 0));
        return;
      }
      if (!["application/connect+proto", "text/event-stream"].includes(String(response.headers["content-type"]).split(";")[0])) {
        this.close(new CursorProtocolError("INVALID_RESPONSE", "Unexpected Cursor split response content type"));
        return;
      }
      this.#settleReady();
    });
    this.#track(req);
    const timer = this.#timer(() => this.close(new CursorProtocolError("TIMEOUT", "Cursor split response header timeout")), LIMITS.handshakeMs);
    req.on("error", () => this.close(new CursorProtocolError("TRANSPORT", "Cursor split response could not be opened")));
    req.once("close", () => {
      this.#clear(timer);
      if (!this.#readySettled) this.close(new CursorProtocolError("TRANSPORT", "Cursor split response closed before headers"));
    });
    req.end(body);
  }
  /** KV data can pipeline. Tools/actions await their individual delivery acknowledgement. */
  write(frame, buffered = false) {
    if (this.#closed) return Promise.reject(this.#failure);
    if (frame.length < 5 || frame[0] !== 0 || Buffer.from(frame).readUInt32BE(1) !== frame.length - 5) {
      throw new Error("SplitChannel requires one uncompressed Connect message");
    }
    if (frame.length - 5 > LIMITS.frameBytes || this.#writes >= LIMITS.queuedWrites || this.#bytes + frame.length > LIMITS.queuedWriteBytes) {
      const error = limitError();
      this.close(error);
      return Promise.reject(error);
    }
    const payload = Buffer.from(frame.subarray(5));
    this.#writes++;
    this.#bytes += frame.length;
    let delivered;
    const admitted = this.#admission.then(async () => {
      while (!this.#closed && (this.#pending.size >= SPLIT_LIMITS.parallel || this.#nextSequence - this.#acknowledgedThrough >= SPLIT_LIMITS.sequenceWindow)) {
        await new Promise((resolve) => this.#waiters.add(resolve));
      }
      if (this.#closed) throw this.#failure;
      const sequence = this.#nextSequence++;
      const sent = this.#append(sequence, payload).then(() => {
        this.#acknowledged.add(sequence);
        while (this.#acknowledged.delete(this.#acknowledgedThrough)) this.#acknowledgedThrough++;
      }).catch((error) => {
        const failure = this.#failure ?? safeError(error, "Cursor append delivery failed");
        this.close(failure);
        throw failure;
      });
      delivered = sent.finally(() => {
        this.#pending.delete(delivered);
        this.#writes--;
        this.#bytes -= frame.length;
        this.#wake();
      });
      this.#pending.add(delivered);
      void delivered.catch(() => {
      });
    }).catch((error) => {
      if (!delivered) {
        this.#writes--;
        this.#bytes -= frame.length;
      }
      throw error;
    });
    this.#admission = admitted.catch(() => {
    });
    return buffered ? admitted : admitted.then(() => delivered);
  }
  async #append(sequence, payload) {
    const raw = encodeAppend(this.#id, sequence, payload);
    const compressed = raw.length >= 1024;
    const body = compressed ? await compress2(raw) : raw;
    if (this.#closed) throw this.#failure;
    const timeoutMs = 6e4 + Math.ceil(raw.length / 131072 * 1e3);
    const started = Date.now();
    await new Promise((resolve, reject) => {
      const req = this.#request(new URL(APPEND_PATH, this.#origin), {
        method: "POST",
        agent: this.#agent,
        headers: {
          ...this.#headers,
          "content-type": "application/proto",
          "content-length": String(body.length),
          "connect-timeout-ms": String(timeoutMs),
          ...compressed ? { "content-encoding": "gzip" } : {}
        }
      }, (response) => {
        const chunks = [];
        let length = 0;
        response.on("data", (chunk) => {
          length += chunk.length;
          if (length > SPLIT_LIMITS.replyBytes) {
            this.close(limitError());
            return;
          }
          chunks.push(chunk);
        });
        response.on("error", () => reject(this.#failure ?? new CursorProtocolError("TRANSPORT", "Cursor append response failed")));
        response.on("end", () => {
          this.#clear(timer);
          if (this.#closed) {
            reject(this.#failure);
            return;
          }
          if (response.statusCode !== 200) {
            let error = httpError(response.statusCode ?? 0);
            try {
              const detail = JSON.parse(Buffer.concat(chunks).toString("utf8"));
              if (typeof detail.code === "string") error = statusError(detail.code, detail.message);
            } catch {
            }
            reject(error);
            return;
          }
          if (String(response.headers["content-type"]).split(";")[0] !== "application/proto" || length !== 0) {
            reject(new CursorProtocolError("INVALID_RESPONSE", "Invalid Cursor append acknowledgement"));
            return;
          }
          this.#trace?.("upload", { bytes: payload.length, wireBytes: body.length, elapsedMs: Date.now() - started });
          resolve();
        });
      });
      this.#track(req);
      const timer = this.#timer(() => {
        const error = new CursorProtocolError("TIMEOUT", "Cursor append delivery timeout");
        this.close(error);
        reject(error);
      }, timeoutMs);
      req.on("error", () => reject(this.#failure ?? new CursorProtocolError("TRANSPORT", "Cursor append request failed")));
      req.once("close", () => {
        this.#clear(timer);
        reject(this.#failure ?? new CursorProtocolError("TRANSPORT", "Cursor append closed before acknowledgement"));
      });
      req.end(body);
    });
  }
  async *frames() {
    const decoder = new FrameDecoder();
    let ended2 = false;
    try {
      await this.ready;
      for await (const chunk of this.#response) {
        if (this.#failure) throw this.#failure;
        for (const frame of decoder.push(chunk)) {
          if (ended2) throw new CursorProtocolError("INVALID_RESPONSE", "Cursor sent data after end-stream");
          if (frame.flags & 1) {
            if (this.#response.headers["connect-content-encoding"] !== "gzip") throw new CursorProtocolError("INVALID_RESPONSE", "Unsupported Cursor frame compression");
            try {
              frame.payload = gunzipSync2(frame.payload, { maxOutputLength: LIMITS.inflatedBytes });
            } catch {
              throw limitError();
            }
          }
          if (frame.flags & 2) {
            terminalError(frame.payload);
            ended2 = true;
          }
          yield frame;
        }
      }
      decoder.finish();
      if (this.#failure) throw this.#failure;
    } catch (error) {
      throw this.#failure ?? safeError(error, "Cursor split response failed");
    }
  }
  async flush() {
    await this.#admission;
    await Promise.all(this.#pending);
    if (this.#failure) throw this.#failure;
  }
  #wake() {
    for (const wake of this.#waiters) wake();
    this.#waiters.clear();
  }
  close(error = new CursorProtocolError("CLOSED", "Cursor Run is closed")) {
    if (this.#closed) return;
    this.#closed = true;
    this.#failure = error;
    this.#signal?.removeEventListener("abort", this.#onAbort);
    this.#settleReady(error);
    for (const timer of this.#timers) clearTimeout(timer);
    this.#timers.clear();
    this.#wake();
    for (const req of this.#requests) req.destroy();
    this.#response?.destroy();
    this.#agent.destroy();
  }
};

// src/protocol/run.ts
var CursorConnection = class {
  events = new RunEvents();
  #lifetime = new AbortController();
  #channel;
  #blobs;
  #definitions;
  #names;
  #cwd;
  #instructions;
  #pending = /* @__PURE__ */ new Map();
  #pendingBytes = 0;
  #seen = /* @__PURE__ */ new Set();
  #closed = false;
  #failure;
  #heartbeat;
  #execHeartbeat;
  #idle;
  #pump = Promise.resolve();
  #signal;
  #onAbort;
  #usage = { inputTokens: 0, outputTokens: 0 };
  #haveUsage = false;
  #trace;
  #kvGets = 0;
  #firstText = false;
  #firstThinking = false;
  #ended;
  #terminalCheckpoint = false;
  #save;
  #persistence = Promise.resolve();
  #recoveryBarrier = false;
  #stepOutput = [];
  #outputBytes = 0;
  /** Exact host request whose uncommitted output this Run may recover. */
  requestKey = "";
  constructor(request, options, blobs, definitions) {
    this.#blobs = blobs;
    const conversation = request.conversation;
    const context = conversation?.contextTokens ?? conversation?.state?.tokenDetails?.usedTokens;
    if (context !== void 0 && context > 0) this.#usage.contextTokens = context;
    this.#definitions = definitions;
    this.#names = new Set(request.tools.map((tool) => tool.name));
    this.#cwd = request.cwd;
    this.#instructions = request.messages.filter((message2) => message2.role === "system" || message2.role === "developer").map((message2) => message2.content.filter((item) => item.type === "text").map((item) => item.text).join("\n")).filter(Boolean).join("\n\n");
    this.#signal = options.signal;
    this.#trace = options.trace;
    this.#save = request.onCheckpoint;
    this.#onAbort = () => this.#fail(new CursorProtocolError("ABORTED", "Cursor Run was aborted"));
    const transport = { ...options, signal: this.#lifetime.signal };
    this.#channel = options.runTransport === "split" ? new SplitChannel(transport) : new H2Channel(RUN_PATH, transport, true);
    this.#signal?.addEventListener("abort", this.#onAbort, { once: true });
    if (this.#signal?.aborted) this.#onAbort();
  }
  /** Shared conversation whose checkpoint this Run advances, when one is supplied. */
  conversation;
  /** Anchor of the host history this Run started from. */
  anchor = "";
  /** Text of the current model step, reset when the host answers its tools. */
  #stepText = [];
  /** Whether the latest checkpoint arrived after the latest model output. */
  #checkpointCurrent = false;
  /** Injections Cursor has not yet taken into the turn. */
  #injections = /* @__PURE__ */ new Map();
  /** Identifies this Run to context injections. */
  runId = randomUUID3();
  #persist() {
    if (!this.conversation || !this.#save) return this.#persistence;
    const save = this.#save, conversation = this.conversation;
    this.#persistence = this.#persistence.then(() => save(conversation));
    void this.#persistence.catch(() => {
    });
    return this.#persistence;
  }
  #rememberOutput(type, text) {
    this.#outputBytes += Buffer.byteLength(text);
    if (this.#outputBytes > LIMITS.eventBytes) {
      this.#recoveryBarrier = true;
      if (this.conversation) this.conversation.recovery = void 0;
      this.#stepOutput = [];
      return;
    }
    const last = this.#stepOutput.at(-1);
    if (last?.type === type) last.text += text;
    else this.#stepOutput.push({ type, text });
  }
  replayOutput(output) {
    for (const event of output) {
      this.#rememberOutput(event.type, event.text);
      if (event.type === "text") this.#stepText.push(event.text);
      this.events.push({ ...event });
    }
  }
  async start(request) {
    ;
    request.runId = this.runId;
    this.#pump = this.#read().catch((error) => {
      if (!this.#closed) this.#fail(safeError(error, "Invalid Cursor Run response"));
    });
    try {
      await this.#write(create5(AgentClientMessageSchema, { message: { case: "runRequest", value: request } }));
      this.#trace?.("opening-sent");
      await this.#channel.ready;
      this.#trace?.("headers");
      if (this.#failure) throw this.#failure;
      if (!this.#closed) {
        this.#heartbeat = setInterval(() => {
          void this.#write(create5(AgentClientMessageSchema, { message: { case: "clientHeartbeat", value: create5(ClientHeartbeatSchema) } })).catch((error) => this.#fail(safeError(error)));
        }, LIMITS.heartbeatMs);
        this.#heartbeat.unref();
        this.#execHeartbeat = setInterval(() => {
          for (const { exec } of this.#pending.values()) {
            void this.#write(create5(AgentClientMessageSchema, {
              message: { case: "execClientControlMessage", value: create5(ExecClientControlMessageSchema, {
                message: { case: "heartbeat", value: create5(ExecClientHeartbeatSchema, { id: exec.id }) }
              }) }
            })).catch((error) => this.#fail(safeError(error)));
          }
        }, 3e3);
        this.#execHeartbeat.unref();
        this.#armIdle();
      }
    } catch (error) {
      const failure = this.#failure ?? safeError(error);
      this.#fail(failure);
      throw failure;
    }
  }
  async #write(message2) {
    if (this.#closed) throw this.#failure ?? new CursorProtocolError("CLOSED", "Cursor Run is closed");
    const written = this.#channel.write(frameMessage(toBinary4(WireAgentClientMessageSchema, message2)), message2.message.case === "kvClientMessage");
    if (message2.message.case === "execClientMessage") {
      const closed = this.#channel.write(frameMessage(toBinary4(WireAgentClientMessageSchema, create5(AgentClientMessageSchema, {
        message: { case: "execClientControlMessage", value: create5(ExecClientControlMessageSchema, {
          message: { case: "streamClose", value: create5(ExecClientStreamCloseSchema, { id: message2.message.value.id }) }
        }) }
      }))));
      await Promise.all([written, closed]);
    } else await written;
  }
  #armIdle() {
    if (this.#ended) return;
    clearTimeout(this.#idle);
    this.#idle = void 0;
    if (this.#closed || this.#ended || this.#pending.size) return;
    this.#idle = setTimeout(() => this.#fail(new CursorProtocolError("TIMEOUT", "Cursor Run response timeout")), LIMITS.idleMs);
    this.#idle.unref();
  }
  #dispose() {
    if (this.#closed) return;
    this.#closed = true;
    clearInterval(this.#heartbeat);
    clearInterval(this.#execHeartbeat);
    clearTimeout(this.#idle);
    this.#heartbeat = void 0;
    this.#execHeartbeat = void 0;
    this.#idle = void 0;
    this.#signal?.removeEventListener("abort", this.#onAbort);
    this.#pending.clear();
    this.#pendingBytes = 0;
    this.#seen.clear();
    if (!this.conversation) this.#blobs.clear();
    this.#definitions = [];
    this.#lifetime.abort();
    this.#channel.close();
  }
  #fail(error) {
    if (this.#closed) return;
    this.#failure = error;
    if (error.code === "ABORTED" || error.code === "CLOSED") {
      if (this.conversation) this.conversation.recovery = void 0;
      void this.#persist();
    }
    this.events.finish(error);
    this.#dispose();
  }
  async #read() {
    try {
      for await (const frame of this.#channel.frames()) {
        if (this.#closed) return;
        if ((frame.flags & 2) !== 0) break;
        let message2;
        try {
          message2 = fromBinary2(WireAgentServerMessageSchema, frame.payload);
        } catch {
          throw new CursorProtocolError("INVALID_RESPONSE", "Invalid Cursor protobuf response");
        }
        this.#armIdle();
        await this.#handle(message2);
        if (this.#closed) return;
      }
    } catch (error) {
      if (!this.#ended || !this.#terminalCheckpoint || !(error instanceof CursorProtocolError) || !["TRANSPORT", "TIMEOUT"].includes(error.code)) throw error;
    }
    if (this.#closed) return;
    if (!this.#ended) throw new CursorProtocolError("TRANSPORT", "Cursor stream ended before turnEnded");
    await this.#completeTurn();
  }
  async #completeTurn() {
    if (!this.#ended) throw new Error("Cannot complete an unterminated Cursor turn");
    clearInterval(this.#heartbeat);
    clearInterval(this.#execHeartbeat);
    if (!this.#terminalCheckpoint) await this.#channel.flush?.();
    else {
      try {
        await this.#channel.flush?.();
      } catch (error) {
        if (!(error instanceof CursorProtocolError) || !["TRANSPORT", "TIMEOUT", "CLOSED"].includes(error.code)) throw error;
      }
    }
    if (this.conversation) {
      this.conversation.resume = this.#checkpointCurrent ? { anchor: this.anchor, finalText: textKey([{ type: "text", text: this.#stepText.join("") }]) } : void 0;
      this.conversation.recovery = void 0;
      this.conversation.notes.push(...this.#injections.values());
      this.#injections.clear();
      await this.#persist();
    }
    if (this.#haveUsage || this.#usage.contextTokens !== void 0) this.events.push({ type: "usage", usage: { ...this.#usage } });
    this.events.push({ type: "done", reason: "stop" });
    this.events.finish();
    this.#dispose();
  }
  async #handle(message2) {
    switch (message2.message.case) {
      case "kvServerMessage":
        if (message2.message.value.message.case === "getBlobArgs" && ++this.#kvGets % 100 === 1) this.#trace?.("kv-get", { count: this.#kvGets });
        await this.#write(kvResponse(message2.message.value, this.#blobs));
        return;
      case "execServerMessage":
        await this.#exec(message2.message.value);
        return;
      case "interactionQuery":
        await this.#write(rejectInteraction(message2.message.value));
        return;
      case "conversationCheckpointUpdate": {
        if (this.conversation) {
          const state = message2.message.value;
          this.conversation.state = state;
          if ((state.tokenDetails?.usedTokens ?? 0) > 0) this.conversation.contextTokens = state.tokenDetails.usedTokens;
          this.#checkpointCurrent = true;
          if (this.#ended) {
            this.#terminalCheckpoint = true;
            this.conversation.resume = { anchor: this.anchor, finalText: textKey([{ type: "text", text: this.#stepText.join("") }]) };
            this.conversation.recovery = void 0;
          } else if (!this.#recoveryBarrier && this.#pending.size === 0) {
            const checkpointHash = createHash4("sha256").update(toBinary4(ConversationStateStructureSchema, state)).digest("hex");
            const previous = this.conversation.recovery;
            this.conversation.recovery = {
              requestKey: this.requestKey,
              checkpointHash,
              attempts: previous?.checkpointHash === checkpointHash ? previous.attempts : 0,
              output: this.#stepOutput.map((event) => ({ ...event }))
            };
          } else this.conversation.recovery = void 0;
          await this.#persist();
        }
        const used = message2.message.value.tokenDetails?.usedTokens;
        if (used !== void 0 && used > 0) {
          this.#usage.contextTokens = used;
          this.events.push({ type: "usage", usage: { ...this.#usage } });
        }
        return;
      }
      case "interactionUpdate":
        await this.#interaction(message2.message.value);
        return;
      case "execServerControlMessage": {
        if (message2.message.value.message.case !== "abort") throw new CursorProtocolError("UNSUPPORTED_CONTROL", "Unsupported Cursor execution control request");
        const id = message2.message.value.message.value.id;
        if ([...this.#pending.values()].some((pending) => pending.exec.id === id)) throw new CursorProtocolError("ABORTED", "Cursor canceled an outstanding tool request");
        return;
      }
      default:
        throw new CursorProtocolError("UNSUPPORTED_CONTROL", "Unsupported Cursor server message");
    }
  }
  async #exec(exec) {
    switch (exec.message.case) {
      case "requestContextArgs":
        this.#trace?.("request-context", { kvGets: this.#kvGets });
        await this.#write(contextResponse(exec, this.#definitions, this.#cwd, this.#instructions));
        return;
      case "mcpStateExecArgs":
        await this.#write(mcpStateResponse(exec, this.#definitions));
        return;
      case "mcpAllowlistPrecheckArgs":
        await this.#write(execResponse(exec, { case: "mcpAllowlistPrecheckResult", value: create5(McpAllowlistPrecheckResultSchema, { allowlisted: false }) }));
        return;
      case "shellAllowlistPrecheckArgs":
        await this.#write(execResponse(exec, { case: "shellAllowlistPrecheckResult", value: create5(ShellAllowlistPrecheckResultSchema, { allowlisted: false }) }));
        return;
      case "webFetchAllowlistPrecheckArgs":
        await this.#write(execResponse(exec, { case: "webFetchAllowlistPrecheckResult", value: create5(WebFetchAllowlistPrecheckResultSchema, { allowlisted: false }) }));
        return;
      case "mcpArgs": {
        if (this.#ended) throw new CursorProtocolError("INVALID_RESPONSE", "Cursor requested a tool after turnEnded");
        const args = exec.message.value;
        const name = advertisedName(args, this.#names);
        if (args.smartModeApprovalOnly) {
          await this.#write(execResponse(exec, { case: "mcpResult", value: create5(McpResultSchema, {
            result: name ? { case: "approved", value: create5(McpApprovedSchema) } : { case: "rejected", value: create5(McpRejectedSchema, { reason: "The requested tool is not advertised by DeepSeek Harness." }) }
          }) }));
          return;
        }
        if (!name) {
          await this.#write(execResponse(exec, { case: "mcpResult", value: create5(McpResultSchema, {
            result: { case: "toolNotFound", value: create5(McpToolNotFoundSchema, { availableTools: [...this.#names] }) }
          }) }));
          return;
        }
        const identity = createHash4("sha256").update(JSON.stringify([exec.id, exec.execId])).digest("hex");
        if (this.#seen.has(identity)) throw new CursorProtocolError("INVALID_RESPONSE", "Cursor repeated an MCP execution envelope");
        const bytes = toBinary4(ExecServerMessageSchema, exec).byteLength;
        if (this.#pending.size >= LIMITS.pendingCalls || this.#seen.size >= LIMITS.seenCalls || this.#pendingBytes + bytes > LIMITS.pendingBytes) throw limitError();
        const argumentsJSON = decodeArguments(args.args);
        const callKey = randomUUID3();
        this.#seen.add(identity);
        this.#pending.set(callKey, { exec, bytes });
        this.#pendingBytes += bytes;
        this.#armIdle();
        this.#checkpointCurrent = false;
        this.#recoveryBarrier = true;
        if (this.conversation) this.conversation.recovery = void 0;
        await this.#persist();
        this.events.push({ type: "tool", callKey, name, arguments: argumentsJSON });
        return;
      }
      default:
        await this.#write(rejectNative(exec));
    }
  }
  async #interaction(update) {
    if (this.#ended && ["textDelta", "thinkingDelta", "toolRequestsListed"].includes(update.message.case ?? "")) {
      throw new CursorProtocolError("INVALID_RESPONSE", "Cursor emitted model output after turnEnded");
    }
    switch (update.message.case) {
      case "textDelta":
        this.#checkpointCurrent = false;
        this.#rememberOutput("text", update.message.value.text);
        this.#stepText.push(update.message.value.text);
        if (!this.#firstText) {
          this.#firstText = true;
          this.#trace?.("first-text", { kvGets: this.#kvGets });
        }
        this.events.push({ type: "text", text: update.message.value.text });
        return;
      case "thinkingDelta":
        this.#checkpointCurrent = false;
        this.#rememberOutput("reasoning", update.message.value.text);
        if (!this.#firstThinking) {
          this.#firstThinking = true;
          this.#trace?.("first-thinking", { kvGets: this.#kvGets });
        }
        this.events.push({ type: "reasoning", text: update.message.value.text });
        return;
      case "tokenDelta": {
        const total = this.#usage.outputTokens + update.message.value.tokens;
        if (update.message.value.tokens < 0 || !Number.isSafeInteger(total)) throw new CursorProtocolError("INVALID_RESPONSE", "Invalid Cursor token count");
        this.#usage.outputTokens = total;
        this.#haveUsage = true;
        return;
      }
      case "contextInjectionState": {
        const state = update.message.value.state?.state;
        if (state?.case === "delivered") this.#injections.delete(update.message.value.injectionId);
        this.#trace?.("injection", { state: state?.case ?? "unknown", ...state?.value?.reason ? { reason: state.value.reason } : {} });
        return;
      }
      case "toolRequestsListed":
        this.events.push({ type: "tools-listed", count: update.message.value.callCount });
        return;
      case "turnEnded":
        if (this.#ended || this.#pending.size) throw new CursorProtocolError("INVALID_RESPONSE", "Invalid Cursor terminal turn state");
        this.#ended = update.message.value;
        this.#applyFinalUsage(update.message.value);
        this.#trace?.("turn-usage", { ...this.#usage });
        if (this.conversation) this.conversation.recovery = void 0;
        clearTimeout(this.#idle);
        this.#idle = setTimeout(() => this.#channel.close(new CursorProtocolError("TIMEOUT", "Cursor terminal checkpoint drain timed out")), LIMITS.terminalDrainMs);
        this.#idle.unref();
        await this.#persist();
        return;
      // These are display/status updates, never authority to execute a tool.
      default:
        return;
    }
  }
  #applyFinalUsage(ended2) {
    for (const name of ["inputTokens", "outputTokens", "cacheReadTokens", "cacheWriteTokens", "reasoningTokens"]) {
      const value = ended2[name];
      if (value === void 0) continue;
      if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) throw new CursorProtocolError("INVALID_RESPONSE", "Invalid Cursor token count");
      this.#usage[name] = Number(value);
      this.#haveUsage = true;
    }
    if (ended2.inputTokens !== void 0) {
      this.#usage.inputTokens = Math.max(0, Number(ended2.inputTokens) - (this.#usage.cacheReadTokens ?? 0) - (this.#usage.cacheWriteTokens ?? 0));
    }
  }
  async reply(callKey, result) {
    if (this.#closed) throw this.#failure ?? new CursorProtocolError("CLOSED", "Cursor Run is closed");
    const pending = this.#pending.get(callKey);
    if (!pending) throw new CursorProtocolError("INVALID_CALL_KEY", "Unknown or already answered Cursor tool call");
    this.#stepText = [];
    const response = execResponse(pending.exec, { case: "mcpResult", value: create5(McpResultSchema, {
      result: { case: "success", value: mcpContent(result) }
    }) });
    this.#pending.delete(callKey);
    this.#pendingBytes -= pending.bytes;
    this.#armIdle();
    try {
      await this.#write(response);
    } catch (error) {
      const failure = this.#failure ?? safeError(error);
      this.#fail(failure);
      throw failure;
    }
  }
  async inject(content) {
    if (this.#closed) throw this.#failure ?? new CursorProtocolError("CLOSED", "Cursor Run is closed");
    const injectionId = randomUUID3();
    this.#recoveryBarrier = true;
    if (this.conversation) this.conversation.recovery = void 0;
    await this.#persist();
    this.#injections.set(injectionId, content);
    const message2 = {
      $typeName: "agent.v1.AgentClientMessage",
      message: { case: "conversationAction", value: { $typeName: "agent.v1.ConversationAction", action: { case: "injectContextAction", value: {
        $typeName: "agent.v1.InjectContextAction",
        injectionId,
        expectedRunId: this.runId,
        userContext: { $typeName: "agent.v1.UserContextInjection", userMessage: userMessage([{ role: "user", content }], this.#blobs, `inject:${injectionId}`) }
      } } } }
    };
    this.#trace?.("inject", { parts: content.length });
    this.#checkpointCurrent = false;
    try {
      await this.#write(message2);
    } catch (error) {
      const failure = this.#failure ?? safeError(error);
      this.#fail(failure);
      throw failure;
    }
  }
  async cancel(_reason) {
    this.#fail(new CursorProtocolError("ABORTED", "Cursor Run was aborted"));
    await this.#pump;
    await this.#persistence;
  }
  async close() {
    this.#fail(new CursorProtocolError("CLOSED", "Cursor Run was closed"));
    await this.#pump;
    await this.#persistence;
  }
};
var textKey = (content) => content.filter((item) => item.type === "text").map((item) => item.text).join("").replace(/\s+/gu, "").slice(0, 4096);
function anchorOf(messages) {
  const first = messages.find((message2) => message2.role !== "system" && message2.role !== "developer");
  return first ? messagesDigest([first]) : "";
}
function resumePlan(conversation, messages) {
  const rebuild = (reason) => ({ active: -1, reason });
  if (!conversation) return rebuild("ephemeral");
  if (!conversation.state) return rebuild("no-checkpoint");
  const resume = conversation.resume;
  if (!resume) return rebuild("unfinished-turn");
  const active = activeUserIndex(messages);
  if (active < 0) return rebuild("tool-result-tail");
  let last = active - 1;
  while (last >= 0 && messages[last].role !== "assistant") last--;
  if (last < 0) return rebuild("missing-assistant");
  if (messages[last].sourceProvider && messages[last].sourceProvider !== "cursor") return rebuild("foreign-provider");
  if (textKey(messages[last].content) !== resume.finalText) return rebuild("assistant-changed");
  if (anchorOf(messages) !== resume.anchor) return rebuild("history-rewritten");
  return { active, reason: "terminal-checkpoint" };
}
function resetCursorConversation(value) {
  const conversation = value;
  Object.assign(conversation, { blobs: new BlobStore(), state: void 0, contextTokens: void 0, hostHistory: void 0, resume: void 0, recovery: void 0, notes: [] });
}
function createCursorConversation() {
  const store = { kind: "cursor-conversation", id: randomUUID3(), blobEncryptionKey: randomBytes(32).toString("hex"), blobs: new BlobStore(), notes: [] };
  return store;
}
async function openCursorRun(request, options) {
  validateTransport(options);
  const conversation = request.conversation;
  const definitions = toolDefinitions(request.tools);
  const requestKey = createHash4("sha256").update(JSON.stringify([messagesDigest(request.messages), request.selection, request.tools, request.cwd ?? null])).digest("hex");
  const recovery = conversation?.state && conversation.recovery?.requestKey === requestKey ? conversation.recovery : void 0;
  if (recovery && recovery.attempts >= 2) throw new CursorProtocolError("CURSOR_RECOVERY_STALLED", "Cursor checkpoint recovery made no progress");
  const { active, reason } = recovery ? { active: -1, reason: "interrupted-checkpoint" } : resumePlan(conversation, request.messages);
  const blobs = new BlobStore();
  let built;
  if (conversation && recovery) {
    recovery.attempts++;
    built = buildResumeRequest({ ...request, conversationId: conversation.id }, conversation.blobs, conversation.state, -1);
  } else if (conversation && active >= 0) {
    const notes = conversation.notes.splice(0);
    const messages = notes.length ? [...request.messages.slice(0, active), ...notes.map((content) => ({ role: "user", content })), ...request.messages.slice(active)] : request.messages;
    built = buildResumeRequest({ ...request, messages, conversationId: conversation.id }, conversation.blobs, conversation.state, active);
  } else {
    if (conversation && (conversation.state || conversation.blobs.bytes > LIMITS.conversationBytes)) {
      resetCursorConversation(conversation);
    }
    built = buildRunRequest(conversation ? { ...request, conversationId: conversation.id } : request, conversation?.blobs ?? blobs);
  }
  const store = conversation?.blobs ?? blobs;
  const historyBytes = built.conversationState.rootPromptMessagesJson.reduce((total, id) => total + (store.get(id)?.byteLength ?? 0), 0);
  const transport = options.runTransport && options.runTransport !== "auto" ? options.runTransport : historyBytes >= 4 * 1024 * 1024 ? "split" : "http2";
  const blobEncryptionKey = conversation?.blobEncryptionKey ?? randomBytes(32).toString("hex");
  const headers = Object.fromEntries(Object.entries(options.headers ?? {}).filter(([name]) => name.toLowerCase() !== "x-blob-encryption-key"));
  options = {
    ...options,
    headers: { ...headers, "x-blob-encryption-key": blobEncryptionKey },
    runTransport: transport,
    endpoint: transport === "split" ? options.endpoint ?? DEFAULT_ENDPOINT : await cursorRunEndpoint(options)
  };
  options.trace?.(recovery ? "recover-checkpoint" : active >= 0 ? "resume-checkpoint" : "rebuild-history", { messages: request.messages.length, reason, transport, historyBytes });
  if (conversation) {
    conversation.resume = void 0;
    if (!recovery) conversation.recovery = void 0;
    await request.onCheckpoint?.(conversation);
  }
  if (options.signal?.aborted) throw new CursorProtocolError("ABORTED", "Cursor Run was aborted");
  const connection = new CursorConnection(request, options, conversation?.blobs ?? blobs, definitions);
  connection.conversation = conversation;
  connection.anchor = anchorOf(request.messages);
  connection.requestKey = requestKey;
  if (recovery) connection.replayOutput(recovery.output);
  await connection.start(built);
  return connection;
}
async function fetchCursorUsableModels(options) {
  const bytes = await requestUnary("/agent.v1.AgentService/GetUsableModels", toBinary4(GetUsableModelsRequestSchema, create5(GetUsableModelsRequestSchema)), options);
  try {
    return fromBinary2(GetUsableModelsResponseSchema, bytes).models.map((model) => ({
      modelId: model.modelId,
      displayName: model.displayName,
      ...model.displayModelId ? { displayModelId: model.displayModelId } : {},
      ...model.displayNameShort ? { displayNameShort: model.displayNameShort } : {},
      ...model.maxMode !== void 0 ? { maxMode: model.maxMode } : {},
      ...model.thinkingDetails ? { thinkingDetails: toJson2(ThinkingDetailsSchema, model.thinkingDetails) } : {}
    }));
  } catch {
    throw new CursorProtocolError("INVALID_RESPONSE", "Invalid Cursor model catalog protobuf");
  }
}

// src/protocol/snapshot.ts
import { promisify as promisify3 } from "node:util";
import { createHash as createHash5, randomBytes as randomBytes2 } from "node:crypto";
import { gzip as gzip3, gunzip } from "node:zlib";
import { fromBinary as fromBinary3, toBinary as toBinary5 } from "@bufbuild/protobuf";
var compress3 = promisify3(gzip3);
var decompress = promisify3(gunzip);
var blobHashes = /* @__PURE__ */ new WeakMap();
function blobHash(data) {
  let hash = blobHashes.get(data);
  if (!hash) {
    hash = createHash5("sha256").update(data).digest("hex");
    blobHashes.set(data, hash);
  }
  return hash;
}
function packHash(entries) {
  return createHash5("sha256").update(JSON.stringify(entries.map(([id, data]) => [Buffer.from(id).toString("base64"), blobHash(data)]))).digest("hex");
}
function blobPacks(entries) {
  const packs = [];
  let pack = [], bytes = 0;
  for (const entry of entries) {
    if (entry[1].byteLength >= 64 * 1024) {
      if (pack.length) {
        packs.push(pack);
        pack = [];
        bytes = 0;
      }
      packs.push([entry]);
      continue;
    }
    if (pack.length && (pack.length >= 64 || bytes + entry[1].byteLength > 1024 * 1024)) {
      packs.push(pack);
      pack = [];
      bytes = 0;
    }
    pack.push(entry);
    bytes += entry[1].byteLength;
  }
  if (pack.length) packs.push(pack);
  return packs;
}
async function snapshotCursorConversation(value, external) {
  const store = value;
  if (!/^[0-9a-f]{64}$/u.test(store.blobEncryptionKey)) throw new Error("Invalid Cursor blob cache key");
  const snapshot = {
    version: external ? 4 : 2,
    id: store.id,
    blobEncryptionKey: store.blobEncryptionKey,
    resume: store.resume,
    recovery: store.recovery,
    contextTokens: store.contextTokens,
    hostHistory: store.hostHistory,
    notes: store.notes.map((content) => content.map((item) => item.type === "image" ? { ...item, data: Uint8Array.from(item.data) } : item)),
    state: store.state ? toBinary5(ConversationStateStructureSchema, store.state) : void 0,
    blobs: store.blobs.entries()
  };
  let references;
  if (external) {
    references = [];
    for (const pack of blobPacks(snapshot.blobs)) {
      const hash = packHash(pack);
      if (!external.has(hash)) {
        const json2 = JSON.stringify(pack.map(([id, data]) => [Buffer.from(id).toString("base64"), Buffer.from(data).toString("base64")]));
        await external.put(hash, (await compress3(json2)).toString("base64"));
      }
      references.push(hash);
    }
  }
  const json = JSON.stringify({ ...snapshot, ...references ? { blobs: references } : {} }, (_key, item) => item instanceof Uint8Array ? { cursorBytes: Buffer.from(item).toString("base64") } : item);
  return (await compress3(json)).toString("base64");
}
async function restoreCursorConversation(snapshot, external) {
  const bytes = await decompress(Buffer.from(snapshot, "base64"), { maxOutputLength: LIMITS.blobBytes * 2 });
  const value = JSON.parse(bytes.toString("utf8"), (_key, item) => item && typeof item === "object" && typeof item.cursorBytes === "string" && Object.keys(item).length === 1 ? new Uint8Array(Buffer.from(item.cursorBytes, "base64")) : item);
  if (![1, 2, 3, 4].includes(value.version) || typeof value.id !== "string" || !value.id || value.resume !== void 0 && (typeof value.resume.anchor !== "string" || typeof value.resume.finalText !== "string") || value.state !== void 0 && !(value.state instanceof Uint8Array) || value.version === 1 && (!value.resume || !value.state) || value.version >= 2 && (typeof value.blobEncryptionKey !== "string" || !/^[0-9a-f]{64}$/u.test(value.blobEncryptionKey)) || value.contextTokens !== void 0 && (!Number.isSafeInteger(value.contextTokens) || value.contextTokens <= 0) || value.hostHistory !== void 0 && (!Number.isSafeInteger(value.hostHistory.count) || value.hostHistory.count < 0 || typeof value.hostHistory.digest !== "string") || !Array.isArray(value.blobs) || !Array.isArray(value.notes)) {
    throw new Error("Invalid Cursor conversation snapshot");
  }
  if (value.recovery !== void 0) {
    const recovery = value.recovery;
    if (!value.state || value.resume || typeof recovery.requestKey !== "string" || typeof recovery.checkpointHash !== "string" || !Number.isSafeInteger(recovery.attempts) || recovery.attempts < 0 || !Array.isArray(recovery.output) || recovery.output.some((event) => !event || !["text", "reasoning"].includes(String(event.type)) || typeof event.text !== "string")) {
      throw new Error("Invalid Cursor recovery checkpoint");
    }
  }
  const blobs = new BlobStore();
  if (value.version === 4) {
    for (const hash of value.blobs) {
      if (typeof hash !== "string" || !/^[0-9a-f]{64}$/u.test(hash)) throw new Error("Invalid Cursor blob pack reference");
      const encoded = external?.get(hash);
      if (encoded === void 0) throw new Error("Cursor snapshot blob pack is missing");
      const packed = JSON.parse((await decompress(Buffer.from(encoded, "base64"), { maxOutputLength: LIMITS.blobBytes * 2 })).toString("utf8"));
      if (!Array.isArray(packed) || packed.length > 64 || packed.some((entry) => !Array.isArray(entry) || entry.length !== 2 || entry.some((part) => typeof part !== "string"))) throw new Error("Invalid Cursor blob pack");
      const entries = packed.map(([id, data]) => [new Uint8Array(Buffer.from(id, "base64")), new Uint8Array(Buffer.from(data, "base64"))]);
      if (packHash(entries) !== hash) throw new Error("Cursor blob pack checksum mismatch");
      for (const [id, data] of entries) blobs.set(id, data);
    }
  } else {
    for (const [id, payload] of value.blobs) {
      if (!(id instanceof Uint8Array)) throw new Error("Invalid Cursor snapshot blob id");
      let data = payload;
      if (value.version === 3) {
        if (typeof payload !== "string" || !/^[0-9a-f]{64}$/u.test(payload)) throw new Error("Invalid Cursor snapshot blob reference");
        const encoded = external?.get(payload);
        if (encoded === void 0) throw new Error("Cursor snapshot blob is missing");
        data = new Uint8Array(await decompress(Buffer.from(encoded, "base64"), { maxOutputLength: LIMITS.blobBytes }));
        if (blobHash(data) !== payload) throw new Error("Cursor snapshot blob checksum mismatch");
      }
      if (!(data instanceof Uint8Array)) throw new Error("Invalid Cursor snapshot blob");
      blobs.set(id, data);
    }
  }
  const store = {
    kind: "cursor-conversation",
    id: value.id,
    blobEncryptionKey: value.blobEncryptionKey ?? randomBytes2(32).toString("hex"),
    resume: value.resume,
    recovery: value.recovery,
    contextTokens: value.contextTokens,
    hostHistory: value.hostHistory,
    notes: value.notes,
    blobs,
    state: value.state ? fromBinary3(ConversationStateStructureSchema, value.state) : void 0
  };
  return store;
}
export {
  CursorProtocolError,
  createCursorConversation,
  fetchCursorUsableModels,
  openCursorRun,
  requestUnary,
  resetCursorConversation,
  restoreCursorConversation,
  snapshotCursorConversation
};
