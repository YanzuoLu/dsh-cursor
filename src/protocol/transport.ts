/** Direct Run transport and shared Connect framing. No listener or subprocess. */
import { randomUUID } from 'node:crypto'
import { connect, constants } from 'node:http2'
import type { ClientHttp2Session, ClientHttp2Stream, IncomingHttpHeaders } from 'node:http2'
import { gzip, gunzipSync } from 'node:zlib'
import { promisify } from 'node:util'
import { attributionHeaders } from '@deepseek-ai/dsh-llm'
import type { CursorTransportOptions } from '../contracts.js'
import { CursorProtocolError, LIMITS, httpError, limitError, safeError, statusError } from './errors.js'

export const RUN_PATH = '/agent.v1.AgentService/Run'
export const DEFAULT_ENDPOINT = 'https://api2.cursor.sh'
export const CLIENT_VERSION = 'cli-2026.09.02-c22c1a3'
const compress = promisify(gzip)
const COMPRESS_MIN_BYTES = 1024
const FORBIDDEN = new Set(['connection', 'keep-alive', 'proxy-connection', 'transfer-encoding', 'upgrade', 'http2-settings', 'host', 'content-length', 'te', 'content-type', 'connect-protocol-version', 'authorization', 'x-cursor-agent-allowed-tools'])

export function protocolHeaders(options: CursorTransportOptions): Record<string, string> {
  const headers: Record<string, string> = {
    'x-cursor-client-type': 'cli',
    'x-cursor-client-version': CLIENT_VERSION,
    'x-ghost-mode': 'true',
    'x-request-id': randomUUID(),
    'x-dsh-plugin': 'dsh-cursor/0.1.0',
  }
  for (const [name, value] of Object.entries(options.headers ?? {})) {
    const field = name.toLowerCase()
    if (!field.startsWith(':') && !FORBIDDEN.has(field)) headers[field] = value
  }
  // The native DSH identity wins. Do not replace its User-Agent afterwards.
  for (const [name, value] of Object.entries(attributionHeaders())) headers[name.toLowerCase()] = value
  headers.authorization = `Bearer ${options.accessToken}`
  headers['x-cursor-agent-allowed-tools'] = 'mcp_tool_call,get_mcp_tools_tool_call'
  return headers
}

export function validateTransport(options: CursorTransportOptions): string {
  if (options.proxyUrl !== undefined) throw new CursorProtocolError('UNSUPPORTED_PROXY', 'Cursor HTTP/2 proxy transport is not supported')
  if (options.signal?.aborted) throw new CursorProtocolError('ABORTED', 'Cursor Run was aborted')
  let endpoint: URL
  try { endpoint = new URL(options.endpoint ?? DEFAULT_ENDPOINT) }
  catch { throw new CursorProtocolError('INVALID_REQUEST', 'Invalid Cursor endpoint') }
  if (!['http:', 'https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password || endpoint.pathname !== '/' || endpoint.search || endpoint.hash) {
    throw new CursorProtocolError('INVALID_REQUEST', 'Cursor endpoint must be an HTTP(S) origin')
  }
  return endpoint.origin
}

/** Node's stable error code only; never the message, which may echo request data. */
function nodeCode(error: unknown): string {
  return error && typeof error === 'object' && 'code' in error && typeof error.code === 'string' ? error.code : 'unknown'
}

export function frameMessage(payload: Uint8Array, flags = 0): Buffer {
  if (payload.byteLength > LIMITS.frameBytes) throw limitError()
  const frame = Buffer.allocUnsafe(payload.byteLength + 5)
  frame[0] = flags
  frame.writeUInt32BE(payload.byteLength, 1)
  frame.set(payload, 5)
  return frame
}

export interface ConnectFrame { flags: number; payload: Buffer }
export interface RunChannel {
  readonly ready: Promise<void>
  /** Buffered KV writes may return on bounded admission; other writes await delivery. */
  write(frame: Uint8Array, buffered?: boolean): Promise<void>
  frames(): AsyncIterable<ConnectFrame>
  /** Complete buffered replies before publishing a terminal turn. */
  flush?(): Promise<void>
  close(error?: CursorProtocolError): void
}

/** Allocate only after validating a complete five-byte header. */
export class FrameDecoder {
  #header = Buffer.alloc(5)
  #headerUsed = 0
  #payload?: Buffer
  #payloadUsed = 0
  #flags = 0
  push(chunk: Uint8Array): ConnectFrame[] {
    const frames: ConnectFrame[] = []
    let offset = 0
    while (offset < chunk.byteLength) {
      if (!this.#payload) {
        const count = Math.min(5 - this.#headerUsed, chunk.byteLength - offset)
        this.#header.set(chunk.subarray(offset, offset + count), this.#headerUsed)
        this.#headerUsed += count
        offset += count
        if (this.#headerUsed < 5) break
        this.#flags = this.#header[0]!
        if ((this.#flags & ~3) !== 0) throw new CursorProtocolError('INVALID_RESPONSE', 'Unsupported Cursor frame flags')
        const length = this.#header.readUInt32BE(1)
        if (length > LIMITS.frameBytes) throw limitError()
        this.#payload = Buffer.allocUnsafe(length)
        this.#payloadUsed = 0
      }
      const count = Math.min(this.#payload.length - this.#payloadUsed, chunk.byteLength - offset)
      this.#payload.set(chunk.subarray(offset, offset + count), this.#payloadUsed)
      this.#payloadUsed += count
      offset += count
      if (this.#payloadUsed === this.#payload.length) {
        frames.push({ flags: this.#flags, payload: this.#payload })
        this.#payload = undefined
        this.#headerUsed = 0
      }
    }
    return frames
  }
  finish(): void {
    if (this.#headerUsed !== 0 || this.#payload) throw new CursorProtocolError('INVALID_RESPONSE', 'Cursor sent an incomplete frame')
  }
}

export function terminalError(payload: Uint8Array): void {
  let result: unknown
  try { result = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(payload)) }
  catch { throw new CursorProtocolError('INVALID_RESPONSE', 'Invalid Cursor end-stream envelope') }
  if (!result || typeof result !== 'object' || Array.isArray(result)) throw new CursorProtocolError('INVALID_RESPONSE', 'Invalid Cursor end-stream envelope')
  if ('error' in result && result.error !== undefined) {
    const error = result.error
    const code = error && typeof error === 'object' && 'code' in error && typeof error.code === 'string' ? error.code : ''
    const message = error && typeof error === 'object' && 'message' in error ? error.message : undefined
    throw statusError(code, message)
  }
}

export class H2Channel {
  readonly session!: ClientHttp2Session
  readonly stream!: ClientHttp2Stream
  readonly ready: Promise<void>
  #readyResolve!: () => void
  #readyReject!: (error: Error) => void
  #readySettled = false
  #handshake?: NodeJS.Timeout
  #signal?: AbortSignal
  #onAbort: () => void
  #failure?: CursorProtocolError
  #closed = false
  #streaming: boolean
  #trace?: CursorTransportOptions['trace']
  #writes = 0
  #writeBytes = 0
  #writeTail: Promise<void> = Promise.resolve()
  #responseHeaders: IncomingHttpHeaders = {}
  #trailers: IncomingHttpHeaders = {}

  constructor(path: string, options: CursorTransportOptions, streaming: boolean) {
    const origin = validateTransport(options)
    this.#streaming = streaming
    this.#trace = options.trace
    if (!path.startsWith('/') || path.startsWith('//') || /[\r\n?#]/u.test(path)) throw new CursorProtocolError('INVALID_REQUEST', 'Invalid Cursor RPC path')
    this.#onAbort = () => this.close(new CursorProtocolError('ABORTED', 'Cursor request was aborted'))
    this.#signal = options.signal
    this.ready = new Promise((resolve, reject) => { this.#readyResolve = resolve; this.#readyReject = reject })
    // An error may arrive before the caller starts awaiting the opening write.
    void this.ready.catch(() => {})
    try {
      this.session = connect(origin)
      this.session.on('error', error => this.close(new CursorProtocolError('TRANSPORT', `Cursor HTTP/2 session failed (${nodeCode(error)})`)))
      this.stream = this.session.request({
        ...protocolHeaders(options),
        ':method': 'POST', ':path': path,
        'content-type': streaming ? 'application/connect+proto' : 'application/proto',
        'connect-protocol-version': '1',
        ...(streaming ? { 'connect-accept-encoding': 'gzip', 'connect-content-encoding': 'gzip' } : {}),
        te: 'trailers',
      })
    } catch {
      this.session?.destroy()
      throw new CursorProtocolError('TRANSPORT', 'Cursor HTTP/2 request could not be opened')
    }
    this.stream.on('error', error => this.close(new CursorProtocolError('TRANSPORT', `Cursor HTTP/2 stream failed (${nodeCode(error)}, rst ${this.stream.rstCode ?? 'none'})`)))
    this.stream.on('response', headers => {
      this.#responseHeaders = headers
      const status = Number(headers[':status'] ?? 0)
      if (status < 200 || status >= 300) { this.close(httpError(status)); return }
      const contentType = String(headers['content-type'] ?? '').split(';')[0]
      if (contentType !== (streaming ? 'application/connect+proto' : 'application/proto')) {
        this.close(new CursorProtocolError('INVALID_RESPONSE', 'Unexpected Cursor response content type'))
        return
      }
      this.#settleReady()
    })
    this.stream.on('trailers', headers => { this.#trailers = headers })
    this.stream.once('close', () => {
      if (!this.#readySettled) this.#settleReady(this.#failure ?? new CursorProtocolError('TRANSPORT', 'Cursor stream closed before response headers'))
    })
    this.#handshake = setTimeout(() => this.close(new CursorProtocolError('TIMEOUT', 'Cursor response header timeout')), LIMITS.handshakeMs)
    this.#handshake.unref()
    this.#signal?.addEventListener('abort', this.#onAbort, { once: true })
    if (this.#signal?.aborted) this.#onAbort()
  }

  #settleReady(error?: Error): void {
    if (this.#readySettled) return
    this.#readySettled = true
    clearTimeout(this.#handshake)
    this.#handshake = undefined
    if (error) this.#readyReject(error)
    else this.#readyResolve()
  }

  /** Serialized, bounded writes await both the write callback and drain. */
  write(data: Uint8Array): Promise<void> {
    if (this.#closed) return Promise.reject(this.#failure ?? new CursorProtocolError('CLOSED', 'Cursor connection is closed'))
    if (data.byteLength > LIMITS.queuedWriteBytes || this.#writes >= LIMITS.queuedWrites || this.#writeBytes + data.byteLength > LIMITS.queuedWriteBytes) {
      const error = limitError()
      this.close(error)
      return Promise.reject(error)
    }
    this.#writes++
    this.#writeBytes += data.byteLength
    const bytes = Buffer.from(data)
    const written = this.#writeTail.then(async () => {
      if (this.#closed) throw this.#failure ?? new CursorProtocolError('CLOSED', 'Cursor connection is closed')
      // Compression belongs to the ordered write chain. A small heartbeat or
      // result must never overtake an opening frame still being compressed.
      const started = Date.now()
      const frame = this.#streaming && bytes[0] === 0 && bytes.length - 5 >= COMPRESS_MIN_BYTES
        ? frameMessage(await compress(bytes.subarray(5)), 1) : bytes
      return new Promise<void>((resolve, reject) => {
        if (this.#closed) { reject(this.#failure ?? new CursorProtocolError('CLOSED', 'Cursor connection is closed')); return }
        let callbackDone = false
        let drained = false
        const cleanup = (): void => {
          this.stream.off('drain', onDrain)
          this.stream.off('close', onClose)
        }
        const finish = (): void => {
          if (callbackDone && drained) {
            cleanup()
            this.#trace?.('upload', { bytes: bytes.length, wireBytes: frame.length, elapsedMs: Date.now() - started })
            resolve()
          }
        }
        const onDrain = (): void => { drained = true; finish() }
        const onClose = (): void => { cleanup(); reject(this.#failure ?? new CursorProtocolError('TRANSPORT', 'Cursor connection closed during write')) }
        this.stream.once('close', onClose)
        try {
          const accepted = this.stream.write(frame, error => {
            if (error) { cleanup(); reject(safeError(error)); return }
            callbackDone = true
            finish()
          })
          drained = accepted
          if (!accepted) this.stream.once('drain', onDrain)
        } catch { cleanup(); reject(new CursorProtocolError('TRANSPORT', 'Cursor write failed')) }
      })
    }).finally(() => { this.#writes--; this.#writeBytes -= bytes.length })
    this.#writeTail = written.catch(error => { this.close(safeError(error)) })
    return written
  }

  async *frames(): AsyncGenerator<ConnectFrame> {
    const decoder = new FrameDecoder()
    let ended = false
    try {
      await this.ready
      for await (const chunk of this.stream) {
        if (this.#failure) throw this.#failure
        for (const frame of decoder.push(chunk)) {
          if (ended) throw new CursorProtocolError('INVALID_RESPONSE', 'Cursor sent data after end-stream')
          if ((frame.flags & 1) !== 0) {
            if (this.#responseHeaders['connect-content-encoding'] !== 'gzip') throw new CursorProtocolError('INVALID_RESPONSE', 'Unsupported Cursor frame compression')
            try { frame.payload = gunzipSync(frame.payload, { maxOutputLength: LIMITS.inflatedBytes }) }
            catch { throw new CursorProtocolError('LIMIT_EXCEEDED', 'Invalid or oversized Cursor compressed frame') }
          }
          if ((frame.flags & 2) !== 0) {
            terminalError(frame.payload)
            ended = true
          }
          yield frame
        }
      }
      decoder.finish()
      this.checkTrailers()
      if (this.#failure) throw this.#failure
    } catch (error) { throw this.#failure ?? safeError(error, `Invalid Cursor HTTP/2 response (${nodeCode(error)}, rst ${this.stream.rstCode ?? 'none'})`) }
  }

  get failure(): CursorProtocolError | undefined { return this.#failure }

  checkTrailers(): void {
    if (this.#failure) throw this.#failure
    const status = this.#trailers['grpc-status']
    if (status !== undefined && status !== '0') throw statusError(String(status), this.#trailers['grpc-message'])
  }

  end(): void { if (!this.stream.writableEnded && !this.#closed) this.stream.end() }

  close(failure?: CursorProtocolError): void {
    if (this.#closed) return
    this.#closed = true
    this.#failure = failure
    this.#signal?.removeEventListener('abort', this.#onAbort)
    this.#settleReady(failure ?? new CursorProtocolError('CLOSED', 'Cursor connection is closed'))
    clearTimeout(this.#handshake)
    if (!this.stream.closed && !this.stream.destroyed) this.stream.close(constants.NGHTTP2_CANCEL)
    this.stream.destroy()
    this.session.destroy()
  }
}

/** Raw-protobuf unary metadata retains the host's fetch, HTTP/1 and proxy hooks. */
export async function requestUnary(path: string, body: Uint8Array, options: CursorTransportOptions): Promise<Uint8Array> {
  if (options.unaryTransport === 'http2') return requestUnaryHttp2(path, body, options)
  const origin = validateTransport(options)
  if (!path.startsWith('/') || path.startsWith('//') || path.includes('://')) throw new CursorProtocolError('INVALID_REQUEST', 'Invalid Cursor RPC path')
  if (body.byteLength > LIMITS.frameBytes) throw limitError()
  const timeout = AbortSignal.timeout(LIMITS.unaryMs)
  const signal = options.signal ? AbortSignal.any([options.signal, timeout]) : timeout
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
  let response: Response | undefined
  const abort = () => { void reader?.cancel().catch(() => {}) }
  try {
    response = await globalThis.fetch(`${origin}${path}`, {
      method: 'POST', redirect: 'error', signal,
      headers: { ...protocolHeaders(options), 'content-type': 'application/proto', 'connect-protocol-version': '1' },
      body: Buffer.from(body),
    })
    signal.throwIfAborted()
    if (!response.ok) { void response.body?.cancel().catch(() => {}); throw httpError(response.status) }
    if (!response.body) throw new CursorProtocolError('INVALID_RESPONSE', 'Cursor metadata response has no body')
    reader = response.body.getReader()
    signal.addEventListener('abort', abort, { once: true })
    const chunks: Uint8Array[] = []
    let length = 0
    while (true) {
      signal.throwIfAborted()
      const item = await reader.read()
      signal.throwIfAborted()
      if (item.done) break
      length += item.value.byteLength
      if (length > LIMITS.frameBytes) { await reader.cancel(); throw limitError() }
      chunks.push(item.value)
    }
    return new Uint8Array(Buffer.concat(chunks, length))
  } catch (error) {
    if (options.signal?.aborted) throw new CursorProtocolError('ABORTED', 'Cursor metadata request was aborted')
    if (timeout.aborted) throw new CursorProtocolError('TIMEOUT', 'Cursor metadata request timed out')
    throw safeError(error, 'Cursor metadata transport failed')
  } finally {
    signal.removeEventListener('abort', abort)
    if (!reader && signal.aborted) void response?.body?.cancel().catch(() => {})
    reader?.releaseLock()
  }
}

/** Explicit H2 unary option, useful for a caller that owns that transport. */
async function requestUnaryHttp2(path: string, body: Uint8Array, options: CursorTransportOptions): Promise<Uint8Array> {
  validateTransport(options)
  if (body.byteLength > LIMITS.frameBytes) throw limitError()
  const channel = new H2Channel(path, options, false)
  const timer = setTimeout(() => channel.close(new CursorProtocolError('TIMEOUT', 'Cursor unary request timeout')), LIMITS.unaryMs)
  timer.unref()
  try {
    await channel.write(body)
    channel.end()
    await channel.ready
    const chunks: Buffer[] = []
    let length = 0
    for await (const chunk of channel.stream) {
      length += chunk.length
      if (length > LIMITS.frameBytes) throw limitError()
      chunks.push(chunk)
    }
    channel.checkTrailers()
    return new Uint8Array(Buffer.concat(chunks, length))
  } catch (error) { throw channel.failure ?? safeError(error) }
  finally { clearTimeout(timer); channel.close() }
}
