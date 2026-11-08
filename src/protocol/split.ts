import { Agent as HttpsAgent, request as httpsRequest } from 'node:https'
import { Agent as HttpAgent, request as httpRequest } from 'node:http'
import type { ClientRequest, IncomingMessage } from 'node:http'
import { randomUUID } from 'node:crypto'
import { promisify } from 'node:util'
import { gzip, gunzipSync } from 'node:zlib'
import type { CursorTransportOptions } from '../contracts.js'
import { CursorProtocolError, LIMITS, httpError, limitError, safeError, statusError } from './errors.js'
import { FrameDecoder, frameMessage, protocolHeaders, terminalError, validateTransport } from './transport.js'
import type { ConnectFrame, RunChannel } from './transport.js'

const compress = promisify(gzip)
export const SPLIT_LIMITS = Object.freeze({ parallel: 16, sequenceWindow: 64, replyBytes: 256 * 1024 })
export const RUN_STREAM_PATH = '/agent.v1.AgentService/RunSSE'
export const APPEND_PATH = '/aiserver.v1.BidiService/BidiAppend'

function varint(value: number): Buffer {
  const bytes: number[] = []
  do { const low = value % 128; value = Math.floor(value / 128); bytes.push(low | (value ? 128 : 0)) } while (value)
  return Buffer.from(bytes)
}
function bytesField(field: number, bytes: Uint8Array): Buffer {
  return Buffer.concat([varint(field * 8 + 2), varint(bytes.length), bytes])
}
/** SDK wire contract: BidiRequestId #1, BidiAppendRequest #2/#3/#4. */
export function encodeRequestId(id: string): Buffer { return bytesField(1, Buffer.from(id)) }
export function encodeAppend(id: string, sequence: number, payload: Uint8Array): Buffer {
  return Buffer.concat([bytesField(2, encodeRequestId(id)),
    ...(sequence ? [Buffer.concat([Buffer.from([24]), varint(sequence)])] : []), bytesField(4, payload)])
}

/** One native Run, with ordered uploads on bounded parallel HTTP requests. */
export class SplitChannel implements RunChannel {
  readonly ready: Promise<void>
  #resolveReady!: () => void
  #rejectReady!: (error: Error) => void
  #readySettled = false
  #response?: IncomingMessage
  #origin: string
  #id = randomUUID()
  #headers: Record<string, string>
  #agent: HttpsAgent | HttpAgent
  #request: typeof httpRequest
  #trace?: CursorTransportOptions['trace']
  #signal?: AbortSignal
  #onAbort: () => void
  #closed = false
  #failure?: CursorProtocolError
  #requests = new Set<ClientRequest>()
  #timers = new Set<NodeJS.Timeout>()
  #nextSequence = 0
  #acknowledgedThrough = 0
  #acknowledged = new Set<number>()
  #pending = new Set<Promise<void>>()
  #waiters = new Set<() => void>()
  #admission: Promise<void> = Promise.resolve()
  #writes = 0
  #bytes = 0

  constructor(options: CursorTransportOptions) {
    this.#origin = validateTransport(options)
    this.#trace = options.trace
    this.#signal = options.signal
    const secure = this.#origin.startsWith('https:')
    this.#request = secure ? httpsRequest : httpRequest
    this.#agent = secure ? new HttpsAgent({ keepAlive: true, maxSockets: SPLIT_LIMITS.parallel + 1 })
      : new HttpAgent({ keepAlive: true, maxSockets: SPLIT_LIMITS.parallel + 1 })
    this.#headers = { ...protocolHeaders(options), 'x-request-id': this.#id,
      'x-cursor-streaming': 'true', 'connect-protocol-version': '1' }
    this.ready = new Promise((resolve, reject) => { this.#resolveReady = resolve; this.#rejectReady = reject })
    void this.ready.catch(() => {})
    this.#onAbort = () => this.close(new CursorProtocolError('ABORTED', 'Cursor Run was aborted'))
    this.#signal?.addEventListener('abort', this.#onAbort, { once: true })
    if (this.#signal?.aborted) { this.#onAbort(); return }
    this.#open()
  }

  #timer(action: () => void, ms: number): NodeJS.Timeout {
    const timer = setTimeout(() => { this.#timers.delete(timer); action() }, ms)
    timer.unref(); this.#timers.add(timer)
    return timer
  }
  #clear(timer: NodeJS.Timeout): void { clearTimeout(timer); this.#timers.delete(timer) }
  #settleReady(error?: Error): void {
    if (this.#readySettled) return
    this.#readySettled = true
    if (error) this.#rejectReady(error); else this.#resolveReady()
  }
  #track(request: ClientRequest): void {
    this.#requests.add(request)
    request.once('close', () => this.#requests.delete(request))
  }
  #open(): void {
    const body = frameMessage(encodeRequestId(this.#id))
    const req = this.#request(new URL(RUN_STREAM_PATH, this.#origin), {
      method: 'POST', agent: this.#agent, headers: { ...this.#headers,
        'content-type': 'application/connect+proto', 'connect-accept-encoding': 'gzip', 'content-length': String(body.length) },
    }, response => {
      this.#clear(timer)
      this.#response = response
      response.on('error', () => this.close(new CursorProtocolError('TRANSPORT', 'Cursor split response failed')))
      if (this.#closed) { response.destroy(); return }
      if (response.statusCode !== 200) { this.close(httpError(response.statusCode ?? 0)); return }
      // Cursor's SDK accepts binary Connect frames under this SSE content type.
      if (!['application/connect+proto', 'text/event-stream'].includes(String(response.headers['content-type']).split(';')[0]!)) {
        this.close(new CursorProtocolError('INVALID_RESPONSE', 'Unexpected Cursor split response content type')); return
      }
      this.#settleReady()
    })
    this.#track(req)
    const timer = this.#timer(() => this.close(new CursorProtocolError('TIMEOUT', 'Cursor split response header timeout')), LIMITS.handshakeMs)
    req.on('error', () => this.close(new CursorProtocolError('TRANSPORT', 'Cursor split response could not be opened')))
    req.once('close', () => {
      this.#clear(timer)
      if (!this.#readySettled) this.close(new CursorProtocolError('TRANSPORT', 'Cursor split response closed before headers'))
    })
    req.end(body)
  }

  /** KV data can pipeline. Tools/actions await their individual delivery acknowledgement. */
  write(frame: Uint8Array, buffered = false): Promise<void> {
    if (this.#closed) return Promise.reject(this.#failure!)
    if (frame.length < 5 || frame[0] !== 0 || Buffer.from(frame).readUInt32BE(1) !== frame.length - 5) {
      throw new Error('SplitChannel requires one uncompressed Connect message')
    }
    if (frame.length - 5 > LIMITS.frameBytes || this.#writes >= LIMITS.queuedWrites || this.#bytes + frame.length > LIMITS.queuedWriteBytes) {
      const error = limitError(); this.close(error); return Promise.reject(error)
    }
    const payload = Buffer.from(frame.subarray(5))
    this.#writes++; this.#bytes += frame.length
    let delivered: Promise<void> | undefined
    const admitted = this.#admission.then(async () => {
      while (!this.#closed && (this.#pending.size >= SPLIT_LIMITS.parallel
        || this.#nextSequence - this.#acknowledgedThrough >= SPLIT_LIMITS.sequenceWindow)) {
        await new Promise<void>(resolve => this.#waiters.add(resolve))
      }
      if (this.#closed) throw this.#failure
      const sequence = this.#nextSequence++
      const sent = this.#append(sequence, payload).then(() => {
        this.#acknowledged.add(sequence)
        while (this.#acknowledged.delete(this.#acknowledgedThrough)) this.#acknowledgedThrough++
      }).catch(error => {
        const failure = this.#failure ?? safeError(error, 'Cursor append delivery failed')
        this.close(failure); throw failure
      })
      delivered = sent.finally(() => {
        this.#pending.delete(delivered!)
        this.#writes--; this.#bytes -= frame.length
        this.#wake()
      })
      this.#pending.add(delivered)
      // Buffered writers receive eventual errors through the response reader.
      void delivered.catch(() => {})
    }).catch(error => {
      if (!delivered) { this.#writes--; this.#bytes -= frame.length }
      throw error
    })
    this.#admission = admitted.catch(() => {})
    return buffered ? admitted : admitted.then(() => delivered!)
  }

  async #append(sequence: number, payload: Buffer): Promise<void> {
    const raw = encodeAppend(this.#id, sequence, payload)
    const compressed = raw.length >= 1024
    const body = compressed ? await compress(raw) : raw
    if (this.#closed) throw this.#failure
    const timeoutMs = 60_000 + Math.ceil(raw.length / 131_072 * 1000)
    const started = Date.now()
    await new Promise<void>((resolve, reject) => {
      const req = this.#request(new URL(APPEND_PATH, this.#origin), {
        method: 'POST', agent: this.#agent, headers: { ...this.#headers,
          'content-type': 'application/proto', 'content-length': String(body.length),
          'connect-timeout-ms': String(timeoutMs), ...(compressed ? { 'content-encoding': 'gzip' } : {}),
        },
      }, response => {
        const chunks: Buffer[] = []
        let length = 0
        response.on('data', (chunk: Buffer) => {
          length += chunk.length
          if (length > SPLIT_LIMITS.replyBytes) { this.close(limitError()); return }
          chunks.push(chunk)
        })
        response.on('error', () => reject(this.#failure ?? new CursorProtocolError('TRANSPORT', 'Cursor append response failed')))
        response.on('end', () => {
          this.#clear(timer)
          if (this.#closed) { reject(this.#failure); return }
          if (response.statusCode !== 200) {
            let error = httpError(response.statusCode ?? 0)
            try {
              const detail = JSON.parse(Buffer.concat(chunks).toString('utf8')) as { code?: unknown; message?: unknown }
              if (typeof detail.code === 'string') error = statusError(detail.code, detail.message)
            } catch { /* Non-JSON errors retain their HTTP classification. */ }
            reject(error); return
          }
          if (String(response.headers['content-type']).split(';')[0] !== 'application/proto' || length !== 0) {
            reject(new CursorProtocolError('INVALID_RESPONSE', 'Invalid Cursor append acknowledgement')); return
          }
          this.#trace?.('upload', { bytes: payload.length, wireBytes: body.length, elapsedMs: Date.now() - started })
          resolve()
        })
      })
      this.#track(req)
      const timer = this.#timer(() => {
        const error = new CursorProtocolError('TIMEOUT', 'Cursor append delivery timeout')
        this.close(error); reject(error)
      }, timeoutMs)
      req.on('error', () => reject(this.#failure ?? new CursorProtocolError('TRANSPORT', 'Cursor append request failed')))
      req.once('close', () => {
        this.#clear(timer)
        reject(this.#failure ?? new CursorProtocolError('TRANSPORT', 'Cursor append closed before acknowledgement'))
      })
      req.end(body)
    })
  }

  async *frames(): AsyncGenerator<ConnectFrame> {
    const decoder = new FrameDecoder()
    let ended = false
    try {
      await this.ready
      for await (const chunk of this.#response!) {
        if (this.#failure) throw this.#failure
        for (const frame of decoder.push(chunk)) {
          if (ended) throw new CursorProtocolError('INVALID_RESPONSE', 'Cursor sent data after end-stream')
          if (frame.flags & 1) {
            if (this.#response!.headers['connect-content-encoding'] !== 'gzip') throw new CursorProtocolError('INVALID_RESPONSE', 'Unsupported Cursor frame compression')
            try { frame.payload = gunzipSync(frame.payload, { maxOutputLength: LIMITS.inflatedBytes }) }
            catch { throw limitError() }
          }
          if (frame.flags & 2) { terminalError(frame.payload); ended = true }
          yield frame
        }
      }
      decoder.finish()
      if (this.#failure) throw this.#failure
    } catch (error) { throw this.#failure ?? safeError(error, 'Cursor split response failed') }
  }

  async flush(): Promise<void> {
    await this.#admission
    await Promise.all(this.#pending)
    if (this.#failure) throw this.#failure
  }

  #wake(): void { for (const wake of this.#waiters) wake(); this.#waiters.clear() }
  close(error = new CursorProtocolError('CLOSED', 'Cursor Run is closed')): void {
    if (this.#closed) return
    this.#closed = true; this.#failure = error
    this.#signal?.removeEventListener('abort', this.#onAbort)
    this.#settleReady(error)
    for (const timer of this.#timers) clearTimeout(timer)
    this.#timers.clear(); this.#wake()
    for (const req of this.#requests) req.destroy()
    this.#response?.destroy(); this.#agent.destroy()
  }
}
