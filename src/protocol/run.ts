import { createHash, randomBytes, randomUUID } from 'node:crypto'
import { create, fromBinary, toBinary, toJson } from '@bufbuild/protobuf'
import type { CursorContent, CursorConversation, CursorMessage, CursorRun, CursorRunRequest, CursorToolResult, CursorTransportOptions, CursorUsableModel, CursorUsage } from '../contracts.js'
import { BlobStore } from './blobs.js'
import { cursorRunEndpoint } from './endpoint.js'
import { decodeArguments, mcpContent } from './content.js'
import { advertisedName, contextResponse, execResponse, kvResponse, mcpStateResponse, rejectInteraction, rejectNative, toolDefinitions } from './control.js'
import { RunEvents } from './events.js'
import { CursorProtocolError, LIMITS, limitError, safeError } from './errors.js'
import { activeUserIndex, buildResumeRequest, buildRunRequest, messagesDigest, userMessage } from './history.js'
import { WireAgentClientMessageSchema, WireAgentServerMessageSchema } from './schema.js'
import type { WireAgentServerMessage, WireInteractionUpdate, WireModelCallCompleted, WireTurnEnded } from './schema.js'
import { H2Channel, RUN_PATH, DEFAULT_ENDPOINT, frameMessage, requestUnary, validateTransport } from './transport.js'
import type { RunChannel } from './transport.js'
import { SplitChannel } from './split.js'
import {
  AgentClientMessageSchema, ConversationStateStructureSchema, ClientHeartbeatSchema, ExecClientControlMessageSchema, ExecClientHeartbeatSchema, ExecClientStreamCloseSchema, ExecServerMessageSchema, GetUsableModelsRequestSchema,
  GetUsableModelsResponseSchema, McpAllowlistPrecheckResultSchema, McpApprovedSchema,
  McpRejectedSchema, McpResultSchema, McpToolNotFoundSchema,
  ShellAllowlistPrecheckResultSchema, ThinkingDetailsSchema, WebFetchAllowlistPrecheckResultSchema,
} from './vendor/agent_pb.js'
import type { AgentClientMessage, AgentRunRequest, ConversationStateStructure, ExecServerMessage, McpToolDefinition } from './vendor/agent_pb.js'

/**
 * Cursor's token counters. Its input includes cached reads and writes; the
 * returned input is disjoint from them, as native DSH input is.
 */
function tokenCounters(counters: WireTurnEnded | WireModelCallCompleted): Partial<CursorUsage> {
  const usage: Partial<CursorUsage> = {}
  for (const name of ['inputTokens', 'outputTokens', 'cacheReadTokens', 'cacheWriteTokens', 'reasoningTokens'] as const) {
    const value = counters[name]
    if (value === undefined) continue
    if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) throw new CursorProtocolError('INVALID_RESPONSE', 'Invalid Cursor token count')
    usage[name] = Number(value)
  }
  if (usage.inputTokens !== undefined) {
    usage.inputTokens = Math.max(0, usage.inputTokens - (usage.cacheReadTokens ?? 0) - (usage.cacheWriteTokens ?? 0))
  }
  return usage
}

/** Owns one wire Run only. Session/account eviction belongs to its coordinator. */
class CursorConnection implements CursorRun {
  readonly events = new RunEvents()
  #lifetime = new AbortController()
  #channel: RunChannel
  #blobs: BlobStore
  #definitions: McpToolDefinition[]
  #names: ReadonlySet<string>
  #cwd?: string
  #instructions?: string
  #pending = new Map<string, { exec: ExecServerMessage; bytes: number }>()
  #pendingBytes = 0
  #seen = new Set<string>()
  #closed = false
  #failure?: CursorProtocolError
  #heartbeat?: NodeJS.Timeout
  #execHeartbeat?: NodeJS.Timeout
  #idle?: NodeJS.Timeout
  #pump: Promise<void> = Promise.resolve()
  #signal?: AbortSignal
  #onAbort: () => void
  #usage: CursorUsage = { inputTokens: 0, outputTokens: 0 }
  #haveUsage = false
  #trace?: CursorTransportOptions['trace']
  #kvGets = 0
  #firstText = false
  #firstThinking = false
  #ended?: WireTurnEnded
  #terminalCheckpoint = false
  #save?: CursorRunRequest['onCheckpoint']
  #persistence: Promise<void> = Promise.resolve()
  #recoveryBarrier = false
  #stepOutput: { type: 'text' | 'reasoning'; text: string }[] = []
  #outputBytes = 0
  /** Exact host request whose uncommitted output this Run may recover. */
  requestKey = ''

  constructor(request: CursorRunRequest, options: CursorTransportOptions, blobs: BlobStore, definitions: McpToolDefinition[]) {
    this.#blobs = blobs
    const conversation = request.conversation as ConversationStore | undefined
    const context = conversation?.contextTokens ?? conversation?.state?.tokenDetails?.usedTokens
    if (context !== undefined && context > 0) this.#usage.contextTokens = context
    this.#definitions = definitions
    this.#names = new Set(request.tools.map(tool => tool.name))
    this.#cwd = request.cwd
    this.#instructions = request.messages.filter(message => message.role === 'system' || message.role === 'developer')
      .map(message => message.content.filter(item => item.type === 'text').map(item => item.text).join('\n')).filter(Boolean).join('\n\n')
    this.#signal = options.signal
    this.#trace = options.trace
    this.#save = request.onCheckpoint
    this.#onAbort = () => this.#fail(new CursorProtocolError('ABORTED', 'Cursor Run was aborted'))
    const transport = { ...options, signal: this.#lifetime.signal }
    this.#channel = options.runTransport === 'split' ? new SplitChannel(transport) : new H2Channel(RUN_PATH, transport, true)
    this.#signal?.addEventListener('abort', this.#onAbort, { once: true })
    if (this.#signal?.aborted) this.#onAbort()
  }

  /** Shared conversation whose checkpoint this Run advances, when one is supplied. */
  conversation?: ConversationStore
  /** Anchor of the host history this Run started from. */
  anchor = ''
  /** Text of the current model step, reset when the host answers its tools. */
  #stepText: string[] = []
  /** Whether the latest checkpoint arrived after the latest model output. */
  #checkpointCurrent = false
  /** Injections Cursor has not yet taken into the turn. */
  #injections = new Map<string, CursorContent[]>()

  /** Identifies this Run to context injections. */
  readonly runId = randomUUID()

  #persist(): Promise<void> {
    if (!this.conversation || !this.#save) return this.#persistence
    const save = this.#save, conversation = this.conversation
    this.#persistence = this.#persistence.then(() => save(conversation))
    // A signal callback cannot await. cancel/close and the reader join this chain.
    void this.#persistence.catch(() => {})
    return this.#persistence
  }

  #rememberOutput(type: 'text' | 'reasoning', text: string): void {
    this.#outputBytes += Buffer.byteLength(text)
    if (this.#outputBytes > LIMITS.eventBytes) {
      this.#recoveryBarrier = true
      if (this.conversation) this.conversation.recovery = undefined
      this.#stepOutput = []
      return
    }
    const last = this.#stepOutput.at(-1)
    if (last?.type === type) last.text += text
    else this.#stepOutput.push({ type, text })
  }

  replayOutput(output: readonly { type: 'text' | 'reasoning'; text: string }[]): void {
    for (const event of output) {
      this.#rememberOutput(event.type, event.text)
      if (event.type === 'text') this.#stepText.push(event.text)
      this.events.push({ ...event })
    }
  }

  async start(request: AgentRunRequest): Promise<void> {
    ;(request as AgentRunRequest & { runId: string }).runId = this.runId
    this.#pump = this.#read().catch(error => { if (!this.#closed) this.#fail(safeError(error, 'Invalid Cursor Run response')) })
    try {
      await this.#write(create(AgentClientMessageSchema, { message: { case: 'runRequest', value: request } }))
      this.#trace?.('opening-sent')
      await this.#channel.ready
      this.#trace?.('headers')
      if (this.#failure) throw this.#failure
      if (!this.#closed) {
        this.#heartbeat = setInterval(() => {
          void this.#write(create(AgentClientMessageSchema, { message: { case: 'clientHeartbeat', value: create(ClientHeartbeatSchema) } }))
            .catch(error => this.#fail(safeError(error)))
        }, LIMITS.heartbeatMs)
        this.#heartbeat.unref()
        this.#execHeartbeat = setInterval(() => {
          for (const { exec } of this.#pending.values()) {
            void this.#write(create(AgentClientMessageSchema, {
              message: { case: 'execClientControlMessage', value: create(ExecClientControlMessageSchema, {
                message: { case: 'heartbeat', value: create(ExecClientHeartbeatSchema, { id: exec.id }) },
              }) },
            })).catch(error => this.#fail(safeError(error)))
          }
        }, 3000)
        this.#execHeartbeat.unref()
        this.#armIdle()
      }
    } catch (error) {
      const failure = this.#failure ?? safeError(error)
      this.#fail(failure)
      throw failure
    }
  }

  async #write(message: AgentClientMessage): Promise<void> {
    if (this.#closed) throw this.#failure ?? new CursorProtocolError('CLOSED', 'Cursor Run is closed')
    const written = this.#channel.write(frameMessage(toBinary(WireAgentClientMessageSchema, message)), message.message.case === 'kvClientMessage')
    // Reserve both ordered writes before awaiting either acknowledgement. A
    // fast TurnEnded must not close the Run before the exec close is admitted.
    if (message.message.case === 'execClientMessage') {
      const closed = this.#channel.write(frameMessage(toBinary(WireAgentClientMessageSchema, create(AgentClientMessageSchema, {
        message: { case: 'execClientControlMessage', value: create(ExecClientControlMessageSchema, {
          message: { case: 'streamClose', value: create(ExecClientStreamCloseSchema, { id: message.message.value.id }) },
        }) },
      }))))
      await Promise.all([written, closed])
    } else await written
  }

  #armIdle(): void {
    if (this.#ended) return
    clearTimeout(this.#idle)
    this.#idle = undefined
    // Host tools can legitimately run longer than a model's idle timeout.
    if (this.#closed || this.#ended || this.#pending.size) return
    this.#idle = setTimeout(() => this.#fail(new CursorProtocolError('TIMEOUT', 'Cursor Run response timeout')), LIMITS.idleMs)
    this.#idle.unref()
  }

  #dispose(): void {
    if (this.#closed) return
    this.#closed = true
    clearInterval(this.#heartbeat)
    clearInterval(this.#execHeartbeat)
    clearTimeout(this.#idle)
    this.#heartbeat = undefined
    this.#execHeartbeat = undefined
    this.#idle = undefined
    this.#signal?.removeEventListener('abort', this.#onAbort)
    this.#pending.clear()
    this.#pendingBytes = 0
    this.#seen.clear()
    // A conversation store outlives its Runs; only a Run-local store is freed here.
    if (!this.conversation) this.#blobs.clear()
    this.#definitions = []
    this.#lifetime.abort()
    this.#channel.close()
  }

  #fail(error: CursorProtocolError): void {
    if (this.#closed) return
    this.#failure = error
    if (error.code === 'ABORTED' || error.code === 'CLOSED') {
      if (this.conversation) this.conversation.recovery = undefined
      void this.#persist()
    }
    this.events.finish(error)
    this.#dispose()
  }

  async #read(): Promise<void> {
    try {
      for await (const frame of this.#channel.frames()) {
        if (this.#closed) return
        if ((frame.flags & 2) !== 0) break
        let message: WireAgentServerMessage
        try { message = fromBinary(WireAgentServerMessageSchema, frame.payload) }
        catch { throw new CursorProtocolError('INVALID_RESPONSE', 'Invalid Cursor protobuf response') }
        this.#armIdle()
        await this.#handle(message)
        if (this.#closed) return
      }
    } catch (error) {
      // A fully acknowledged terminal checkpoint is final, not a retry point.
      if (!this.#ended || !this.#terminalCheckpoint || !(error instanceof CursorProtocolError)
        || !['TRANSPORT', 'TIMEOUT'].includes(error.code)) throw error
    }
    if (this.#closed) return
    if (!this.#ended) throw new CursorProtocolError('TRANSPORT', 'Cursor stream ended before turnEnded')
    await this.#completeTurn()
  }

  async #completeTurn(): Promise<void> {
    if (!this.#ended) throw new Error('Cannot complete an unterminated Cursor turn')
    clearInterval(this.#heartbeat)
    clearInterval(this.#execHeartbeat)
    // Flush normally while the transport is live. A final checkpoint already
    // proves completion if the server then closes the transport abnormally.
    if (!this.#terminalCheckpoint) await this.#channel.flush?.()
    else {
      try { await this.#channel.flush?.() }
      catch (error) {
        if (!(error instanceof CursorProtocolError) || !['TRANSPORT', 'TIMEOUT', 'CLOSED'].includes(error.code)) throw error
      }
    }
    if (this.conversation) {
      this.conversation.resume = this.#checkpointCurrent
        ? { anchor: this.anchor, finalText: textKey([{ type: 'text', text: this.#stepText.join('') }]) } : undefined
      this.conversation.recovery = undefined
      this.conversation.notes.push(...this.#injections.values())
      this.#injections.clear()
      await this.#persist()
    }
    if (this.#haveUsage || this.#usage.contextTokens !== undefined) this.events.push({ type: 'usage', usage: { ...this.#usage } })
    this.events.push({ type: 'done', reason: 'stop' })
    this.events.finish()
    this.#dispose()
  }

  async #handle(message: WireAgentServerMessage): Promise<void> {
    switch (message.message.case) {
      case 'kvServerMessage':
        if (message.message.value.message.case === 'getBlobArgs' && ++this.#kvGets % 100 === 1) this.#trace?.('kv-get', { count: this.#kvGets })
        await this.#write(kvResponse(message.message.value, this.#blobs)); return
      case 'execServerMessage': await this.#exec(message.message.value); return
      case 'interactionQuery': await this.#write(rejectInteraction(message.message.value)); return
      case 'conversationCheckpointUpdate': {
        if (this.conversation) {
          const state = message.message.value
          this.conversation.state = state
          if ((state.tokenDetails?.usedTokens ?? 0) > 0) this.conversation.contextTokens = state.tokenDetails!.usedTokens
          this.#checkpointCurrent = true
          if (this.#ended) {
            this.#terminalCheckpoint = true
            this.conversation.resume = { anchor: this.anchor, finalText: textKey([{ type: 'text', text: this.#stepText.join('') }]) }
            this.conversation.recovery = undefined
          } else if (!this.#recoveryBarrier && this.#pending.size === 0) {
            const checkpointHash = createHash('sha256').update(toBinary(ConversationStateStructureSchema, state)).digest('hex')
            const previous = this.conversation.recovery
            this.conversation.recovery = {
              requestKey: this.requestKey, checkpointHash,
              attempts: previous?.checkpointHash === checkpointHash ? previous.attempts : 0,
              output: this.#stepOutput.map(event => ({ ...event })),
            }
          } else this.conversation.recovery = undefined
          await this.#persist()
        }
        const used = message.message.value.tokenDetails?.usedTokens
        if (used !== undefined && used > 0) {
          // Occupancy of the context Cursor is about to use, per checkpoint. Final
          // TurnEnded counters aggregate every model call of the turn instead.
          this.#usage.contextTokens = used
          this.events.push({ type: 'usage', usage: { ...this.#usage } })
        }
        return
      }
      case 'interactionUpdate': await this.#interaction(message.message.value); return
      case 'execServerControlMessage': {
        if (message.message.value.message.case !== 'abort') throw new CursorProtocolError('UNSUPPORTED_CONTROL', 'Unsupported Cursor execution control request')
        const id = message.message.value.message.value.id
        if ([...this.#pending.values()].some(pending => pending.exec.id === id)) throw new CursorProtocolError('ABORTED', 'Cursor canceled an outstanding tool request')
        return
      }
      default: throw new CursorProtocolError('UNSUPPORTED_CONTROL', 'Unsupported Cursor server message')
    }
  }

  async #exec(exec: ExecServerMessage): Promise<void> {
    switch (exec.message.case) {
      case 'requestContextArgs': this.#trace?.('request-context', { kvGets: this.#kvGets }); await this.#write(contextResponse(exec, this.#definitions, this.#cwd, this.#instructions)); return
      case 'mcpStateExecArgs': await this.#write(mcpStateResponse(exec, this.#definitions)); return
      case 'mcpAllowlistPrecheckArgs':
        await this.#write(execResponse(exec, { case: 'mcpAllowlistPrecheckResult', value: create(McpAllowlistPrecheckResultSchema, { allowlisted: false }) })); return
      case 'shellAllowlistPrecheckArgs':
        await this.#write(execResponse(exec, { case: 'shellAllowlistPrecheckResult', value: create(ShellAllowlistPrecheckResultSchema, { allowlisted: false }) })); return
      case 'webFetchAllowlistPrecheckArgs':
        await this.#write(execResponse(exec, { case: 'webFetchAllowlistPrecheckResult', value: create(WebFetchAllowlistPrecheckResultSchema, { allowlisted: false }) })); return
      case 'mcpArgs': {
        if (this.#ended) throw new CursorProtocolError('INVALID_RESPONSE', 'Cursor requested a tool after turnEnded')
        const args = exec.message.value
        const name = advertisedName(args, this.#names)
        if (args.smartModeApprovalOnly) {
          await this.#write(execResponse(exec, { case: 'mcpResult', value: create(McpResultSchema, { result: name
            ? { case: 'approved', value: create(McpApprovedSchema) }
            : { case: 'rejected', value: create(McpRejectedSchema, { reason: 'The requested tool is not advertised by DeepSeek Harness.' }) },
          }) }))
          return
        }
        if (!name) {
          await this.#write(execResponse(exec, { case: 'mcpResult', value: create(McpResultSchema, {
            result: { case: 'toolNotFound', value: create(McpToolNotFoundSchema, { availableTools: [...this.#names] }) },
          }) }))
          return
        }
        const identity = createHash('sha256').update(JSON.stringify([exec.id, exec.execId])).digest('hex')
        if (this.#seen.has(identity)) throw new CursorProtocolError('INVALID_RESPONSE', 'Cursor repeated an MCP execution envelope')
        const bytes = toBinary(ExecServerMessageSchema, exec).byteLength
        if (this.#pending.size >= LIMITS.pendingCalls || this.#seen.size >= LIMITS.seenCalls || this.#pendingBytes + bytes > LIMITS.pendingBytes) throw limitError()
        const argumentsJSON = decodeArguments(args.args)
        const callKey = randomUUID()
        this.#seen.add(identity)
        // Retain the precise raw envelope, including its original toolCallId.
        this.#pending.set(callKey, { exec, bytes })
        this.#pendingBytes += bytes
        this.#armIdle()
        this.#checkpointCurrent = false
        this.#recoveryBarrier = true
        if (this.conversation) this.conversation.recovery = undefined
        await this.#persist()
        this.events.push({ type: 'tool', callKey, name, arguments: argumentsJSON })
        return
      }
      default: await this.#write(rejectNative(exec))
    }
  }

  async #interaction(update: WireInteractionUpdate): Promise<void> {
    if (this.#ended && ['textDelta', 'thinkingDelta', 'toolRequestsListed'].includes(update.message.case ?? '')) {
      throw new CursorProtocolError('INVALID_RESPONSE', 'Cursor emitted model output after turnEnded')
    }
    switch (update.message.case) {
      case 'textDelta': this.#checkpointCurrent = false; this.#rememberOutput('text', update.message.value.text); this.#stepText.push(update.message.value.text); if (!this.#firstText) { this.#firstText = true; this.#trace?.('first-text', { kvGets: this.#kvGets }) } this.events.push({ type: 'text', text: update.message.value.text }); return
      case 'thinkingDelta': this.#checkpointCurrent = false; this.#rememberOutput('reasoning', update.message.value.text); if (!this.#firstThinking) { this.#firstThinking = true; this.#trace?.('first-thinking', { kvGets: this.#kvGets }) } this.events.push({ type: 'reasoning', text: update.message.value.text }); return
      case 'tokenDelta': {
        const total = this.#usage.outputTokens + update.message.value.tokens
        if (update.message.value.tokens < 0 || !Number.isSafeInteger(total)) throw new CursorProtocolError('INVALID_RESPONSE', 'Invalid Cursor token count')
        this.#usage.outputTokens = total
        this.#haveUsage = true
        return
      }
      case 'contextInjectionState': {
        const state = update.message.value.state?.state
        if (state?.case === 'delivered') this.#injections.delete(update.message.value.injectionId)
        this.#trace?.('injection', { state: state?.case ?? 'unknown', ...(state?.value?.reason ? { reason: state.value.reason } : {}) })
        return
      }
      case 'toolRequestsListed':
        this.events.push({ type: 'tools-listed', count: update.message.value.callCount })
        return
      case 'modelCallCompleted': {
        const counters = tokenCounters(update.message.value)
        if (counters.inputTokens !== undefined || counters.outputTokens !== undefined) {
          this.events.push({ type: 'call-usage', usage: { inputTokens: 0, outputTokens: 0, ...counters } })
        }
        return
      }
      case 'turnEnded':
        if (this.#ended || this.#pending.size) throw new CursorProtocolError('INVALID_RESPONSE', 'Invalid Cursor terminal turn state')
        this.#ended = update.message.value
        this.#applyFinalUsage(update.message.value)
        this.#trace?.('turn-usage', { ...this.#usage })
        if (this.conversation) this.conversation.recovery = undefined
        clearTimeout(this.#idle)
        this.#idle = setTimeout(() => this.#channel.close(new CursorProtocolError('TIMEOUT', 'Cursor terminal checkpoint drain timed out')), LIMITS.terminalDrainMs)
        this.#idle.unref()
        // Keep reading: SDK checkpoints can follow TurnEnded on the same stream.
        await this.#persist()
        return
      // These are display/status updates, never authority to execute a tool.
      default: return
    }
  }

  #applyFinalUsage(ended: WireTurnEnded): void {
    const counters = tokenCounters(ended)
    if (Object.keys(counters).length) this.#haveUsage = true
    // Streamed tokenDelta estimates are only booked at finality.
    Object.assign(this.#usage, counters)
  }

  async reply(callKey: string, result: CursorToolResult): Promise<void> {
    if (this.#closed) throw this.#failure ?? new CursorProtocolError('CLOSED', 'Cursor Run is closed')
    const pending = this.#pending.get(callKey)
    if (!pending) throw new CursorProtocolError('INVALID_CALL_KEY', 'Unknown or already answered Cursor tool call')
    // Answering a tool starts the next model step.
    this.#stepText = []
    const response = execResponse(pending.exec, { case: 'mcpResult', value: create(McpResultSchema, {
      result: { case: 'success', value: mcpContent(result) },
    }) })
    // Consume once before awaiting any I/O so concurrent replies cannot duplicate.
    this.#pending.delete(callKey)
    this.#pendingBytes -= pending.bytes
    this.#armIdle()
    try { await this.#write(response) }
    catch (error) {
      const failure = this.#failure ?? safeError(error)
      this.#fail(failure)
      throw failure
    }
  }

  async inject(content: CursorContent[]): Promise<void> {
    if (this.#closed) throw this.#failure ?? new CursorProtocolError('CLOSED', 'Cursor Run is closed')
    const injectionId = randomUUID()
    this.#recoveryBarrier = true
    if (this.conversation) this.conversation.recovery = undefined
    await this.#persist()
    this.#injections.set(injectionId, content)
    const message = {
      $typeName: 'agent.v1.AgentClientMessage',
      message: { case: 'conversationAction', value: { $typeName: 'agent.v1.ConversationAction', action: { case: 'injectContextAction', value: {
        $typeName: 'agent.v1.InjectContextAction', injectionId, expectedRunId: this.runId,
        userContext: { $typeName: 'agent.v1.UserContextInjection', userMessage: userMessage([{ role: 'user', content }], this.#blobs, `inject:${injectionId}`) },
      } } } },
    } as unknown as AgentClientMessage
    this.#trace?.('inject', { parts: content.length })
    // A queued or rejected injection is still in host history and reaches the next turn.
    this.#checkpointCurrent = false
    try { await this.#write(message) }
    catch (error) {
      const failure = this.#failure ?? safeError(error)
      this.#fail(failure)
      throw failure
    }
  }

  async cancel(_reason?: unknown): Promise<void> {
    this.#fail(new CursorProtocolError('ABORTED', 'Cursor Run was aborted'))
    await this.#pump
    await this.#persistence
  }

  async close(): Promise<void> {
    this.#fail(new CursorProtocolError('CLOSED', 'Cursor Run was closed'))
    await this.#pump
    await this.#persistence
  }
}

export interface ConversationStore extends CursorConversation {
  /** Stable identity of this bound host session, including authoritative history reseeds. */
  id: string
  /** Stable per-session remote blob-cache key. Private persisted state, never diagnostics. */
  blobEncryptionKey: string
  blobs: BlobStore
  state?: ConversationStateStructure
  /** Last measured occupancy; a terminal checkpoint's zero is not a measurement. */
  contextTokens?: number
  /** Coordinator's committed host prefix, including pruning and image offload semantics. */
  hostHistory?: { count: number; digest: string }
  /**
   * Set when a turn completed and its final checkpoint followed every output.
   * The next turn may then continue that checkpoint instead of reseeding.
   */
  resume?: Provenance
  /** Retry only this unchanged, uncommitted host request before any host tool action. */
  recovery?: {
    requestKey: string
    checkpointHash: string
    attempts: number
    output: { type: 'text' | 'reasoning'; text: string }[]
  }
  /** Mid-turn messages Cursor did not take into its turn; they open the next one. */
  notes: CursorContent[][]
}

/** What identifies the host history a checkpoint continues, as in oakimov's turn provenance. */
interface Provenance {
  /** First conversation message; host compaction replaces it. */
  anchor: string
  /** Whitespace-free text (first 4 KiB) of the final step this provider produced. */
  finalText: string
}

const textKey = (content: readonly CursorContent[]): string =>
  content.filter(item => item.type === 'text').map(item => item.text).join('').replace(/\s+/gu, '').slice(0, 4096)

function anchorOf(messages: readonly CursorMessage[]): string {
  const first = messages.find(message => message.role !== 'system' && message.role !== 'developer')
  return first ? messagesDigest([first]) : ''
}

/**
 * Continue the checkpoint when the host history still starts the same way and
 * its latest assistant message is the final step this provider produced, so
 * no compaction or other model's work is hidden from Cursor. The coordinator
 * additionally checks the exact committed host prefix, including tool pruning,
 * instructions and image offloading, before it permits checkpoint reuse.
 */
function resumePlan(conversation: ConversationStore | undefined, messages: readonly CursorMessage[]): { active: number; reason: string } {
  const rebuild = (reason: string) => ({ active: -1, reason })
  if (!conversation) return rebuild('ephemeral')
  if (!conversation.state) return rebuild('no-checkpoint')
  const resume = conversation.resume
  if (!resume) return rebuild('unfinished-turn')
  const active = activeUserIndex(messages)
  if (active < 0) return rebuild('tool-result-tail')
  let last = active - 1
  while (last >= 0 && messages[last]!.role !== 'assistant') last--
  if (last < 0) return rebuild('missing-assistant')
  if (messages[last]!.sourceProvider && messages[last]!.sourceProvider !== 'cursor') return rebuild('foreign-provider')
  if (textKey(messages[last]!.content) !== resume.finalText) return rebuild('assistant-changed')
  if (anchorOf(messages) !== resume.anchor) return rebuild('history-rewritten')
  return { active, reason: 'terminal-checkpoint' }
}

/** Rebuild authoritative history while retaining the SDK-style agent/cache identity. */
export function resetCursorConversation(value: CursorConversation): void {
  const conversation = value as ConversationStore
  Object.assign(conversation, { blobs: new BlobStore(), state: undefined, contextTokens: undefined, hostHistory: undefined, resume: undefined, recovery: undefined, notes: [] })
}

/** Create the state one host conversation shares across all its Cursor Runs. */
export function createCursorConversation(): CursorConversation {
  const store: ConversationStore = { kind: 'cursor-conversation', id: randomUUID(), blobEncryptionKey: randomBytes(32).toString('hex'), blobs: new BlobStore(), notes: [] }
  return store
}

export async function openCursorRun(request: CursorRunRequest, options: CursorTransportOptions): Promise<CursorRun> {
  validateTransport(options)
  const conversation = request.conversation as ConversationStore | undefined
  const definitions = toolDefinitions(request.tools)
  const requestKey = createHash('sha256').update(JSON.stringify([messagesDigest(request.messages), request.selection, request.tools, request.cwd ?? null])).digest('hex')
  const recovery = conversation?.state && conversation.recovery?.requestKey === requestKey ? conversation.recovery : undefined
  if (recovery && recovery.attempts >= 2) throw new CursorProtocolError('CURSOR_RECOVERY_STALLED', 'Cursor checkpoint recovery made no progress')
  const { active, reason } = recovery ? { active: -1, reason: 'interrupted-checkpoint' } : resumePlan(conversation, request.messages)
  // A Run outside a host conversation owns a private store.
  const blobs = new BlobStore()
  let built: AgentRunRequest
  if (conversation && recovery) {
    recovery.attempts++
    built = buildResumeRequest({ ...request, conversationId: conversation.id }, conversation.blobs, conversation.state!, -1)
  } else if (conversation && active >= 0) {
    const notes = conversation.notes.splice(0)
    const messages = notes.length
      ? [...request.messages.slice(0, active), ...notes.map(content => ({ role: 'user' as const, content })), ...request.messages.slice(active)]
      : request.messages
    built = buildResumeRequest({ ...request, messages, conversationId: conversation.id }, conversation.blobs, conversation.state!, active)
  } else {
    if (conversation && (conversation.state || conversation.blobs.bytes > LIMITS.conversationBytes)) {
      // Replace the checkpoint from host authority, not the session/cache identity.
      resetCursorConversation(conversation)
    }
    built = buildRunRequest(conversation ? { ...request, conversationId: conversation.id } : request, conversation?.blobs ?? blobs)
  }
  const store = conversation?.blobs ?? blobs
  const historyBytes = built.conversationState!.rootPromptMessagesJson.reduce((total, id) => total + (store.get(id)?.byteLength ?? 0), 0)
  // A sequential bidi stream is efficient for small requests. Large histories
  // otherwise serialize megabytes of old images ahead of the first model token.
  const transport = options.runTransport && options.runTransport !== 'auto' ? options.runTransport
    : historyBytes >= 4 * 1024 * 1024 ? 'split' : 'http2'
  const blobEncryptionKey = conversation?.blobEncryptionKey ?? randomBytes(32).toString('hex')
  // The private key is owned by this conversation, not caller-supplied headers.
  const headers = Object.fromEntries(Object.entries(options.headers ?? {}).filter(([name]) => name.toLowerCase() !== 'x-blob-encryption-key'))
  options = { ...options, headers: { ...headers, 'x-blob-encryption-key': blobEncryptionKey }, runTransport: transport,
    endpoint: transport === 'split' ? options.endpoint ?? DEFAULT_ENDPOINT : await cursorRunEndpoint(options),
  }
  options.trace?.(recovery ? 'recover-checkpoint' : active >= 0 ? 'resume-checkpoint' : 'rebuild-history', { messages: request.messages.length, reason, transport, historyBytes })
  // Until this turn completes under a fresh checkpoint, nothing is resumable.
  if (conversation) {
    conversation.resume = undefined
    if (!recovery) conversation.recovery = undefined
    await request.onCheckpoint?.(conversation)
  }
  if (options.signal?.aborted) throw new CursorProtocolError('ABORTED', 'Cursor Run was aborted')
  const connection = new CursorConnection(request, options, conversation?.blobs ?? blobs, definitions)
  connection.conversation = conversation
  connection.anchor = anchorOf(request.messages)
  connection.requestKey = requestKey
  if (recovery) connection.replayOutput(recovery.output)
  await connection.start(built)
  return connection
}

export async function fetchCursorUsableModels(options: CursorTransportOptions): Promise<CursorUsableModel[]> {
  const bytes = await requestUnary('/agent.v1.AgentService/GetUsableModels', toBinary(GetUsableModelsRequestSchema, create(GetUsableModelsRequestSchema)), options)
  try {
    return fromBinary(GetUsableModelsResponseSchema, bytes).models.map(model => ({
      modelId: model.modelId, displayName: model.displayName,
      ...(model.displayModelId ? { displayModelId: model.displayModelId } : {}),
      ...(model.displayNameShort ? { displayNameShort: model.displayNameShort } : {}),
      ...(model.maxMode !== undefined ? { maxMode: model.maxMode } : {}),
      ...(model.thinkingDetails ? { thinkingDetails: toJson(ThinkingDetailsSchema, model.thinkingDetails) } : {}),
    }))
  } catch { throw new CursorProtocolError('INVALID_RESPONSE', 'Invalid Cursor model catalog protobuf') }
}
