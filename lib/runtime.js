import { createHash, randomUUID } from 'node:crypto'
import { LlmError, ToolCallId, createAssistantMessage } from '@deepseek-ai/dsh-llm'
import { openCursorRun, createCursorConversation, resetCursorConversation, CursorProtocolError } from './protocol.js'
import { projectRequest, projectToolResult, requestFingerprint } from './projection.js'

const MAX_QUEUED_EVENTS = 2048
const MAX_CONVERSATIONS = 64
const MAX_QUEUED_BYTES = 8 * 1024 * 1024
const eventBytes = event => Buffer.byteLength(event.text ?? event.arguments ?? '') + Buffer.byteLength(event.name ?? '') + 128

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical)
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]))
  return value
}

function digest(value) {
  return createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex')
}

function failure(message, code = 'CURSOR_CONTEXT_CHANGED', cause) {
  return new LlmError(message, code, cause === undefined ? undefined : { cause })
}

function deferred() {
  let resolve
  const promise = new Promise(done => { resolve = done })
  return { promise, resolve }
}

function assertOptions(options, auxiliary) {
  // Cursor has no verified wire fields for these generic sampling controls.
  // They are intentionally ignored; only the captured model selection is sent.
  if (!auxiliary && options.tools?.some(tool => tool.deferLoading)) {
    throw failure('Cursor does not expose deferred tool loading in its wire contract', 'UNSUPPORTED_OPTION')
  }
}

/**
 * Owns a Cursor run for one native user turn. Tool bodies and approvals belong
 * exclusively to the outer DSH agent loop. A successful tool handoff parks the
 * run without cancelling its transport or its turn-level abort listener.
 */
export class CursorRuntime {
  constructor(ctx, auth, catalog, hooks = {}) {
    this.ctx = ctx
    this.logger = typeof ctx?.logger === 'function' ? ctx.logger('cursor') : undefined
    this.auth = auth
    this.catalog = catalog
    this.openRun = hooks.openCursorRun ?? openCursorRun
    this.project = hooks.projectRequest ?? projectRequest
    this.projectResult = hooks.projectToolResult ?? projectToolResult
    this.fingerprint = hooks.requestFingerprint ?? requestFingerprint
    this.resolveScope = hooks.resolveScope ?? (() => ({}))
    this.attachments = hooks.attachments
    this.batchQuietMs = hooks.batchQuietMs ?? 100
    this.maxScopes = Math.min(hooks.maxScopes ?? 32, 32)
    this.scopes = new Map()
    /** Cursor conversation per host session, as OMP and the DSH Cursor plugins keep it. */
    this.conversations = new Map()
    this.createConversation = hooks.createCursorConversation ?? createCursorConversation
    this.conversationCache = hooks.conversationCache
    this.owned = new Set()
    this.resolving = new Set()
    this.accountKnown = false
    this.accountLoginId = undefined
    this.disposed = false
  }

  attachmentService() {
    return this.attachments ?? this.ctx?.get?.('attachments')
  }

  projectionOptions(signal) {
    const fs = this.ctx?.get?.('fs')
    return {
      attachments: this.attachmentService(), signal,
      ...(fs ? { mapHostPath: hostPath => fs.processPathFromHostPath(hostPath) } : {}),
    }
  }

  snapshotHistory(scope) {
    scope.historyFingerprint = this.fingerprint({ ...scope.initialOptions, messages: scope.expectedMessages })
  }

  attachSignal(scope, signal) {
    if (!signal || scope.listeners.has(signal)) return
    const abort = () => {
      const error = failure('Cursor run cancelled by the caller', 'ABORTED', signal.reason)
      this.stopInBackground(scope, error)
    }
    scope.listeners.set(signal, abort)
    if (signal.aborted) abort()
    else signal.addEventListener('abort', abort, { once: true })
  }

  stopInBackground(scope, error) {
    this.closeScope(scope, error, true).catch(cleanupError => { scope.cleanupError = cleanupError })
  }

  makeScope(options, resolved, key, auxiliary, ephemeral) {
    if (this.owned.size >= this.maxScopes || (!ephemeral && this.scopes.size >= this.maxScopes)) {
      throw failure('Cursor already owns the maximum number of active conversation scopes', 'CURSOR_SCOPE_LIMIT')
    }
    const ready = deferred()
    const expectedMessages = structuredClone(options.messages)
    const scope = {
      key, auxiliary, sessionId: options.sessionId, resolved,
      scopeDigest: digest([options.sessionId ?? null, resolved.cwd ?? null, resolved.generation ?? null]),
      controller: new AbortController(), ready,
      initialOptions: { ...options, messages: expectedMessages }, expectedMessages,
      baseMessageCount: options.messages.length,
      queue: [], queueBytes: 0, listeners: new Map(), pending: new Map(), seenCallKeys: new Set(),
      usage: {}, busy: false, status: 'opening', eof: false,
      hadToolActions: false,
    }
    this.snapshotHistory(scope)
    this.owned.add(scope)
    if (!ephemeral) this.scopes.set(key, scope)
    return scope
  }

  binding(options, captured, resolved, selection, loginId, tools) {
    return digest({
      model: options.model, selection, loginId, settings: captured.settings,
      cwd: resolved.cwd, generation: resolved.generation, tools,
    })
  }

  async grant(scope) {
    scope.controller.signal.throwIfAborted()
    const grant = await this.auth.getGrant(scope.controller.signal)
    scope.controller.signal.throwIfAborted()
    if (!grant?.access || !grant.loginId || grant.type !== 'oauth') {
      throw failure('An explicit Cursor login is required', 'CURSOR_AUTH_REQUIRED')
    }
    if (this.accountKnown && this.accountLoginId !== grant.loginId) {
      throw failure('The Cursor account changed during this user turn')
    }
    if (scope.loginId !== undefined && scope.loginId !== grant.loginId) {
      throw failure('The Cursor run belongs to a different login')
    }
    this.accountKnown = true
    this.accountLoginId = grant.loginId
    scope.loginId = grant.loginId
    return grant
  }

  /** Resume terminal state across agent eviction and plugin/process restarts. */
  async conversationFor(scope, loginId, settings) {
    if (scope.auxiliary || !scope.sessionId) return { id: randomUUID() }
    // Object-generation ids change on host reload. Durable identity uses the
    // actual session, workspace, account and transport instead.
    const { runTransport: _transport, ...identitySettings } = settings
    const binding = digest([loginId, scope.resolved.cwd ?? null, identitySettings])
    let entry = this.conversations.get(scope.sessionId)
    if (!entry || entry.binding !== binding) {
      const restored = await this.conversationCache?.load(scope.sessionId, binding)
      scope.controller.signal.throwIfAborted()
      entry = { id: randomUUID(), loginId, binding, store: restored ?? this.createConversation() }
    }
    scope.conversation = entry
    // The protocol persists eligibility changes before opening a Run. Keep the
    // stable blob key and latest checkpoint instead of deleting the whole record.
    scope.controller.signal.throwIfAborted()
    // Refresh insertion order so eviction drops the least recently used session.
    this.conversations.delete(scope.sessionId)
    this.conversations.set(scope.sessionId, entry)
    while (this.conversations.size > MAX_CONVERSATIONS) this.conversations.delete(this.conversations.keys().next().value)
    return entry
  }

  /** One concise log line per Run milestone: how it opened, first output and injections. */
  traceFor(scope) {
    const logger = this.logger
    const started = Date.now()
    const session = scope.sessionId ?? 'ephemeral'
    const shown = new Set(['resume-checkpoint', 'recover-checkpoint', 'rebuild-history', 'first-thinking', 'first-text', 'inject', 'injection'])
    return (event, detail) => {
      const elapsedMs = Date.now() - started
      if (event === 'resume-checkpoint' || event === 'recover-checkpoint' || event === 'rebuild-history') {
        scope.diagnostics = { opening: event, messages: detail?.messages,
          ...(detail?.reason ? { reason: detail.reason } : {}),
          ...(detail?.transport ? { transport: detail.transport, historyBytes: detail.historyBytes } : {}),
        }
      } else if (event === 'upload') {
        scope.diagnostics ??= {}
        scope.diagnostics.upload ??= { bytes: 0, wireBytes: 0, lastPayloadMs: 0 }
        scope.diagnostics.upload.bytes += detail.bytes
        scope.diagnostics.upload.wireBytes += detail.wireBytes
        if (detail.bytes >= 1024) scope.diagnostics.upload.lastPayloadMs = elapsedMs
      } else if (event === 'first-thinking' || event === 'first-text') {
        scope.diagnostics ??= {}
        scope.diagnostics[event === 'first-text' ? 'firstTextMs' : 'firstThinkingMs'] = elapsedMs
      }
      if (!shown.has(event)) return
      logger?.warn(`cursor: ${session} ${event} +${elapsedMs}ms ${detail ? JSON.stringify(detail) : ''}`)
    }
  }

  async open(scope, options, captured, selection, tools) {
    const grant = await this.grant(scope)
    scope.binding = this.binding(options, captured, scope.resolved, selection, grant.loginId, tools)
    scope.policyBinding = this.binding(options, captured, scope.resolved, selection, grant.loginId, [])
    scope.toolNames = new Set(tools.map(tool => tool.name))
    const projectedOptions = scope.auxiliary ? { ...options, tools: [], toolHistory: undefined } : options
    const messages = await this.project(projectedOptions, this.projectionOptions(scope.controller.signal))
    if (scope.auxiliary && messages.some(message => message.role === 'tool' || message.content.some(block => block.type !== 'text' && block.type !== 'reasoning'))) {
      throw failure('Cursor auxiliary requests require a text-only history projection', 'CURSOR_AUXILIARY_TEXT_ONLY')
    }
    scope.controller.signal.throwIfAborted()
    const conversation = await this.conversationFor(scope, grant.loginId, captured.settings)
    scope.controller.signal.throwIfAborted()
    if (conversation.store) {
      const covered = conversation.store.hostHistory
      const changed = covered && this.fingerprint({ ...options, messages: options.messages.slice(0, covered.count) }) !== covered.digest
      // Legacy checkpoints cannot attest the current DSH projection. Establish
      // one authoritative baseline, then track every future effective rewrite.
      if (changed || (conversation.store.resume && !covered)) resetCursorConversation(conversation.store)
    }
    scope.run = await this.openRun({
      conversationId: conversation.id, ...(conversation.store ? { conversation: conversation.store } : {}),
      selection, messages, tools,
      ...(conversation.store && this.conversationCache ? {
        onCheckpoint: store => this.conversationCache.save(scope.sessionId, conversation.binding, store),
      } : {}),
      ...(scope.resolved.cwd === undefined ? {} : { cwd: scope.resolved.cwd }),
    }, {
      accessToken: grant.access, ...captured.settings, signal: scope.controller.signal,
      trace: this.traceFor(scope),
    })
    scope.ready.resolve(scope.run)
    scope.pump = this.pump(scope)
    scope.controller.signal.throwIfAborted()
    scope.status = 'draining'
  }

  async pump(scope) {
    try {
      for await (const event of scope.run.events) {
        const bytes = eventBytes(event)
        if (scope.queue.length >= MAX_QUEUED_EVENTS || scope.queueBytes + bytes > MAX_QUEUED_BYTES) {
          throw failure('Cursor event buffer limit exceeded', 'CURSOR_BUFFER_LIMIT')
        }
        scope.queueBytes += bytes
        scope.queue.push(event)
        scope.wake?.()
        if (event.type === 'done') {
          scope.sawDone = true
          break
        }
      }
      if (!scope.sawDone && !scope.controller.signal.aborted) {
        scope.failure ??= failure('Cursor transport ended before completing this user turn', 'CURSOR_RUN_LOST')
      }
    } catch (error) {
      if (!scope.controller.signal.aborted) {
        scope.failure ??= !scope.hadToolActions && error instanceof CursorProtocolError
          ? new LlmError(error.message, error.code, error.status === undefined ? undefined : { status: error.status })
          : error instanceof LlmError ? error
          : failure(`Cursor transport was lost (${error?.code ?? 'unknown'}: ${error?.message ?? error})`, 'CURSOR_RUN_LOST', error)
      }
    } finally {
      scope.eof = true
      scope.wake?.()
      if (scope.failure && !scope.closing) this.stopInBackground(scope, scope.failure)
    }
  }

  async nextEvent(scope, timeoutMs) {
    const deadline = timeoutMs === undefined ? undefined : Date.now() + timeoutMs
    while (true) {
      if (scope.failure) throw scope.failure
      scope.controller.signal.throwIfAborted()
      if (scope.queue.length) {
        const event = scope.queue.shift()
        scope.queueBytes -= eventBytes(event)
        return event
      }
      if (scope.eof) throw failure('Cursor transport ended without a terminal event', 'CURSOR_RUN_LOST')
      const remaining = deadline === undefined ? undefined : Math.max(0, deadline - Date.now())
      if (remaining === 0) return undefined
      const timedOut = await new Promise(resolve => {
        let timer
        const wake = () => {
          clearTimeout(timer)
          if (scope.wake === wake) scope.wake = undefined
          resolve(false)
        }
        scope.wake = wake
        if (remaining !== undefined) timer = setTimeout(() => {
          if (scope.wake === wake) scope.wake = undefined
          resolve(true)
        }, remaining)
      })
      if (timedOut) return undefined
    }
  }

  recordUsage(scope, usage) {
    if (usage.cacheReadTokens !== undefined) {
      scope.diagnostics ??= {}
      // Aggregate billing counters are durable diagnostics, not occupancy.
      scope.diagnostics.turnUsage = { ...usage }
      this.logger?.warn(`cursor: ${scope.sessionId ?? 'ephemeral'} turn usage ${JSON.stringify({ input: usage.inputTokens, cacheRead: usage.cacheReadTokens, cacheWrite: usage.cacheWriteTokens, output: usage.outputTokens, context: usage.contextTokens })}`)
    }
    if (usage.outputTokens !== undefined) scope.usage.outputTokens = Math.max(scope.usage.outputTokens ?? 0, usage.outputTokens)
    if (usage.contextTokens !== undefined) scope.usage.contextTokens = usage.contextTokens
  }

  /**
   * One DSH step reports the context it actually occupied, from Cursor's latest
   * checkpoint. TurnEnded input and cache counters sum every model call of the
   * whole Run, so they would overstate context many times over and trigger
   * needless compaction. Without a checkpoint DSH keeps its own estimate.
   */
  usageChunk(scope) {
    if (scope.usage.contextTokens === undefined) return undefined
    const outputTokens = Math.max(0, (scope.usage.outputTokens ?? 0) - (scope.reportedOutput ?? 0))
    scope.reportedOutput = scope.usage.outputTokens ?? 0
    return { type: 'usage', usage: { inputTokens: scope.usage.contextTokens, outputTokens } }
  }

  async collectTools(scope, first) {
    const tools = [first]
    const deferredEvents = []
    let deadline = Date.now() + this.batchQuietMs
    try {
      while (!scope.listedCount || tools.length < scope.listedCount) {
        const event = await this.nextEvent(scope, Math.max(0, deadline - Date.now()))
        if (!event) break
        if (event.type === 'tool') {
          tools.push(event)
          deadline = Date.now() + this.batchQuietMs
        } else if (event.type === 'tools-listed') {
          scope.listedCount = event.count
        } else if (event.type === 'usage') {
          this.recordUsage(scope, event.usage)
        } else {
          deferredEvents.push(event)
          if (event.type === 'done') break
        }
      }
    } finally {
      const bytes = deferredEvents.reduce((sum, event) => sum + eventBytes(event), 0)
      if (scope.queue.length + deferredEvents.length > MAX_QUEUED_EVENTS || scope.queueBytes + bytes > MAX_QUEUED_BYTES) {
        throw failure('Cursor event buffer limit exceeded', 'CURSOR_BUFFER_LIMIT')
      }
      scope.queue.unshift(...deferredEvents)
      scope.queueBytes += bytes
      scope.listedCount = undefined
    }
    if (scope.failure) throw scope.failure
    return tools
  }

  validateResults(scope, options) {
    const prefix = options.messages.slice(0, scope.expectedMessages.length)
    if (this.fingerprint({ ...options, messages: prefix }) !== scope.historyFingerprint) {
      throw failure('DSH history changed while a Cursor tool handoff was pending')
    }
    if (scope.pending.size === 0) throw failure('A Cursor user turn is already running without a pending tool handoff', 'CURSOR_RUN_BUSY')
    const results = new Map()
    const injected = []
    const suffix = options.messages.slice(scope.expectedMessages.length)
    for (const message of suffix) {
      // Steering and host notes added during the step join the running turn.
      if (message.role === 'user' && typeof scope.run?.inject === 'function') { injected.push(message); continue }
      if (message.role !== 'tool' || !scope.pending.has(message.toolCallId)) {
        throw failure('Cursor expected a native tool result with an exact pending toolCallId', 'CURSOR_TOOL_RESULT_MISMATCH')
      }
      if (results.has(message.toolCallId)) {
        throw failure('Cursor received duplicate results for the same pending toolCallId', 'CURSOR_TOOL_RESULT_DUPLICATE')
      }
      results.set(message.toolCallId, message)
    }
    if (results.size !== scope.pending.size) {
      throw failure('Cursor is missing one or more pending native tool results', 'CURSOR_TOOL_RESULT_MISSING')
    }
    return { results, suffix, injected }
  }

  canRebaseCompletedHandoff(scope, options, captured, selection, tools, grant) {
    if (!scope.pending.size || scope.failure || scope.policyBinding !== this.binding(options, captured, scope.resolved, selection, grant.loginId, [])) return false
    const prefix = options.messages.slice(0, scope.expectedMessages.length)
    const suffix = options.messages.slice(scope.expectedMessages.length)
    const changed = this.fingerprint({ ...options, messages: prefix }) !== scope.historyFingerprint
      || scope.binding !== this.binding(options, captured, scope.resolved, selection, grant.loginId, tools)
      || suffix.some(message => message.role === 'developer' || message.role === 'system'
        || (message.role === 'user' && typeof scope.run?.inject !== 'function'))
    if (!changed) return false
    const results = new Set()
    const calls = new Set()
    for (const message of options.messages) {
      if (message.role === 'tool' && scope.pending.has(message.toolCallId)) {
        if (results.has(message.toolCallId)) return false
        results.add(message.toolCallId)
      }
      if (message.role === 'assistant') for (const block of message.content) {
        const pending = block.type === 'tool-call' ? scope.pending.get(block.id) : undefined
        if (pending && pending.name === block.name && pending.arguments === block.arguments) calls.add(block.id)
      }
    }
    return results.size === scope.pending.size && calls.size === scope.pending.size
  }

  async resume(scope, options, captured, selection, tools, capturedGrant) {
    if (scope.failure) throw scope.failure
    const grant = capturedGrant ?? await this.grant(scope)
    scope.deliveryStarted = false
    if (scope.binding !== this.binding(options, captured, scope.resolved, selection, grant.loginId, tools)) {
      throw failure('The model, account, endpoint, workspace generation, or tool schemas changed during a Cursor tool handoff')
    }
    const { results, suffix, injected } = this.validateResults(scope, options)
    // Enforce the route budget over the entire effective history before any
    // reply. Separate parallel results must not each get an independent budget.
    await this.project(options, this.projectionOptions(scope.controller.signal))
    // Validate and project every result before sending any part of the batch.
    const projected = await Promise.all([...scope.pending].map(async ([id, call]) => {
      const message = results.get(id)
      const result = await this.projectResult(message, this.projectionOptions(scope.controller.signal))
      return { callKey: call.callKey, result: { ...result, isError: message.isError === true } }
    }))
    // Project steering before writing anything, so a failure leaves the step untouched.
    const injection = injected.length
      ? (await this.project({ ...options, system: undefined, messages: injected, tools: [], toolHistory: undefined },
        this.projectionOptions(scope.controller.signal))).map(message => message.content)
      : []
    scope.deliveryStarted = true
    for (const { callKey, result } of projected) {
      scope.controller.signal.throwIfAborted()
      if (scope.failure) throw scope.failure
      try {
        await scope.run.reply(callKey, result)
      } catch (error) {
        throw failure('Cursor tool-result delivery was interrupted; recovery requires the committed host history', 'CURSOR_RUN_LOST', error)
      }
    }
    if (injection.length) {
      // One injection per host message, so each can later be matched in history.
      try { for (const content of injection) await scope.run.inject(content) }
      catch (error) {
        throw failure('Cursor could not accept a message added during this turn', 'CURSOR_RUN_LOST', error)
      }
    }
    scope.pending.clear()
    scope.expectedMessages.push(...structuredClone(suffix))
    this.snapshotHistory(scope)
    scope.status = 'draining'
  }

  async *drain(scope, options) {
    const blocks = []
    let current
    let index = 0
    const endBlock = () => {
      if (!current) return undefined
      const block = { type: current.type, text: current.text }
      const chunk = { type: 'block-end', index: current.index, block }
      blocks.push(block)
      current = undefined
      return chunk
    }
    while (true) {
      const event = await this.nextEvent(scope)
      if (event.type === 'text' || event.type === 'reasoning') {
        if (current?.type !== event.type) {
          const ended = endBlock()
          if (ended) yield ended
          current = { type: event.type, index: index++, text: '' }
          yield { type: 'block-start', index: current.index, blockType: event.type }
        }
        current.text += event.text
        scope.emitted = true
        yield { type: event.type === 'text' ? 'text-delta' : 'reasoning-delta', index: current.index, text: event.text }
      } else if (event.type === 'usage') {
        this.recordUsage(scope, event.usage)
      } else if (event.type === 'tools-listed') {
        scope.listedCount = event.count
      } else if (event.type === 'tool') {
        const ended = endBlock()
        if (ended) yield ended
        if (scope.auxiliary) throw failure('Cursor requested a tool in a text-only auxiliary run', 'CURSOR_UNEXPECTED_TOOL')
        const calls = await this.collectTools(scope, event)
        for (const call of calls) {
          if (!scope.toolNames.has(call.name)) throw failure('Cursor requested a tool outside this native request schema', 'CURSOR_UNEXPECTED_TOOL')
          if (scope.seenCallKeys.has(call.callKey)) throw failure('Cursor repeated an already received execution callKey', 'CURSOR_DUPLICATE_CALL')
          scope.seenCallKeys.add(call.callKey)
          const id = ToolCallId(`cursor_${randomUUID()}`)
          scope.pending.set(id, { callKey: call.callKey, name: call.name, arguments: call.arguments })
          const block = { type: 'tool-call', id, name: call.name, arguments: call.arguments }
          const blockIndex = index++
          blocks.push(block)
          yield { type: 'block-start', index: blockIndex, blockType: 'tool-call' }
          yield { type: 'tool-call-delta', index: blockIndex, id, name: call.name, argumentsDelta: call.arguments }
          yield { type: 'block-end', index: blockIndex, block }
        }
        scope.hadToolActions = true
        scope.expectedMessages.push(createAssistantMessage({
          content: blocks, source: { provider: options.provider, model: options.model },
        }))
        this.snapshotHistory(scope)
        const usage = this.usageChunk(scope)
        if (usage) yield usage
        scope.status = 'parked'
        yield {
          type: 'finish', reason: { kind: 'tool-calls' },
          replayState: { response: { version: 1, scope: scope.scopeDigest, callIds: [...scope.pending.keys()],
            ...(scope.diagnostics ? { diagnostics: structuredClone(scope.diagnostics) } : {}),
          } },
        }
        return
      } else if (event.type === 'done') {
        const ended = endBlock()
        if (ended) yield ended
        const usage = this.usageChunk(scope)
        if (usage) yield usage
        if (scope.conversation) {
          const completed = [...scope.expectedMessages, createAssistantMessage({
            content: blocks, source: { provider: options.provider, model: options.model },
          })]
          scope.conversation.store.hostHistory = {
            count: completed.length, digest: this.fingerprint({ ...options, messages: completed }),
          }
          await this.conversationCache?.save(scope.sessionId, scope.conversation.binding, scope.conversation.store)
        }
        await this.closeScope(scope, undefined, false)
        if (this.scopes.get(scope.key) === scope) this.scopes.delete(scope.key)
        yield { type: 'finish', reason: { kind: event.reason },
          ...(scope.diagnostics ? { replayState: { response: {
            version: 1, scope: scope.scopeDigest, diagnostics: structuredClone(scope.diagnostics),
          } } } : {}),
        }
        return
      }
    }
  }

  async *stream(options, captured) {
    if (this.disposed) throw failure('Cursor runtime has been disposed', 'CURSOR_DISPOSED')
    const auxiliary = options.purpose === 'compaction' || options.purpose === 'session-title'
    assertOptions(options, auxiliary)
    const tools = auxiliary ? [] : structuredClone(options.tools ?? [])
    if (!auxiliary && !options.sessionId && tools.length) {
      throw failure('Native Cursor tool handoff requires GenerateOptions.sessionId', 'CURSOR_SESSION_REQUIRED')
    }
    options.signal?.throwIfAborted()
    const ticket = { sessionId: options.sessionId, auxiliary }
    this.resolving.add(ticket)
    let resolved
    try {
      resolved = await this.resolveScope(options)
    } finally {
      this.resolving.delete(ticket)
    }
    options.signal?.throwIfAborted()
    if (ticket.failure) throw ticket.failure
    if (this.disposed) throw failure('Cursor runtime has been disposed', 'CURSOR_DISPOSED')
    const ephemeral = auxiliary || !options.sessionId
    const key = ephemeral ? randomUUID() : JSON.stringify([options.sessionId, resolved.cwd ?? null, resolved.generation ?? null])
    let scope = ephemeral ? undefined : this.scopes.get(key)
    if (!ephemeral) {
      for (const other of this.scopes.values()) {
        if (other.sessionId === options.sessionId && other.key !== key) {
          const error = failure('The workspace scope of this Cursor user turn changed')
          await this.closeScope(other, error, true)
          throw error
        }
      }
    }
    if (scope?.failure) {
      // Protocol eligibility decides whether this exact uncommitted request
      // can resume its checkpoint. Host tool barriers instead rebuild from
      // DSH's committed calls/results; they never replay a result on a lost exec.
      await this.closeScope(scope, scope.failure, true)
      if (this.scopes.get(key) === scope) this.scopes.delete(key)
      scope = undefined
    }
    if (scope?.busy) throw failure('Concurrent model steps cannot consume the same Cursor run', 'CURSOR_RUN_BUSY')
    const existing = scope !== undefined
    scope ??= this.makeScope(options, resolved, key, auxiliary, ephemeral)
    scope.busy = true
    this.attachSignal(scope, options.signal)
    try {
      const selection = this.catalog.select(captured.entry, options.reasoningEffort)
      if (existing) {
        const grant = await this.grant(scope)
        if (this.canRebaseCompletedHandoff(scope, options, captured, selection, tools, grant)) {
          // The host has changed its effective context, and every emitted call
          // already has a durable result. Rebuild from that authority rather
          // than continuing with hidden stale instructions or offloaded images.
          await this.closeScope(scope, undefined, true)
          this.scopes.delete(key)
          scope = this.makeScope(options, resolved, key, auxiliary, ephemeral)
          scope.busy = true
          this.attachSignal(scope, options.signal)
          await this.open(scope, options, captured, selection, tools)
        } else await this.resume(scope, options, captured, selection, tools, grant)
      } else await this.open(scope, options, captured, selection, tools)
      yield* this.drain(scope, options)
    } catch (error) {
      if (error?.code === 'IMAGE_OFFLOAD_REQUIRED' && scope.status === 'parked' && !scope.deliveryStarted && !scope.controller.signal.aborted) {
        // No result was delivered. Native image offloading can retry the same
        // committed host calls; the rewritten projection is rebased above.
        throw error
      }
      scope.ready.resolve(scope.run)
      await this.closeScope(scope, error, true)
      if (this.scopes.get(key) === scope) this.scopes.delete(key)
      if (error instanceof CursorProtocolError) {
        throw new LlmError(error.message, error.code, error.status === undefined ? undefined : { status: error.status })
      }
      throw error
    } finally {
      // A completed tool handoff is not cancellation. Abandoning a partial
      // response, however, must not leave a hidden run or unread output behind.
      scope.busy = false
      if (scope.status === 'opening' || scope.status === 'draining') {
        await this.closeScope(scope, failure('Cursor response consumer stopped', 'ABORTED'), true)
        if (this.scopes.get(scope.key) === scope) this.scopes.delete(scope.key)
      }
    }
  }

  closeScope(scope, error, cancel) {
    if (error) scope.failure ??= error
    if (cancel && !scope.controller.signal.aborted) scope.controller.abort(error)
    scope.wake?.()
    if (scope.closing) return scope.closing
    scope.closing = (async () => {
      const errors = []
      const run = await scope.ready.promise
      if (run) {
        if (cancel) {
          try { await run.cancel(error) } catch (cause) { errors.push(cause) }
        }
        try { await run.close() } catch (cause) { errors.push(cause) }
      }
      await scope.pump
      for (const [signal, listener] of scope.listeners) signal.removeEventListener('abort', listener)
      scope.listeners.clear()
      scope.queue.length = 0
      scope.queueBytes = 0
      scope.status = error ? 'lost' : 'done'
      this.owned.delete(scope)
      if (errors.length) throw new AggregateError(errors, 'Cursor run cleanup failed')
    })()
    return scope.closing
  }

  async closeTurn(sessionId, reason = 'Native turn ended') {
    const error = failure(String(reason), 'CURSOR_TURN_CLOSED')
    for (const ticket of this.resolving) if (ticket.sessionId === sessionId && !ticket.auxiliary) ticket.failure = error
    const matching = new Set([...this.owned, ...this.scopes.values()].filter(scope => scope.sessionId === sessionId && !scope.auxiliary))
    for (const scope of matching) if (this.scopes.get(scope.key) === scope) this.scopes.delete(scope.key)
    await this.closeMany(matching, error)
  }

  async closeSession(sessionId, reason = 'Native session closed') {
    const error = failure(String(reason), 'CURSOR_SESSION_CLOSED')
    for (const ticket of this.resolving) if (ticket.sessionId === sessionId) ticket.failure = error
    const matching = new Set([...this.owned, ...this.scopes.values()].filter(scope => scope.sessionId === sessionId))
    for (const scope of matching) if (this.scopes.get(scope.key) === scope) this.scopes.delete(scope.key)
    this.conversations.delete(sessionId)
    await this.closeMany(matching, error)
  }

  async invalidateAccount(loginId) {
    if (this.accountKnown && this.accountLoginId === loginId) return
    this.accountKnown = true
    this.accountLoginId = loginId
    const error = failure('The Cursor login changed or was removed')
    for (const [id, entry] of this.conversations) if (entry.loginId !== loginId) this.conversations.delete(id)
    for (const ticket of this.resolving) ticket.failure = error
    const matching = [...this.owned].filter(scope => loginId === undefined || scope.loginId !== loginId)
    await this.closeMany(matching, error)
  }

  async closeMany(scopes, error) {
    const settled = await Promise.allSettled([...scopes].map(scope => this.closeScope(scope, error, true)))
    const errors = settled.filter(result => result.status === 'rejected').map(result => result.reason)
    if (errors.length) throw new AggregateError(errors, 'Cursor runtime cleanup failed')
  }

  async dispose() {
    this.disposed = true
    const all = new Set([...this.owned, ...this.scopes.values()])
    this.scopes.clear()
    this.conversations.clear()
    await this.closeMany(all, failure('Cursor runtime disposed', 'CURSOR_DISPOSED'))
  }
}
