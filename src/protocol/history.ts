/**
 * Structured DTO history, adapted from Noir history.ts and OMP's modern root records.
 * MIT attribution in UPSTREAM.md. No settings, file reads or added prompt material.
 */
import { createHash } from 'node:crypto'
import { clone, create, toBinary } from '@bufbuild/protobuf'
import type { JsonValue } from '@bufbuild/protobuf'
import type { CursorContent, CursorMessage, CursorRunRequest } from '../contracts.js'
import { BlobStore } from './blobs.js'
import { encodeArguments, mcpContent, parseArguments, textOf } from './content.js'
import { CursorProtocolError } from './errors.js'
import {
  AgentConversationTurnStructureSchema, AgentRunRequestSchema, AssistantMessageSchema,
  ConversationActionSchema, ConversationStateStructureSchema, ConversationStepSchema,
  ConversationTurnStructureSchema, McpArgsSchema, McpToolCallSchema, McpToolResultSchema,
  ModelDetailsSchema, RequestedModelSchema, RequestedModel_ModelParameterbytesSchema,
  ResumeActionSchema, SelectedContextSchema, SelectedImageSchema,
  SelectedImage_BlobIdWithDataSchema, SelectedImage_DimensionSchema,
  ThinkingMessageSchema, ToolCallSchema, UserMessageActionSchema, UserMessageSchema,
} from './vendor/agent_pb.js'
import type { AgentRunRequest, ConversationStateStructure, ConversationStep, SelectedImage, UserMessage } from './vendor/agent_pb.js'

export const MCP_PROVIDER = 'dsh'

function uuid(seed: string): string {
  const bytes = createHash('sha256').update(seed).digest().subarray(0, 16)
  bytes[6] = (bytes[6]! & 15) | 64
  bytes[8] = (bytes[8]! & 63) | 128
  const hex = bytes.toString('hex')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}

function wireCallId(id: string): string {
  if (!id) throw new CursorProtocolError('INVALID_REQUEST', 'Cursor history tool-call IDs must not be empty')
  return /^[a-zA-Z0-9_-]+$/u.test(id) ? id : `dsh_${createHash('sha256').update(id).digest('base64url')}`
}

export function asStructuredSelectedImages(content: readonly CursorContent[], blobs: BlobStore): SelectedImage[] {
  return content.flatMap(item => {
    if (item.type !== 'image') return []
    const blobId = blobs.put(item.data)
    return [create(SelectedImageSchema, {
      uuid: uuid(`image:${Buffer.from(blobId).toString('hex')}`),
      mimeType: item.mediaType,
      dimension: create(SelectedImage_DimensionSchema, { width: item.width, height: item.height }),
      dataOrBlobId: { case: 'blobIdWithData', value: create(SelectedImage_BlobIdWithDataSchema, { blobId, data: item.data }) },
    })]
  })
}

export function userMessage(messages: readonly CursorMessage[], blobs: BlobStore, seed: string): UserMessage {
  const content = messages.flatMap(message => message.content)
  const selectedImages = asStructuredSelectedImages(content, blobs)
  return create(UserMessageSchema, {
    text: textOf(content), messageId: uuid(seed),
    ...(selectedImages.length ? { selectedContext: create(SelectedContextSchema, { selectedImages }) } : {}),
  })
}

function rootContent(content: readonly CursorContent[]): JsonValue[] {
  return content.map((item): JsonValue => {
    switch (item.type) {
      case 'text': return { type: 'text', text: item.text }
      case 'reasoning': return { type: 'reasoning', text: item.text }
      case 'image': return { type: 'image', image: `data:${item.mediaType};base64,${Buffer.from(item.data).toString('base64')}`, mediaType: item.mediaType }
      case 'tool-call': return { type: 'tool-call', toolCallId: wireCallId(item.id), toolName: item.name, args: parseArguments(item.arguments) }
    }
  })
}

/**
 * Cursor builds the model prompt from root records and reads tool results as
 * text. Images a tool returned are kept as the user-role image parts Cursor
 * does read, placed right after that batch of results.
 */
function rootResult(message: CursorMessage): string {
  return message.content.map(item => item.type === 'image' ? `[${item.mediaType} image, attached below]` : item.type === 'text' ? item.text : '')
    .filter(Boolean).join('\n')
}

/** A trailing user run opens a new turn. A tool-result tail resumes the old turn. */
function activeIndex(messages: readonly CursorMessage[]): number {
  if (messages.at(-1)?.role !== 'user') return -1
  let index = messages.length - 1
  while (index > 0 && messages[index - 1]?.role === 'user') index--
  return index
}

function sameHead(previous: readonly Uint8Array[], head: readonly Uint8Array[]): boolean {
  return previous.length >= head.length && head.every((id, index) => Buffer.from(previous[index]!).equals(id))
}

/** One system blob per host instruction message, so an unchanged prefix keeps its blob ids. */
function systemPrompts(messages: readonly CursorMessage[]): string[] {
  return messages.filter(message => message.role === 'system' || message.role === 'developer')
    .map(message => textOf(message.content).trim()).filter(Boolean)
}

/**
 * Root history is rebuilt from host messages on every request, as in OMP: host
 * compaction and edits are authoritative. Non-history checkpoint fields are
 * carried over when the system head is unchanged, and the Cursor conversation id
 * stays stable so the server can reuse its state and prompt cache.
 */
export function buildRunRequest(request: CursorRunRequest, blobs: BlobStore, cached?: ConversationStateStructure): AgentRunRequest {
  const active = activeIndex(request.messages)
  const historyEnd = active < 0 ? request.messages.length : active
  const prompts = systemPrompts(request.messages)
  const systemIds = (prompts.length ? prompts : ['You are a helpful assistant.'])
    .map(content => blobs.put(new TextEncoder().encode(JSON.stringify({ role: 'system', content }))))
  const roots: Uint8Array[] = []
  const pushRoot = (value: JsonValue): void => { roots.push(blobs.put(new TextEncoder().encode(JSON.stringify(value)))) }
  const results = new Map<string, CursorMessage>()
  const calls = new Set<string>()
  for (let index = 0; index < historyEnd; index++) {
    const message = request.messages[index]!
    if (message.role === 'tool') {
      if (!message.toolCallId) throw new CursorProtocolError('INVALID_REQUEST', 'Cursor history tool results require toolCallId')
      results.set(message.toolCallId, message)
    }
    if (message.role === 'assistant') for (const item of message.content) {
      if (item.type !== 'tool-call') continue
      if (calls.has(item.id)) throw new CursorProtocolError('INVALID_REQUEST', 'Duplicate Cursor history tool-call ID')
      calls.add(item.id)
    }
  }
  let toolImages: CursorContent[] = []
  const flushToolImages = (): void => {
    if (!toolImages.length) return
    pushRoot({ role: 'user', content: rootContent([{ type: 'text', text: 'Images returned by the preceding tool results:' }, ...toolImages]) })
    toolImages = []
  }
  for (let index = 0; index < historyEnd; index++) {
    const message = request.messages[index]!
    // Host instructions head the root as system and <rules> entries below.
    if (message.role === 'system' || message.role === 'developer') continue
    if (message.role !== 'tool') flushToolImages()
    if (message.role === 'tool') {
      if (!calls.has(message.toolCallId!)) throw new CursorProtocolError('INVALID_REQUEST', 'Cursor history contains an unpaired tool result')
      pushRoot({ role: 'tool', id: wireCallId(message.toolCallId!), content: [{
        type: 'tool-result', toolCallId: wireCallId(message.toolCallId!), toolName: message.toolName ?? '',
        result: rootResult(message), ...(message.isError ? { isError: true } : {}),
      }] })
      toolImages.push(...message.content.filter(item => item.type === 'image'))
    } else {
      pushRoot({ role: message.role, content: rootContent(message.content) })
    }
  }
  flushToolImages()

  // With history, Cursor replaces the system entries with its own prompt and no
  // longer derives <rules> from RequestContext. Carry the host instructions as
  // the native CLI does, immediately after the system entries.
  const head = [...systemIds]
  if (roots.length && prompts.length) {
    head.push(blobs.put(new TextEncoder().encode(JSON.stringify({ role: 'user', content: `<rules>\n${prompts.join('\n\n')}\n</rules>` }))))
  }
  roots.unshift(...head)

  const turns: Uint8Array[] = []
  let current: { user: Uint8Array; steps: Uint8Array[] } | undefined
  const flushTurn = (): void => {
    if (!current) return
    turns.push(blobs.put(toBinary(ConversationTurnStructureSchema, create(ConversationTurnStructureSchema, {
      turn: { case: 'agentConversationTurn', value: create(AgentConversationTurnStructureSchema, { userMessage: current.user, steps: current.steps }) },
    }))))
    current = undefined
  }
  const pushStep = (step: ConversationStep): void => { current?.steps.push(blobs.put(toBinary(ConversationStepSchema, step))) }
  for (let index = 0; index < historyEnd; index++) {
    const message = request.messages[index]!
    if (message.role === 'user') {
      flushTurn()
      current = { user: blobs.put(toBinary(UserMessageSchema, userMessage([message], blobs, `${request.conversationId}:user:${index}`))), steps: [] }
    } else if (message.role === 'assistant') {
      for (const item of message.content) {
        if (item.type === 'text') pushStep(create(ConversationStepSchema, { message: { case: 'assistantMessage', value: create(AssistantMessageSchema, { text: item.text }) } }))
        if (item.type === 'reasoning') pushStep(create(ConversationStepSchema, { message: { case: 'thinkingMessage', value: create(ThinkingMessageSchema, { text: item.text }) } }))
        if (item.type === 'tool-call') {
          const result = results.get(item.id)
          pushStep(create(ConversationStepSchema, { message: { case: 'toolCall', value: create(ToolCallSchema, {
            toolCallId: wireCallId(item.id), tool: { case: 'mcpToolCall', value: create(McpToolCallSchema, {
              args: create(McpArgsSchema, { name: item.name, toolName: item.name, providerIdentifier: MCP_PROVIDER, toolCallId: wireCallId(item.id), args: encodeArguments(item.arguments) }),
              ...(result ? { result: create(McpToolResultSchema, { result: { case: 'success', value: mcpContent(result) } }) } : {}),
            }) },
          }) } }))
        }
      }
    }
  }
  flushTurn()

  const action = active < 0
    ? create(ConversationActionSchema, { action: { case: 'resumeAction', value: create(ResumeActionSchema) } })
    : create(ConversationActionSchema, { action: { case: 'userMessageAction', value: create(UserMessageActionSchema, {
      userMessage: userMessage(request.messages.slice(active), blobs, `${request.conversationId}:active:${active}`),
    }) } })
  const conversationState = cached && sameHead(cached.rootPromptMessagesJson, systemIds)
    ? clone(ConversationStateStructureSchema, cached) : create(ConversationStateStructureSchema)
  conversationState.rootPromptMessagesJson = roots
  conversationState.turns = turns
  // A checkpoint taken mid-turn may list calls this rebuilt history already settles.
  conversationState.pendingToolCalls = []
  return create(AgentRunRequestSchema, {
    conversationId: request.conversationId,
    conversationState,
    action,
    ...modelFields(request),
  })
}

function modelFields(request: CursorRunRequest) {
  return {
    modelDetails: create(ModelDetailsSchema, { modelId: request.selection.wireModelId, maxMode: request.selection.maxMode }),
    requestedModel: create(RequestedModelSchema, {
      modelId: request.selection.requestedModelId ?? request.selection.wireModelId,
      maxMode: request.selection.maxMode === true,
      parameters: (request.selection.parameters ?? []).map(parameter => create(RequestedModel_ModelParameterbytesSchema, { id: parameter.key, value: parameter.value })),
    }),
  }
}

/** Stable digest of host messages; image bytes are hashed, never copied. */
export function messagesDigest(messages: readonly CursorMessage[]): string {
  const hash = createHash('sha256')
  for (const message of messages) {
    hash.update(JSON.stringify([message.role, message.toolCallId ?? null, message.isError === true]))
    for (const item of message.content) {
      hash.update(item.type === 'image' ? `image:${createHash('sha256').update(item.data).digest('hex')}` : JSON.stringify(item))
    }
    hash.update('\u0000')
  }
  return hash.digest('hex')
}

/** Index of the trailing user run that forms a new turn, or -1. */
export function activeUserIndex(messages: readonly CursorMessage[]): number {
  return activeIndex(messages)
}

/**
 * Continue the server's own checkpoint with only the new user turn, as the
 * Cursor CLI does. Referenced records still live in the client's KV store and
 * may be fetched again. Checkpoint reuse alone does not prove a model cache hit.
 */
export function buildResumeRequest(request: CursorRunRequest, blobs: BlobStore, checkpoint: ConversationStateStructure, active: number): AgentRunRequest {
  return create(AgentRunRequestSchema, {
    conversationId: request.conversationId,
    conversationState: checkpoint,
    action: active < 0
      ? create(ConversationActionSchema, { action: { case: 'resumeAction', value: create(ResumeActionSchema) } })
      : create(ConversationActionSchema, { action: { case: 'userMessageAction', value: create(UserMessageActionSchema, {
        userMessage: userMessage(request.messages.slice(active), blobs, `${request.conversationId}:active:${request.messages.length}:${active}`),
      }) } }),
    ...modelFields(request),
  })
}
