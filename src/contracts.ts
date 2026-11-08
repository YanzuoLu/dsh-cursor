/** Provider-neutral data crossing the DSH projection / Cursor wire boundary. */
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json }
export type CursorContent =
  | { type: 'text'; text: string }
  | { type: 'reasoning'; text: string }
  | { type: 'image'; data: Uint8Array; mediaType: string; width: number; height: number }
  | { type: 'tool-call'; id: string; name: string; arguments: string }
export interface CursorMessage {
  role: 'system' | 'developer' | 'user' | 'assistant' | 'tool'
  content: CursorContent[]
  toolCallId?: string
  toolName?: string
  isError?: boolean
  /** Host provenance only; never encoded as conversation content. */
  sourceProvider?: string
}
export interface CursorTool {
  name: string
  description: string
  parameters: Record<string, unknown>
}
export interface CursorSelection {
  wireModelId: string
  requestedModelId?: string
  maxMode?: boolean
  parameters?: { key: string; value: string }[]
}
/** Opaque per-conversation wire state, created by `createCursorConversation`. */
export interface CursorConversation {
  readonly kind: 'cursor-conversation'
}
export interface CursorRunRequest {
  /** Stable for one host conversation so Cursor can reuse its server-side state and cache. */
  conversationId: string
  /** Blob store and latest checkpoint shared by every Run of this conversation. */
  conversation?: CursorConversation
  selection: CursorSelection
  messages: CursorMessage[]
  tools: CursorTool[]
  cwd?: string
  /** Coordinator-owned durable checkpoint sink; never receives transport credentials. */
  onCheckpoint?: (conversation: CursorConversation) => void | Promise<void>
}
export interface CursorTransportOptions {
  accessToken: string
  endpoint?: string
  /** A request-lifetime signal owned by the coordinator, not a completed model step. */
  signal?: AbortSignal
  headers?: Record<string, string>
  proxyUrl?: string
  /** Unary metadata uses the host fetch by default; HTTP/2 remains an explicit transport choice. */
  unaryTransport?: 'fetch' | 'http2'
  /** Auto pipelines large histories via Cursor's native RunSSE/BidiAppend protocol. */
  runTransport?: 'auto' | 'http2' | 'split'
  /** Optional diagnostic sink for Run milestones; no data beyond names and counts. */
  trace?: (event: string, detail?: Record<string, number | string>) => void
}
export interface CursorToolResult {
  content: CursorContent[]
  isError?: boolean
}
export interface CursorUsage {
  inputTokens: number
  outputTokens: number
  cacheReadTokens?: number
  cacheWriteTokens?: number
  reasoningTokens?: number
  contextTokens?: number
}
export type CursorEvent =
  | { type: 'text'; text: string }
  | { type: 'reasoning'; text: string }
  /** An actual MCP exec request, never a display-only tool update. */
  | { type: 'tool'; callKey: string; name: string; arguments: string }
  | { type: 'tools-listed'; count: number }
  | { type: 'usage'; usage: CursorUsage }
  | { type: 'done'; reason: 'stop' | 'max-tokens' }
export interface CursorRun {
  readonly events: AsyncIterable<CursorEvent>
  reply(callKey: string, result: CursorToolResult): Promise<void>
  /** Add a user message to the running turn, as Cursor's own steering does. */
  inject(content: CursorContent[]): Promise<void>
  cancel(reason?: unknown): Promise<void>
  close(): Promise<void>
}
export interface CursorUsableModel {
  modelId: string
  displayName: string
  displayModelId?: string
  displayNameShort?: string
  maxMode?: boolean
  thinkingDetails?: unknown
  /** Only authoritative upstream metadata; absence must not become a guessed capability. */
  contextWindow?: number
  inputModalities?: ('text' | 'image')[]
}
