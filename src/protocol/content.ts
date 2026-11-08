import { create, fromBinary, fromJson, toBinary, toJson } from '@bufbuild/protobuf'
import type { JsonValue } from '@bufbuild/protobuf'
import { ValueSchema } from '@bufbuild/protobuf/wkt'
import type { CursorContent, CursorToolResult } from '../contracts.js'
import { CursorProtocolError } from './errors.js'
import {
  McpImageContentSchema, McpSuccessSchema, McpTextContentSchema, McpToolResultContentItemSchema,
} from './vendor/agent_pb.js'
import type { McpSuccess } from './vendor/agent_pb.js'

export function parseArguments(raw: string): Record<string, JsonValue> {
  let parsed: unknown
  try { parsed = JSON.parse(raw) }
  catch { throw new CursorProtocolError('INVALID_REQUEST', 'Cursor tool arguments must be a JSON object') }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new CursorProtocolError('INVALID_REQUEST', 'Cursor tool arguments must be a JSON object')
  return parsed as Record<string, JsonValue>
}

export function encodeArguments(raw: string): Record<string, Uint8Array> {
  return Object.fromEntries(Object.entries(parseArguments(raw)).map(([name, value]) => [name, toBinary(ValueSchema, fromJson(ValueSchema, value))]))
}

/** The exec envelope's protobuf Value map is authoritative, including nested JSON. */
export function decodeArguments(args: Record<string, Uint8Array>): string {
  try {
    return JSON.stringify(Object.fromEntries(Object.entries(args).map(([name, value]) => [name, toJson(ValueSchema, fromBinary(ValueSchema, value))])))
  } catch { throw new CursorProtocolError('INVALID_RESPONSE', 'Invalid Cursor MCP argument encoding') }
}

export function textOf(content: readonly CursorContent[]): string {
  return content.filter(item => item.type === 'text').map(item => item.text).join('\n')
}

export function mcpContent(result: CursorToolResult): McpSuccess {
  return create(McpSuccessSchema, {
    isError: result.isError === true,
    content: result.content.map(item => {
      if (item.type === 'image') return create(McpToolResultContentItemSchema, {
        content: { case: 'image', value: create(McpImageContentSchema, { data: item.data, mimeType: item.mediaType }) },
      })
      if (item.type === 'text') return create(McpToolResultContentItemSchema, {
        content: { case: 'text', value: create(McpTextContentSchema, { text: item.text }) },
      })
      throw new CursorProtocolError('INVALID_REQUEST', 'Cursor tool results support text and image content only')
    }),
  })
}
