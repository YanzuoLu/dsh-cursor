import { create, fromJson, toBinary } from '@bufbuild/protobuf'
import type { JsonValue } from '@bufbuild/protobuf'
import { ValueSchema } from '@bufbuild/protobuf/wkt'
import type { CursorTool } from '../contracts.js'
import { BlobStore } from './blobs.js'
import { MCP_PROVIDER } from './history.js'
import { CursorProtocolError, LIMITS, limitError } from './errors.js'
import { WireRequestContextSchema, WireMcpMetaToolOptionsSchema } from './schema.js'
import {
  AgentClientMessageSchema, ExecClientMessageSchema, GetBlobResultSchema,
  ForceBackgroundShellResultSchema, ForceBackgroundShellStatus,
  ForceBackgroundSubagentResultSchema, ForceBackgroundSubagentStatus,
  InteractionResponseSchema, KvClientMessageSchema, McpStateExecResultSchema,
  McpStateServerSchema, McpStateSuccessSchema, McpToolDefinitionSchema,
  RequestContextEnvSchema, RequestContextResultSchema, McpDescriptorSchema, McpToolDescriptorSchema,
  RequestContextSuccessSchema, SetBlobResultSchema, CursorRuleSchema, CursorRuleTypeSchema, CursorRuleTypeGlobalSchema,
} from './vendor/agent_pb.js'
import type {
  AgentClientMessage, ExecClientMessage, ExecServerMessage, InteractionQuery,
  KvClientMessage, KvServerMessage, McpArgs, McpToolDefinition,
} from './vendor/agent_pb.js'

type ExecCase = NonNullable<ExecServerMessage['message']['case']>
type ResponseCase = NonNullable<ExecClientMessage['message']['case']>
const DENIALS: Partial<Record<ExecCase, ResponseCase>> = {
  shellArgs: 'shellResult', shellStreamArgs: 'shellStream', miniSweAgentBashArgs: 'miniSweAgentBashResult',
  readArgs: 'readResult', redactedReadArgs: 'redactedReadResult', writeArgs: 'writeResult', deleteArgs: 'deleteResult',
  lsArgs: 'lsResult', grepArgs: 'grepResult', diagnosticsArgs: 'diagnosticsResult',
  backgroundShellSpawnArgs: 'backgroundShellSpawnResult', writeShellStdinArgs: 'writeShellStdinResult',
  fetchArgs: 'fetchResult', computerUseArgs: 'computerUseResult', recordScreenArgs: 'recordScreenResult',
  subagentArgs: 'subagentResult', subagentAwaitArgs: 'subagentAwaitResult',
  piBashArgs: 'piBashResult', piReadArgs: 'piReadResult', piWriteArgs: 'piWriteResult',
  piEditArgs: 'piEditResult', piLsArgs: 'piLsResult', piFindArgs: 'piFindResult', piGrepArgs: 'piGrepResult',
  canvasDiagnosticsArgs: 'canvasDiagnosticsResult', conversationSearchArgs: 'conversationSearchResult',
  agentStoreConflictArgs: 'agentStoreConflictResult', smartModeClassifierArgs: 'smartModeClassifierResult',
  listMcpResourcesExecArgs: 'listMcpResourcesExecResult', readMcpResourceExecArgs: 'readMcpResourceExecResult',
}
const REASON = 'Native execution is not supported by this client. Tools are owned by DeepSeek Harness.'

export function execResponse(exec: ExecServerMessage, message: ExecClientMessage['message']): AgentClientMessage {
  return create(AgentClientMessageSchema, {
    message: { case: 'execClientMessage', value: create(ExecClientMessageSchema, { id: exec.id, execId: exec.execId, message }) },
  })
}

export function toolDefinitions(tools: readonly CursorTool[]): McpToolDefinition[] {
  if (tools.length > LIMITS.toolCount) throw limitError()
  const seen = new Set<string>()
  let bytes = 0
  return tools.map(tool => {
    if (!tool.name || seen.has(tool.name)) throw new CursorProtocolError('INVALID_REQUEST', 'Cursor tool names must be unique and nonempty')
    seen.add(tool.name)
    const definition = create(McpToolDefinitionSchema, {
      name: tool.name, toolName: tool.name, description: tool.description, providerIdentifier: MCP_PROVIDER,
      inputSchema: toBinary(ValueSchema, fromJson(ValueSchema, tool.parameters as JsonValue)),
    })
    bytes += toBinary(McpToolDefinitionSchema, definition).byteLength
    if (bytes > LIMITS.toolBytes) throw limitError()
    return definition
  })
}

/** No suffix aliases. A populated toolName is matched exactly to the declaration. */
export function advertisedName(args: McpArgs, names: ReadonlySet<string>): string | undefined {
  if (args.providerIdentifier && args.providerIdentifier !== MCP_PROVIDER) return undefined
  if (args.serverIdentifier && args.serverIdentifier !== MCP_PROVIDER) return undefined
  const name = args.toolName || args.name
  if (!names.has(name)) return undefined
  return name
}

export function contextResponse(exec: ExecServerMessage, definitions: McpToolDefinition[], cwd?: string, instructions?: string): AgentClientMessage {
  return execResponse(exec, { case: 'requestContextResult', value: create(RequestContextResultSchema, {
    result: { case: 'success', value: create(RequestContextSuccessSchema, {
      requestContext: create(WireRequestContextSchema, {
        mcpInfoComplete: true,
        mcpMetaToolOptions: create(WireMcpMetaToolOptionsSchema, {
          enabled: true,
          mcpDescriptors: definitions.length ? [create(McpDescriptorSchema, {
            serverName: MCP_PROVIDER, serverIdentifier: MCP_PROVIDER,
            tools: definitions.map(tool => create(McpToolDescriptorSchema, { toolName: tool.toolName })),
          })] : [],
        }),
        // Cursor's standard instruction carrier. Content is the exact host text,
        // not a provider-authored persona, tool manifest or filesystem rule.
        rules: instructions ? [create(CursorRuleSchema, {
          fullPath: 'DSH system instructions', content: instructions,
          type: create(CursorRuleTypeSchema, { type: { case: 'global', value: create(CursorRuleTypeGlobalSchema) } }),
        })] : [],
        ...(cwd !== undefined ? { env: create(RequestContextEnvSchema, { workspacePaths: [cwd] }) } : {}),
      }),
    }) },
  }) })
}

export function mcpStateResponse(exec: ExecServerMessage, definitions: McpToolDefinition[]): AgentClientMessage {
  if (exec.message.case !== 'mcpStateExecArgs') throw new Error('Expected MCP state request')
  const requested = exec.message.value.serverIdentifiers
  return execResponse(exec, { case: 'mcpStateExecResult', value: create(McpStateExecResultSchema, {
    result: { case: 'success', value: create(McpStateSuccessSchema, {
      servers: requested.length === 0 || requested.includes(MCP_PROVIDER)
        ? [create(McpStateServerSchema, { serverName: MCP_PROVIDER, serverIdentifier: MCP_PROVIDER, tools: definitions })] : [],
    }) },
  }) })
}

export function kvResponse(message: KvServerMessage, blobs: BlobStore): AgentClientMessage {
  let response: KvClientMessageShape
  if (message.message.case === 'getBlobArgs') {
    const data = blobs.get(message.message.value.blobId)
    response = { case: 'getBlobResult', value: create(GetBlobResultSchema, data === undefined ? {} : { blobData: data }) }
  } else if (message.message.case === 'setBlobArgs') {
    blobs.set(message.message.value.blobId, message.message.value.blobData)
    response = { case: 'setBlobResult', value: create(SetBlobResultSchema) }
  } else throw new CursorProtocolError('UNSUPPORTED_CONTROL', 'Unsupported Cursor key-value request')
  return create(AgentClientMessageSchema, { message: { case: 'kvClientMessage', value: create(KvClientMessageSchema, { id: message.id, message: response }) } })
}
type KvClientMessageShape = KvClientMessage['message']

/** All denials are encoded through the actual generated response descriptor. */
export function rejectNative(exec: ExecServerMessage): AgentClientMessage {
  // No native shell or subagent can exist in this client to be backgrounded.
  if (exec.message.case === 'forceBackgroundShellArgs') return execResponse(exec, {
    case: 'forceBackgroundShellResult', value: create(ForceBackgroundShellResultSchema, { status: ForceBackgroundShellStatus.NOT_FOUND }),
  })
  if (exec.message.case === 'forceBackgroundSubagentArgs') return execResponse(exec, {
    case: 'forceBackgroundSubagentResult', value: create(ForceBackgroundSubagentResultSchema, { status: ForceBackgroundSubagentStatus.NOT_FOUND }),
  })
  const responseCase = exec.message.case ? DENIALS[exec.message.case] : undefined
  const result = ExecClientMessageSchema.fields.find(field => field.localName === responseCase)
  if (result?.fieldKind === 'message') {
    for (const denial of ['rejected', 'error', 'failure']) {
      const field = result.message.fields.find(candidate => candidate.localName === denial && candidate.oneof !== undefined)
      if (field?.fieldKind !== 'message') continue
      const reason = field.message.fields.find(candidate => ['reason', 'error', 'errorMessage', 'message'].includes(candidate.localName) && candidate.fieldKind === 'scalar' && candidate.scalar === 9)
      if (!reason) continue
      const response = fromJson(ExecClientMessageSchema, {
        id: exec.id, execId: exec.execId,
        [result.jsonName]: { [field.jsonName]: { [reason.jsonName]: REASON } },
      })
      return create(AgentClientMessageSchema, { message: { case: 'execClientMessage', value: response } })
    }
  }
  throw new CursorProtocolError('UNSUPPORTED_EXEC', 'Unsupported Cursor execution request has no safe denial schema')
}

export function rejectInteraction(query: InteractionQuery): AgentClientMessage {
  let response: JsonValue
  if (query.query.case === 'askQuestionInteractionQuery') {
    response = { id: query.id, askQuestionInteractionResponse: { result: { rejected: { reason: REASON } } } }
  } else if (query.query.case === 'switchModeRequestQuery') {
    response = { id: query.id, switchModeRequestResponse: { rejected: { reason: REASON } } }
  } else if (query.query.case === 'createPlanRequestQuery') {
    response = { id: query.id, createPlanRequestResponse: { result: { error: { error: REASON } } } }
  } else {
    throw new CursorProtocolError('UNSUPPORTED_INTERACTION', 'Unsupported Cursor interaction query has no safe denial schema')
  }
  return create(AgentClientMessageSchema, { message: { case: 'interactionResponse', value: fromJson(InteractionResponseSchema, response) } })
}
