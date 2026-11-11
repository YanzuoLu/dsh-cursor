/**
 * The Noir schema is kept intact. Only the named OMP additions below are applied.
 * Copyright (c) 2026 NOirBRight, 2025 Mario Zechner, 2025-2026 Can Bölük,
 * 2026 Stencil Labs, Inc. MIT notices and precise provenance in UPSTREAM.md.
 */
import { clone, create, createFileRegistry } from '@bufbuild/protobuf'
import type { GenMessage } from '@bufbuild/protobuf/codegenv2'
import {
  DescriptorProtoSchema, FieldDescriptorProtoSchema, FieldDescriptorProto_Label,
  FieldDescriptorProto_Type, OneofDescriptorProtoSchema, FileDescriptorProtoSchema,
} from '@bufbuild/protobuf/wkt'
import { file_agent } from './vendor/agent_pb.js'
import type { AgentClientMessage, AgentServerMessage, InteractionUpdate, TurnEndedUpdate, RequestContext, McpDescriptor } from './vendor/agent_pb.js'

export type WireMcpMetaToolOptions = {
  $typeName: 'agent.v1.McpMetaToolOptions'
  enabled: boolean
  mcpDescriptors: McpDescriptor[]
}
export type WireRequestContext = RequestContext & {
  mcpMetaToolOptions?: WireMcpMetaToolOptions
  mcpInfoComplete: boolean
}

export type WireTurnEnded = TurnEndedUpdate & {
  inputTokens?: bigint
  outputTokens?: bigint
  cacheReadTokens?: bigint
  cacheWriteTokens?: bigint
  reasoningTokens?: bigint
}
export type WireModelCallCompleted = {
  $typeName: 'agent.v1.ModelCallCompletedUpdate'
  modelCallId: string
  inputTokens?: bigint
  outputTokens?: bigint
  cacheReadTokens?: bigint
  cacheWriteTokens?: bigint
  reasoningTokens?: bigint
}
export type WireInteractionUpdate = Omit<InteractionUpdate, 'message'> & {
  message: Exclude<InteractionUpdate['message'], { case: 'turnEnded' }>
    | { case: 'turnEnded'; value: WireTurnEnded }
    | { case: 'modelCallCompleted'; value: WireModelCallCompleted }
    | { case: 'toolRequestsListed'; value: { $typeName: 'agent.v1.ToolRequestsListedUpdate'; callCount: number } }
    | { case: 'contextInjectionState'; value: { injectionId: string; state?: { state: { case: string | undefined; value?: { reason?: string } } } } }
}
export type WireAgentServerMessage = Omit<AgentServerMessage, 'message'> & {
  message: Exclude<AgentServerMessage['message'], { case: 'interactionUpdate' }>
    | { case: 'interactionUpdate'; value: WireInteractionUpdate }
}

const descriptor = clone(FileDescriptorProtoSchema, file_agent.proto)

// Optional int64 token counters numbered from `first`, as TurnEndedUpdate and
// ModelCallCompletedUpdate carry them.
function addTokenCounters(message: (typeof descriptor.messageType)[number], first: number) {
  for (const [index, name] of ['input_tokens', 'output_tokens', 'cache_read_tokens', 'cache_write_tokens', 'reasoning_tokens'].entries()) {
    const oneofIndex = message.oneofDecl.length
    message.oneofDecl.push(create(OneofDescriptorProtoSchema, { name: `_${name}` }))
    message.field.push(create(FieldDescriptorProtoSchema, {
      name, jsonName: name.replace(/_([a-z])/gu, (_, letter: string) => letter.toUpperCase()),
      number: index + first, label: FieldDescriptorProto_Label.OPTIONAL,
      type: FieldDescriptorProto_Type.INT64, proto3Optional: true, oneofIndex,
    }))
  }
}
addTokenCounters(descriptor.messageType.find(message => message.name === 'TurnEndedUpdate')!, 1)
// One model call's own usage, sent before the turn's aggregate TurnEndedUpdate.
// Field numbers follow @magpie-community/opencode-cursor-auth (MIT), usageOf.
const modelCall = create(DescriptorProtoSchema, {
  name: 'ModelCallCompletedUpdate',
  field: [create(FieldDescriptorProtoSchema, {
    name: 'model_call_id', jsonName: 'modelCallId', number: 1, label: FieldDescriptorProto_Label.OPTIONAL,
    type: FieldDescriptorProto_Type.STRING,
  })],
})
addTokenCounters(modelCall, 2)
descriptor.messageType.push(modelCall)
descriptor.messageType.find(message => message.name === 'InteractionUpdate')!.field.push(create(FieldDescriptorProtoSchema, {
  name: 'model_call_completed', jsonName: 'modelCallCompleted', number: 31, label: FieldDescriptorProto_Label.OPTIONAL,
  type: FieldDescriptorProto_Type.MESSAGE, typeName: '.agent.v1.ModelCallCompletedUpdate', oneofIndex: 0,
}))
descriptor.messageType.push(create(DescriptorProtoSchema, {
  name: 'ToolRequestsListedUpdate',
  field: [create(FieldDescriptorProtoSchema, {
    name: 'call_count', jsonName: 'callCount', number: 1, label: FieldDescriptorProto_Label.OPTIONAL,
    type: FieldDescriptorProto_Type.UINT32,
  })],
}))
descriptor.messageType.find(message => message.name === 'InteractionUpdate')!.field.push(create(FieldDescriptorProtoSchema, {
  name: 'tool_requests_listed', jsonName: 'toolRequestsListed', number: 27, label: FieldDescriptorProto_Label.OPTIONAL,
  type: FieldDescriptorProto_Type.MESSAGE, typeName: '.agent.v1.ToolRequestsListedUpdate', oneofIndex: 0,
}))
// Mid-run context injection, as Cursor SDK `Run.steer` sends it. Field numbers
// follow oakimov/cursor-opencode-provider (MIT), protocol/messages.ts.
const field = (name: string, jsonName: string, number: number, type: FieldDescriptorProto_Type, typeName?: string, oneofIndex?: number) =>
  create(FieldDescriptorProtoSchema, { name, jsonName, number, label: FieldDescriptorProto_Label.OPTIONAL, type, ...(typeName ? { typeName } : {}), ...(oneofIndex === undefined ? {} : { oneofIndex }) })
const { STRING, MESSAGE, INT32, INT64 } = FieldDescriptorProto_Type
descriptor.messageType.push(
  create(DescriptorProtoSchema, { name: 'SystemContextInjection', field: [field('producer', 'producer', 1, STRING), field('content', 'content', 2, STRING)] }),
  create(DescriptorProtoSchema, { name: 'UserContextInjection', field: [
    field('user_message', 'userMessage', 1, MESSAGE, '.agent.v1.UserMessage'),
    field('request_context', 'requestContext', 2, MESSAGE, '.agent.v1.RequestContext'),
  ] }),
  create(DescriptorProtoSchema, { name: 'InjectContextAction', field: [
    field('injection_id', 'injectionId', 1, STRING), field('expected_run_id', 'expectedRunId', 2, STRING),
    field('user_context', 'userContext', 3, MESSAGE, '.agent.v1.UserContextInjection'),
    field('system_context', 'systemContext', 4, MESSAGE, '.agent.v1.SystemContextInjection'),
  ] }),
  create(DescriptorProtoSchema, { name: 'ContextInjectionDelivered', field: [field('step', 'step', 1, INT32), field('delivery_batch_id', 'deliveryBatchId', 2, STRING), field('delivered_at_ms', 'deliveredAtMs', 3, INT64)] }),
  create(DescriptorProtoSchema, { name: 'ContextInjectionRejected', field: [field('reason', 'reason', 1, STRING)] }),
  ...['ContextInjectionQueued', 'ContextInjectionQueuedForNextTurn', 'ContextInjectionCancelled'].map(name => create(DescriptorProtoSchema, { name })),
  create(DescriptorProtoSchema, { name: 'ContextInjectionState', oneofDecl: [create(OneofDescriptorProtoSchema, { name: 'state' })], field: [
    field('queued', 'queued', 1, MESSAGE, '.agent.v1.ContextInjectionQueued', 0),
    field('delivered', 'delivered', 2, MESSAGE, '.agent.v1.ContextInjectionDelivered', 0),
    field('queued_for_next_turn', 'queuedForNextTurn', 3, MESSAGE, '.agent.v1.ContextInjectionQueuedForNextTurn', 0),
    field('cancelled', 'cancelled', 4, MESSAGE, '.agent.v1.ContextInjectionCancelled', 0),
    field('rejected', 'rejected', 5, MESSAGE, '.agent.v1.ContextInjectionRejected', 0),
  ] }),
  create(DescriptorProtoSchema, { name: 'ContextInjectionStateUpdate', field: [
    field('injection_id', 'injectionId', 1, STRING), field('state', 'state', 2, MESSAGE, '.agent.v1.ContextInjectionState'),
  ] }),
)
const message = (name: string) => descriptor.messageType.find(item => item.name === name)!
message('ConversationAction').field.push(field('inject_context_action', 'injectContextAction', 19, MESSAGE, '.agent.v1.InjectContextAction', 0))
message('InteractionUpdate').field.push(field('context_injection_state', 'contextInjectionState', 23, MESSAGE, '.agent.v1.ContextInjectionStateUpdate', 0))
// The run id an injection's expected_run_id refers to.
message('AgentRunRequest').field.push(field('run_id', 'runId', 25, STRING))

// CLI-style MCP advertisement. Full definitions are returned on exec #36,
// not mixed into RequestContext.tools on each new Run.
descriptor.messageType.push(create(DescriptorProtoSchema, {
  name: 'McpMetaToolOptions', field: [
    field('enabled', 'enabled', 1, FieldDescriptorProto_Type.BOOL),
    create(FieldDescriptorProtoSchema, {
      name: 'mcp_descriptors', jsonName: 'mcpDescriptors', number: 2,
      label: FieldDescriptorProto_Label.REPEATED,
      type: MESSAGE, typeName: '.agent.v1.McpDescriptor',
    }),
  ],
}))
message('RequestContext').field.push(
  field('mcp_meta_tool_options', 'mcpMetaToolOptions', 34, MESSAGE, '.agent.v1.McpMetaToolOptions'),
  field('mcp_info_complete', 'mcpInfoComplete', 36, FieldDescriptorProto_Type.BOOL),
)

const registry = createFileRegistry(descriptor, () => undefined)
export const WireRequestContextSchema = registry.getMessage('agent.v1.RequestContext')! as GenMessage<WireRequestContext>
export const WireMcpMetaToolOptionsSchema = registry.getMessage('agent.v1.McpMetaToolOptions')! as GenMessage<WireMcpMetaToolOptions>
export const WireAgentServerMessageSchema = registry.getMessage('agent.v1.AgentServerMessage')! as GenMessage<WireAgentServerMessage>
export const WireInteractionUpdateSchema = registry.getMessage('agent.v1.InteractionUpdate')! as GenMessage<WireInteractionUpdate>
export const WireTurnEndedSchema = registry.getMessage('agent.v1.TurnEndedUpdate')! as GenMessage<WireTurnEnded>
export const WireAgentClientMessageSchema = registry.getMessage('agent.v1.AgentClientMessage')! as GenMessage<AgentClientMessage>
