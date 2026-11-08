import { createHash } from 'node:crypto'
import { requestImageDimensions } from '@deepseek-ai/dsh-attachment'
import {
  LlmError, fileHandleText, offloadedImageText, projectOffloadedImages,
  projectToolUpdates, requestImageHandleText, requiredImageOffload, textOnlyImageText, resolveImageAttachmentAccess,
} from '@deepseek-ai/dsh-llm'

/** Same request-image budget as the other DSH providers. */
export const IMAGE_BUDGET = Object.freeze({ representation: 'base64', maxBytes: 20 * 1024 * 1024 })
const IMAGE_TARGET = Object.freeze({ maxPixels: 2048 * 2048, maxBytes: 1024 * 1024 })

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical)
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]))
  }
  return value
}

/** Hash only model-visible semantics, not durable ids or opaque replay payloads. */
export function requestFingerprint(options) {
  const value = {
    system: options.system,
    messages: (options.messages ?? []).map(message => ({
      role: message.role,
      // Native offloading is an effective-context change, not a display hint.
      content: message.content.map(block => block.type === 'image'
        ? { type: 'image', attachmentId: block.attachment.attachmentId, offloaded: block.offloaded === true } : block),
      ...(message.role === 'tool' ? { toolCallId: message.toolCallId, isError: message.isError === true } : {}),
    })),
  }
  return createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex')
}

function toolNames(messages) {
  const names = new Map()
  for (const message of messages) {
    if (message.role !== 'assistant') continue
    for (const block of message.content) if (block.type === 'tool-call') names.set(String(block.id), block.name)
  }
  return names
}

async function imageVersions(messages, attachments, signal) {
  const refs = new Map()
  for (const message of messages) for (const block of message.content) {
    if (block.type !== 'image' || block.offloaded === true) continue
    if (message.role !== 'user' && message.role !== 'tool') {
      throw new LlmError('Cursor cannot replay image output in this message role.', 'UNSUPPORTED_CONTENT')
    }
    refs.set(block.attachment.attachmentId, block.attachment)
  }
  if (refs.size && !attachments) throw new LlmError('The attachment service is required for Cursor image input.', 'UNSUPPORTED_CONTENT')
  const versions = new Map()
  // Preserve cancellation and avoid multiplying raster work for long histories.
  for (const [id, ref] of refs) {
    signal?.throwIfAborted()
    const target = { ...requestImageDimensions(ref.width, ref.height, IMAGE_TARGET.maxPixels), maxBytes: IMAGE_TARGET.maxBytes }
    versions.set(id, await attachments.readImageRequest(ref, target, signal))
  }
  const count = requiredImageOffload(messages, IMAGE_BUDGET, block => {
    const version = versions.get(block.attachment.attachmentId)
    return version?.data.byteLength ?? 0
  })
  if (count > 0) {
    throw new LlmError('Cursor request images need additional native offloading.', 'IMAGE_OFFLOAD_REQUIRED', { offloadImages: count })
  }
  return versions
}

function plainToolCall(block) {
  return JSON.stringify({ type: 'tool-call', id: String(block.id), name: block.name, arguments: block.arguments })
}

function projectBlocks(message, versions, auxiliary, imageAccess = () => undefined) {
  const content = []
  for (const block of message.content) {
    switch (block.type) {
      case 'text':
        content.push({ type: 'text', text: block.text })
        break
      case 'reasoning':
        content.push({ type: 'reasoning', text: block.text })
        break
      case 'tool-call':
        content.push(!auxiliary && message.role === 'assistant'
          ? { type: 'tool-call', id: String(block.id), name: block.name, arguments: block.arguments }
          : { type: 'text', text: plainToolCall(block) })
        break
      case 'image': {
        if (auxiliary || block.offloaded === true) {
          content.push({ type: 'text', text: block.offloaded === true
            ? offloadedImageText(block.attachment, imageAccess(block.attachment)) : textOnlyImageText(block.attachment) })
          break
        }
        const version = versions.get(block.attachment.attachmentId)
        if (!version) throw new LlmError('Cursor image request version is missing.', 'UNSUPPORTED_CONTENT')
        content.push({ type: 'text', text: requestImageHandleText(block.attachment, version, imageAccess(block.attachment)) })
        content.push({ type: 'image', data: version.data, mediaType: version.mediaType,
          width: version.width, height: version.height })
        break
      }
      case 'file':
        // Native request assembly normally performs this projection already.
        content.push({ type: 'text', text: fileHandleText(block.attachment, undefined) })
        break
      case 'tool-addition':
      case 'tool-removal':
        // The current effective schema catalog is sent as protocol data, not as
        // another provider-authored tool manifest in the system instructions.
        break
      default:
        throw new LlmError(`Cursor does not support content block type ${block.type}.`, 'UNSUPPORTED_CONTENT')
    }
  }
  return content
}

/** Convert only this request projection; never inspect the loaded Session log. */
export async function projectRequest(options, { attachments, signal = options.signal, mapHostPath } = {}) {
  signal?.throwIfAborted()
  const auxiliary = options.purpose === 'compaction' || options.purpose === 'session-title'
  const projected = projectToolUpdates(options.messages ?? [], options.tools, undefined, options.toolHistory)
  const imageAccess = attachments && mapHostPath ? ref => resolveImageAttachmentAccess(attachments, mapHostPath, ref) : () => undefined
  const messages = projectOffloadedImages(projected.messages, ref => offloadedImageText(ref, imageAccess(ref)))
  const versions = auxiliary ? new Map() : await imageVersions(messages, attachments, signal)
  const names = toolNames(messages)
  const out = []
  if (options.system !== undefined && options.system.length) out.push({ role: 'system', content: [{ type: 'text', text: options.system }] })
  for (const message of messages) {
    signal?.throwIfAborted()
    const content = projectBlocks(message, versions, auxiliary, imageAccess)
    if (message.role === 'tool') {
      if (auxiliary) {
        out.push({ role: 'user', content: [{ type: 'text', text: JSON.stringify({
          type: 'tool-result', toolCallId: String(message.toolCallId),
          isError: message.isError === true, content,
        }) }] })
      } else {
        out.push({ role: 'tool', toolCallId: String(message.toolCallId),
          toolName: names.get(String(message.toolCallId)), isError: message.isError === true, content })
      }
    } else {
      out.push({ role: message.role, content,
        ...(message.role === 'assistant' && message.source?.provider ? { sourceProvider: message.source.provider } : {}),
      })
    }
  }
  return out
}

/** Native tool results remain data; no tool body is called from this conversion. */
export async function projectToolResult(message, { attachments, signal, mapHostPath } = {}) {
  if (message.role !== 'tool') throw new LlmError('Cursor continuation requires a native tool-role message.', 'INVALID_TOOL_RESULT')
  signal?.throwIfAborted()
  const imageAccess = attachments && mapHostPath ? ref => resolveImageAttachmentAccess(attachments, mapHostPath, ref) : () => undefined
  const projected = projectOffloadedImages([message], ref => offloadedImageText(ref, imageAccess(ref)))
  const versions = await imageVersions(projected, attachments, signal)
  return { content: projectBlocks(projected[0], versions, false, imageAccess), isError: message.isError === true }
}
