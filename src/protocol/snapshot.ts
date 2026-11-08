import { promisify } from 'node:util'
import { createHash, randomBytes } from 'node:crypto'
import { gzip, gunzip } from 'node:zlib'
import { fromBinary, toBinary } from '@bufbuild/protobuf'
import type { CursorConversation } from '../contracts.js'
import type { ConversationStore } from './run.js'
import { BlobStore } from './blobs.js'
import { LIMITS } from './errors.js'
import { ConversationStateStructureSchema } from './vendor/agent_pb.js'

const compress = promisify(gzip)
const decompress = promisify(gunzip)

export interface SnapshotBlobStore {
  has(hash: string): boolean
  put(hash: string, encoded: string): Promise<void>
  get(hash: string): string | undefined
}
const blobHashes = new WeakMap<Uint8Array, string>()
function blobHash(data: Uint8Array): string {
  let hash = blobHashes.get(data)
  if (!hash) { hash = createHash('sha256').update(data).digest('hex'); blobHashes.set(data, hash) }
  return hash
}

type BlobEntry = [Uint8Array, Uint8Array]
function packHash(entries: BlobEntry[]): string {
  return createHash('sha256').update(JSON.stringify(entries.map(([id, data]) => [Buffer.from(id).toString('base64'), blobHash(data)]))).digest('hex')
}
function blobPacks(entries: BlobEntry[]): BlobEntry[][] {
  const packs: BlobEntry[][] = []
  let pack: BlobEntry[] = [], bytes = 0
  for (const entry of entries) {
    if (entry[1].byteLength >= 64 * 1024) {
      if (pack.length) { packs.push(pack); pack = []; bytes = 0 }
      packs.push([entry]); continue
    }
    if (pack.length && (pack.length >= 64 || bytes + entry[1].byteLength > 1024 * 1024)) {
      packs.push(pack); pack = []; bytes = 0
    }
    pack.push(entry); bytes += entry[1].byteLength
  }
  if (pack.length) packs.push(pack)
  return packs
}

/** Private conversation state, including its stable blob key and recovery eligibility. */
export async function snapshotCursorConversation(value: CursorConversation, external?: SnapshotBlobStore): Promise<string> {
  const store = value as ConversationStore
  if (!/^[0-9a-f]{64}$/u.test(store.blobEncryptionKey)) throw new Error('Invalid Cursor blob cache key')
  // Capture metadata and immutable blob byte references before the first await.
  const snapshot = {
    version: external ? 4 : 2, id: store.id, blobEncryptionKey: store.blobEncryptionKey,
    resume: store.resume, recovery: store.recovery, contextTokens: store.contextTokens, hostHistory: store.hostHistory,
    notes: store.notes.map(content => content.map(item => item.type === 'image'
      ? { ...item, data: Uint8Array.from(item.data) } : item)),
    state: store.state ? toBinary(ConversationStateStructureSchema, store.state) : undefined,
    blobs: store.blobs.entries(),
  }
  let references: string[] | undefined
  if (external) {
    references = []
    // Small records share bounded immutable packs; a large image is never
    // rewritten merely because another tiny checkpoint record was appended.
    for (const pack of blobPacks(snapshot.blobs)) {
      const hash = packHash(pack)
      if (!external.has(hash)) {
        const json = JSON.stringify(pack.map(([id, data]) => [Buffer.from(id).toString('base64'), Buffer.from(data).toString('base64')]))
        await external.put(hash, (await compress(json)).toString('base64'))
      }
      references.push(hash)
    }
  }
  const json = JSON.stringify({ ...snapshot, ...(references ? { blobs: references } : {}) }, (_key, item) => item instanceof Uint8Array
    ? { cursorBytes: Buffer.from(item).toString('base64') } : item)
  return (await compress(json)).toString('base64')
}

/** Legacy terminal snapshots acquire one key on restore; v2 also retains safe in-flight checkpoints. */
export async function restoreCursorConversation(snapshot: string, external?: SnapshotBlobStore): Promise<CursorConversation> {
  const bytes = await decompress(Buffer.from(snapshot, 'base64'), { maxOutputLength: LIMITS.blobBytes * 2 })
  const value = JSON.parse(bytes.toString('utf8'), (_key, item) => item && typeof item === 'object'
    && typeof item.cursorBytes === 'string' && Object.keys(item).length === 1
    ? new Uint8Array(Buffer.from(item.cursorBytes, 'base64')) : item)
  if (![1, 2, 3, 4].includes(value.version) || typeof value.id !== 'string' || !value.id
    || (value.resume !== undefined && (typeof value.resume.anchor !== 'string' || typeof value.resume.finalText !== 'string'))
    || (value.state !== undefined && !(value.state instanceof Uint8Array))
    || (value.version === 1 && (!value.resume || !value.state))
    || (value.version >= 2 && (typeof value.blobEncryptionKey !== 'string' || !/^[0-9a-f]{64}$/u.test(value.blobEncryptionKey)))
    || (value.contextTokens !== undefined && (!Number.isSafeInteger(value.contextTokens) || value.contextTokens <= 0))
    || (value.hostHistory !== undefined && (!Number.isSafeInteger(value.hostHistory.count)
      || value.hostHistory.count < 0 || typeof value.hostHistory.digest !== 'string'))
    || !Array.isArray(value.blobs) || !Array.isArray(value.notes)) {
    throw new Error('Invalid Cursor conversation snapshot')
  }
  if (value.recovery !== undefined) {
    const recovery = value.recovery
    if (!value.state || value.resume || typeof recovery.requestKey !== 'string' || typeof recovery.checkpointHash !== 'string'
      || !Number.isSafeInteger(recovery.attempts) || recovery.attempts < 0 || !Array.isArray(recovery.output)
      || recovery.output.some((event: { type?: unknown; text?: unknown }) => !event
        || !['text', 'reasoning'].includes(String(event.type)) || typeof event.text !== 'string')) {
      throw new Error('Invalid Cursor recovery checkpoint')
    }
  }
  const blobs = new BlobStore()
  if (value.version === 4) {
    for (const hash of value.blobs) {
      if (typeof hash !== 'string' || !/^[0-9a-f]{64}$/u.test(hash)) throw new Error('Invalid Cursor blob pack reference')
      const encoded = external?.get(hash)
      if (encoded === undefined) throw new Error('Cursor snapshot blob pack is missing')
      const packed = JSON.parse((await decompress(Buffer.from(encoded, 'base64'), { maxOutputLength: LIMITS.blobBytes * 2 })).toString('utf8'))
      if (!Array.isArray(packed) || packed.length > 64 || packed.some(entry => !Array.isArray(entry)
        || entry.length !== 2 || entry.some(part => typeof part !== 'string'))) throw new Error('Invalid Cursor blob pack')
      const entries: BlobEntry[] = packed.map(([id, data]: [string, string]) => [new Uint8Array(Buffer.from(id, 'base64')), new Uint8Array(Buffer.from(data, 'base64'))])
      if (packHash(entries) !== hash) throw new Error('Cursor blob pack checksum mismatch')
      for (const [id, data] of entries) blobs.set(id, data)
    }
  } else {
    for (const [id, payload] of value.blobs) {
      if (!(id instanceof Uint8Array)) throw new Error('Invalid Cursor snapshot blob id')
      let data = payload
      if (value.version === 3) {
        if (typeof payload !== 'string' || !/^[0-9a-f]{64}$/u.test(payload)) throw new Error('Invalid Cursor snapshot blob reference')
        const encoded = external?.get(payload)
        if (encoded === undefined) throw new Error('Cursor snapshot blob is missing')
        data = new Uint8Array(await decompress(Buffer.from(encoded, 'base64'), { maxOutputLength: LIMITS.blobBytes }))
        if (blobHash(data) !== payload) throw new Error('Cursor snapshot blob checksum mismatch')
      }
      if (!(data instanceof Uint8Array)) throw new Error('Invalid Cursor snapshot blob')
      blobs.set(id, data)
    }
  }
  const store: ConversationStore = {
    kind: 'cursor-conversation', id: value.id,
    blobEncryptionKey: value.blobEncryptionKey ?? randomBytes(32).toString('hex'),
    resume: value.resume, recovery: value.recovery, contextTokens: value.contextTokens, hostHistory: value.hostHistory, notes: value.notes, blobs,
    state: value.state ? fromBinary(ConversationStateStructureSchema, value.state) : undefined,
  }
  return store
}
