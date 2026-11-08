import { createHash } from 'node:crypto'
import { LIMITS, limitError } from './errors.js'

/** Conversation-scoped KV storage; the coordinator owns snapshot persistence. */
export class BlobStore {
  #items = new Map<string, Uint8Array>()
  #bytes = 0

  get bytes(): number { return this.#bytes }
  get(id: Uint8Array): Uint8Array | undefined { return this.#items.get(Buffer.from(id).toString('hex')) }
  put(data: Uint8Array): Uint8Array {
    const id = new Uint8Array(createHash('sha256').update(data).digest())
    this.set(id, data)
    return id
  }
  set(id: Uint8Array, data: Uint8Array): void {
    if (id.byteLength > 128) throw limitError()
    const key = Buffer.from(id).toString('hex')
    const previous = this.#items.get(key)
    const bytes = this.#bytes - (previous?.byteLength ?? 0) + data.byteLength
    if ((!previous && this.#items.size >= LIMITS.blobCount) || bytes > LIMITS.blobBytes) throw limitError()
    this.#items.set(key, Uint8Array.from(data))
    this.#bytes = bytes
  }
  entries(): [Uint8Array, Uint8Array][] {
    return [...this.#items].map(([id, data]) => [new Uint8Array(Buffer.from(id, 'hex')), data])
  }
  clear(): void { this.#items.clear(); this.#bytes = 0 }
}
