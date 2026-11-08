import { snapshotCursorConversation, restoreCursorConversation } from './protocol.js'

const MAX_AGE_MS = 24 * 60 * 60 * 1000
const MAX_RECORDS = 64

/** Native durable KV owns atomic records; immutable blobs are written once. */
export function createConversationCache(records, logger, blobs) {
  const warn = () => logger?.warn('cursor: conversation cache unavailable; continuing with in-memory state')
  let tail = Promise.resolve()
  const enqueue = action => {
    const result = tail.then(action)
    tail = result.catch(() => {})
    return result
  }
  const repository = blobs && {
    has: hash => blobs.get(hash) !== undefined,
    get: hash => blobs.get(hash)?.encoded,
    put: (hash, encoded) => blobs.put(hash, { encoded }),
  }
  async function collect() {
    const ordered = [...records.entries()].sort((a, b) => b[1].savedAt - a[1].savedAt)
    for (const [index, [id, record]] of ordered.entries()) {
      if (index >= MAX_RECORDS || Date.now() - record.savedAt > MAX_AGE_MS) await records.delete(id)
    }
    if (blobs) {
      const retained = new Set([...records.entries()].flatMap(([, record]) => record.blobRefs ?? []))
      for (const hash of blobs.keys()) if (!retained.has(hash)) await blobs.delete(hash)
    }
  }
  return {
    load(sessionId, binding) {
      return enqueue(async () => {
        const record = records.get(sessionId)
        if (!record || record.binding !== binding || Date.now() - record.savedAt > MAX_AGE_MS) return undefined
        try { return await restoreCursorConversation(record.snapshot, repository) }
        catch { warn(); return undefined }
      })
    },
    save(sessionId, binding, store) {
      return enqueue(async () => {
        const references = new Set()
        let storageFailure
        const sink = repository && {
          ...repository,
          has: hash => { references.add(hash); return repository.has(hash) },
          put: async (hash, encoded) => {
            try { await repository.put(hash, encoded) }
            catch (error) { storageFailure = error; throw error }
          },
        }
        let snapshot
        try { snapshot = await snapshotCursorConversation(store, sink) }
        catch (error) {
          if (storageFailure !== error) throw error
          warn(); return
        }
        try {
          // Publish metadata only after every referenced blob is durable. Queue
          // GC with saves so it cannot remove another in-flight snapshot's blobs.
          const previous = records.get(sessionId)
          // An identity-only opening record must not evict the old immutable
          // blobs before the replacement checkpoint can reference them again.
          const blobRefs = references.size > 0 || previous?.binding !== binding
            ? [...references] : previous?.blobRefs ?? []
          await records.put(sessionId, { binding, savedAt: Date.now(), snapshot,
            ...(repository ? { blobRefs } : {}),
          })
          await collect()
        } catch { warn() }
      })
    },
    discard(sessionId) {
      return enqueue(async () => {
        try { await records.delete(sessionId); await collect() } catch { warn() }
      })
    },
    flush: () => tail,
  }
}
