import type { CursorEvent } from '../contracts.js'
import { CursorProtocolError, LIMITS, limitError } from './errors.js'

/** Reader leases may end between tool steps without ending the live Run. */
export class RunEvents implements AsyncIterable<CursorEvent> {
  #queue: { event: CursorEvent; bytes: number }[] = []
  #bytes = 0
  #ended = false
  #failure?: Error
  #lease?: object
  #waiting?: { lease: object; resolve: (value: IteratorResult<CursorEvent>) => void; reject: (error: Error) => void }

  push(event: CursorEvent): void {
    if (this.#ended) return
    const bytes = Buffer.byteLength(JSON.stringify(event))
    if (bytes > LIMITS.eventBytes) throw limitError()
    if (this.#waiting) {
      const waiting = this.#waiting
      this.#waiting = undefined
      waiting.resolve({ done: false, value: event })
      return
    }
    if (this.#queue.length >= LIMITS.eventCount || this.#bytes + bytes > LIMITS.eventBytes) throw limitError()
    this.#queue.push({ event, bytes })
    this.#bytes += bytes
  }

  finish(failure?: Error): void {
    if (this.#ended) return
    this.#ended = true
    this.#failure = failure
    if (failure) {
      this.#queue.length = 0
      this.#bytes = 0
    }
    const waiting = this.#waiting
    this.#waiting = undefined
    if (waiting) {
      if (failure) waiting.reject(failure)
      else waiting.resolve({ done: true, value: undefined })
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<CursorEvent> {
    if (this.#lease) throw new CursorProtocolError('INVALID_STATE', 'Cursor Run already has an active events reader')
    const lease = {}
    this.#lease = lease
    let returned = false
    const release = (): void => {
      returned = true
      if (this.#lease === lease) this.#lease = undefined
      if (this.#waiting?.lease === lease) {
        const waiting = this.#waiting
        this.#waiting = undefined
        waiting.resolve({ done: true, value: undefined })
      }
    }
    return {
      next: async () => {
        if (returned) return { done: true, value: undefined }
        if (this.#waiting) throw new CursorProtocolError('INVALID_STATE', 'Concurrent Cursor event reads are not supported')
        const item = this.#queue.shift()
        if (item) {
          this.#bytes -= item.bytes
          return { done: false, value: item.event }
        }
        if (this.#ended) {
          release()
          if (this.#failure) throw this.#failure
          return { done: true, value: undefined }
        }
        return new Promise<IteratorResult<CursorEvent>>((resolve, reject) => {
          this.#waiting = { lease, resolve, reject }
        })
      },
      return: async () => { release(); return { done: true, value: undefined } },
      throw: async error => { release(); throw error },
    }
  }
}
