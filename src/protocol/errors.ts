/** Stable, deliberately payload-free errors crossing the provider boundary. */
export class CursorProtocolError extends Error {
  constructor(readonly code: string, message: string, readonly status?: number) {
    super(message)
    this.name = 'CursorProtocolError'
  }
}

export function safeError(error: unknown, fallback = 'Cursor HTTP/2 transport failed'): CursorProtocolError {
  return error instanceof CursorProtocolError ? error : new CursorProtocolError('TRANSPORT', fallback)
}

export function httpError(status: number): CursorProtocolError {
  const code = status === 401 || status === 403 ? 'AUTH'
    : status === 429 ? 'RATE_LIMIT' : status >= 500 ? 'SERVER' : 'INVALID_REQUEST'
  return new CursorProtocolError(code, `Cursor request returned HTTP ${status}`, status)
}

/** Keep a short diagnostic, with anything resembling a credential redacted. */
function serverDetail(text: unknown): string {
  if (typeof text !== 'string' || !text.trim()) return ''
  return text.replace(/bearer\s+\S+/giu, '[redacted]').replace(/[A-Za-z0-9_\-.]{32,}/gu, '[redacted]').replace(/\s+/gu, ' ').trim().slice(0, 300)
}

/** Report Cursor's status code and a redacted server message, never headers or abort reasons. */
export function statusError(code: string, message?: unknown): CursorProtocolError {
  const known: Record<string, string> = {
    unauthenticated: 'AUTH', permission_denied: 'AUTH', '16': 'AUTH', '7': 'AUTH',
    resource_exhausted: 'RATE_LIMIT', '8': 'RATE_LIMIT',
    canceled: 'ABORTED', cancelled: 'ABORTED', '1': 'ABORTED',
    deadline_exceeded: 'TIMEOUT', '4': 'TIMEOUT', invalid_argument: 'INVALID_REQUEST', '3': 'INVALID_REQUEST',
  }
  const detail = serverDetail(message)
  return new CursorProtocolError(known[code] ?? 'SERVER',
    `Cursor returned ${code || 'an unknown'} error${detail ? `: ${detail}` : ''}`)
}

export const LIMITS = Object.freeze({
  frameBytes: 16 * 1024 * 1024,
  inflatedBytes: 32 * 1024 * 1024,
  eventBytes: 8 * 1024 * 1024,
  eventCount: 2048,
  blobBytes: 256 * 1024 * 1024,
  blobCount: 65_536,
  /** A conversation store above this size is reset and rebuilt from host history. */
  conversationBytes: 192 * 1024 * 1024,
  pendingCalls: 128,
  pendingBytes: 16 * 1024 * 1024,
  seenCalls: 8192,
  toolCount: 2048,
  toolBytes: 16 * 1024 * 1024,
  queuedWriteBytes: 32 * 1024 * 1024,
  queuedWrites: 256,
  handshakeMs: 15_000,
  unaryMs: 30_000,
  idleMs: 120_000,
  terminalDrainMs: 6_000,
  heartbeatMs: 5_000,
})

export function limitError(): CursorProtocolError {
  return new CursorProtocolError('LIMIT_EXCEEDED', 'Cursor protocol buffer limit exceeded')
}
