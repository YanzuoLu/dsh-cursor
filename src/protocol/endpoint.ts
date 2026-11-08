import { createHash } from 'node:crypto'
import type { CursorTransportOptions } from '../contracts.js'
import { CursorProtocolError, httpError, safeError } from './errors.js'
import { DEFAULT_ENDPOINT, protocolHeaders } from './transport.js'

const resolved = new Map<string, string>()

/** Discover the account's Run origin. Metadata RPCs remain on the API origin. */
export async function cursorRunEndpoint(options: CursorTransportOptions): Promise<string> {
  if (options.endpoint !== undefined) return options.endpoint
  const key = createHash('sha256').update(options.accessToken).digest('hex')
  const known = resolved.get(key)
  if (known) return known
  const timeout = AbortSignal.timeout(15_000)
  const signal = options.signal ? AbortSignal.any([options.signal, timeout]) : timeout
  let response: Response
  try {
    response = await fetch(`${DEFAULT_ENDPOINT}/aiserver.v1.ServerConfigService/GetServerConfig`, {
      method: 'POST', signal,
      headers: { ...protocolHeaders(options), 'content-type': 'application/json', accept: 'application/json' },
      body: JSON.stringify({ telem_enabled: false }),
    })
  } catch (error) {
    if (options.signal?.aborted) throw new CursorProtocolError('ABORTED', 'Cursor routing request was aborted')
    if (timeout.aborted) throw new CursorProtocolError('TIMEOUT', 'Cursor routing request timed out')
    throw safeError(error, 'Cursor routing request failed')
  }
  if (!response.ok) throw httpError(response.status)
  let raw: unknown
  try {
    const body = await response.json() as { agentUrlConfig?: { agentnUrl?: string; agentUrl?: string } }
    raw = body.agentUrlConfig?.agentnUrl || body.agentUrlConfig?.agentUrl
  } catch { throw new CursorProtocolError('INVALID_RESPONSE', 'Invalid Cursor routing response') }
  let url: URL
  try { url = new URL(typeof raw === 'string' && !raw.includes('://') ? `https://${raw}` : String(raw)) }
  catch { throw new CursorProtocolError('INVALID_RESPONSE', 'Cursor routing response has no valid agent origin') }
  if (url.protocol !== 'https:' || !/^([a-z0-9-]+\.)+cursor\.sh$/iu.test(url.hostname) || url.username || url.password) {
    throw new CursorProtocolError('INVALID_RESPONSE', 'Cursor routing response has an invalid agent origin')
  }
  resolved.set(key, url.origin)
  if (resolved.size > 64) resolved.delete(resolved.keys().next().value!)
  return url.origin
}
