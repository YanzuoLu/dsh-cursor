import { createHash, randomBytes, randomUUID } from 'node:crypto'
import { setTimeout as delay } from 'node:timers/promises'
import { attributionHeaders, LlmError } from '@deepseek-ai/dsh-llm'

/**
 * Cursor's public Deep Control PKCE / poll protocol and IDE refresh client.
 * Reviewed against can1357/oh-my-pi (MIT), commit
 * 650f3a108229ee11bf68ba3377f4e8c3391cd424, packages/ai/src/registry/oauth/cursor.ts.
 * No browser opener, credential discovery, account-profile API, or SDK is used.
 */
const LOGIN_URL = 'https://cursor.com/loginDeepControl'
const POLL_URL = 'https://api2.cursor.sh/auth/poll'
const REFRESH_URL = 'https://api2.cursor.sh/oauth/token'
const IDE_CLIENT_ID = 'KbZUR41cY7W6zRSdpSUJ7I7mLYBKOCmB'
const MAX_AUTH_RESPONSE_BYTES = 64 * 1024

export function cursorAuthError(code) {
  const messages = {
    AUTH: '请重新登录 Cursor',
    CANCELLED: 'Cursor 请求已取消',
    TIMEOUT: 'Cursor 请求超时，请重试',
    NETWORK: '无法连接 Cursor，请重试',
    RATE_LIMIT: 'Cursor 请求过于频繁，请稍后重试',
    PROVIDER_ERROR: 'Cursor 服务暂时不可用，请稍后重试',
    PROVIDER_RESPONSE: 'Cursor 返回了无效响应，请重试',
  }
  return new LlmError(messages[code] ?? messages.PROVIDER_RESPONSE, code)
}

export function checkCursorSignal(signal) {
  if (signal?.aborted) throw cursorAuthError(signal.reason?.name === 'TimeoutError' ? 'TIMEOUT' : 'CANCELLED')
}

/** Also settles promptly when an injected fetch/body reader ignores its signal. */
export async function cursorAbortable(operation, signal) {
  if (signal?.aborted) {
    // The operation may already be a rejected fetch/read promise.
    void Promise.resolve(operation).catch(() => {})
    checkCursorSignal(signal)
  }
  if (!signal) return await operation
  let withdraw
  const cancelled = new Promise((resolve, reject) => {
    withdraw = () => reject(cursorAuthError(signal.reason?.name === 'TimeoutError' ? 'TIMEOUT' : 'CANCELLED'))
    signal.addEventListener('abort', withdraw, { once: true })
  })
  try {
    return await Promise.race([operation, cancelled])
  } catch (error) {
    checkCursorSignal(signal)
    throw error
  } finally {
    signal.removeEventListener('abort', withdraw)
  }
}

export async function withCursorDeadline(signals, timeoutMs, run) {
  const controller = new AbortController()
  const signal = AbortSignal.any([...signals.filter(Boolean), controller.signal])
  const timer = setTimeout(() => controller.abort(new DOMException('Cursor deadline', 'TimeoutError')), timeoutMs)
  try {
    checkCursorSignal(signal)
    return await cursorAbortable(Promise.resolve().then(() => run(signal)), signal)
  } finally {
    clearTimeout(timer)
  }
}

/** JWT expiry is only scheduling metadata, not a claim of signature verification.
 * Opaque tokens use a one-hour refresh deadline, matching the reviewed protocol.
 */
export function cursorTokenExpiry(access, now = Date.now()) {
  const parts = access.split('.')
  if (parts.length === 3 && parts[1]) {
    try {
      const payload = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'))
      if (typeof payload?.exp === 'number' && Number.isFinite(payload.exp) && payload.exp > 0) {
        const expires = payload.exp * 1000
        if (Number.isFinite(expires)) return expires
      }
    } catch {
      // An opaque or malformed JWT is not an authentication assertion.
    }
  }
  return now + 60 * 60 * 1000
}

async function fetchAuth(fetchImpl, url, init, signal) {
  try {
    return await cursorAbortable(Promise.resolve().then(() => fetchImpl(url, {
      ...init,
      headers: { ...init.headers, ...attributionHeaders() },
      signal,
    })).then(response => {
      if (signal?.aborted) {
        void discard(response)
        checkCursorSignal(signal)
      }
      return response
    }), signal)
  } catch (error) {
    checkCursorSignal(signal)
    if (error instanceof LlmError) throw error
    // Fetch errors can contain the poll URL and its private verifier.
    throw cursorAuthError('NETWORK')
  }
}

async function readAuthJson(response, signal) {
  if (!response.body) throw cursorAuthError('PROVIDER_RESPONSE')
  const reader = response.body.getReader()
  const chunks = []
  let size = 0
  let completed = false
  try {
    while (true) {
      const { done, value } = await cursorAbortable(reader.read(), signal)
      if (done) break
      size += value.byteLength
      if (size > MAX_AUTH_RESPONSE_BYTES) throw cursorAuthError('PROVIDER_RESPONSE')
      chunks.push(value)
    }
    completed = true
    checkCursorSignal(signal)
    const body = Buffer.concat(chunks, size).toString('utf8')
    let value
    try {
      value = JSON.parse(body)
    } catch {
      throw cursorAuthError('PROVIDER_RESPONSE')
    }
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw cursorAuthError('PROVIDER_RESPONSE')
    return value
  } catch (error) {
    checkCursorSignal(signal)
    if (error instanceof LlmError) throw error
    throw cursorAuthError('NETWORK')
  } finally {
    if (!completed) void reader.cancel().catch(() => {})
    reader.releaseLock()
  }
}

function httpError(status) {
  if (status === 401 || status === 403) return cursorAuthError('AUTH')
  if (status === 429) return cursorAuthError('RATE_LIMIT')
  return cursorAuthError(status >= 500 ? 'PROVIDER_ERROR' : 'PROVIDER_RESPONSE')
}

async function discard(response) {
  if (response.body) void response.body.cancel().catch(() => {})
}

function nonempty(value) {
  return typeof value === 'string' && value.length > 0 && !/[\r\n]/u.test(value)
}

/** Explicit manual-open login only. The verifier never appears in a notice. */
export async function loginCursorOAuth({ signal, notify, fetch: fetchImpl = globalThis.fetch,
  timeoutMs = 5 * 60 * 1000, requestTimeoutMs = 15_000, pollDelayMs = 1_000 } = {}) {
  return await withCursorDeadline([signal], timeoutMs, async loginSignal => {
    const verifier = randomBytes(32).toString('base64url')
    const challenge = createHash('sha256').update(verifier).digest('base64url')
    const uuid = randomUUID()
    const params = new URLSearchParams({ challenge, uuid, mode: 'login', redirectTarget: 'cli' })
    notify({ message: '请手动打开链接登录 Cursor，完成后此处会自动继续', url: `${LOGIN_URL}?${params}` })
    const pollUrl = new URL(POLL_URL)
    pollUrl.search = new URLSearchParams({ uuid, verifier }).toString()
    let errors = 0
    let waitMs = pollDelayMs
    for (let attempt = 0; attempt < 150; attempt += 1) {
      checkCursorSignal(loginSignal)
      if (attempt > 0) {
        await cursorAbortable(delay(waitMs, undefined, { signal: loginSignal }), loginSignal)
      }
      try {
        const tokens = await withCursorDeadline([loginSignal], requestTimeoutMs, async requestSignal => {
          const response = await fetchAuth(fetchImpl, pollUrl, { method: 'GET', headers: { accept: 'application/json' } }, requestSignal)
          if (response.status === 404) {
            await discard(response)
            return undefined
          }
          if (!response.ok) {
            await discard(response)
            throw httpError(response.status)
          }
          const data = await readAuthJson(response, requestSignal)
          if (!nonempty(data.accessToken) || !nonempty(data.refreshToken)) throw cursorAuthError('PROVIDER_RESPONSE')
          return { access: data.accessToken, refresh: data.refreshToken, expires: cursorTokenExpiry(data.accessToken) }
        })
        if (tokens) return tokens
        errors = 0
        waitMs = Math.min(waitMs * 1.2, 10_000)
      } catch (error) {
        checkCursorSignal(loginSignal)
        if (!(error instanceof LlmError) || error.code === 'AUTH') throw error
        errors += 1
        if (errors >= 3) throw error
      }
    }
    throw cursorAuthError('TIMEOUT')
  })
}

export async function refreshCursorOAuth(grant, { signal, fetch: fetchImpl = globalThis.fetch } = {}) {
  return await withCursorDeadline([signal], 15_000, async requestSignal => {
    const response = await fetchAuth(fetchImpl, REFRESH_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'application/json' },
      body: JSON.stringify({ grant_type: 'refresh_token', client_id: IDE_CLIENT_ID, refresh_token: grant.refresh }),
    }, requestSignal)
    if (!response.ok) {
      if (response.status === 400) {
        const data = await readAuthJson(response, requestSignal)
        if (data.error === 'invalid_grant') throw cursorAuthError('AUTH')
      } else {
        await discard(response)
      }
      throw httpError(response.status)
    }
    const data = await readAuthJson(response, requestSignal)
    if (data.shouldLogout === true || data.error === 'invalid_grant') throw cursorAuthError('AUTH')
    if (!nonempty(data.access_token)) throw cursorAuthError('PROVIDER_RESPONSE')
    return {
      access: data.access_token,
      refresh: nonempty(data.refresh_token) ? data.refresh_token : grant.refresh,
      expires: cursorTokenExpiry(data.access_token),
    }
  })
}
