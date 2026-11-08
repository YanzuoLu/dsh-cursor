import { randomUUID } from 'node:crypto'
import { credentialKey } from '@deepseek-ai/dsh-credentials'
import { LlmError } from '@deepseek-ai/dsh-llm'
import { checkCursorSignal, cursorAbortable, cursorAuthError, cursorTokenExpiry,
  loginCursorOAuth, refreshCursorOAuth, withCursorDeadline } from './oauth.js'

export const CURSOR_GRANT_KEY = credentialKey('dsh-cursor', 'default')
const REFRESH_SKEW_MS = 60_000

function grantFromRecord(record) {
  const grant = record?.kind === 'grant' ? record.payload : undefined
  if (!grant || grant.type !== 'oauth' || typeof grant.loginId !== 'string' || !grant.loginId
    || typeof grant.access !== 'string' || !grant.access || /[\r\n]/u.test(grant.access)
    || typeof grant.refresh !== 'string' || !grant.refresh || /[\r\n]/u.test(grant.refresh)
    || typeof grant.expires !== 'number' || !Number.isFinite(grant.expires)) return undefined
  return grant
}

function sameGrant(left, right) {
  return left?.loginId === right?.loginId && left?.access === right?.access && left?.refresh === right?.refresh
}

function tokensToGrant(tokens, previous) {
  if (!tokens || typeof tokens.access !== 'string' || !tokens.access || /[\r\n]/u.test(tokens.access)) {
    throw cursorAuthError('PROVIDER_RESPONSE')
  }
  const refresh = tokens.refresh || previous?.refresh
  if (typeof refresh !== 'string' || !refresh || /[\r\n]/u.test(refresh)) throw cursorAuthError('PROVIDER_RESPONSE')
  const expires = tokens.expires ?? cursorTokenExpiry(tokens.access)
  if (typeof expires !== 'number' || !Number.isFinite(expires)) throw cursorAuthError('PROVIDER_RESPONSE')
  const accountId = previous?.accountId ?? tokens.accountId
  return {
    type: 'oauth',
    loginId: previous?.loginId ?? randomUUID(),
    access: tokens.access,
    refresh,
    expires,
    ...(typeof accountId === 'string' && accountId ? { accountId } : {}),
  }
}

/**
 * Single native grant owner. Construction, registerFlow(), and metadata() never
 * authenticate or make HTTP requests. The parent explicitly calls registerFlow()
 * and disposes this instance with its plugin effect.
 *
 * Optional test/transport seams:
 * login({ signal, notify, fetch }) -> { access, refresh, expires?, accountId? }
 * refresh(grant, { signal, fetch }) -> { access, refresh?, expires? }
 * Grant values never leave getGrant(); metadata() is safe for status surfaces.
 */
export function createCursorAuth(ctx, options = {}) {
  const login = options.login ?? loginCursorOAuth
  const refresh = options.refresh ?? refreshCursorOAuth
  const fetchImpl = options.fetch ?? ((...args) => globalThis.fetch(...args))
  const lifetime = new AbortController()
  const refreshes = new Set()
  let unregister
  let disposed = false
  let epoch = 0
  let rejectedGrant

  function active(signal) {
    checkCursorSignal(signal)
    checkCursorSignal(lifetime.signal)
  }

  async function metadata() {
    const grant = grantFromRecord(await ctx.credentials.readRecord(CURSOR_GRANT_KEY))
    if (!grant) return Object.freeze({ key: CURSOR_GRANT_KEY, configured: false, needsLogin: true })
    return Object.freeze({
      key: CURSOR_GRANT_KEY,
      configured: true,
      loginId: grant.loginId,
      expires: grant.expires,
      needsLogin: sameGrant(grant, rejectedGrant),
    })
  }

  async function getGrant(signal) {
    active(signal)
    const operationSignal = AbortSignal.any([lifetime.signal, ...(signal ? [signal] : [])])
    const initial = grantFromRecord(await cursorAbortable(ctx.credentials.readRecord(CURSOR_GRANT_KEY), operationSignal))
    active(signal)
    if (!initial || sameGrant(initial, rejectedGrant)) throw cursorAuthError('AUTH')
    if (initial.expires > Date.now() + REFRESH_SKEW_MS) return Object.freeze({ ...initial })

    const generation = epoch
    const controller = new AbortController()
    refreshes.add(controller)
    const refreshSignal = AbortSignal.any([operationSignal, controller.signal])
    try {
      // Native modifyRecord is the cross-process FIFO lock. Its callback sees
      // the latest grant, not the stale record that triggered this refresh.
      const record = await cursorAbortable(ctx.credentials.modifyRecord(CURSOR_GRANT_KEY, async current => {
        checkCursorSignal(refreshSignal)
        const latest = grantFromRecord(current)
        if (!latest || sameGrant(latest, rejectedGrant)) throw cursorAuthError('AUTH')
        if (latest.expires > Date.now() + REFRESH_SKEW_MS) return undefined
        let tokens
        try {
          tokens = await withCursorDeadline([refreshSignal], 15_000, requestSignal =>
            refresh(Object.freeze({ ...latest }), { signal: requestSignal, fetch: fetchImpl }))
        } catch (error) {
          if (error instanceof LlmError && error.code === 'AUTH') rejectedGrant = { ...latest }
          throw error
        }
        checkCursorSignal(refreshSignal)
        if (generation !== epoch) throw cursorAuthError('CANCELLED')
        const payload = tokensToGrant(tokens, latest)
        rejectedGrant = undefined
        return { kind: 'grant', payload }
      }), refreshSignal)
      active(signal)
      const grant = grantFromRecord(record)
      if (!grant) throw cursorAuthError('AUTH')
      return Object.freeze({ ...grant })
    } finally {
      refreshes.delete(controller)
    }
  }

  function registerFlow() {
    active()
    if (unregister) return unregister
    const remove = ctx.authorization.registerFlow({
      key: CURSOR_GRANT_KEY,
      label: 'Cursor',
      methods: [{ id: 'oauth', label: '浏览器登录' }],
      async run(session) {
        active(session.signal)
        epoch += 1
        for (const controller of refreshes) controller.abort()
        const signal = AbortSignal.any([session.signal, lifetime.signal])
        const tokens = await withCursorDeadline([signal], 5 * 60 * 1000, loginSignal =>
          login({ signal: loginSignal, notify: notice => session.notify(notice), fetch: fetchImpl }))
        active(session.signal)
        const payload = tokensToGrant(tokens)
        // Native commit owns the cancellation/commit admission boundary. Do not
        // duplicate its write through modifyRecord or persist outside the seam.
        await session.commit({ kind: 'grant', payload })
        rejectedGrant = undefined
      },
    })
    let registered = true
    unregister = () => {
      if (!registered) return
      registered = false
      remove()
      unregister = undefined
    }
    return unregister
  }

  function dispose() {
    if (disposed) return
    disposed = true
    epoch += 1
    lifetime.abort()
    unregister?.()
  }

  return Object.freeze({ key: CURSOR_GRANT_KEY, getGrant, registerFlow, metadata, dispose })
}
