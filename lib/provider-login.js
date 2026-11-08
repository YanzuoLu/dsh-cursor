import { randomUUID } from 'node:crypto'
import { credentialKey } from '@deepseek-ai/dsh-credentials'

const LOGIN_TIMEOUT_MS = 15 * 60 * 1000
const MAX_INPUT_LENGTH = 8192
const MESSAGES = {
  NO_FLOW: '原生 OAuth 登录尚未就绪，请确认对应模型提供方已启用。',
  ALREADY_IN_FLIGHT: '此提供方正在其他页面登录，请先完成或取消原来的登录。',
  NO_PROMPT: '这个登录问题已结束，请使用当前页面中的最新问题。',
  INVALID_ANSWER: '输入内容无效，请检查后重试。',
  LOGIN_FAILED: '登录未完成，请检查当前凭据状态后重试。',
  LOGIN_TIMEOUT: '登录等待超时，请重新开始。',
  DISPOSED: '登录界面已停止，请刷新页面。',
  OPERATION_FAILED: '认证操作未完成，请重试。',
}

export class ProviderLoginError extends Error {
  constructor(code) {
    super(MESSAGES[code] ?? MESSAGES.OPERATION_FAILED)
    this.name = 'ProviderLoginError'
    this.code = code in MESSAGES ? code : 'OPERATION_FAILED'
  }
}

function withdrawn() {
  return new DOMException('Authorization prompt withdrawn', 'AbortError')
}

function publicPrompt(request) {
  if (!['text', 'secret', 'select'].includes(request.kind) || typeof request.message !== 'string') {
    throw new TypeError('Unsupported native authorization prompt')
  }
  const view = { id: randomUUID(), kind: request.kind, message: request.message.slice(0, 2048) }
  if (request.kind === 'select') {
    if (!Array.isArray(request.options) || request.options.length > 100 || request.options.some(option =>
      typeof option.id !== 'string' || !option.id || option.id.length > 256 || typeof option.label !== 'string')) {
      throw new TypeError('Invalid native authorization choices')
    }
    view.options = request.options.map(option => ({
      id: option.id, label: option.label.slice(0, 512),
      ...(typeof option.description === 'string' ? { description: option.description.slice(0, 1024) } : {}),
    }))
  } else if (typeof request.placeholder === 'string') view.placeholder = request.placeholder.slice(0, 1024)
  return view
}

/** A surface for native authorization, not an OAuth implementation or credential writer. */
export function createProviderLogin(ctx, { provider, label, allowedAuthHosts, recordKey }) {
  const key = recordKey ?? credentialKey('llm-pi-ai', provider)
  const hosts = new Set(allowedAuthHosts.map(host => host.toLowerCase()))
  let flow
  let generation = 0
  let disposed = false
  let logoutTask
  const tasks = new Set()

  function current(candidate) {
    return !disposed && flow === candidate && candidate.generation === generation
  }

  function accepting(candidate) {
    return current(candidate) && candidate.status === 'pending' && !candidate.controller.signal.aborted
  }

  function withdrawPrompt(candidate) {
    const pending = candidate?.prompt
    if (!pending) return
    candidate.prompt = undefined
    pending.cleanup()
    pending.reject(withdrawn())
  }

  function stop(candidate) {
    if (!candidate || candidate.status !== 'pending') return
    candidate.controller.abort(withdrawn())
    withdrawPrompt(candidate)
    candidate.ready.resolve()
  }

  function authUrl(raw) {
    if (typeof raw !== 'string' || raw.length > MAX_INPUT_LENGTH) return undefined
    let url
    try { url = new URL(raw) } catch { return undefined }
    return url.protocol === 'https:' && !url.username && !url.password && hosts.has(url.hostname.toLowerCase())
      ? url.href : undefined
  }

  async function status() {
    const entry = ctx.authorization.describe(key)
    const record = await ctx.credentials.readRecord(key)
    const oauth = record?.kind === 'grant' && record.payload?.type === 'oauth'
    const expires = oauth && typeof record.payload.expires === 'number' && Number.isFinite(record.payload.expires)
      ? record.payload.expires : undefined
    const pending = flow?.status === 'pending'
    return {
      authenticated: Boolean(record),
      ...(record ? { credentialKind: oauth ? 'oauth' : record.kind } : {}),
      ...(expires === undefined ? {} : { expires, expired: expires <= Date.now() }),
      available: Boolean(entry?.methods.some(method => method.id === 'oauth')),
      inFlight: Boolean(entry?.inFlight),
      login: {
        status: flow?.status ?? 'idle',
        ...(pending && flow.authUrl ? { authUrl: flow.authUrl } : {}),
        ...(pending && flow.notice ? { notice: flow.notice } : {}),
        ...(pending && flow.deviceCode ? { deviceCode: flow.deviceCode } : {}),
        ...(pending && flow.prompt ? { prompt: flow.prompt.view } : {}),
        ...(flow?.status === 'error' ? { error: MESSAGES[flow.errorCode] ?? MESSAGES.LOGIN_FAILED } : {}),
      },
    }
  }

  async function startLogin() {
    if (disposed) throw new ProviderLoginError('DISPOSED')
    if (logoutTask) throw new ProviderLoginError('ALREADY_IN_FLIGHT')
    if (flow?.status === 'pending') {
      await flow.ready.promise
      return status()
    }
    const entry = ctx.authorization.describe(key)
    if (!entry?.methods.some(method => method.id === 'oauth')) throw new ProviderLoginError('NO_FLOW')
    if (entry.inFlight) throw new ProviderLoginError('ALREADY_IN_FLIGHT')
    const candidate = {
      generation: ++generation, controller: new AbortController(),
      ready: Promise.withResolvers(), status: 'pending',
    }
    flow = candidate
    const timeout = setTimeout(() => {
      candidate.timedOut = true
      stop(candidate)
    }, LOGIN_TIMEOUT_MS)
    timeout.unref?.()
    candidate.task = (async () => {
      try {
        const outcome = await ctx.authorization.begin({
          key, method: 'oauth', signal: candidate.controller.signal,
          interaction: {
            notify(notice) {
              if (!accepting(candidate)) return
              if (typeof notice.message === 'string') candidate.notice = notice.message.slice(0, 2048)
              const url = authUrl(notice.url)
              if (url) candidate.authUrl = url
              if (typeof notice.code === 'string') candidate.deviceCode = notice.code.slice(0, 256)
              candidate.ready.resolve()
            },
            prompt(request) {
              if (!accepting(candidate) || request.signal?.aborted) return Promise.reject(withdrawn())
              withdrawPrompt(candidate)
              const view = publicPrompt(request)
              const result = Promise.withResolvers()
              const pending = {
                view, resolve: result.resolve, reject: result.reject,
                cleanup() { request.signal?.removeEventListener('abort', abort) },
              }
              const abort = () => {
                if (candidate.prompt === pending) candidate.prompt = undefined
                pending.cleanup()
                result.reject(withdrawn())
              }
              candidate.prompt = pending
              request.signal?.addEventListener('abort', abort, { once: true })
              candidate.ready.resolve()
              return result.promise
            },
          },
        })
        if (current(candidate)) candidate.status = outcome.status === 'authorized' ? 'complete'
          : candidate.timedOut ? 'error' : 'cancelled'
        if (candidate.timedOut) candidate.errorCode = 'LOGIN_TIMEOUT'
      } catch (error) {
        if (current(candidate)) {
          candidate.status = candidate.controller.signal.aborted && !candidate.timedOut ? 'cancelled' : 'error'
          candidate.errorCode = candidate.timedOut ? 'LOGIN_TIMEOUT'
            : error?.code === 'NO_FLOW' || error?.code === 'UNKNOWN_METHOD' ? 'NO_FLOW'
            : error?.code === 'ALREADY_IN_FLIGHT' ? 'ALREADY_IN_FLIGHT' : 'LOGIN_FAILED'
        }
      } finally {
        clearTimeout(timeout)
        withdrawPrompt(candidate)
        candidate.authUrl = undefined
        candidate.notice = undefined
        candidate.deviceCode = undefined
        candidate.ready.resolve()
      }
    })()
    tasks.add(candidate.task)
    candidate.task.finally(() => tasks.delete(candidate.task)).catch(() => {})
    await candidate.ready.promise
    return status()
  }

  return {
    status,
    startLogin,
    async answer({ promptId, input }) {
      const candidate = flow
      const pending = candidate?.prompt
      if (!candidate || !accepting(candidate) || !pending || pending.view.id !== promptId) {
        throw new ProviderLoginError('NO_PROMPT')
      }
      if (typeof input !== 'string' || !input.trim() || input.length > MAX_INPUT_LENGTH ||
          (pending.view.kind === 'select' && !pending.view.options.some(option => option.id === input))) {
        throw new ProviderLoginError('INVALID_ANSWER')
      }
      candidate.prompt = undefined
      pending.cleanup()
      pending.resolve(input)
      return status()
    },
    async cancelLogin() {
      const candidate = flow
      stop(candidate)
      if (candidate?.task) await candidate.task
      return status()
    },
    logout() {
      if (logoutTask) return logoutTask
      const candidate = flow
      if (ctx.authorization.describe(key)?.inFlight && candidate?.status !== 'pending') {
        return Promise.reject(new ProviderLoginError('ALREADY_IN_FLIGHT'))
      }
      const operation = (async () => {
        stop(candidate)
        if (candidate?.task) await candidate.task
        if (ctx.authorization.describe(key)?.inFlight) throw new ProviderLoginError('ALREADY_IN_FLIGHT')
        await ctx.credentials.deleteRecord(key)
        if (flow === candidate) flow = undefined
        return status()
      })()
      logoutTask = operation
      operation.finally(() => { if (logoutTask === operation) logoutTask = undefined }).catch(() => {})
      return operation
    },
    async dispose() {
      disposed = true
      generation++
      stop(flow)
      await Promise.allSettled([...tasks, ...(logoutTask ? [logoutTask] : [])])
    },
  }
}
