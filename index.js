import { randomUUID } from 'node:crypto'
import { z } from 'zod'
import { defineDomain, domainTable } from '@deepseek-ai/dsh-storage-domain'
import { launchEnvironmentOf } from '@deepseek-ai/dsh-launch-environment'
import z2 from '@deepseek-ai/schemastery'
import { RetryPolicySchema, resolveRetryPolicy } from '@deepseek-ai/dsh-llm'
import { CursorAdapter } from './lib/adapter.js'
import { createCursorAuth } from './lib/auth.js'
import { createCursorCatalog } from './lib/catalog.js'
import { createConversationCache } from './lib/conversation-cache.js'
import { createProviderLogin } from './lib/provider-login.js'
import { mountProviderLoginRoutes } from './lib/provider-login-routes.js'

export const name = 'cursor'
export const inject = ['llm', 'credentials', 'authorization', 'storageDomain']
export { CursorAdapter } from './lib/adapter.js'

const cacheSpec = defineDomain({
  name: 'cursor_catalog', version: 1, invalidRecords: 'backup-and-skip',
  tables: { cache: domainTable(z.object({
    loginId: z.string(), checkedAt: z.number(), entries: z.array(z.unknown()),
  })) },
})

const conversationSpec = defineDomain({
  name: 'cursor_conversations', version: 1, layout: 'per-record', invalidRecords: 'backup-and-skip',
  tables: {
    states: domainTable(z.object({
      binding: z.string(), savedAt: z.number(), snapshot: z.string(), blobRefs: z.array(z.string()).optional(),
    })),
    blobs: domainTable(z.object({ encoded: z.string() })),
  },
})

/** In-process native route. Transient Run failures recover from committed host history. */
const DEFAULT_RETRY_POLICY = {
  mode: 'normal', maxRetries: 35,
  retryableCodes: ['EMPTY_RESPONSE', 'RATE_LIMIT', 'SERVER', 'TIMEOUT', 'TRANSPORT', 'INVALID_RESPONSE', 'CURSOR_RUN_LOST'],
  backoff: { initialDelayMs: 2000, maxDelayMs: 120000, jitterRatio: 0.1 },
}
export const Config = z2.object({
  retryPolicy: RetryPolicySchema.description('Cursor model-request retry policy'),
  runTransport: z2.union(['auto', 'http2', 'split']).description('Auto uses bounded parallel upload for large histories').default('auto'),
})

export async function apply(ctx, config) {
  return await ctx.effect(async function* () {
    const domain = await ctx.storageDomain.open(cacheSpec)
    // Acquire inside the already-owned effect so cancelled initialization still
    // collects and releases a domain whose asynchronous open finished late.
    yield () => domain.close()
    const records = domain.table('cache')
    const conversations = await ctx.storageDomain.open(conversationSpec)
    yield () => conversations.close()
    const conversationCache = createConversationCache(conversations.table('states'), ctx.logger, conversations.table('blobs'))
    const lifetime = new AbortController()
    const operations = new Set()
    const auth = createCursorAuth(ctx)
    let catalog, adapter, controller
    let accountTail = Promise.resolve()
    yield async () => {
      lifetime.abort()
      try {
        await controller?.dispose()
        await auth.dispose()
        await adapter?.dispose()
        await catalog?.dispose()
        await accountTail
        await Promise.allSettled(operations)
        await conversationCache.flush()
      } finally { await conversations.close(); await domain.close() }
    }
    const initial = await auth.metadata()
    // An async-generator checkpoint lets Cordis stop setup before activating
    // services if this instance was withdrawn while metadata was being read.
    yield undefined
    const persisted = records.get('default')
    catalog = createCursorCatalog(auth, {
      cached: initial.configured && persisted?.loginId === initial.loginId ? persisted : undefined,
      save: value => value === null ? records.delete('default') : records.put('default', value),
    })
    const sessionGenerations = new WeakMap()
    const resolveScope = options => {
      const agents = ctx.get('agents')
      const session = options.sessionId
        ? agents?.get(options.sessionId)?.session ?? ctx.get('sessions')?.get(options.sessionId)
        : agents?.currentInitiator()?.session
      if (!session) return {}
      if (!sessionGenerations.has(session)) sessionGenerations.set(session, randomUUID())
      return { cwd: session.header.cwd, generation: sessionGenerations.get(session) }
    }
    const getSettings = () => {
      const environment = launchEnvironmentOf(ctx)
      const proxyUrl = ['HTTPS_PROXY', 'https_proxy', 'ALL_PROXY', 'all_proxy', 'HTTP_PROXY', 'http_proxy']
        .map(key => environment.get(key)?.value).find(value => typeof value === 'string' && value.trim())
      return { ...(proxyUrl ? { proxyUrl } : {}),
        ...(config?.runTransport && config.runTransport !== 'auto' ? { runTransport: config.runTransport } : {}),
      }
    }
    adapter = new CursorAdapter(ctx, auth, catalog, {
      resolveScope, getSettings, conversationCache,
      retryPolicy: resolveRetryPolicy(config?.retryPolicy ?? DEFAULT_RETRY_POLICY, 'cursor.retryPolicy'),
    })
    controller = createProviderLogin(ctx, {
      provider: 'cursor', label: 'Cursor', recordKey: auth.key,
      allowedAuthHosts: ['cursor.com', 'www.cursor.com'],
    })
    let observedLoginId = initial.loginId
    let catalogLoads = 0
    let catalogError
    const baseStatus = controller.status
    controller.status = async () => {
      const [status, metadata] = await Promise.all([baseStatus(), auth.metadata()])
      return { ...status,
        needsLogin: metadata.configured && metadata.needsLogin,
        catalog: { models: catalog.entries().length, loading: catalogLoads > 0,
          ...(catalogError ? { error: catalogError } : {}) },
      }
    }
    const notifyModels = () => ctx.emit('llm/adapters-updated')
    const track = operation => {
      const promise = Promise.resolve().then(operation)
      operations.add(promise)
      promise.finally(() => operations.delete(promise)).catch(() => {})
      return promise
    }
    const syncAccount = () => {
      const operation = accountTail.then(async () => {
        const metadata = await auth.metadata()
        if (metadata.loginId === observedLoginId) return metadata
        observedLoginId = metadata.loginId
        catalogError = undefined
        await adapter.invalidateAccount(metadata.loginId)
        await catalog.clear()
        if (!lifetime.signal.aborted) notifyModels()
        return metadata
      })
      accountTail = operation.catch(() => {})
      return operation
    }
    const refreshModels = signal => track(async () => {
      const upstream = signal ? AbortSignal.any([signal, lifetime.signal]) : lifetime.signal
      await syncAccount()
      upstream.throwIfAborted()
      catalogLoads++
      catalogError = undefined
      try {
        await catalog.refresh(upstream)
        upstream.throwIfAborted()
        notifyModels()
      } catch (error) {
        if (!upstream.aborted) catalogError = '模型目录读取失败，请点击刷新模型重试。'
        throw error
      } finally { catalogLoads-- }
    })
    yield auth.registerFlow()
    yield ctx.llm.registerAdapter(['cursor'], adapter)
    yield ctx.on('credentials/record-updated', key => {
      if (key !== auth.key || lifetime.signal.aborted) return
      return track(syncAccount).catch(() => {
        if (!lifetime.signal.aborted) ctx.logger.warn('cursor: account metadata synchronization failed')
      })
    })
    yield ctx.on('authorization/settled', (key, settlement) => {
      if (key !== auth.key || settlement !== 'authorized' || lifetime.signal.aborted) return
      return refreshModels(lifetime.signal).catch(() => {
        if (!lifetime.signal.aborted) ctx.logger.warn('cursor: explicit login catalog refresh failed')
      })
    })
    yield ctx.on('session/disposed', session => {
      if (!lifetime.signal.aborted) return track(() => adapter.closeSession(String(session.id)))
    }, { global: true })
    yield ctx.on('session/event', (session, event) => {
      if (event.type === 'turn/end' && !lifetime.signal.aborted) return track(() => adapter.closeTurn(String(session.id), 'Native turn ended'))
    }, { global: true })
    const routes = ctx.inject(['connection'], child => child.effect(
      () => mountProviderLoginRoutes(child, controller, { apiPath: '/api/dsh-cursor', refreshModels }),
      'cursor authenticated login routes',
    ))
    yield () => routes.dispose()
  }, 'cursor provider lifetime')
}
