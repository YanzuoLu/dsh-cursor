import { LlmAdapter, LlmError, attributionHeaders } from '@deepseek-ai/dsh-llm'
import { CursorRuntime } from './runtime.js'

export const CURSOR_PROVIDER = 'cursor'
export const DEFAULT_CURSOR_ENDPOINT = 'https://api2.cursor.sh'

function immutable(value) {
  const clone = structuredClone(value)
  const freeze = item => {
    if (item && typeof item === 'object') {
      for (const child of Object.values(item)) freeze(child)
      Object.freeze(item)
    }
    return item
  }
  return freeze(clone)
}

/** Native DSH adapter backed only by the explicitly refreshed Cursor catalog. */
export class CursorAdapter extends LlmAdapter {
  constructor(ctx, auth, catalog, hooks = {}) {
    super()
    this.catalog = catalog
    this.getSettings = hooks.getSettings ?? (() => ({}))
    this.retryPolicy = hooks.retryPolicy
    this.runtime = hooks.runtime ?? new CursorRuntime(ctx, auth, catalog, hooks)
  }

  assertProvider(provider) {
    if (provider !== CURSOR_PROVIDER) throw new LlmError(`Cursor does not own provider "${provider}"`, 'NO_ADAPTER')
  }

  providerRetryPolicy(provider) {
    this.assertProvider(provider)
    return this.retryPolicy
  }

  providerInfo(provider) {
    this.assertProvider(provider)
    return { id: CURSOR_PROVIDER, name: 'Cursor' }
  }

  async listModels(provider) {
    this.assertProvider(provider)
    return this.catalog.entries().map(entry => immutable(entry.info))
  }

  capture(provider, model, signal) {
    this.assertProvider(provider)
    signal?.throwIfAborted()
    const entry = this.catalog.snapshot(model)
    if (!entry) throw new LlmError('Cursor model metadata is unavailable. Explicitly sign in and refresh the model catalog', 'CURSOR_MODEL_UNAVAILABLE')
    const settings = this.getSettings()
    return immutable({
      entry,
      settings: {
        // Direct Run discovers its regional origin. Split Run uses Cursor's
        // API gateway. An explicit endpoint overrides either for tests/operators.
        ...(settings.endpoint === undefined ? {} : { endpoint: settings.endpoint }),
        headers: { ...settings.headers, ...attributionHeaders() },
        ...(settings.runTransport === undefined ? {} : { runTransport: settings.runTransport }),
        ...(settings.proxyUrl === undefined ? {} : { proxyUrl: settings.proxyUrl }),
      },
    })
  }

  async resolveModel(provider, model, signal) {
    this.assertProvider(provider)
    signal?.throwIfAborted()
    const entry = this.catalog.snapshot(model)
    if (!entry) throw new LlmError('Cursor model metadata is unavailable. Explicitly sign in and refresh the model catalog', 'CURSOR_MODEL_UNAVAILABLE')
    return immutable(entry.info)
  }

  async prepareCall(provider, model, signal) {
    const captured = this.capture(provider, model, signal)
    return Object.freeze({
      model: captured.entry.info,
      stream: options => {
        if (options.provider !== provider || options.model !== model) {
          throw new LlmError('The prepared Cursor route changed before dispatch', 'INVALID_PREPARED_CALL')
        }
        return this.runtime.stream(options, captured)
      },
    })
  }

  async *stream(options) {
    const prepared = await this.prepareCall(options.provider, options.model, options.signal)
    yield* prepared.stream(options)
  }

  closeTurn(sessionId, reason) {
    return this.runtime.closeTurn(sessionId, reason)
  }

  closeSession(sessionId, reason) {
    return this.runtime.closeSession(sessionId, reason)
  }

  invalidateAccount(loginId) {
    return this.runtime.invalidateAccount(loginId)
  }

  dispose() {
    return this.runtime.dispose()
  }
}

export default CursorAdapter
