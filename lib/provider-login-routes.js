import { ProviderLoginError } from './provider-login.js'

function json(status, body) {
  return Response.json(body, {
    status,
    headers: { 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' },
  })
}

function failure(status, code, message) {
  return json(status, { ok: false, error: { code, message } })
}

/** Mount a fixed provider's native login surface beneath authenticated Connection. */
export function mountProviderLoginRoutes(ctx, controller, { apiPath, refreshModels }) {
  if (!/^\/api\/[a-z0-9]+(?:-[a-z0-9]+)*$/.test(apiPath)) throw new TypeError('Invalid login API path')
  const actions = {
    status: () => controller.status(),
    'start-login': () => controller.startLogin(),
    answer: body => controller.answer(body),
    'cancel-login': () => controller.cancelLogin(),
    logout: () => controller.logout(),
  }
  if (refreshModels) actions['refresh-models'] = async (_body, request) => {
    await refreshModels(request.signal)
    return controller.status()
  }
  const disposers = Object.entries(actions).map(([action, execute]) => ctx.connection.fetch.register({
    path: `${apiPath}/${action}`,
    methods: ['POST'],
    requestBody: 'buffered',
    async fetch(request) {
      const admission = ctx.connection.admit(request)
      if ('rejection' in admission) return failure(admission.rejection, 'UNAUTHORIZED', '请先登录 DSH。')
      if (request.method !== 'POST') return failure(405, 'METHOD_NOT_ALLOWED', '只接受 POST 请求。')
      if (request.headers.get('content-type')?.split(';', 1)[0].trim().toLowerCase() !== 'application/json') {
        return failure(415, 'INVALID_CONTENT_TYPE', '请求必须使用 application/json。')
      }
      let body
      try {
        const text = await request.text()
        if (text.length > 65536) return failure(413, 'INVALID_REQUEST', '请求内容过长。')
        body = JSON.parse(text)
      } catch {
        return failure(400, 'INVALID_REQUEST', '请求必须包含 JSON 对象。')
      }
      if (!body || typeof body !== 'object' || Array.isArray(body) ||
          Object.keys(body).some(key => action !== 'answer' || !['promptId', 'input'].includes(key)) ||
          (action === 'answer' && (typeof body.promptId !== 'string' || !body.promptId || body.promptId.length > 64 ||
            typeof body.input !== 'string' || !body.input.trim() || body.input.length > 8192))) {
        return failure(400, 'INVALID_REQUEST', '请求参数无效。')
      }
      try {
        return json(200, { ok: true, value: await execute(body, request) })
      } catch (error) {
        if (error instanceof ProviderLoginError) {
          const status = error.code === 'NO_FLOW' ? 503
            : error.code === 'ALREADY_IN_FLIGHT' || error.code === 'NO_PROMPT' ? 409
            : error.code === 'INVALID_ANSWER' ? 400 : error.code === 'DISPOSED' ? 410 : 500
          return failure(status, error.code, error.message)
        }
        return failure(500, 'OPERATION_FAILED', '认证操作未完成，请重试。')
      }
    },
  }))
  return async () => { await Promise.all(disposers.map(dispose => dispose())) }
}
