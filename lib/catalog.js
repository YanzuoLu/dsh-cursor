import { attributionHeaders, LlmError, ReasoningEffortId } from '@deepseek-ai/dsh-llm'
import { fetchCursorUsableModels, requestUnary } from './protocol.js'
import { CURSOR_AVAILABLE_MODELS_PATH, decodeAvailableModelsResponse, encodeAvailableModelsRequest } from './catalog-codec.js'
import { checkCursorSignal, cursorAbortable, cursorAuthError, withCursorDeadline } from './oauth.js'

const EMPTY = Object.freeze([])
const REASONING_PARAMETERS = new Set(['effort', 'reasoning', 'reasoning_effort', 'thinking', 'thinking_effort'])

function freeze(value) {
  if (value && typeof value === 'object') {
    for (const child of Object.values(value)) freeze(child)
    Object.freeze(value)
  }
  return value
}

function text(value) {
  return typeof value === 'string' ? value.trim() : ''
}

function positive(value) {
  return Number.isSafeInteger(value) && value > 0
}

function selectionKey(selection) {
  return JSON.stringify([selection.wireModelId, selection.maxMode ?? false,
    (selection.parameters ?? []).map(p => [p.key, p.value]).sort((a, b) => a[0].localeCompare(b[0]))])
}

function parameterValues(definition) {
  return [...definition?.parameterType?.booleanParameter?.values ?? [],
    ...definition?.parameterType?.enumParameter?.values ?? []]
}

function allowedVariant(details, variant) {
  if (variant.confirmationDialogue?.blocksSubmission === true) return false
  return !(variant.parameterValues ?? []).some(parameter => {
    const definition = (details.parameterDefinitions ?? []).find(item => item.id === parameter.id)
    return parameterValues(definition).find(item => item.value === parameter.value)?.blockedByAdminAllowlist === true
  })
}

function variantParameters(variant) {
  const parameters = (variant.parameterValues ?? []).map(p => ({ key: text(p.id), value: text(p.value) }))
  if (parameters.some(p => !p.key) || new Set(parameters.map(p => p.key)).size !== parameters.length) return undefined
  return parameters.sort((a, b) => a.key.localeCompare(b.key))
}

function declaredEffort(details, parameters) {
  const axes = parameters.filter(p => REASONING_PARAMETERS.has(p.key))
  if (!axes.length) return undefined
  const main = axes.find(p => p.key !== 'thinking') ?? axes[0]
  const definition = (details.parameterDefinitions ?? []).find(item => item.id === main.key)
  const declared = parameterValues(definition).find(item => item.value === main.value)
  const id = main.key === 'thinking' && ['false', 'off', 'none'].includes(main.value) ? 'off'
    : main.key === 'thinking' && ['true', 'on'].includes(main.value) ? 'on' : main.value
  if (!id) return undefined
  return { id, name: text(declared?.displayName) || main.value,
    signature: JSON.stringify(axes.map(p => [p.key, p.value])) }
}

function contextLimit(details, selection) {
  const parameter = selection.parameters?.find(p => p.key === 'context')?.value
  if (parameter) {
    const match = /^(\d+(?:\.\d+)?)([km])?$/iu.exec(parameter)
    if (match) {
      const amount = Math.round(Number(match[1]) * (match[2]?.toLowerCase() === 'm' ? 1_000_000
        : match[2]?.toLowerCase() === 'k' ? 1_000 : 1))
      if (positive(amount)) return amount
    }
  }
  const limit = selection.maxMode === true ? details.contextTokenLimitForMaxMode : details.contextTokenLimit
  return positive(limit) ? limit : undefined
}

function displayName(value) {
  return text(value).replace(/[\u200B-\u200D\uFEFF]/gu, '')
    .replace(/\bno[\s_-]*zdr\b/giu, '')
    .replace(/\b\d+(?:\.\d+)?[km]\b/giu, '')
    .replace(/\(\s*\)|\[\s*\]/gu, '')
    .replace(/·/gu, ' ')
    .replace(/\s+/gu, ' ').replace(/\s*[·|/]\s*$/u, '').trim()
}

function familyOf(entry) {
  return entry.presentation?.family ?? entry.defaultSelection.requestedModelId ?? entry.defaultSelection.wireModelId
}
function laneOf(entry) {
  return (entry.defaultSelection.parameters ?? []).filter(p => p.key !== 'context' && !REASONING_PARAMETERS.has(p.key))
}
function laneKey(entry) {
  return JSON.stringify(laneOf(entry).toSorted((a, b) => a.key.localeCompare(b.key)))
}

// Explicit compatibility table for legacy routes observed in Cursor's usable
// catalog. Only these known families are folded; unknown suffixes stay opaque.
const LEGACY_FABLE_FAMILIES = [
  ['claude-fable-5', 'Claude Fable 5'],
  ['claude-fable-5-1', 'Claude Fable 5.1'],
].flatMap(([id, name]) => [
  { id, name },
  { id: `${id}-thinking`, name: `${name} Thinking` },
])
const LEGACY_FABLE_EFFORTS = [
  ['low', 'Low'], ['medium', 'Medium'], ['high', 'High'], ['xhigh', 'Extra High'], ['max', 'Max'],
]

function mergeLegacyFable(entries) {
  const groups = []
  const consumed = new Set()
  for (const family of LEGACY_FABLE_FAMILIES) {
    if (entries.some(entry => entry.info.id === family.id)) continue
    const candidates = LEGACY_FABLE_EFFORTS.flatMap(([id, name]) => {
      const entry = entries.find(candidate => candidate.info.id === `${family.id}-${id}`
        && candidate.defaultSelection.wireModelId === candidate.info.id
        && candidate.defaultSelection.requestedModelId === undefined
        && candidate.defaultSelection.maxMode !== true
        && !candidate.defaultSelection.parameters?.length && !candidate.efforts?.length)
      return entry ? [{ id, name, entry }] : []
    })
    if (candidates.length < 2) continue
    const selected = candidates.find(candidate => candidate.id === 'high') ?? candidates[0]
    // Do not merge routes with conflicting capability metadata into one static
    // host model. The known legacy rows agree; later upstream changes may not.
    const capabilities = entry => JSON.stringify([entry.info.context, [...entry.info.inputModalities].sort()])
    if (candidates.some(candidate => capabilities(candidate.entry) !== capabilities(selected.entry))) continue
    const merged = {
      info: { ...selected.entry.info, id: family.id, name: family.name,
        reasoning: { efforts: candidates.map(({ id, name }) => ({ id: ReasoningEffortId(id), name })),
          defaultEffort: ReasoningEffortId(selected.id) },
      },
      defaultSelection: selected.entry.defaultSelection,
      efforts: candidates.map(({ id, name, entry }) => ({ id: ReasoningEffortId(id), name, selection: entry.defaultSelection })),
      aliases: candidates.map(({ entry }) => entry.info.id),
      effortAliases: candidates.map(({ id, entry }) => ({ id: entry.info.id, effort: ReasoningEffortId(id) })),
    }
    groups.push(merged)
    for (const { entry } of candidates) consumed.add(entry)
  }
  let emitted = false
  return entries.flatMap(entry => {
    if (!consumed.has(entry)) return [entry]
    if (emitted) return []
    emitted = true
    return groups
  })
}

/** Picker policy also applies to older durable caches, without any network I/O. */
function catalogView(entries) {
  entries = mergeLegacyFable(entries)
  const maxima = new Map()
  for (const entry of entries) {
    const limit = Math.max(entry.info.context?.contextWindow ?? 0, entry.presentation?.maxContext ?? 0)
    maxima.set(familyOf(entry), Math.max(maxima.get(familyOf(entry)) ?? 0, limit))
  }
  const kept = entries.filter(entry => !entry.info.context?.contextWindow
    || entry.info.context.contextWindow >= maxima.get(familyOf(entry)))
    .map(entry => ({ ...entry, info: { ...entry.info }, ...(entry.aliases ? { aliases: [...entry.aliases] } : {}) }))
  for (const entry of kept) {
    const peers = kept.filter(other => familyOf(other) === familyOf(entry))
    const lane = laneOf(entry)
    let name = displayName(entry.presentation?.name ?? entry.info.name) || familyOf(entry)
    if (!entry.presentation && lane.some(p => p.key === 'fast' && p.value === 'true')) name = name.replace(/\s+(?:·\s*)?fast$/iu, '')
    const labels = []
    for (const parameter of lane) {
      if (parameter.value === 'false') continue
      if (parameter.key === 'fast' && parameter.value === 'true') labels.push('Fast')
      else if (parameter.value === 'true' || peers.some(other => laneOf(other).find(p => p.key === parameter.key)?.value !== parameter.value)) {
        labels.push(displayName(entry.presentation?.labels?.[parameter.key] ?? `${parameter.key}=${parameter.value}`))
      }
    }
    if (entry.defaultSelection.maxMode === true && peers.some(other => laneKey(other) === laneKey(entry)
      && other.defaultSelection.maxMode !== true)) labels.push('Max Mode')
    entry.info.name = [name, ...new Set(labels.filter(label => !name.toLowerCase().endsWith(label.toLowerCase())))].filter(Boolean).join(' ')
  }
  // Existing saved selections may still name a removed short-context row.
  // Resolve those ids to the corresponding full-context route, never list or
  // dispatch the truncated route and never change Fast/other lane parameters.
  const visible = new Set(kept.map(entry => entry.info.id))
  for (const entry of kept) if (entry.aliases) entry.aliases = [...new Set(entry.aliases)].filter(id => !visible.has(id))
  for (const removed of entries.filter(entry => !visible.has(entry.info.id))) {
    const matches = kept.filter(entry => familyOf(entry) === familyOf(removed) && laneKey(entry) === laneKey(removed))
    const target = matches.find(entry => entry.defaultSelection.maxMode === removed.defaultSelection.maxMode) ?? matches[0]
    if (!target) continue
    target.aliases = [...new Set([...(target.aliases ?? []), removed.info.id, ...(removed.aliases ?? [])])].filter(id => !visible.has(id))
  }
  return kept
}

function presentation(details, selection, name) {
  if (!details) return undefined
  const limits = (details.variants ?? []).map(variant => {
    const parameters = variantParameters(variant)
    return parameters ? contextLimit(details, { maxMode: variant.isMaxMode, parameters }) : undefined
  }).filter(positive)
  const maxContext = limits.length ? Math.max(...limits) : undefined
  const labels = Object.fromEntries((selection.parameters ?? [])
    .filter(p => p.key !== 'context' && !REASONING_PARAMETERS.has(p.key))
    .map(parameter => {
      const definition = details.parameterDefinitions?.find(item => item.id === parameter.key)
      const value = parameterValues(definition).find(item => item.value === parameter.value)
      return [parameter.key, text(value?.displayName) || (parameter.value === 'true' ? text(definition?.name) : '') || `${parameter.key}=${parameter.value}`]
    }))
  return { family: text(details.name), name, ...(maxContext ? { maxContext } : {}), labels }
}

function modelInfo(id, name, details, selection, legacy) {
  const modalities = details ? details.supportsImages === true ? ['text', 'image'] : ['text']
    : legacy.inputModalities?.includes('image') ? ['text', 'image'] : ['text']
  const limit = details ? contextLimit(details, selection) : legacy.contextWindow
  return {
    provider: 'cursor', id, name,
    inputModalities: modalities,
    ...(positive(limit) ? { context: { contextWindow: limit } } : {}),
  }
}

function legacyEntry(row, details) {
  const selection = {
    wireModelId: row.modelId,
    ...(typeof row.maxMode === 'boolean' ? { maxMode: row.maxMode } : {}),
  }
  const name = text(details?.clientDisplayName) || text(row.displayName) || row.modelId
  return {
    info: modelInfo(row.modelId, name, details, selection, row),
    defaultSelection: selection,
    ...(details ? { presentation: presentation(details, selection, name) } : {}),
  }
}

/** Maps only discovered rows. It never strips '-max', creates premium fallback
 * SKUs, infers vision from a model name, or invents a thinking-effort ladder.
 */
export function cursorCatalogEntries(usableModels, rich) {
  if (!Array.isArray(usableModels)) throw cursorAuthError('PROVIDER_RESPONSE')
  const usable = new Map()
  const usableRows = new Map()
  for (const row of usableModels) {
    if (!row || !text(row.modelId)) continue
    const id = text(row.modelId)
    const normalized = { ...row, modelId: id }
    const route = JSON.stringify([id, row.maxMode === true])
    if (!usableRows.has(route)) usableRows.set(route, normalized)
    if (!usable.has(id) || usable.get(id).maxMode === true && row.maxMode !== true) usable.set(id, normalized)
  }
  const entries = []
  const represented = new Set()
  const claimed = new Set()
  function appendLegacy(id, details) {
    const rows = [...usableRows.values()].filter(row => row.modelId === id)
    for (const row of rows) {
      const entry = legacyEntry(row, details)
      const uiId = rows.length > 1 && row.maxMode === true ? `${id}@max=true` : id
      if (claimed.has(uiId)) continue
      entry.info.id = uiId
      entries.push(entry)
      claimed.add(uiId)
    }
    represented.add(id)
  }
  for (const details of rich?.models ?? []) {
    const base = text(details.name)
    if (!base) continue
    const aliases = [base, ...(details.legacySlugs ?? []), ...(details.idAliases ?? [])].map(text).filter(Boolean)
    const relatedIds = [...aliases, ...(details.variants ?? []).map(v => text(v.legacySlug)).filter(Boolean)]
    if (details.supportsAgent === false || details.isChatOnly === true || details.onlySupportsCmdK === true
      || details.isHidden === true || details.degradationStatus === 2) {
      for (const id of relatedIds) if (usable.has(id)) represented.add(id)
      continue
    }
    if (!(details.variants?.length)) {
      for (const id of aliases) {
        const row = usable.get(id)
        if (!row || represented.has(id) || claimed.has(id)) continue
        appendLegacy(id, details)
      }
      continue
    }
    const groups = new Map()
    const seen = new Set()
    const baseEntitled = aliases.some(id => usable.has(id))
    const defaultRank = variant => variant.isDefaultNonMaxConfig === true ? 0 : variant.isDefaultMaxConfig === true ? 1 : 2
    for (const variant of details.variants.toSorted((left, right) => defaultRank(left) - defaultRank(right))) {
      const slug = text(variant.legacySlug)
      if (slug && usable.has(slug)) represented.add(slug)
      if (!allowedVariant(details, variant) || (!baseEntitled && !usable.has(slug))) continue
      const parameters = variantParameters(variant)
      if (!parameters) continue
      const usableRoute = usable.has(slug) ? slug : aliases.find(alias => usable.has(alias))
      if (!usableRoute) continue
      const selection = { wireModelId: usableRoute, requestedModelId: base,
        ...(typeof variant.isMaxMode === 'boolean' ? { maxMode: variant.isMaxMode } : {}),
        ...(parameters.length ? { parameters } : {}),
      }
      const signature = selectionKey(selection)
      if (seen.has(signature)) continue
      seen.add(signature)
      const lane = parameters.filter(p => !REASONING_PARAMETERS.has(p.key))
      const key = JSON.stringify([lane, variant.isMaxMode === true])
      const candidate = { variant, selection, effort: declaredEffort(details, parameters), lane }
      if (!groups.has(key)) groups.set(key, [])
      groups.get(key).push(candidate)
    }
    const lanes = [...groups.values()]
    const flagged = candidate => candidate.selection.maxMode === true
      ? candidate.variant.isDefaultMaxConfig === true : candidate.variant.isDefaultNonMaxConfig === true
    const primary = lanes.find(group => group.some(candidate => candidate.variant.isDefaultNonMaxConfig === true)) ?? lanes[0]
    for (const candidates of lanes) {
      const selected = candidates.find(flagged) ?? candidates[0]
      const variant = selected.variant
      let id = candidates === primary ? base : text(variant.legacySlug) || text(variant.variantStringRepresentation)
      if (!id || claimed.has(id)) {
        id = `${base}@${selected.lane.map(p => `${encodeURIComponent(p.key)}=${encodeURIComponent(p.value)}`).join(',')}${selected.selection.maxMode === true ? ',max=true' : ''}`
      }
      if (claimed.has(id)) continue
      const row = usable.get(text(variant.legacySlug)) ?? aliases.map(alias => usable.get(alias)).find(Boolean)
      const baseName = text(details.clientDisplayName) || text(row?.displayName) || base
      const info = modelInfo(id, baseName, details, selected.selection)
      const effortCandidates = candidates.filter(candidate => candidate.effort)
      const effortRoutes = []
      for (const candidate of effortCandidates) {
        const conflicts = effortCandidates.some(other => other.effort.id === candidate.effort.id
          && other.effort.signature !== candidate.effort.signature)
        const effortId = conflicts ? candidate.effort.signature : candidate.effort.id
        const current = effortRoutes.find(route => route.id === effortId)
        if (current) continue
        effortRoutes.push({ id: ReasoningEffortId(effortId), name: candidate.effort.name, selection: candidate.selection })
      }
      if (effortRoutes.length) {
        const defaultEffort = effortRoutes.find(route => selectionKey(route.selection) === selectionKey(selected.selection))?.id
        info.reasoning = { efforts: effortRoutes.map(({ id: effortId, name: effortName }) => ({ id: effortId, name: effortName })),
          ...(defaultEffort ? { defaultEffort } : {}),
        }
      }
      entries.push({ info, defaultSelection: selected.selection,
        presentation: presentation(details, selected.selection, baseName),
        ...(effortRoutes.length ? { efforts: effortRoutes } : {}),
      })
      claimed.add(id)
    }
    for (const id of aliases) if (usable.has(id)) represented.add(id)
  }
  for (const id of usable.keys()) {
    if (represented.has(id) || claimed.has(id)) continue
    appendLegacy(id)
  }
  return freeze(catalogView(entries))
}

function validSelection(selection) {
  return selection && text(selection.wireModelId)
    && (selection.requestedModelId === undefined || text(selection.requestedModelId))
    && (selection.maxMode === undefined || typeof selection.maxMode === 'boolean')
    && (selection.parameters === undefined || Array.isArray(selection.parameters)
      && selection.parameters.every(p => text(p?.key) && typeof p.value === 'string')
      && new Set(selection.parameters.map(p => p.key)).size === selection.parameters.length)
}

function cachedSelection(selection) {
  return { wireModelId: selection.wireModelId,
    ...(selection.requestedModelId === undefined ? {} : { requestedModelId: selection.requestedModelId }),
    ...(selection.maxMode === undefined ? {} : { maxMode: selection.maxMode }),
    ...(selection.parameters === undefined ? {} : { parameters: selection.parameters.map(({ key, value }) => ({ key, value })) }),
  }
}

function validPresentation(value) {
  return value && text(value.family) && text(value.name)
    && (value.maxContext === undefined || positive(value.maxContext))
    && value.labels && typeof value.labels === 'object' && !Array.isArray(value.labels)
    && Object.values(value.labels).every(label => typeof label === 'string')
}

function cachedState(cached) {
  if (!cached || !text(cached.loginId) || !positive(cached.checkedAt) || !Array.isArray(cached.entries)) return undefined
  const entries = cached.entries
  const ids = new Set()
  for (const entry of entries) {
    if (!entry?.info || entry.info.provider !== 'cursor' || !text(entry.info.id) || !text(entry.info.name)
      || ids.has(entry.info.id) || !validSelection(entry.defaultSelection)
      || entry.presentation !== undefined && !validPresentation(entry.presentation)
      || entry.aliases !== undefined && (!Array.isArray(entry.aliases) || entry.aliases.some(id => !text(id)))
      || entry.effortAliases !== undefined && (!Array.isArray(entry.effortAliases)
        || entry.effortAliases.some(alias => !text(alias?.id) || !entry.aliases?.includes(alias.id)
          || !entry.efforts?.some(effort => effort.id === alias.effort)))
      || !Array.isArray(entry.info.inputModalities) || !entry.info.inputModalities.includes('text')
      || entry.info.inputModalities.some(mode => mode !== 'text' && mode !== 'image')
      || entry.info.context && !positive(entry.info.context.contextWindow)
      || entry.efforts && (!Array.isArray(entry.efforts) || entry.efforts.some(effort =>
        !text(effort?.id) || !text(effort.name) || !validSelection(effort.selection)))) return undefined
    if (entry.info.reasoning && (!Array.isArray(entry.info.reasoning.efforts)
      || entry.info.reasoning.efforts.some(effort => !text(effort?.id) || !text(effort.name)
        || !entry.efforts?.some(route => route.id === effort.id))
      || entry.info.reasoning.defaultEffort !== undefined
        && !entry.efforts?.some(route => route.id === entry.info.reasoning.defaultEffort))) return undefined
    ids.add(entry.info.id)
  }
  // Admit only the cache contract, not arbitrary unknown fields from an older
  // producer. In particular credential payloads have no persistence path here.
  return freeze({ loginId: cached.loginId, checkedAt: cached.checkedAt,
    entries: catalogView(entries.map(entry => ({
      info: { provider: 'cursor', id: entry.info.id, name: entry.info.name,
        inputModalities: [...entry.info.inputModalities],
        ...(entry.info.context ? { context: { contextWindow: entry.info.context.contextWindow } } : {}),
        ...(entry.info.reasoning ? { reasoning: {
          efforts: entry.info.reasoning.efforts.map(({ id, name }) => ({ id, name })),
          ...(entry.info.reasoning.defaultEffort === undefined ? {} : { defaultEffort: entry.info.reasoning.defaultEffort }),
        } } : {}),
      },
      defaultSelection: cachedSelection(entry.defaultSelection),
      ...(entry.presentation ? { presentation: {
        family: entry.presentation.family, name: entry.presentation.name,
        ...(entry.presentation.maxContext ? { maxContext: entry.presentation.maxContext } : {}),
        labels: { ...entry.presentation.labels },
      } } : {}),
      ...(entry.aliases ? { aliases: [...entry.aliases] } : {}),
      ...(entry.effortAliases ? { effortAliases: entry.effortAliases.map(({ id, effort }) => ({ id, effort })) } : {}),
      ...(entry.efforts ? { efforts: entry.efforts.map(({ id, name, selection }) => ({ id, name, selection: cachedSelection(selection) })) } : {}),
    }))),
  })
}

/**
 * Last-known account-scoped catalog. The parent admits cached only after its
 * loginId matches auth.metadata(), and calls clear() on own-grant deletion or
 * loginId replacement. entries(), snapshot(), and select() never read auth or
 * perform I/O. Captured entries stay valid immutable values across refreshes.
 *
 * fetchModels(opts) returns exact usable wire rows. Optional fetchMetadata(opts)
 * returns the decoded AvailableModels response. save(snapshot | null) is the
 * parent's durable non-secret cache adapter. clear() empties synchronously and
 * returns the queued persistence promise. Failed refreshes retain prior rows.
 */
export function createCursorCatalog(auth, options = {}) {
  const fetchModels = options.fetchModels ?? fetchCursorUsableModels
  const unary = options.requestUnary ?? requestUnary
  const save = options.save ?? (() => {})
  let state = cachedState(options.cached)
  let generation = 0
  let pending
  let disposed = false
  let persistence = Promise.resolve()

  function enqueue(action) {
    const result = persistence.then(action)
    persistence = result.catch(() => {})
    return result
  }

  function entries() {
    return state?.entries ?? EMPTY
  }

  function snapshot(modelId) {
    const exact = entries().find(entry => entry.info.id === modelId)
    if (exact) return exact
    const compatible = entries().find(entry => entry.aliases?.includes(modelId))
    if (!compatible) return undefined
    const effortId = compatible.effortAliases?.find(alias => alias.id === modelId)?.effort
    const route = compatible.efforts?.find(effort => effort.id === effortId)
    return freeze({ ...compatible,
      info: { ...compatible.info, id: modelId,
        ...(route ? { reasoning: { ...compatible.info.reasoning, defaultEffort: route.id } } : {}),
      },
      ...(route ? { defaultSelection: route.selection } : {}),
    })
  }

  function select(entry, effort) {
    if (!entry || !validSelection(entry.defaultSelection)) throw new LlmError('Cursor 模型不在当前目录中，请刷新模型列表', 'MODEL_NOT_FOUND')
    if (effort === undefined) return entry.defaultSelection
    const advertised = entry.efforts?.find(candidate => candidate.id === effort)
    if (!advertised) throw new LlmError('此 Cursor 模型不支持所选思考级别', 'INVALID_REASONING_EFFORT')
    return advertised.selection
  }

  function clear() {
    if (disposed) return Promise.reject(cursorAuthError('CANCELLED'))
    generation += 1
    pending?.abort()
    state = undefined
    return enqueue(() => save(null))
  }

  async function refresh(signal) {
    checkCursorSignal(signal)
    if (disposed) throw cursorAuthError('CANCELLED')
    const mine = ++generation
    pending?.abort()
    const controller = new AbortController()
    pending = controller
    const operationSignal = AbortSignal.any([controller.signal, ...(signal ? [signal] : [])])
    const live = () => {
      checkCursorSignal(operationSignal)
      if (disposed || generation !== mine) throw cursorAuthError('CANCELLED')
    }
    let accountChanged = false
    try {
      const grant = await cursorAbortable(auth.getGrant(operationSignal), operationSignal)
      live()
      const verifyAccount = async () => {
        const current = await cursorAbortable(auth.metadata(), operationSignal)
        live()
        if (!current.configured || current.loginId !== grant.loginId || current.needsLogin) {
          accountChanged = true
          throw cursorAuthError('AUTH')
        }
      }
      if (state && state.loginId !== grant.loginId) {
        state = undefined
        await enqueue(async () => { live(); await save(null) })
        live()
      }
      const nextEntries = await withCursorDeadline([operationSignal], options.timeoutMs ?? 15_000, async requestSignal => {
        const transport = {
          accessToken: grant.access,
          ...(options.endpoint ? { endpoint: options.endpoint } : {}),
          ...(options.proxyUrl ? { proxyUrl: options.proxyUrl } : {}),
          headers: { ...options.headers, ...attributionHeaders() },
          signal: requestSignal,
        }
        const rows = await cursorAbortable(fetchModels(transport), requestSignal)
        let rich
        try {
          rich = await withCursorDeadline([requestSignal], options.metadataTimeoutMs ?? 2_000, async metadataSignal => {
            const metadataTransport = { ...transport, signal: metadataSignal }
            return options.fetchMetadata
              ? await cursorAbortable(options.fetchMetadata(metadataTransport), metadataSignal)
              : decodeAvailableModelsResponse(await cursorAbortable(unary(CURSOR_AVAILABLE_MODELS_PATH,
                encodeAvailableModelsRequest(), metadataTransport), metadataSignal))
          })
        } catch {
          // Rich metadata is optional. Keep actual usable rows, but never invent
          // image support, context limits, or thinking efforts from legacy IDs.
          checkCursorSignal(requestSignal)
        }
        checkCursorSignal(requestSignal)
        return cursorCatalogEntries(rows, rich)
      })
      live()
      await verifyAccount()
      const next = freeze({ loginId: grant.loginId, checkedAt: Date.now(), entries: nextEntries })
      await enqueue(async () => {
        live()
        await verifyAccount()
        await save(JSON.parse(JSON.stringify(next)))
        try {
          live()
          await verifyAccount()
        } catch (error) {
          // A durable write already admitted cannot be unsent. Correct it inside
          // the same save queue before any later clear/refresh write can run.
          const current = await auth.metadata()
          if (state && (!current.configured || current.loginId !== state.loginId)) state = undefined
          await save(state ? JSON.parse(JSON.stringify(state)) : null)
          throw error
        }
        state = next
      })
      live()
      return state.entries
    } catch (error) {
      if (accountChanged && generation === mine) await clear()
      throw error
    } finally {
      if (pending === controller) pending = undefined
    }
  }

  function dispose() {
    if (!disposed) {
      disposed = true
      generation += 1
      pending?.abort()
    }
    return persistence
  }

  return Object.freeze({ entries, snapshot, select, refresh, clear, dispose })
}
