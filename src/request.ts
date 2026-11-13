import { normalizeContext } from '@earendil-works/pi-ai'
import type { ModelAuth, StreamOptions } from '@earendil-works/pi-ai'
import type { AnnouncementDependencies } from './announcement.ts'
import { mergeHeaders } from './lift.ts'
import { PI_UPSTREAM_ACCOUNT_ID } from './managed.ts'
import type {
  AccountRequestContext,
  MultiProviderServiceContext,
  ProviderAttemptFailure,
} from './types.ts'

class RequestError extends Error {}

function cancelled(): RequestError {
  const error = new RequestError('multiprovider: request cancelled')
  error.name = 'AbortError'
  return error
}

// Observe both outcomes even when the caller stops waiting. A late acquisition
// must release its lease; delayed auth must never start a callback after abort.
function abortable<T>(
  signal: AbortSignal,
  operation: () => T | PromiseLike<T>,
  lateValue?: (value: T) => void,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    if (signal.aborted) { reject(cancelled()); return }
    const abort = () => reject(cancelled())
    signal.addEventListener('abort', abort, { once: true })
    Promise.resolve().then(() => {
      if (signal.aborted) throw cancelled()
      return operation()
    }).then(value => {
      if (signal.aborted) {
        lateValue?.(value)
        reject(cancelled())
      } else resolve(value)
    }, reject).finally(() => signal.removeEventListener('abort', abort)).catch(reject)
  })
}

function safeFailure(error: unknown): ProviderAttemptFailure {
  const failure: ProviderAttemptFailure = { message: 'Account request failed', outputStarted: false }
  if (error === null || typeof error !== 'object') return failure
  // Do not retain the original error, its message, or arbitrary headers.
  try {
    const value = error as { status?: unknown; code?: unknown; headers?: unknown }
    if (typeof value.status === 'number' && Number.isInteger(value.status)
      && value.status >= 100 && value.status <= 599) failure.status = value.status
    if (value.code === 'NETWORK_ERROR' && failure.status === undefined) {
      failure.code = 'NETWORK_ERROR'
      failure.message = 'Network request failed'
    }
    if (value.headers !== null && typeof value.headers === 'object') {
      for (const [name, header] of Object.entries(value.headers)) {
        if (name.toLowerCase() !== 'retry-after' || typeof header !== 'string') continue
        const trimmed = header.trim()
        if (trimmed.length <= 128 && (/^\d+(?:\.\d+)?$/.test(trimmed)
          || /^[A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2} GMT$/.test(trimmed))) {
          failure.headers = { 'retry-after': trimmed }
        }
      }
    }
  } catch {
    // Even errors with throwing property accessors must not escape unsanitized.
  }
  return failure
}

export async function runAccountRequest<T>(
  deps: AnnouncementDependencies,
  providerId: string,
  ctx: MultiProviderServiceContext,
  options: { signal: AbortSignal; modelId: string },
  request: (account: { accountId: string; auth: ModelAuth; signal: AbortSignal }) => Promise<T>,
): Promise<T> {
  const { signal, modelId } = options
  const attempted = new Set<string>()
  const checkReady = () => {
    if (signal.aborted) throw cancelled()
    if (deps.isReady !== undefined && deps.isReady() !== true) {
      throw new RequestError('multiprovider: service not ready')
    }
    if (!deps.scheduler.hasProvider(providerId) || deps.getIntegration(providerId) === undefined) {
      throw new RequestError('multiprovider: pool unavailable')
    }
  }

  try {
    const generation = deps.getGeneration?.()
    while (true) {
      checkReady()
      const integration = deps.getIntegration(providerId)!
      const provider = deps.getBaseProvider(providerId, ctx)
      const checkCurrent = () => {
        checkReady()
        if (deps.getGeneration?.() !== generation
          || deps.getIntegration(providerId) !== integration
          || deps.getBaseProvider(providerId, ctx) !== provider) {
          throw new RequestError('multiprovider: service changed')
        }
      }
      const models = provider?.getModels().filter(candidate => candidate.provider === providerId) ?? []
      const template = models.find(candidate => candidate.id === modelId) ?? models[0]
      // Native endpoints may expose models outside Pi's chat catalog. This clone
      // is auth/exclusion metadata only; never register it or call provider inference.
      const model = template === undefined ? undefined
        : template.id === modelId ? template : { ...template, id: modelId, provider: providerId }
      if (provider === undefined || model === undefined) {
        throw new RequestError('multiprovider: model unavailable')
      }
      let requestOptions: StreamOptions & Record<string, unknown> = {}
      let upstreamBaseUrl: string | undefined
      const excluded = new Set(attempted)
      const pool = (await abortable(signal, () => deps.scheduler.snapshot(providerId)))
        .providers.find(item => item.id === providerId)
      checkCurrent()
      const upstream = pool?.accounts.find(account => account.id === PI_UPSTREAM_ACCOUNT_ID)
      if (upstream?.status === 'ready' && !attempted.has(PI_UPSTREAM_ACCOUNT_ID)) {
        // Use Pi's original registry resolver, including the managed auth marker.
        // Never turn an unmarked credential into an eligible managed upstream.
        const ambient = await abortable(signal, () => ctx.modelRegistry.getApiKeyAndHeaders(model))
        checkCurrent()
        if (ambient.ok) {
          upstreamBaseUrl = ambient.baseUrl
          requestOptions = {
            ...(ambient.apiKey === undefined ? {} : { apiKey: ambient.apiKey }),
            ...(ambient.headers === undefined ? {} : { headers: ambient.headers }),
            ...(ambient.env === undefined ? {} : { env: ambient.env }),
          }
        } else excluded.add(PI_UPSTREAM_ACCOUNT_ID)
      } else excluded.add(PI_UPSTREAM_ACCOUNT_ID)
      const requestContext: AccountRequestContext = {
        provider, model, context: normalizeContext({ messages: [] }), requestOptions, signal,
      }
      const integrationExcluded = await abortable(signal,
        () => integration.excludeAccountIds?.(requestContext) ?? [])
      for (const id of integrationExcluded) excluded.add(id)
      checkCurrent()
      const affinityKey = deps.affinityKeyFor(integration, { ...ctx, model }, providerId)
      const lease = await abortable(signal, () => deps.scheduler.acquire({
        providerId, affinityKey, excludeAccountIds: excluded,
      }), late => late.release({ status: 'cancelled' })).catch(() => {
        if (signal.aborted) throw cancelled()
        throw new RequestError('multiprovider: no eligible account')
      })
      attempted.add(lease.accountId)
      let settled = false
      try {
        checkCurrent()
        const resolution = await abortable(signal,
          () => {
            checkCurrent()
            return integration.resolveAuth(lease.account, signal, requestContext)
          })
        checkCurrent()
        // Ambient credentials belong only to pi:default. Stored auth stands
        // alone, even if an upstream resolver returned unmarked headers.
        const upstreamOptions = lease.accountId === PI_UPSTREAM_ACCOUNT_ID ? requestOptions : {}
        const applied: StreamOptions & Record<string, unknown> = { ...upstreamOptions }
        delete applied.apiKey
        if (resolution.auth.apiKey !== undefined) applied.apiKey = resolution.auth.apiKey
        const headers = mergeHeaders(upstreamOptions.headers, resolution.auth.headers)
        if (headers !== undefined) applied.headers = headers
        if (resolution.env !== undefined) applied.env = { ...upstreamOptions.env, ...resolution.env }
        const baseUrl = resolution.auth.baseUrl
          ?? (lease.accountId === PI_UPSTREAM_ACCOUNT_ID ? upstreamBaseUrl : undefined)
        const sanitized = integration.sanitizeRequestOptions?.({
          ...requestContext,
          model: baseUrl === undefined ? model : { ...model, baseUrl },
          requestOptions: applied, account: lease.account, resolution,
        }) ?? applied
        const auth: ModelAuth = {
          ...(sanitized.apiKey === undefined ? {} : { apiKey: sanitized.apiKey }),
          ...(sanitized.headers === undefined ? {} : { headers: sanitized.headers }),
          ...(baseUrl === undefined ? {} : { baseUrl }),
        }
        const result = await abortable(signal, () => {
          checkCurrent()
          return request({ accountId: lease.accountId, auth, signal })
        })
        checkCurrent()
        settled = true
        lease.release({ status: 'success' })
        return result
      } catch (error) {
        if (signal.aborted) throw cancelled()
        checkCurrent()
        if (error instanceof RequestError) throw error
        settled = true
        const disposition = lease.release({ status: 'failure', error: safeFailure(error) })
        if (!disposition?.retryable) throw new RequestError('multiprovider: account request failed')
      } finally {
        if (!settled) lease.release({ status: 'cancelled' })
      }
    }
  } catch (error) {
    if (signal.aborted) throw cancelled()
    if (error instanceof RequestError) throw error
    throw new RequestError('multiprovider: account request failed')
  }
}
