import type { Api, AuthResult, Model, Provider } from '@earendil-works/pi-ai'
import { ModelRegistry, ModelRuntime } from '@earendil-works/pi-coding-agent'
import { liftProvider } from '../src/lift.ts'
import { describe, expect, it, vi } from 'vitest'
import { createServiceAnnouncement } from '../src/announcement.ts'
import { MultiAuthStore } from '../src/auth-store.ts'
import { createManagedIntegration, PI_UPSTREAM_ACCOUNT_ID } from '../src/managed.ts'
import { MultiProviderService } from '../src/service.ts'
import type { MultiProviderIntegration, MultiProviderServiceContext, ProviderAccount } from '../src/types.ts'

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}

function harness(options: { upstream?: boolean; managed?: boolean; ready?: () => boolean } = {}) {
  const model = { id: 'target', provider: 'example', baseUrl: 'https://example.test' } as Model<Api>
  const provider = {
    id: 'example', name: 'Example', getModels: () => [model],
    auth: { apiKey: { name: 'Key', resolve: vi.fn(async () => ({
      auth: { apiKey: 'upstream', headers: { authorization: 'Bearer upstream' } },
    })) } },
  } as unknown as Provider<Api>
  const accounts: ProviderAccount<string>[] = [
    ...(options.upstream ? [{
      id: PI_UPSTREAM_ACCOUNT_ID, label: 'Default', authKind: 'custom' as const,
      credentialRef: PI_UPSTREAM_ACCOUNT_ID,
    }] : []),
    ...['a', 'b'].map(id => ({ id, label: id, authKind: 'api-key' as const, credentialRef: id })),
  ]
  const store = {
    getPool: async () => ({ includeUpstream: options.upstream, accounts: accounts.filter(account => account.id !== PI_UPSTREAM_ACCOUNT_ID) }),
    hasAccounts: async () => true,
    resolveAccount: vi.fn(async (_provider: unknown, id: string) => ({ auth: { apiKey: 'key-' + id } })),
  }
  const integration: MultiProviderIntegration<Api, unknown> = options.managed
    ? createManagedIntegration(provider, store as unknown as MultiAuthStore) as MultiProviderIntegration<Api, unknown>
    : {
      id: 'example', label: 'Example', accounts: () => accounts,
      resolveAuth: vi.fn(async account => ({ auth: { apiKey: 'key-' + account.id } })),
    }
  const registryAuth = vi.fn(async () => {
    const resolution = await integration.auth?.apiKey?.resolve({} as never)
    return { ok: true as const, ...resolution?.auth }
  })
  const ctx = {
    model: { id: 'unrelated', provider: 'other' },
    sessionManager: { getSessionId: () => 'session' },
    modelRegistry: { getApiKeyAndHeaders: registryAuth, getProvider: () => provider },
  } as unknown as MultiProviderServiceContext
  const scheduler = new MultiProviderService({ now: () => 1_000, randomInt: () => 0 })
  const unregister = scheduler.registerProvider(integration)
  const affinityKeyFor = vi.fn((_integration: unknown, context: MultiProviderServiceContext) =>
    context.sessionManager.getSessionId())
  const deps = {
    scheduler, getIntegration: (id: string) => id === 'example' ? integration : undefined,
    getBaseProvider: () => provider, affinityKeyFor,
    ...(options.ready === undefined ? {} : { isReady: options.ready }),
  }
  const announcement = createServiceAnnouncement(deps)
  const controller = new AbortController()
  const run = <T>(callback: Parameters<NonNullable<typeof announcement.runWithAccount<T>>>[3]) =>
    announcement.runWithAccount!('example', ctx, { signal: controller.signal, modelId: 'target' }, callback)
  const snapshot = async () => (await scheduler.snapshot()).providers[0]!.accounts
  return { run, scheduler, integration, controller, ctx, deps, announcement, accounts, snapshot,
    registryAuth, affinityKeyFor, model, provider, store, unregister }
}

describe('shared scheduler requests', () => {
  it('shares inFlight with other leases, resolves integration auth and treats any result as success', async () => {
    const h = harness()
    const other = await h.scheduler.acquire({ providerId: 'example', affinityKey: 'session' })
    const pending = deferred<number>()
    const entered = deferred<void>()
    const result = h.run(async account => {
      expect(account).toEqual({ accountId: 'a', auth: { apiKey: 'key-a' }, signal: h.controller.signal })
      entered.resolve()
      return pending.promise
    })
    await entered.promise
    expect((await h.snapshot())[0]!.inFlight).toBe(2)
    pending.resolve(0)
    expect(await result).toBe(0)
    expect((await h.snapshot())[0]).toMatchObject({ inFlight: 1, consecutiveFailures: 0 })
    other.release()
    expect((await h.snapshot())[0]!.inFlight).toBe(0)
    expect(h.integration.resolveAuth).toHaveBeenCalledWith(h.accounts[0], h.controller.signal,
      expect.objectContaining({ provider: h.provider, model: h.model, signal: h.controller.signal }))
    expect(h.affinityKeyFor).toHaveBeenCalledWith(h.integration,
      expect.objectContaining({ model: h.model }), 'example')
  })

  it('preserves only safe HTTP metadata and honors retry-after on 429', async () => {
    const h = harness()
    const classify = vi.fn<NonNullable<typeof h.integration.classifyFailure>>(() => undefined)
    h.integration.classifyFailure = classify
    const seen: string[] = []
    expect(await h.run(async ({ accountId }) => {
      seen.push(accountId)
      if (accountId === 'a') throw Object.assign(new Error('secret raw error'), {
        status: 429, headers: { 'Retry-After': '120', authorization: 'secret', 'x-secret': 'secret' },
      })
      return 'done'
    })).toBe('done')
    expect(seen).toEqual(['a', 'b'])
    expect(classify.mock.calls[0]).toEqual([{
      message: 'Account request failed', status: 429, headers: { 'retry-after': '120' },
      outputStarted: false,
    }, h.accounts[0]])
    expect((await h.snapshot())[0]).toMatchObject({
      inFlight: 0, cooldownUntil: 121_000, consecutiveFailures: 1, lastFailureKind: 'rate-limit',
    })
    expect((await h.snapshot())[1]).toMatchObject({ inFlight: 0, consecutiveFailures: 0 })
  })

  it.each([400, 404, 422])('does not rotate on fatal HTTP %s', async status => {
    const h = harness()
    const request = vi.fn(async () => { throw Object.assign(new Error('secret'), { status }) })
    await expect(h.run(request)).rejects.toThrow('multiprovider: account request failed')
    expect(request).toHaveBeenCalledTimes(1)
    expect((await h.snapshot())[0]).toMatchObject({ inFlight: 0, lastFailureKind: 'fatal' })
  })

  it('recognizes only explicit network codes, without fabricating an HTTP status', async () => {
    const h = harness()
    const classify = vi.fn<NonNullable<typeof h.integration.classifyFailure>>(() => undefined)
    h.integration.classifyFailure = classify
    const request = vi.fn().mockRejectedValueOnce(Object.assign(new Error('secret'), {
      code: 'NETWORK_ERROR',
    })).mockResolvedValue('ok')
    expect(await h.run(request)).toBe('ok')
    expect(classify.mock.calls[0]![0]).toEqual({
      message: 'Network request failed', code: 'NETWORK_ERROR', outputStarted: false,
    })
    expect((await h.snapshot())[0]!.lastFailureKind).toBe('transient')
  })

  it.each([new Error('Invalid Google decision response'), new Error('429 secret'), { status: '429' }])(
    'does not infer retryability from untrusted text or invalid status', async error => {
      const h = harness()
      const request = vi.fn(async () => { throw error })
      await expect(h.run(request)).rejects.toThrow('multiprovider: account request failed')
      expect(request).toHaveBeenCalledTimes(1)
    })

  it('honors the registered failure disposition instead of forcing retry', async () => {
    const h = harness()
    h.integration.classifyFailure = () => ({ kind: 'fatal', retryable: false })
    const request = vi.fn(async () => { throw { status: 429 } })
    await expect(h.run(request)).rejects.toThrow('multiprovider: account request failed')
    expect(request).toHaveBeenCalledTimes(1)
  })

  it('excludes visited accounts even if a custom retryable disposition has no cooldown', async () => {
    const h = harness()
    h.integration.classifyFailure = () => ({ kind: 'transient', retryable: true, cooldownMs: 0 })
    const acquire = vi.spyOn(h.scheduler, 'acquire')
    const seen: string[] = []
    await expect(h.run(async ({ accountId }) => {
      seen.push(accountId)
      throw { status: 503 }
    })).rejects.toThrow('multiprovider: no eligible account')
    expect(seen).toEqual(['a', 'b'])
    expect([...acquire.mock.calls.at(-1)![0].excludeAccountIds!]).toEqual(expect.arrayContaining(['a', 'b']))
    expect((await h.snapshot()).every(account => account.inFlight === 0)).toBe(true)
  })

  it('honors disabled accounts, cooldowns, explicit pins and session affinity', async () => {
    const h = harness()
    await h.scheduler.pinAccount('example', 'session', 'b')
    expect(await h.run(async ({ accountId }) => accountId)).toBe('b')
    await h.scheduler.updatePool('example', { accounts: [
      { accountId: 'b', enabled: false, weight: 1, priority: 0 },
    ] })
    expect(await h.run(async ({ accountId }) => accountId)).toBe('a')
    const lease = await h.scheduler.acquire({ providerId: 'example' })
    lease.release({ status: 'failure', error: { message: 'limited', status: 429, outputStarted: false } })
    const request = vi.fn()
    await expect(h.run(request)).rejects.toThrow('multiprovider: no eligible account')
    expect(request).not.toHaveBeenCalled()
  })

  it('honors integration exclusions with the target request context', async () => {
    const h = harness()
    const exclude = vi.fn(() => ['a'])
    h.integration.excludeAccountIds = exclude
    expect(await h.run(async ({ accountId }) => accountId)).toBe('b')
    expect(exclude).toHaveBeenCalledWith(expect.objectContaining({
      provider: h.provider, model: h.model, signal: h.controller.signal,
    }))
  })

  it('resolves eligible managed upstream through registry markers and strips internal headers', async () => {
    const h = harness({ upstream: true, managed: true })
    expect(await h.run(async account => account)).toEqual({
      accountId: PI_UPSTREAM_ACCOUNT_ID,
      auth: { apiKey: 'upstream', headers: { authorization: 'Bearer upstream' } },
      signal: h.controller.signal,
    })
    expect(h.registryAuth).toHaveBeenCalledWith(h.model)
    expect(h.store.resolveAccount).not.toHaveBeenCalled()
  })

  it('does not resolve disabled upstream and resolves stored auth through the managed integration', async () => {
    const h = harness({ upstream: true, managed: true })
    await h.scheduler.updatePool('example', { accounts: [
      { accountId: PI_UPSTREAM_ACCOUNT_ID, enabled: false, weight: 1, priority: 0 },
    ] })
    expect(await h.run(async account => account.auth)).toEqual({ apiKey: 'key-a' })
    expect(h.registryAuth).not.toHaveBeenCalled()
    expect(h.store.resolveAccount).toHaveBeenCalledWith(h.provider, 'a', h.controller.signal)
  })

  it('never fabricates upstream markers or bypasses upstream exclusions', async () => {
    const h = harness({ upstream: true, managed: true })
    h.registryAuth.mockResolvedValue({
      ok: true, apiKey: 'unmarked', headers: { authorization: 'Bearer unmarked' },
    })
    expect(await h.run(async account => account.auth)).toEqual({ apiKey: 'key-a' })
    expect(h.store.resolveAccount).toHaveBeenCalledTimes(1)
  })

  it('does not leak upstream headers onto stored credentials after failover', async () => {
    const h = harness({ upstream: true, managed: true })
    const seen: string[] = []
    expect(await h.run(async account => {
      seen.push(account.accountId)
      if (account.accountId === PI_UPSTREAM_ACCOUNT_ID) throw { status: 429 }
      return account.auth
    })).toEqual({ apiKey: 'key-a' })
    expect(seen).toEqual([PI_UPSTREAM_ACCOUNT_ID, 'a'])
  })

  it.each([false, undefined])('fails closed when readiness returns %s', async ready => {
    const h = harness({ ready: () => ready as boolean })
    const request = vi.fn()
    await expect(h.run(request)).rejects.toThrow('multiprovider: service not ready')
    expect(request).not.toHaveBeenCalled()
  })

  it('checks readiness again before retry', async () => {
    let ready = true
    const h = harness({ ready: () => ready })
    const request = vi.fn(async () => { ready = false; throw { status: 429 } })
    await expect(h.run(request)).rejects.toThrow('multiprovider: service not ready')
    expect(request).toHaveBeenCalledTimes(1)
  })

  it('fails closed for missing integration, pool, or model', async () => {
    const h = harness()
    const request = vi.fn()
    await expect(h.announcement.runWithAccount!('missing', h.ctx,
      { signal: h.controller.signal, modelId: 'target' }, request)).rejects.toThrow('multiprovider: pool unavailable')
    vi.spyOn(h.provider, 'getModels').mockReturnValue([])
    await expect(h.announcement.runWithAccount!('example', h.ctx,
      { signal: h.controller.signal, modelId: 'missing' }, request)).rejects.toThrow('multiprovider: model unavailable')
    h.unregister()
    await expect(h.run(request)).rejects.toThrow('multiprovider: pool unavailable')
    expect(request).not.toHaveBeenCalled()
  })

  it('rejects an already-aborted caller without resolving auth or starting a request', async () => {
    const h = harness()
    h.controller.abort(new Error('secret reason'))
    const request = vi.fn()
    await expect(h.run(request)).rejects.toMatchObject({ name: 'AbortError', message: 'multiprovider: request cancelled' })
    expect(h.integration.resolveAuth).not.toHaveBeenCalled()
    expect(request).not.toHaveBeenCalled()
  })

  it.each(['auth', 'request'] as const)('releases exactly once when cancellation interrupts delayed %s', async phase => {
    const h = harness()
    const pending = deferred<AuthResult>()
    const entered = deferred<void>()
    const acquired = vi.spyOn(h.scheduler, 'acquire')
    let release: ReturnType<typeof vi.spyOn>
    acquired.mockImplementation(async options => {
      const lease = await MultiProviderService.prototype.acquire.call(h.scheduler, options)
      release = vi.spyOn(lease, 'release')
      return lease
    })
    if (phase === 'auth') h.integration.resolveAuth = vi.fn(() => { entered.resolve(); return pending.promise })
    const request = vi.fn(async () => { entered.resolve(); return pending.promise })
    const result = h.run(request)
    await entered.promise
    h.controller.abort(new Error('private abort'))
    await expect(result).rejects.toMatchObject({ name: 'AbortError', message: 'multiprovider: request cancelled' })
    expect((await h.snapshot())[0]).toMatchObject({ inFlight: 0, consecutiveFailures: 0 })
    expect(release!).toHaveBeenCalledExactlyOnceWith({ status: 'cancelled' })
    pending.resolve({ auth: { apiKey: 'late' } })
    await Promise.resolve()
    await Promise.resolve()
    expect(request).toHaveBeenCalledTimes(phase === 'auth' ? 0 : 1)
    expect(release!).toHaveBeenCalledTimes(1)
  })

  it('releases a lease acquired after caller cancellation', async () => {
    const h = harness()
    const pending = deferred<void>()
    const entered = deferred<void>()
    const finished = deferred<void>()
    vi.spyOn(h.scheduler, 'acquire').mockImplementation(async options => {
      entered.resolve()
      await pending.promise
      const lease = await MultiProviderService.prototype.acquire.call(h.scheduler, options)
      const release = lease.release
      lease.release = outcome => { const result = release(outcome); finished.resolve(); return result }
      return lease
    })
    const request = vi.fn()
    const result = h.run(request)
    await entered.promise
    h.controller.abort()
    await expect(result).rejects.toMatchObject({ name: 'AbortError' })
    pending.resolve()
    await finished.promise
    expect(request).not.toHaveBeenCalled()
    expect((await h.snapshot()).every(account => account.inFlight === 0)).toBe(true)
  })

  it('observes late request rejections after cancellation without retrying', async () => {
    const h = harness()
    const pending = deferred<never>()
    const entered = deferred<void>()
    const request = vi.fn(() => { entered.resolve(); return pending.promise })
    const result = h.run(request)
    await entered.promise
    h.controller.abort()
    await expect(result).rejects.toMatchObject({ name: 'AbortError' })
    pending.reject(new Error('late secret'))
    await Promise.resolve()
    expect(request).toHaveBeenCalledTimes(1)
  })

  it('cancels a lease if readiness changes while auth resolves', async () => {
    let ready = true
    const h = harness({ ready: () => ready })
    h.integration.resolveAuth = async () => { ready = false; return { auth: { apiKey: 'key' } } }
    const request = vi.fn()
    await expect(h.run(request)).rejects.toThrow('multiprovider: service not ready')
    expect(request).not.toHaveBeenCalled()
    expect((await h.snapshot())[0]).toMatchObject({ inFlight: 0, consecutiveFailures: 0 })
  })

  it('respects least-inflight policy when affinity is off', async () => {
    const h = harness()
    await h.scheduler.updatePool('example', { policy: 'least-inflight', affinity: false })
    const other = await h.scheduler.acquire({ providerId: 'example' })
    expect(other.accountId).toBe('a')
    expect(await h.run(async ({ accountId }) => accountId)).toBe('b')
    other.release()
  })

  it('retains upstream base URL without inheriting it for stored accounts', async () => {
    const h = harness({ upstream: true, managed: true })
    vi.spyOn(h.provider.auth.apiKey!, 'resolve').mockResolvedValue({
      auth: { apiKey: 'upstream', baseUrl: 'https://upstream.test' },
    })
    expect(await h.run(async ({ auth }) => auth)).toEqual({
      apiKey: 'upstream', baseUrl: 'https://upstream.test',
    })
    await h.scheduler.pinAccount('example', 'session', 'a')
    expect(await h.run(async ({ auth }) => auth)).toEqual({
      apiKey: 'key-a', baseUrl: 'https://example.test',
    })
  })

  it('does not let a network code override a fatal HTTP status', async () => {
    const h = harness()
    const request = vi.fn(async () => { throw { status: 400, code: 'NETWORK_ERROR' } })
    await expect(h.run(request)).rejects.toThrow('multiprovider: account request failed')
    expect(request).toHaveBeenCalledTimes(1)
  })

  it.each([
    ['Thu, 01 Jan 1970 00:02:01 GMT', 121_000],
    ['9999999999', 3_601_000],
    ['not a delay', 61_000],
    ['120\r\nauthorization: secret', 61_000],
  ])('handles retry-after %s safely', async (retryAfter, expected) => {
    const h = harness()
    const request = vi.fn().mockRejectedValueOnce({
      status: 429, headers: { 'retry-after': retryAfter },
    }).mockResolvedValue('ok')
    expect(await h.run(request)).toBe('ok')
    expect((await h.snapshot())[0]!.cooldownUntil).toBe(expected)
  })

  it('keeps integration cooldown overrides authoritative', async () => {
    const h = harness()
    h.integration.classifyFailure = () => ({ kind: 'rate-limit', retryable: true, cooldownMs: 10 })
    const request = vi.fn().mockRejectedValueOnce({
      status: 429, headers: { 'retry-after': '120' },
    }).mockResolvedValue('ok')
    expect(await h.run(request)).toBe('ok')
    expect((await h.snapshot())[0]!.cooldownUntil).toBe(1_010)
  })

  it('uses an unlisted native model only as target-provider auth context', async () => {
    const h = harness()
    const chat = { ...h.model, provider: 'google', id: 'gemini-2.5-flash' }
    const provider = { ...h.provider, id: 'google', getModels: () => [chat] }
    h.unregister()
    h.integration.id = 'google'
    h.scheduler.registerProvider(h.integration)
    const announcement = createServiceAnnouncement({
      ...h.deps, getBaseProvider: () => provider,
      getIntegration: id => id === 'google' ? h.integration : undefined,
    })
    expect(await announcement.runWithAccount!('google', h.ctx, {
      signal: h.controller.signal, modelId: 'gemini-decision-1',
    }, async ({ accountId }) => accountId)).toBe('a')
    expect(h.integration.resolveAuth).toHaveBeenCalledWith(h.accounts[0], h.controller.signal,
      expect.objectContaining({
        provider, model: { ...chat, id: 'gemini-decision-1' },
      }))
    expect(provider.getModels()).toEqual([chat])
    expect(h.ctx.model?.provider).toBe('other')
  })

  it('ignores an unrelated broken integration while inspecting eligibility', async () => {
    const h = harness()
    h.scheduler.registerProvider({
      id: 'broken', label: 'Broken', accounts: () => { throw new Error('unrelated') },
    })
    expect(await h.run(async () => 'ok')).toBe('ok')
  })

  it.each(['exclusion', 'acquire', 'auth', 'sanitize'] as const)(
    'rejects a replacement integration during %s without mixing account auth', async phase => {
      const h = harness()
      let current = h.integration
      const next = { ...h.integration, resolveAuth: vi.fn(async () => ({ auth: { apiKey: 'new' } })) }
      const replace = () => {
        h.unregister()
        h.scheduler.registerProvider(next)
        current = next
      }
      const request = vi.fn(async () => 'ok')
      if (phase === 'exclusion') h.integration.excludeAccountIds = async () => { replace(); return [] }
      if (phase === 'acquire') {
        vi.spyOn(h.scheduler, 'acquire').mockImplementation(async options => {
          replace()
          return MultiProviderService.prototype.acquire.call(h.scheduler, options)
        })
      }
      if (phase === 'auth') h.integration.resolveAuth = vi.fn(async () => { replace(); return { auth: { apiKey: 'old' } } })
      if (phase === 'sanitize') h.integration.sanitizeRequestOptions = attempt => { replace(); return attempt.requestOptions }
      const announcement = createServiceAnnouncement({ ...h.deps, getIntegration: () => current })
      await expect(announcement.runWithAccount!('example', h.ctx,
        { signal: h.controller.signal, modelId: 'target' }, request))
        .rejects.toThrow('multiprovider: service changed')
      expect(request).not.toHaveBeenCalled()
      expect(next.resolveAuth).not.toHaveBeenCalled()
      expect((await h.snapshot()).every(account => account.inFlight === 0 && account.consecutiveFailures === 0)).toBe(true)
    })

  it('rejects completed reconciliation even when integration identity is reused', async () => {
    const h = harness()
    let generation = 0
    h.integration.resolveAuth = async () => { generation++; return { auth: { apiKey: 'old' } } }
    const announcement = createServiceAnnouncement({ ...h.deps, getGeneration: () => generation })
    const request = vi.fn(async () => 'ok')
    await expect(announcement.runWithAccount!('example', h.ctx,
      { signal: h.controller.signal, modelId: 'target' }, request))
      .rejects.toThrow('multiprovider: service changed')
    expect(request).not.toHaveBeenCalled()
    expect((await h.snapshot())[0]).toMatchObject({ inFlight: 0, consecutiveFailures: 0 })
  })

  it.each([1, 2, 3, 4, 5, 6, 7, 8])('never records success after cancellation at microtask %s', async ticks => {
    const h = harness()
    const releases: { status: string; aborted: boolean }[] = []
    vi.spyOn(h.scheduler, 'acquire').mockImplementation(async options => {
      const lease = await MultiProviderService.prototype.acquire.call(h.scheduler, options)
      const release = lease.release
      lease.release = outcome => {
        releases.push({ status: outcome?.status ?? 'cancelled', aborted: h.controller.signal.aborted })
        return release(outcome)
      }
      return lease
    })
    await h.run(async () => {
      const abortLater = (remaining: number) => queueMicrotask(() => {
        if (remaining === 0) h.controller.abort()
        else abortLater(remaining - 1)
      })
      abortLater(ticks)
      return 'ok'
    }).catch(error => { expect(error.name).toBe('AbortError') })
    expect(releases).toHaveLength(1)
    expect(releases).not.toContainEqual({ status: 'success', aborted: true })
  })

  it('resolves an unlisted native model through the real in-memory Pi registry', async () => {
    const h = harness({ upstream: true })
    const chat: Model<Api> = {
      id: 'gemini-2.5-flash', provider: 'google', name: 'Chat', api: 'google-generative-ai',
      baseUrl: 'https://example.test', reasoning: false, input: ['text'],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: 1_000, maxTokens: 100,
    }
    const inference = vi.fn()
    const provider = {
      ...h.provider, id: 'google', getModels: () => [chat],
      stream: inference, streamSimple: inference,
    }
    const integration = createManagedIntegration(provider, h.store as unknown as MultiAuthStore) as MultiProviderIntegration<Api, unknown>
    const scheduler = new MultiProviderService()
    scheduler.registerProvider(integration)
    const runtime = await ModelRuntime.create({
      credentials: {
        read: async () => undefined, list: async () => [],
        modify: async (_id, fn) => fn(undefined), delete: async () => {},
      },
      modelsPath: null, allowModelNetwork: false, refreshOnCreate: false,
    })
    runtime.registerNativeProvider(liftProvider(provider, scheduler, integration))
    const modelRegistry = new ModelRegistry(runtime)
    const ctx = { ...h.ctx, modelRegistry }
    const announcement = createServiceAnnouncement({
      scheduler, getIntegration: () => integration, getBaseProvider: () => provider,
      affinityKeyFor: () => 'session',
    })
    expect(modelRegistry.find('google', 'gemini-decision-1')).toBeUndefined()
    expect(await announcement.runWithAccount!('google', ctx,
      { signal: h.controller.signal, modelId: 'gemini-decision-1' }, async ({ accountId, auth }) => ({ accountId, auth })))
      .toEqual({ accountId: PI_UPSTREAM_ACCOUNT_ID,
        auth: { apiKey: 'upstream', headers: { authorization: 'Bearer upstream' } } })
    expect(modelRegistry.find('google', 'gemini-decision-1')).toBeUndefined()
    expect(inference).not.toHaveBeenCalled()
  })
})
