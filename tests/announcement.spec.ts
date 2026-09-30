import type { Api, Model, Provider } from '@earendil-works/pi-ai'
import type { ExtensionContext } from '@earendil-works/pi-coding-agent'
import { describe, expect, it, vi } from 'vitest'
import { createServiceAnnouncement } from '../src/announcement.ts'
import {
  MultiProviderService,
  PI_UPSTREAM_ACCOUNT_ID,
  type ActiveAccount,
  type MultiProviderIntegration,
  type MultiProviderServiceContext,
  type ProviderAccount,
} from '../src/index.ts'

const accounts: ProviderAccount<string>[] = [
  {
    id: PI_UPSTREAM_ACCOUNT_ID,
    label: 'Pi default',
    authKind: 'custom',
    credentialRef: PI_UPSTREAM_ACCOUNT_ID,
  },
  { id: 'a', label: 'Work', authKind: 'oauth', credentialRef: 'ref-a' },
  { id: 'b', label: 'Personal', authKind: 'oauth', credentialRef: 'ref-b' },
]

interface ResolveResult {
  auth: { apiKey?: string; headers?: Record<string, string> }
  source?: string
}

function makeHarness(options: {
  affinity?: boolean
  selectionBias?: 'first-account' | 'none'
  resolve?: ResolveResult | Error
} = {}) {
  const resolve: ResolveResult | Error = options.resolve
    ?? { auth: { apiKey: 'token-a' }, source: 'Work · Test OAuth' }
  const integration: MultiProviderIntegration<Api, unknown> = {
    id: 'example',
    label: 'Example',
    accounts: () => accounts,
    resolveAuth: async () => {
      if (resolve instanceof Error) throw resolve
      return resolve
    },
  }
  const scheduler = new MultiProviderService({
    ...(options.affinity === undefined ? {} : { affinity: options.affinity }),
    randomInt: () => 1,
    randomId: () => 'lease-1',
  })
  const unregister = scheduler.registerProvider({
    id: 'example',
    label: 'Example',
    accounts: () => accounts,
    ...(options.selectionBias === undefined ? {} : { selectionBias: options.selectionBias }),
  })
  const model = { id: 'model', provider: 'example' } as unknown as Model<Api>
  const provider = {
    id: 'example',
    name: 'Example',
    getModels: () => [model],
  } as unknown as Provider<Api>
  const ctx = {
    model: undefined,
    sessionManager: { getSessionId: () => 'session-1' },
    modelRegistry: { getProvider: () => provider },
  } as unknown as MultiProviderServiceContext
  const announcement = createServiceAnnouncement({
    scheduler,
    getIntegration: providerId => (providerId === 'example' ? integration : undefined),
    getBaseProvider: () => provider,
    affinityKeyFor: () => 'session-1',
  })
  return { scheduler, announcement, ctx, integration, unregister }
}

describe('service announcement', () => {
  it('resolves the explicit session pin and its stored credential', async () => {
    const { scheduler, announcement, ctx } = makeHarness({ affinity: false })
    expect(await announcement.getActiveAccount('example', ctx)).toMatchObject({ id: PI_UPSTREAM_ACCOUNT_ID })
    await scheduler.pinAccount('example', 'session-1', 'a')
    expect(await announcement.getActiveAccount('example', ctx)).toEqual({
      id: 'a',
      label: 'Work',
      authKind: 'oauth',
    })
    expect(await announcement.resolveActiveAccountAuth('example', ctx)).toEqual({
      accountId: 'a',
      accessToken: 'token-a',
      label: 'Work',
      source: 'Work · Test OAuth',
    })
  })

  it('reports the last scheduler selection while affinity is on', async () => {
    const { scheduler, announcement, ctx } = makeHarness({ selectionBias: 'none' })
    const lease = await scheduler.acquire({ providerId: 'example', affinityKey: 'session-1' })
    lease.release({ status: 'success' })
    expect(await announcement.getActiveAccount('example', ctx)).toEqual({
      id: 'a',
      label: 'Work',
      authKind: 'oauth',
    })
  })

  it('never resolves credentials for the upstream account or without a pin', async () => {
    const { scheduler, announcement, ctx } = makeHarness({})
    expect(await announcement.resolveActiveAccountAuth('example', ctx)).toBeUndefined()
    await scheduler.pinAccount('example', 'session-1', PI_UPSTREAM_ACCOUNT_ID)
    expect(await announcement.getActiveAccount('example', ctx)).toEqual({
      id: PI_UPSTREAM_ACCOUNT_ID,
      label: 'Pi default',
      authKind: 'custom',
    })
    expect(await announcement.resolveActiveAccountAuth('example', ctx)).toBeUndefined()
  })

  it('guesses upstream, then the first ready account, before any selection', async () => {
    const { scheduler, announcement, ctx } = makeHarness({ affinity: false })
    expect(await announcement.getActiveAccount('example', ctx)).toMatchObject({ id: PI_UPSTREAM_ACCOUNT_ID })
    expect(await announcement.resolveActiveAccountAuth('example', ctx)).toBeUndefined()
    await scheduler.updatePool('example', {
      accounts: [{ accountId: PI_UPSTREAM_ACCOUNT_ID, enabled: false, weight: 1, priority: 0 }],
    })
    expect(await announcement.getActiveAccount('example', ctx)).toMatchObject({ id: 'a' })
    expect(await announcement.resolveActiveAccountAuth('example', ctx)).toMatchObject({ accountId: 'a' })
    await scheduler.updatePool('example', {
      accounts: ['pi:default', 'a', 'b'].map(accountId => ({ accountId, enabled: false, weight: 1, priority: 0 })),
    })
    expect(await announcement.getActiveAccount('example', ctx)).toBeUndefined()
  })

  it('extracts bearer tokens from headers and tolerates resolver failures', async () => {
    const header = makeHarness({
      resolve: { auth: { headers: { Authorization: 'Bearer header-token' } } },
    })
    await header.scheduler.pinAccount('example', 'session-1', 'a')
    expect(await header.announcement.resolveActiveAccountAuth('example', header.ctx)).toEqual({
      accountId: 'a',
      accessToken: 'header-token',
      label: 'Work',
    })

    const failing = makeHarness({ resolve: new Error('resolve failed') })
    await failing.scheduler.pinAccount('example', 'session-1', 'b')
    expect(await failing.announcement.resolveActiveAccountAuth('example', failing.ctx)).toBeUndefined()

    const empty = makeHarness({ resolve: { auth: {} } })
    await empty.scheduler.pinAccount('example', 'session-1', 'a')
    expect(await empty.announcement.resolveActiveAccountAuth('example', empty.ctx)).toBeUndefined()
  })

  it('notifies account-changed listeners per provider and supports unsubscribe', () => {
    const { announcement, ctx } = makeHarness({})
    const commandCtx = ctx as unknown as ExtensionContext
    const events: { providerId: string; account: ActiveAccount | undefined }[] = []
    const unsubscribe = announcement.onActiveAccountChanged('example', event => {
      events.push({ providerId: event.providerId, account: event.account })
    })
    const account: ActiveAccount = { id: 'a', label: 'Work', authKind: 'oauth' }
    announcement.notifyActiveAccountChanged('example', commandCtx, account)
    announcement.notifyActiveAccountChanged('example', commandCtx, undefined)
    announcement.notifyActiveAccountChanged('other', commandCtx, account)
    unsubscribe()
    announcement.notifyActiveAccountChanged('example', commandCtx, account)
    expect(events).toEqual([
      { providerId: 'example', account },
      { providerId: 'example', account: undefined },
    ])
  })

  it('returns undefined for providers without an integration', async () => {
    const { announcement, ctx } = makeHarness({})
    expect(await announcement.getActiveAccount('missing', ctx)).toBeUndefined()
    expect(await announcement.resolveActiveAccountAuth('missing', ctx)).toBeUndefined()
  })

  it('reports pool presence before first selection, independently of affinity', async () => {
    const { scheduler, announcement, ctx, unregister, integration } = makeHarness({ affinity: false })
    const readAccounts = vi.spyOn(integration, 'accounts')
    const resolveAuth = vi.spyOn(integration, 'resolveAuth')
    expect(announcement.hasPool?.('example')).toBe(true)
    expect(announcement.hasPool?.('missing')).toBe(false)
    expect(readAccounts).not.toHaveBeenCalled()
    expect(resolveAuth).not.toHaveBeenCalled()
    expect(await announcement.getActiveAccount('example', ctx)).toMatchObject({ id: PI_UPSTREAM_ACCOUNT_ID })
    scheduler.registerProvider({ id: 'scheduler-only', label: 'Only', accounts: () => accounts })
    expect(announcement.hasPool?.('scheduler-only')).toBe(false)
    unregister()
    expect(announcement.hasPool?.('example')).toBe(false)
  })

  it('binds returned identity to the credential even if selection changes during refresh', async () => {
    const { scheduler, announcement, ctx, integration } = makeHarness()
    await scheduler.pinAccount('example', 'session-1', 'a')
    vi.spyOn(integration, 'resolveAuth').mockImplementationOnce(async account => {
      await scheduler.pinAccount('example', 'session-1', 'b')
      return { auth: { apiKey: 'token-' + account.id } }
    })
    expect(await announcement.resolveActiveAccountAuth('example', ctx)).toEqual({
      accountId: 'a', accessToken: 'token-a', label: 'Work',
    })
    expect(await announcement.getActiveAccount('example', ctx)).toMatchObject({ id: 'b' })
  })
})
