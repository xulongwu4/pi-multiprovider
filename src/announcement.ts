import { normalizeContext } from '@earendil-works/pi-ai'
import type { Api, Provider, ProviderHeaders } from '@earendil-works/pi-ai'
import type { ExtensionContext } from '@earendil-works/pi-coding-agent'
import { PI_UPSTREAM_ACCOUNT_ID } from './managed.ts'
import type { MultiProviderService } from './service.ts'
import type {
  ActiveAccount,
  ActiveAccountAuth,
  ActiveAccountChangedEvent,
  MultiProviderIntegration,
  MultiProviderServiceAnnouncement,
  MultiProviderServiceContext,
} from './types.ts'

export interface AnnouncementDependencies {
  /** Omit for already-initialized standalone schedulers. */
  isReady?(): boolean
  scheduler: MultiProviderService
  getIntegration(providerId: string): MultiProviderIntegration<Api, unknown> | undefined
  getBaseProvider(
    providerId: string,
    ctx: MultiProviderServiceContext,
  ): Provider<Api> | undefined
  affinityKeyFor(
    integration: MultiProviderIntegration<Api, unknown>,
    ctx: MultiProviderServiceContext,
    providerId: string,
  ): string
}

// The public announcement plus the notify hook the bundled extension uses after
// /switch-account changes the session's pinned account.
export interface ServiceAnnouncementHandle extends MultiProviderServiceAnnouncement {
  notifyActiveAccountChanged(
    providerId: string,
    ctx: ExtensionContext,
    account: ActiveAccount | undefined,
  ): void
}

function bearerTokenFromHeaders(headers: ProviderHeaders | undefined): string | undefined {
  for (const [name, value] of Object.entries(headers ?? {})) {
    if (name.toLowerCase() !== 'authorization' || typeof value !== 'string') continue
    const match = /^bearer\s+(.+)$/i.exec(value.trim())
    if (match !== null) return match[1]!.trim()
  }
  return undefined
}

// Builds the in-process service announced on MULTIPROVIDER_SERVICE_EVENT. The
// active account is the session's explicit /switch-account pin, else the
// scheduler's last selection while pool affinity is on, else a best guess
// (upstream login, then the first ready account). Use hasPool to distinguish
// an unpooled provider from a pool with no selection; only the former can safely
// fall back to upstream credentials. Stored account credentials resolve through
// the integration (refreshing OAuth under the account-store lock) so consumers
// never read the private store directly.
export function createServiceAnnouncement(deps: AnnouncementDependencies): ServiceAnnouncementHandle {
  const listeners = new Map<string, Set<(event: ActiveAccountChangedEvent) => void>>()

  const activeAccount = async (
    providerId: string,
    ctx: MultiProviderServiceContext,
  ): Promise<ActiveAccount | undefined> => {
    const integration = deps.getIntegration(providerId)
    if (integration === undefined) return undefined
    let affinity: boolean
    try {
      affinity = deps.scheduler.getPoolPreference(providerId).affinity
    } catch {
      return undefined
    }
    const pin = deps.scheduler.getAffinity(
      providerId,
      deps.affinityKeyFor(integration, ctx, providerId),
    )
    if (pin !== undefined && (pin.explicit || affinity)) {
      const account = (await integration.accounts()).find(
        candidate => candidate.id === pin.accountId,
      )
      return account === undefined
        ? undefined
        : { id: account.id, label: account.label, authKind: account.authKind }
    }
    // No selection yet: report a best guess so followers can show something —
    // the upstream login if ready, else the first ready account in pool order.
    // ponytail: ignores policy (round-robin/priority may pick another on the
    // first request); followers re-read on their next refresh.
    const pool = (await deps.scheduler.snapshot()).providers.find(item => item.id === providerId)
    const ready = pool?.accounts.filter(account => account.status === 'ready') ?? []
    const guess = ready.find(account => account.id === PI_UPSTREAM_ACCOUNT_ID) ?? ready[0]
    return guess === undefined
      ? undefined
      : { id: guess.id, label: guess.label, authKind: guess.authKind }
  }

  return {
    hasPool(providerId) {
      if (deps.isReady?.() === false) return undefined
      return deps.getIntegration(providerId) !== undefined && deps.scheduler.hasProvider(providerId)
    },
    async getActiveAccount(providerId, ctx) {
      return activeAccount(providerId, ctx)
    },
    async resolveActiveAccountAuth(providerId, ctx, signal) {
      const active = await activeAccount(providerId, ctx)
      if (active === undefined || active.id === PI_UPSTREAM_ACCOUNT_ID) return undefined
      const integration = deps.getIntegration(providerId)
      const base = deps.getBaseProvider(providerId, ctx)
      if (integration === undefined || base === undefined) return undefined
      const model = ctx.model ?? base.getModels()[0]
      if (model === undefined) return undefined
      const account = (await integration.accounts()).find(candidate => candidate.id === active.id)
      if (account === undefined) return undefined
      const effectiveSignal = signal ?? new AbortController().signal
      try {
        const resolution = await integration.resolveAuth(account, effectiveSignal, {
          provider: base,
          model,
          context: normalizeContext({ messages: [] }),
          requestOptions: {},
          signal: effectiveSignal,
        })
        const accessToken = resolution.auth.apiKey ?? bearerTokenFromHeaders(resolution.auth.headers)
        if (accessToken === undefined || accessToken.trim() === '') return undefined
        return {
          accountId: account.id,
          accessToken: accessToken.trim(),
          label: active.label,
          ...(resolution.source === undefined ? {} : { source: resolution.source }),
        }
      } catch {
        return undefined
      }
    },
    onActiveAccountChanged(providerId, callback) {
      let callbacks = listeners.get(providerId)
      if (callbacks === undefined) {
        callbacks = new Set()
        listeners.set(providerId, callbacks)
      }
      callbacks.add(callback)
      return () => {
        const current = listeners.get(providerId)
        if (current === undefined) return
        current.delete(callback)
        if (current.size === 0) listeners.delete(providerId)
      }
    },
    notifyActiveAccountChanged(providerId, ctx, account) {
      const callbacks = listeners.get(providerId)
      if (callbacks === undefined) return
      for (const callback of callbacks) {
        try {
          callback({ providerId, account, ctx })
        } catch {
          // A misbehaving listener must not break the switch notification.
        }
      }
    },
  }
}
