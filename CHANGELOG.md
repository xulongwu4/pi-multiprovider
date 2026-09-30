# Changelog

## Unreleased

- Expose optional `hasPool(providerId)` on the cross-extension service so consumers can distinguish unpooled providers from pools without session selections, without reading private account storage. Return `undefined` until reconciliation completes rather than falsely claiming no pool exists.
- Announce the stable service object before and after reconciliation (including pool additions/removals). Consumers should refresh even when the service object is unchanged.
- Catch and report external-registration reconciliation failures rather than leaving rejected promises unhandled. Clear readiness on session shutdown.
- Return `accountId` alongside resolved credentials, bound to the account actually passed to the resolver even if the session selection changes during OAuth refresh.

## 0.10.1

- Validate against Pi 0.99.0, including an offline real-host package-loading probe.
- Declare imported host packages as wildcard peers and pin development dependencies to Pi 0.99.0.
