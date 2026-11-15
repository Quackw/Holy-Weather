# PRICE notification filter (candidate; not deployed)

PRICE detection remains the accepted Decimal midpoint, upward 50/55/60/70/80 thresholds, once per market/side/threshold, REARM=false. Original events and input references are unchanged. LAMP delivery is independent.

Enable `price_notification_filter = true` explicitly in configuration for a future deployment. Default false preserves current runtime behavior and existing configuration hashes. No running observer was changed by this PR.

## Delivery rules

1. A PRICE crossing whose current best bid is not above its previous best bid is retained with `delivery_admission: NOT_QUEUED / ASK_ONLY_CROSSING`. Given an upward midpoint crossing, its increase came from ask. No probability or demand claim is inferred.
2. Eligible events for the same exact market URL (same date and bucket) share a pending card for a fixed 30-second window from its first event. YES and NO remain separate lines; if both move the card says `Both sides moved`. Thresholds and all constituent event IDs remain recorded. This is batching, not cancellation of an opposite side.
3. The window never slides. At most ten constituent PRICE events (five once-only thresholds per side). No merge into claimed UNKNOWN or final receipts. No merging across markets, dates or into LAMP.
4. The oldest constituent price expiry is retained; merging cannot extend freshness. Waiting ends earlier if necessary, one second before expiry. An expired batch cannot be sent. Sender selects other ready entries while a PRICE batch waits.
5. Queue capacity still counts unresolved entries. A full queue may merge into an already pending compatible batch without allocating another slot. Otherwise refusal is recorded per event. Group state and new event are committed atomically.

`event_ids` on the queue row lists all constituents; each later event's admission includes `queue_key`. Delivery audits must follow that key rather than assume one row per event. All admitted and refused event evidence remains protected by existing retention rules. No schema migration, service, dependency or storage-budget increase.

## Evidence and limits

Saved delivered-event queue replay (not raw WebSocket replay, and no actual Telegram sends): 21 historical delivered PRICE events become 8 candidate cards. Twelve ask-only crossings are retained but not queued; the other nine bid-growing events are represented exactly once. One pair of Atlanta YES/NO events shares a card. Source evidence SHA is unchanged. Repeat replay is deterministic. This does not establish profitability or the usefulness of every retained bid change. Small bid changes, wide spreads and later oscillations outside the window can still alert. The 30-second batching latency is an explicit tradeoff.

Offline regressions cover baseline, multi-threshold events, bid growth, falling/unchanged bid, both sides, fixed window, expiry, restart, unknown sends, capacity, LAMP independence, transaction failure, evidence retention and disabling the feature. SQLite integrity/foreign keys and schema v1 checked.

## Rollback

Disable the option using this version to stop applying it to future events; existing pending batches still honor their stored delay and receipts. No database restore, threshold reset or history deletion. Before rolling code back to a pre-policy sender, finish or explicitly expire pending batches: old code understands the existing HTML/receipt but does not honor the additional wait field. UNKNOWN remains non-retryable. Remove the new configuration key before loading a configuration with pre-policy code. No runtime rollback was performed.

## Acceptance hold

The expanded review found lost later bid confirmations at 70/80%. See [acceptance review](PRICE_NOTIFICATION_REVIEW.md). PR 17 is BLOCKED for merge/deployment until this is resolved. `--price-notification-filter` now wires the option through the catalogue CLI; this does not authorize activation or change running wrappers.
