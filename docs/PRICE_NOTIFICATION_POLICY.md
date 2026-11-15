# PRICE delivery policy — PR 17, opt-in, not deployed

The PRICE detector is unchanged: Decimal midpoint, upward 50/55/60/70/80%, once-only, REARM=false. Delivery never creates a replacement PRICE event or changes fired thresholds. LAMP remains independent.

Enable `price_notification_filter` explicitly (catalogue CLI `--price-notification-filter`). Default OFF preserves existing runtime. `price_confirmation_seconds` supports 5, 8, 10; candidate default **8 seconds**. On the fixed saved history 8 retained one additional low-threshold confirmation compared with 5; 10 added none. This is not statistical optimization or evidence of demand/profit.

## Bounded delivery state

- An upward crossing without bid growth creates a normal original PRICE event and a durable `WAITING_BID` queue row. Its original expiry, token, source time, bid and thresholds are retained. It counts toward the existing 1024 unresolved-entry bound.
- Only a later received, later source-time, fresh confirmed book/BBO for the **same token**, with bid above the crossing bid and midpoint at least the highest crossed threshold, confirms delivery. A small bid increase is only a mechanical condition.
- The confirming input is linked through `event_inputs` and recorded on the queue row; original PRICE payload/prices and fired state are not rewritten. Confirmation and its input reference commit together with the input transaction.
- Timeout below 70% becomes explicit `REJECTED / NO_BID_CONFIRMATION_WITHIN_WINDOW`. Timeout containing 70/80% becomes a pending compact card with **Ask-only · not confirmed by bid**. A later quote never upgrades that final confirmation decision.
- GAP, restart, invalid continuity, market resolution and expiry prevent revival. Their outcome is durable and explicit. No waiting state is carried across a new connection as a confirmation opportunity. UNKNOWN is never retried.
- Compatible unclaimed notifications group by exact bucket/date **and side** in a fixed 30-second window, maximum ten event references. YES and NO are separate cards. Original event IDs, constituent delivery outcomes and confirmation evidence remain available; absorbed rows retain `MERGED` and the owning queue key.
- Oldest source/receipt expiry and local midnight still govern every card. Waiting/batching cannot extend the 90-second limit. No merge into SENT/UNKNOWN, across sides, or with LAMP. Warning text survives grouping. Queue-full refusal remains explicit per event; final rows do not consume unresolved capacity.

No new schema, dependency, service, weather logic, PRICE thresholds or storage limits.

## Rollback / compatibility

Schema remains v1 and accepted rule hash is preserved. Disable the filter using this version to stop new waiting admissions; durable existing states keep their rules. Session restart explicitly rejects unconfirmed waiting entries, preserving evidence. SENT/UNKNOWN remain non-repeatable. Tests cover rollback by configuration without restoring any database.

Before returning to pre-policy code, all WAITING_BID and batched pending work must be explicitly finalized using this version: older senders do not understand its timing/grouping. Remove new config keys for old code. Never restore an old database, reset thresholds, or retry UNKNOWN. No live rollback/deployment occurred.
