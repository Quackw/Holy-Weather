# PRICE delivery — final PR 17 candidate

PRICE detection is unchanged: Decimal midpoint of the real side token, upward
50/55/60/70/80%, once per market/side/threshold, REARM=false. One MULTI-CROSS per
update. Delivery never creates another PRICE event or resets a fired threshold.

`price_notification_filter` is opt-in and remains OFF by default. The catalogue
CLI exposes `--price-notification-filter` and `--price-batch-seconds 10|15|30`.
Ordinary batching defaults to **10 seconds**, chosen from the fixed real-history
comparison, not by minimizing messages. All three windows gave the same card
count; 10 seconds had lower delay. This small sample does not prove a universal
noise optimum or a trading advantage.

## Priority 70/80

Any event containing 70% or 80% is priority, including a MULTI-CROSS containing
lower levels. A bid-growing crossing is ready immediately. An ask-only crossing
waits for a later fresh full book/BBO of the same token, later source and receipt
times, bid above the crossing bid, midpoint at least the highest crossed level.
A small bid increase is a mechanical delivery check, not proof of demand.

Default wait is **8 seconds**, with a hard maximum of 8. Prior experiment configs
5/8/10 still load; 10 is explicitly capped to 8 and the effective window is saved
as `confirmation_window_ms`. Stored deadlines remain authoritative on reopen.
A confirmation makes priority work ready immediately. Timeout without confirmation
makes it ready with **Ask-only · not confirmed by bid**, only while original
freshness and continuity remain valid. No additional batch delay is added.

The shared sender still permits one in-flight send and its existing 3.1s pacing.
Ready priority work is selected before ready ordinary work; an existing HTTP
request is not interrupted. Priority readiness does not promise instant delivery
under backlog, rate limits or a stopped/uncertain sender.

## Ordinary 50/55/60

All original events are retained. Ask-only events use the same bounded bid check;
without confirmation they end with explicit
`REJECTED / NO_BID_CONFIRMATION_WITHIN_WINDOW`. Eligible ordinary events group
inside a fixed, non-sliding 10/15/30s window. At most ten event references; no
cross-market, cross-date, cross-side or cross-track merge.

Priority and ordinary work have separate grouping keys, so an old ordinary group
cannot delay or shorten the lifetime of a new priority event. Already pending
same-priority events may combine opportunistically before claim, without an
intentional wait for priority. YES and NO stay independent cards. All source
IDs, absorbed-row owner links, confirmation inputs and warnings remain durable.

## Continuity, expiry and atomicity

`WAITING_BID` counts toward the existing 1024 unresolved-entry bound. Confirmation
input, evidence link and state change commit in the same input transaction.
GAP, restart, missing/invalid bid/ask and BBO sentinels invalidate unclaimed
ask-only work, including already settled confirmation/fallback work. A valid
later quote cannot revive or upgrade its final decision. SENT and UNKNOWN are
not selected or retried. UNKNOWN remains explicit and consumes unresolved
capacity. Queue-full admission refusal remains explicit on each source event.

The oldest constituent source/receipt expiry and local midnight remain in force;
no wait/group extends the 90-second limit. Market retirement is checked before
every catalogue sender tick, independently of throttled global subscription
maintenance. The final HTTP expiry check is preserved. Resolution/date expiry
cannot be bypassed by the 250ms maintenance cadence.

No schema, weather, LAMP algorithm, dependency, service or budget change. One
writer; source events and their evidence remain protected by existing retention.

## Compatibility and rollback

The accepted rule hash and SQLite schema v1 are unchanged. Disabling the filter
using this version stops new policy admissions while existing stored work keeps
its deadlines/status. Old experimental waiting rows remain interpretable.

Before returning to pre-PR17 code, explicitly finalize unclaimed WAITING_BID and
batched PENDING work with this version, preserving source events, evidence and
an explicit rollback reason. Never retry UNKNOWN/SENT or restore an old DB.
Remove policy-only config keys for an old reader. The isolated two-phase check
opened the resulting data with the actual accepted-main code, preserved three
PRICE events and UNKNOWN, added two new inputs after rollback and produced no
repeated PRICE event. No runtime rollback or deployment occurred.

See [comparison and acceptance](PRICE_NOTIFICATION_REVIEW.md) and
[transport diagnosis](PRICE_TRANSPORT_REVIEW.md).
