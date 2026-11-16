# PRICE delivery — grouping and delivered levels

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

## Delivered levels and same-side cards

With the filter ON, a later lower/equal threshold does not produce a new card
after a higher level was delivered for that exact contract, date, token and side.
For example, delivered YES 55%, a dip, then first crossing of YES 50% remains a
PRICE event but gets `NOT_QUEUED / DELIVERED_HIGHER_LEVEL`. The event records the
covering SENT queue key, event references, level and message ID. A lower card
already waiting when the higher ACK arrives is rejected at claim with the same
explicit reason. This does not rearm or remove any detector threshold.

Only a SENT receipt whose saved hash matches its non-test preview counts. UNKNOWN
and TEST do not establish a delivered level. Older receipts are read in place;
their exact non-test preview distinguishes them from historical TEST sends too.
No schema migration, history rewrite or separate high-water table is needed.

Eligible same-side events retain the fixed ordinary grouping window (10 seconds
by default), their IDs, individual admission evidence and oldest expiry. The card
lists only levels above the previously delivered maximum. Mixed events such as
new 50/60/70 after delivered 55 keep all original thresholds in SQLite and show
60/70 in the card. A changed queued preview must be regenerated before claim.
There is no editing or merging of messages already sent to Telegram.

70/80 keep their separate fast lane and original freshness: ordinary pending
work cannot delay them or shorten their expiry. Existing ask-only confirmation,
fallback and GAP refusals are unchanged. If 80 was already delivered, a later
70 is explicitly covered by that higher SENT receipt, rather than silently lost.
Replay reports distinguish actual simulated sends, covered events and refusals;
they never count UNKNOWN as delivery. Filter OFF retains the previous policy.

Compatible rollback reads the added queue fields without changing data. The old
code can send lower levels again for future events, but admission refusals already
recorded by this version remain final: no old-event backfill. Keep current SQLite,
fired thresholds, raw evidence and SENT/UNKNOWN; never restore an old database.

## Reconciliation with the accepted receive loop

PR #25 includes accepted main `7327f03` (PR #26) without changing its catalogue
receive loop. Only `alert_queue.py` differs at runtime from that main. The bounded
50 ms delivery cadence continues to check market lifecycle before sending; this
policy adds no socket timeout, buffering, freshness or storage change.

Offline full-observer checks in both regional configurations cover three ordinary
50/55/60 events in one card, a delivered NO80 covering a subsequent NO70 event,
and new YES70/80 in a separate priority card. All original event IDs and evidence
remain; the covered NO70 has an explicit reason and link to NO80. Priority work
keeps the existing sender pacing without an ordinary ten-second wait. Restart
on the same SQLite neither sends again nor rearms a level. These checks use fake
quotes and ACKs and are not new live-delivery evidence.

The compatible queue rollback is pinned to that exact accepted main. Fixed real
history replays retain original source SHA/clocks and compare both policies on
the same inputs; virtual delivery timings are labelled separately from actual
Telegram ACKs. No running observer is updated by reconciliation or acceptance.

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
