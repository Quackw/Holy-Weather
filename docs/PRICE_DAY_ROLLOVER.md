# PRICE local-day rollover (finite catalogue)

The catalogue now revalidates the exact current station-local date at startup and
when that city's date changes. Existing Gamma selection checks the complete pinned
rules template, station/source, Celsius bucket, open status and real YES/NO token
identities. No new rules, source or detector semantics are accepted automatically.
Original Netflix price.mjs/registry.mjs were rechecked: midpoint, upward levels,
MULTI-CROSS and once-only state are unchanged.

Only affected cities are discovered again. A missing publication/transport outage
allows at most three attempts per city/date, five minutes apart. Attempts and their
next allowed time survive restart. 401/403/429, invalid metadata and an interrupted
IN_PROGRESS attempt stop that city's discovery without blind retry. Verified
cities are revalidated on restart. A durable date high-water prevents reopening a
previous day after a backward clock/date change. A failed city's old markets are
not used as a fallback. Individual skipped-rule reasons and received HTTP evidence
are retained; no fresh price is inferred from successful discovery.

Discovery is synchronous and bounded (12-second request timeout, 4 MB response).
Before refresh the shared socket closes and records a gap. Afterwards all selected
current-day tokens get new baselines, including unchanged-date cities. This small
implementation intentionally does not claim lossless observation during that gap;
there are no invented crossings through the interruption. No new worker/service.
Groups remain at most 16 tokens, one group/second; reconnect budgets are unchanged.

Old pending cards expire before sending; the existing final HTTP check also caps
validity at local midnight. SENT receipts and UNKNOWN attempts never become pending.
Old queued cards are not backfilled when a new market appears. An old token's late
frame cannot be routed into a new day's market. Event, evidence, fired thresholds
and delivery remain in their existing transaction. No PRICE/LAMP state mixing.

Old market metadata, tokens, evidence and receipts remain in SQLite. Only ended
entries leave the in-memory subscription registry. On reopen it loads the latest
retained date per station (at most 176 markets); historical token/condition
collisions are still checked against SQLite. Two complete 176-market days reopen
with 176 in memory and all 352 identities retained. Schema version 1 is unchanged.
No archival, pruning, VACUUM or storage-limit increase is introduced. The 256 MiB
stop guard still applies; indefinite daily storage is not qualified.

The runner can wait for a new publication/day even after every current market
closes, but only within its original finite deadline (maximum 3600 seconds).
Discovery does not reset this deadline; an already-started bounded HTTP request
can complete after it. This is not a permanent supervisor or deployment.

Tests cover all 11 cities at local midnight, DST start/end and repeated hours,
London/Paris with different dates, old pending cards, delayed old-token frames,
new-day baseline, restart dedupe, publication retry budgets, denied/unknown
attempts, bad rules/station/date/tokens, SQLite faults and historical identities.
Network, time progression and Telegram in these tests are synthetic. A real
midnight session has not been run; active observation was not touched.

Rollback: preserve the database and receipts. Accepted PR15 can still read schema
1 but restores the old finite-day behavior and loads every historical market;
a directory exceeding its 176-market lifetime bound cannot be resumed by that
version. Do not delete history or restore an old DB to force rollback. The new
version is required to continue a multi-day catalogue. This limitation is explicit,
not a claim that an old binary supports the new lifecycle.

Saved native London/Paris/Amsterdam streams (301/216/1336 frames) were replayed
through the full catalogue both normally and with an explicitly injected outage,
then repeated. All six result pairs match (events, receipt outcomes, reconnects, bytes and source SHA); zero PRICE events in these
windows. SQLite integrity and foreign keys pass; observed replay data usage is
687324–937276 bytes. These are finite replay measurements, not a daily disk estimate.
Evidence stays outside Git in evidence/price-day-rollover-20261115/.
