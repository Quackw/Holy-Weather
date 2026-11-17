# HOLY WEATHER architecture

Runtime baseline `8d176109b80628716f745b210d87bca00c5c51ed` (PR #30, 17 November 2026).
One project, independent PRICE/LAMP algorithms, shared durable queue implementation,
one Telegram bot. Separately approved continuous PRICE services are installed;
weather/LAMP remains optional single-contract functionality. No trading, own
forecast model or LOCK strategy.

## Entry points and ownership

- `app.observe`: one exact contract, both real YES/NO tokens, optional accepted
  US AWC/LAMP context; stops at the contract's local-day end.
- `catalogue.observe_catalogue`: PRICE-only contracts for `--region europe`
  (11 profiles) or `--region us` (10); shared code, separate directories/rule hashes.
- `service.run` / `recovery.supervise`: approved continuous regional plan, systemd
  credential, persistent controller and workers of at most 1,200 seconds.

One POSIX lock and main-thread SQLite writer own each directory. Regions do not
share a writable database or merged subscriptions. Durable queue state is regional;
each process owns its sender. One bot/channel is not a global pacing coordinator.
Direct app/catalogue entry points are finite: default 120 seconds, configured
maximum one hour. The continuous parent opens successive bounded workers and
retains local-day discovery/recovery state. Installation is separately authorized;
see [runtime/transition contract](docs/PRICE_CONTINUOUS.md).

## Inputs and atomic events

`Gamma verification → native WebSocket → normalize/process → Store.apply → event → queue`

Discovery verifies bucket, rules/rounding, station, timezone, local date, condition
and actual outcome-token mapping; receipt bytes/SHA remain evidence. Gamma
indicative prices and trades never replace native bid/ask. Full book/BBO can
establish PRICE readiness; depth retains top-of-book information but cannot
independently create PRICE crossings. Changed quotes are retained. Unchanged
confirmations coalesce with 15-second checkpoints and explicit counters.
Known duplicates do not erase newer valid state; uncertain changed/out-of-order
input invalidates the affected side. Source/receipt clocks stay distinct.

Catalogue source lead is at most 100 ms, without timestamp rewriting or extending
the 90-second source/receipt age limit. HTTP/heartbeat success alone is not quote
readiness. YES/NO have independent baselines and consumed thresholds; the other
token cannot replace a missing bid or ask on the selected side.

One writer transaction commits compressed/SHA-bound input, sample, research
state, event/evidence, PRICE fired state and queue admission. Confirmation inputs
and delivery changes use the same transaction. SQLite failure rolls back and
propagates; it is not treated as external network unavailability.

Legacy `core.evolve` still records 50/70/90 ask research. Above 4096 changing anchors
it resets that window. PR #22 retains `samples.status=ENGINE_POINT_BOUND` and
original input while separately accepting this specific research-only result as
a usable quote for PRICE. PRICE still validates complete midpoint; other failures
retain rejection/baseline semantics. Source high-water survives the research
reset. Historical research events are preserved. See [isolation](docs/PRICE_RESEARCH_ISOLATION.md).

## Independent producers and durable delivery

PRICE uses Decimal midpoint, upward 50/55/60/70/80%, one MULTI-CROSS per update,
once per market/side/threshold and REARM=false. First quote after GAP/restart is
a baseline; consumed thresholds persist.

Optional KLGA LAMP ALERT is triggered by original new bulletins independently of
price: configured 2°F peak change or a verified bucket boundary, comparing identical
remaining UTC hours. METAR, received-observation maximum and future LAMP peak stay
separate; late receipts cannot revise events. Bounded HTTP workers do not write
SQLite. The regional catalogue rejects weather/LAMP flags; European source
experiments remain deferred, not a third alert strategy.

Namespaced PRICE/LAMP queue records retain event IDs, card/contract, expiry and
explicit outcomes. The filter defaults OFF. When enabled, bid-growing 70/80 is
immediately ready; ask-only waits at most eight seconds with labelled fallback
only while still valid. Ordinary groups default to ten seconds. No cross-side,
contract/date/track grouping. See [policy](docs/PRICE_NOTIFICATION_POLICY.md).

UNKNOWN commits before HTTP; SENT requires a verified ACK. Neither becomes
pending again. A sender failure stops that sender, not ingestion. One HTTP worker
has no SQLite access, one in-flight request and 3.1-second per-sender pacing.
Queue deadlines advance with Telegram OFF. Expiry is checked before HTTP and
capped at local midnight. Priority readiness is not instant-delivery assurance;
there is no exactly-once network guarantee.

The 1024 bound counts unresolved work, including UNKNOWN/unrecognized states.
Terminal receipts remain for dedupe. Queue-full refusal is saved on its source
event; later capacity does not silently backfill refused events.

## Recovery and local-day lifecycle

Subscriptions use groups of at most 16 tokens, at most one group/second. Catalogue
reconnect budget defaults to three (configurable 0–5), with bounded backoff and
the original finite deadline. Refusal/protocol/oversize/storage failures remain
distinct. Gaps preserve evidence, clear readiness and require new baselines;
unknown gap movement is not reconstructed.

Catalogue revalidates affected cities when their local date changes. Discovery
attempts/date high-water survive restart; publication outages have at most three
attempts per city/date, five minutes apart. Refresh closes the shared socket and
records a gap; all current tokens need new baselines. Old markets are not a
fallback, old pending cards expire, and historical identities remain in SQLite.
Only the latest retained date per station loads into the bounded 176-market
memory registry. Rollover does not extend a finite worker's deadline; the continuous
parent opens another window. Old legacy day refusals remain in the audit while
current markets are rechecked under the corrected lifecycle. See
[local-day behavior](docs/PRICE_DAY_ROLLOVER.md).

Continuous ownership includes boot ID, PID, start ticks, UID and executable. The
child inherits the controller flock before exec. Recovery debt is durable before
backoff; the same durable STARTING write consumes its charged marker before spawn,
so an interrupted launch cannot reuse the previous attempt's recovery. Limits are
three recoveries/hour and six/rolling 24 hours, with 30-second exponential backoff
capped at 300 seconds. Manual/fatal stops latch; systemd does not restart exit 78.
There is no independent controller watchdog or OnFailure notification in the unit.

Worker rotation and archive maintenance create a real gap and require new
baselines. Recovery preparation explicitly expires pending cards, including cards
still inside their quote TTL at a planned boundary. Source events remain; this is
a known delivery limitation, not successful delivery. SENT/UNKNOWN never requeue.

## Storage, maintenance and rollback

SQLite schema v1 uses WAL/FULL durability, foreign keys, bounded cache and writer
lock. Unknown schema/application/rule identity fails closed. Up to 8 MiB of the
256-MiB per-directory budget is reserved for completion/WAL/logs. The pre-write
guard stops capture before exhaustion; it is not a filesystem quota against
external writers. Existing writer checkpoints WAL; logs rotate at 512 KiB with
two backups. No automatic VACUUM or limit increase is present.

Explicit prune previews first and requires a stopped matching writer/config.
Unaudited events have a 30-day policy and unreferenced inputs 24 hours;
delivery-audited events, receipts, referenced evidence and latest snapshots stay
protected. SQLite free pages can be reused; physical shrinking is not promised.
Separately authorized continuous plans archive only between closed workers, at
128 MiB: full verified history precedes adoption of a candidate at most 160 MiB.
Per-region archives are bounded at 4 GiB/40 parts, packed dedupe at 96 MiB and
free disk at a minimum 2 GiB. Events, fired levels, receipts, identities and their
evidence remain recoverable across working data and archives. Only verified
redundant scratch copies retire. Closed control attempts are audited into SQLite
before redundant files retire; active control remains bounded at 1 MiB. These
limits can stop observation. Local archives are not independent off-server backup,
and initial live measurements do not establish a daily retention budget.

Compatible code/config rollback retains the current database, new history,
fired levels and receipts. Never restore an old database over new data.
Earlier releases can reintroduce capacity/rollover/maintenance/research defects;
compatibility is version-specific, not permission to run old pruning. Verified
backups are recovery evidence, not a reason to rewind working data.

## Qualification boundary

The runtime baseline passed 785 tests on each Linux/Python 3.11 and 3.13.5,
exact-source rollback and saved-stream replay. The actual systemd path was tested
with synthetic I/O on a disposable runner. Separately approved installation
completed two natural windows per region and real new Telegram ACKs. These facts
do not prove every city's readiness, a real host reboot, overnight/DST behavior
or full 24-hour reliability. Keepalive-timeout root cause, pending-card boundary
expiry, independent health escalation and continuing off-server backup remain
open. See the dated [operations record](docs/OPERATIONS.md) and
[ROADMAP](docs/ROADMAP.md).
