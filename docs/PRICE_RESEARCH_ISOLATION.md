# PRICE isolation from the legacy research window

Base: accepted main `116597bb0ab94578069c5b205eb08b713a41eb9a` (PR 21).
This change affects only the interpretation of `ENGINE_POINT_BOUND` with PRICE
enabled. PRICE thresholds, Decimal midpoint, once-only/REARM=false, event IDs,
queue policy, dependencies, schema v1 and the 90-second/256-MiB limits are intact.

## Reproduced defect and correction

A finite synthetic stream sends 4097 changing asks through the actual
`process → Store.apply → evolve → PRICE → queue` path. The last full quote moves
midpoint through 50/55/60%. Accepted main stores `ENGINE_POINT_BOUND` but loses
that PRICE crossing: the research detector's 4096-point limit is passed to PRICE
as an invalid quote, resetting readiness and delivery confirmation.

The original engine reaches this bound only after its clock, order and ask checks.
Storage now treats that specific research-only result as a fresh quote for PRICE,
which still independently requires a complete valid bid/ask. All other failures,
including missing prices, stale/future/reversed clocks and real GAP/restart,
retain their existing rejection/baseline rules. There is no broad error fallback.

The original `samples.status=ENGINE_POINT_BOUND` remains saved with the exact
input and timestamps. `apply` separately returns `research_status` and quote
`status`; current observation usability follows the latter. The research window
still resets and stays bounded; its accepted source high-water now includes this
valid quote, so a later GAP/restart cannot admit an older source clock. The
PRICE-disabled research mode keeps its previous behavior. No diagnostic-write
stream, source-history rewrite, migration or deletion is introduced.

## Validation boundary

The regression first failed on accepted main, returning no PRICE event. It uses
4097 real application transactions with synthetic market messages, not a live
Polymarket event. Other small boundary fixtures explicitly inject a full research
window to exercise book/BBO/depth, existing WAITING_BID confirmation, invalid
quotes, GAP/restart and transactional SQLite failure without repeating the load.
Existing tests cover both sides, MULTI-CROSS, LAMP independence and queue finality.

The source of `holypolyfoundation/netflix/src/price.mjs` and `registry.mjs` was
rechecked before the change: exact side midpoint, upward 50/55/60/70/80 crossings
and consumed thresholds. HOLY WEATHER's detector source is unchanged. Saved
source behavior is not proof of the currently deployed Netflix bot binary.

Both regional rollback tests extract the exact accepted source above from local
Git history and run old → candidate → old in isolated subprocesses. The candidate
adds a PRICE event at the research boundary; the old reader compares all saved
input/sample/event/evidence/meta rows, retains synthetic SENT/UNKNOWN receipts and
consumed levels, then records a new 80% crossing. Checkout includes Git history so
these tests need no network access. Existing dependency rollback remains separate.

All tests and rollback probes are offline; no Telegram delivery, production
switch, live outage root cause or continuous-service readiness is claimed.

## Rollback

Return the code/configuration to accepted main `116597b`, keeping websockets 17.4
and the current SQLite, including every new record, fired threshold and delivery
receipt. Never restore an older database over new data or retry SENT/UNKNOWN.
The previous research-bound defect returns under that code. Data compatibility
is exercised; a production deployment/supervisor rollback is not authorized here.
