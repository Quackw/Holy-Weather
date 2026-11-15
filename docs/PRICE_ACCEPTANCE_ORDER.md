# Stage 0 audit and stage 1 preparation — 15 November 2026

Scope: documentation and acceptance planning only. No runtime edits, merge,
new sends, deployments or changes to the separately authorized finite session.
GitHub PR metadata and head workflow runs were read afresh; main was verified
with `git ls-remote`. Historical reports below are evidence, not current health.

## Repository snapshot

| Ref | Verified head | Base | CI at that head | Acceptance |
| --- | --- | --- | --- | --- |
| main | 98ae9d1 | accepted PR 11 | connector returned no runs for merge SHA; not freshly confirmed | accepted PRICE/LAMP/queue foundation |
| PR 12, draft | 16b0aae | main 98ae9d1 | checks success, run 43724085442 | PARTIAL: mixed PRICE and forecast research scope |
| PR 13, draft | fc8a172 | main 98ae9d1 | checks success, run 43728925131 | defer: documentation-only source audit |
| PR 14, draft | c64d36d | PR 12 head 16b0aae | checks success, run 43732421275 | PARTIAL: finite catalogue; clock/ordering acceptance pending |

PR 14 depends on PR 12, not PR 13. Do not merge it first or treat the entire
stack as accepted. After accepting the cleaned PR 12, retarget PR 14 to main,
inspect the remaining diff and rerun CI. No history rewriting is needed.
PR descriptions contain earlier counts and pending-live claims; this audit
supersedes those claims without deleting historical evidence.

## Necessary versus deferred changes

- PR 12 PRICE foundation: European city/station/time-zone profiles in core,
  Celsius/rules/token verification in discovery, complete bid/ask readiness in
  storage, local-midnight stop in app, European/DST/readiness regressions.
  Keep the explicit rejection of NOAA LAMP for European stations.
- PR 12 deferred: standalone `mosmix.py`, its tests and forecast qualification
  sections; weather polling during reconnect and LAMP scheduled-timeout recovery
  are separate weather reliability work, not prerequisites for PRICE. Before
  accepting PR 12, separate those changes in a small follow-up commit/branch,
  preserving the current branch point and evidence. Review mixed app/test hunks
  individually rather than dropping whole files. No extraction performed here.
- PR 13: three documentation files, no runtime dependency. Preserve and defer
  until PRICE acceptance. Its README addition must later reconcile with current
  documentation rather than restore the obsolete roadmap.
- PR 14: reuse existing PRICE detector, writer, event evidence and queue; catalogue
  supplies per-market lookup instead of a parallel detector. Its 16-token batches,
  routing and sender pacing are relevant. The legacy ask-based 50/70/90 research
  detector still exists alongside PRICE: it is not Netflix parity and its counts
  must not be reported as PRICE counts. Removal is not part of this audit.
- Ignored `evidence/` contains launchers and real data. The finite server wrapper
  is not a reviewed production supervisor, and must not silently become one.
  No new schema, dependency or service is needed for the PRICE acceptance work.

All reviewed runtime branches retain SQLite schema/user_version 1. Catalogue
metadata uses existing tables. Rule hashes intentionally distinguish catalogue
from single-market configuration and nonzero clock tolerance. Schema compatibility
alone does not authorize opening the same database with an incompatible config.

## Current clock policy — candidate, not final acceptance

Main/PR 12 reject source times later than receipt. PR 14 keeps default tolerance
0 for ordinary configurations and sets 100 ms explicitly for the catalogue;
100 is also the hard maximum. Source and receipt timestamps are never rewritten.
Both ages must satisfy the existing 90-second limit. PRICE queue expiry uses
`min(source_ms, received_ms) + 90000`, capped at station-local midnight.
Nonzero tolerance changes the rule hash; zero preserves the accepted old hash.

Saved Mac evidence found source leads 1–54 ms. The retained-message replay
compared the same 40,590 messages: 0 ms produced 73,340 samples/0 PRICE events;
100 ms produced 9,280 samples/1 historical PRICE event. Source SHA stayed intact.
This is not full wire replay and does not prove 100 ms is the minimum safe value.
Linux NTP synchronization was reported at 14:08 UTC; the subsequent finite Linux
run had zero FUTURE samples. NTP=yes is not a measured bound on exchange clock
error, nor a permanently valid synchronization measurement.

Stage 1 must compare signed source-minus-receipt distributions by host and message
kind, including maxima/quantiles and out-of-order sequences. Evaluate 0 and bounded
candidate settings on immutable saved data; do not tune against profitable alerts.
Keep 100 ms as the current authorized experiment setting pending that review.
Test 100/101 ms, 90,000/90,001 ms, backward and forward wall-clock jumps, monotonic
run duration, equal timestamps, exact duplicates, high-water persistence and
restart. Do not loosen source-order checks merely to hide reorder suppressions.

## Stage 1 work package (prepared, not begun)

1. Pin accepted US PRICE specification/source and current branch SHAs; run the
   existing regressions without changing thresholds, rearm or side identity.
2. Reproduce retained out-of-order confirmations and quantify baseline resets,
   potential missed crossings and false-crossing risk. Distinguish genuine older
   data, exact duplicates and quote/depth messages with equal source timestamps.
3. Replay immutable Mac and Linux evidence at the candidate clock settings;
   record source SHA, deterministic outputs, event/queue identities, byte growth,
   and omitted/coalesced-message limitations. Never backfill old Telegram cards.
4. Audit 16-token subscriptions: each requested token receives a full baseline;
   subsequent confirmations are routed once; gaps/restart never cross baselines.
   Measure actual missing coverage rather than infer it from subscription count.
5. Check all existing PRICE rules, atomic event/queue writes, once-only thresholds,
   unknown delivery, queue limit 1024, explicit expired/full/sender-stopped counts.
6. Run Linux/offline CI and only the necessary separately authorized finite live
   checks. A fresh complete quote is sufficient; no crossing is required for PASS.
7. Report separate code/replay/live/delivery/resource verdicts and stop for review.

## Gaps that prevent permanent PRICE READY

The catalogue is Europe-only, while accepted US monitoring remains single-market;
all-city US catalogue support is not already implemented. Catalogue network gaps
stop the finite run: it does not inherit the single-market recovery loop. Closed
markets are ignored and pending cards expired, but explicit WebSocket unsubscribe
is absent. The 176-market registry bound needs a reviewed next-day lifecycle.
There is no accepted automatic date rollover or permanent supervisor. Queue FULL
retains events but has only a global marker; explicit per-event non-delivery
accounting needs acceptance. 256 MiB is a stop bound, not proof of a full day's
capacity. Unknown sends are not retried; this avoids duplicates, not all losses.
These are review items for later stages, not permission to implement stage 5 now.

## Evidence already available (limited scope)

- London/Paris/Amsterdam finite Linux windows had verified rules and usable
  YES/NO quotes. Other-city readiness is individual, not inferred from 121 listings.
- A later all-city 120-second Linux test saw four real Amsterdam PRICE events,
  Telegram disabled, SQLite 3,993,600 bytes; zero clock-FUTURE samples.
- Authorized 300-second Mac test delivered two actual Amsterdam cards, message
  IDs 362/363; durable reopen caused zero repeat transport calls. SQLite integrity
  and foreign keys passed; closed database 14,381,056 bytes.
- A separately authorized server session carries that state until the specified
  local-day deadline, no new production service. Its running state is not a
  completed day-long acceptance. Do not modify it as part of this docs task.
- Latest six-city forensic sample: five cities had one-sided native quotes;
  London had old complete quotes. Reorder resets also exist and need stage 1
  analysis; do not blame every missing price on a software defect or declare
  the whole ingestion path faultless from these observations.

## Safe acceptance sequence

Approve stage 0 documentation → authorize stage 1 PRICE/clock review → accept
PRICE-only PR 12 after its targeted gates → validate London/Paris/Amsterdam,
then the other profiles → finish/rebase target of PR 14 and accept its finite
catalogue → review real queue/delivery evidence → separately design and authorize
bounded daily qualification. Only after PRICE acceptance resume PR 13/source
research and the independent FORECAST direction. No automatic merges.

Rollback of this stage is a documentation-only revert. Runtime, active session,
historical databases, cursors, secrets and source artifacts are unchanged.
