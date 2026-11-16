# PR 17 — final PRICE delivery acceptance

Code review/qualification only. PR remains Draft pending acceptance; no merge,
deployment, observer restart or Telegram send was performed. Filter remains OFF
by default. [Policy](PRICE_NOTIFICATION_POLICY.md),
[transport review](PRICE_TRANSPORT_REVIEW.md),
[precise preview examples and finite live plan](PRICE_QUALIFICATION_PLAN.md).

## A/B/C — one fixed real-history cohort

35 historically SENT PRICE events: Amsterdam16, US19. The seven qualification-
expired US entries and the final unsent US PENDING are excluded equally from all
policies. Original events, prices, source/receipt times and retained subsequent
samples are unchanged. A=original unfiltered; B=frozen d7f9647 (8s bid wait, all
batches30s); C=priority70/80 immediately ready, ordinary groups10s.

| Metric | A original | B previous PR17 | C final |
|---|---:|---:|---:|
| Source PRICE events retained | 35 | 35 | 35 |
| Simulated cards | 35 | 25 | 23 |
| Amsterdam cards | 16 | 9 | 7 |
| High-level events delivered / original10 | 10 | 5 | 5 |
| High-level threshold tags delivered / original11 | 11 | 6 | 6 |
| Delivered bid-growing at original crossing | 15 | 15 | 15 |
| Delivered after later valid bid confirmation | 0 | 9 | 7 |
| Delivered without bid confirmation | 20 | 1 (labelled) | 1 (labelled) |
| Undelivered source events (explicit reasons) | 0 | 10 | 12 |
| Duplicate event representations | 0 | 0 | 0 |
| Expired notifications in this replay | 0 | 0 | 0 |

All original10 high-level events remain durable. C delivers two directly
bid-growing, two later confirmed, and one explicit ask-only fallback event.
Five high-level ask-only events have explicit continuity rejection (four
AWAITING_BASELINE, one OUT_OF_ORDER). They are not silently recovered through
a broken baseline. High-level delivered counts are events, not eleven separate
cards for eleven level tags.

C's12 omissions: four ordinary bid-confirmation timeouts, six AWAITING_BASELINE
and two OUT_OF_ORDER rejections. Two extra ordinary rejections compared with B
come from the independent review correction: an already confirmed queued card
must still be invalidated when its quote continuity fails before claim. All
source events and refusal evidence remain. No useful/profitable move is declared
lost or captured from a bid uptick alone.

There are no successful multi-event merges in this particular filtered cohort:
card reduction here is caused by explicit eligibility decisions, not proven
batching benefit. Grouping itself is exercised in synthetic regressions. The
available sample does not justify a claim that five cards/day is attainable.

## Delay and window selection

Virtual sender:100ms polls,200ms synthetic API acknowledgement, one in-flight
request,3.1s pacing. UNKNOWN is retained until the synthetic ACK. Two historical
regional windows do not overlap even after92s, so separate paced senders have no
advantage over one paced sender for this particular sample.

| ACK delay | A min / median / max | B min / median / max | C min / median / max |
|---|---|---|---|
| High-level | 0.2 / 0.2 / 0.2s | 30.2 / 30.2 / 30.2s | 0.2 / 0.488 / 8.2s |
| Ordinary | 0.2 / 0.2 / 5.748s | 30.2 / 30.2 / 35.748s | 10.2 / 10.2 / 15.748s |

C windows10/15/30s each give23 cards, Amsterdam7, the same five delivered high-
level events, no duplicates or expiry loss. High-level delay is identical across
windows; ordinary medians are10.2/15.2/30.2s, maxima15.748/20.748/35.748s.
Select10s because the tested larger windows add delay without reducing messages.
Do not infer a general optimum from35 events.

These are modelled delays, not measured new Telegram performance. Actual old
historical source-event→ACK receipts: US min0.148/median0.167/max10.698s;
Amsterdam min0.148/median0.1825/max6.391s. The candidate has sent zero messages.
The historical chart screenshots do not establish bid/ask execution or profit.

## Queue, SQLite and resources

| Queue-only replay resource | A | B | C |
|---|---:|---:|---:|
| SQL write statements after constant reference setup | 210 | 219 | 217 |
| SQLite row changes including same reference setup | 1,263 | 1,272 | 1,270 |
| Peak unresolved entries | 2 | 4 | 3 |
| Max sampled open regional directory incl.WAL/SHM | 756,788 B | 789,700 B | 785,580 B |
| Sum of closed directories for two test regions | 376,872 B | 401,448 B | 409,640 B |

The extra durable wait/refusal/confirmation evidence uses space; fewer cards do
not automatically mean a smaller DB. C closed total is about8.7% above A here,
with no31x growth. These test directories contain labelled placeholder input
anchors for queue replay, not original full market archives. Values are not a
production storage estimate. Day-long CPU, traffic and database growth are
**NOT MEASURED**. The256MiB data limit and bounded WAL/log/history are unchanged.
All replay DBs pass integrity/FK checks and schema remains v1.

## USA transport

**ROOT_CAUSE_UNKNOWN** for the historical final1011 timeout. The finite three-
reconnect budget behaved as configured. A local socket-pair reproduction proves
receive backpressure can prevent reading PONG despite a responsive peer; it does
not identify the past culprit. A measured200-frame regression reproduces815
activity checks on old code; global subscription maintenance is now capped to
one scan/250ms. Retirement still runs before every sender tick.

A bounded existing-meta history keeps32 transitions, per-transition timing
snapshots, errors, reconnect outcomes, app/native PONG clocks, processing times
and frame maxima. No heartbeat/retry/frame/queue limit increase or auto restart.
Exact retained-contract intervals for110 contracts/220 sides, source SHA and the
601-message repeated replay are in the local evidence bundle. Missing earlier
GAPs and original batch-frame timing remain explicit limits.

## Robustness and independent review

The final regressions cover socket failure/exhaustion, no bid, GAP, restart,
MULTI-CROSS, independent YES/NO, queue full, Telegram rejection/UNKNOWN, local
midnight,90s expiry, exact duplicates, SQLite rollback, warning retention,
priority selection/shared pacing and two-phase code rollback to real accepted
main. Native connection factory/handshake and PONG timing are exercised through
a local socket pair, without an external server. Detector source and the core
quote/event functions match accepted main; schema and source-history SHA do not
change. LAMP algorithms/weather files are unchanged and shared-queue LAMP tests
remain green.

One independently authorized reviewer inspected code/tests/evidence, performed
in-memory reproductions and found two P2 issues: resolved pending work could send
between maintenance ticks, and settled ask-only work could survive an invalid
quote before send. Both failed in parent regressions before correction and were
independently rechecked after correction. One P3 subscription-count lag and its
predicate guard were also fixed. Reviewer reports no remaining findings in the
reviewed scope; no claim of live readiness. Full suite/CI are separately executed
by the author, not attributed to the reviewer.

## Acceptance scope

Final suite: **538 tests passed locally**, including the real local-socket
handshake/PONG check. Source and compile checks pass. Exact-candidate Linux CI
is checked on the published PR head before the handoff.

**CODE: PASS within tested scope. USA: ROOT_CAUSE_UNKNOWN.**
**VERDICT: READY FOR MERGE after green exact-candidate CI; not deployed.**

The current branch targets accepted main without merging other forecast/US
launcher experiments. Actual US launcher reconciliation and a finite fresh-data
Telegram-latency qualification require their separate review/permission. Next
step is acceptance of PR17, then the separately approved finite plan. No routine
or autonomous service is enabled by this result.

Provenance: `evidence/price-final-20261115/` contains COMPARISON_ABC.json,
SUMMARY_ABC.json, per-contract USA gaps, source hashes, old failing witnesses,
repeated replays, isolated ROLLBACK.json, independent review and final proof.
Earlier25-card intermediate results are retained as PRE_REVIEW artifacts and are
superseded by the corrected23-card candidate result.
