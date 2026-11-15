# Stage 1 — bounded clock and late-depth review

15 November 2026. Candidate in PR 14 only; no active observer, Telegram send,
production setting or clock configuration changed. PR remains unmerged.

## Evidence and clock conclusion

| Saved source | Frames | Quotes | Source-minus-receipt range | Median | Reverse source order |
| --- | ---: | ---: | --- | ---: | ---: |
| Linux London | 301 | 564 | -245…-16 ms | -18 ms | 10 |
| Linux Paris | 216 | 386 | -2245…-15 ms | -17 ms | 0 |
| Linux Amsterdam | 1336 | 2336 | -984…-16 ms | -20 ms | 57 |
| Mac retained catalogue | 40590 | 73363 | -423266…+54 ms | +48 ms | 915 |

Linux evidence is the closed original three-city wire capture and SQLite from
14:17–14:20 UTC. Its prior NTP synchronization report is supporting context,
not proof about the exchange's clock. Mac evidence is retained native messages,
not every omitted/coalesced wire confirmation. Replays preserve receipt order;
source timestamps are neither sorted nor rewritten. No derived invalid-input
gaps are injected: the actual message handling reproduces those resets itself.
These finite recordings do not prove absence of loss across the entire session.

100 ms is a bounded experiment policy, not a measured required server offset.
All Linux replays give identical events at 0/100 ms. Mac 54/100 ms give identical
PRICE events; 50 ms still rejects part of the measured 51–54 ms population.
Do not choose a new universal threshold from one finite maximum. Existing 0 ms
single-market default, explicit 100 ms catalogue cap, 90-second source AND receipt
freshness, immutable timestamps and queue expiry remain unchanged. Clock reversal,
100/101 ms and 90000/90001 ms boundary tests remain passing.

## Reproduced defect and minimal fix

A fresh BBO is followed by an older `price_change` carrying exactly the same
complete bid/ask, typically 1–2 ms behind. Previously this invalidated the newer
confirmed quote, resetting PRICE. The next genuine crossing became a baseline.
A regression fails on the previous code and passes after the change.

Ignore its effect on state only when it is a depth message, the connection and
side baseline are confirmed, both prices exactly match, receipt time has not
reversed, and both the old and newer quotes pass the clock/freshness policy.
Retain the original message in `inputs` and count `LATE_UNCHANGED_DEPTH`. Do not
replace the current quote, advance high-water, refresh receipt/source time, change
fired thresholds, or create an event from the late message. Repeated identical
ignored bodies share their retained input. No per-packet diagnostic rows added.
Changed prices, incomplete quotes, stale data, reversal, GAP or restart still use
the original conservative reset. No general acceptance of out-of-order prices.

Only app.py and storage.py change at runtime (a conditional guard and optional
state invalidation for evidence retention). No schema, dependency, limit, model,
weather or Telegram formatting change.

## Replay comparison

| Source / tolerance | PRICE before → after | Reverse-order resets before → after | Closed SQLite before → after |
| --- | --- | --- | --- |
| Linux London / 0 and 100 ms | 0 → 0 | 10 → 0 | 122880 → 114688 B |
| Linux Paris / 0 and 100 ms | 0 → 0 | 0 → 0 | 102400 → 102400 B |
| Linux Amsterdam / 0 and 100 ms | 0 → 0 | 57 → 0 | 376832 → 331776 B |
| Mac / 0 ms | 0 → 0 | 29 → 0 | 56270848 → 56270848 B |
| Mac / 50 ms | 0 → 1 | 547 → 5 | 25788416 → 25468928 B |
| Mac / 54 and 100 ms | 1 → 2 | 915 → 8 | 7479296 → 7176192 B |

Recovered retained-stream example: Amsterdam, 15 November, 10°C NO, midpoint
59.55% → 60.20%, upward 60% crossing. Receipt clocks 1794757502965 →
1794757502967; native source clocks both 1794757503014. The intervening depth
confirmation was source 1794757503013, with the same 0.508 bid / 0.683 ask as the
newer BBO. Following BBO bid 0.521 / ask 0.683 gives midpoint 0.602. Equal source
milliseconds with distinct receipts were already permitted; this policy is not
changed. The original Amsterdam 9°C NO 50% event remains. No historical send.
This proves one lost detector crossing on retained evidence, not lost profit or
a guaranteed executable price. Linux windows had no newly recovered crossings.

## Verification and rollback limits

All replay databases have integrity `ok`, zero foreign-key errors; source database
SHA remained unchanged. Repeated 100 ms replays match events, counters and sample
counts for all four sources. Actual IDs can differ between before/after replay
stores because retained input positions differ; original historical events are
never rewritten. New→old→new opening on a copy preserves every logical row and
schema 1. This is code/data compatibility, not a live deployment or crash test.
Returning old code restores the conservative reset defect, not lost history.

Regressions cover a real-shaped late-depth sequence, equal-source-time crossing,
changed/missing/stale data, receipt reversal, GAP, duplicate retained input,
SQLite write rollback, unchanged high-water, and restart baseline. Existing
clock, once-only, YES/NO, MULTI-CROSS, batching and queue tests remain mandatory.
Raw data and repeatable scripts are outside Git in evidence/price-stage1-20261115/.
No private raw market capture or server credential is included in this PR.

Verdict: targeted defect reproduced and fixed; clock policy kept bounded rather
than weakened. This does not close all PRICE stage 1 or permanent readiness:
full grouped-subscription/recovery coverage and later lifecycle/queue accounting
acceptance remain in PRICE_ACCEPTANCE_ORDER.md. No new live run was needed for
this saved-stream fix. Stop for review; do not install into the running session.
