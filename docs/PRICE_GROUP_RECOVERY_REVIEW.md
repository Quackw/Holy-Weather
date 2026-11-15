# PRICE stage 1 completion — grouped subscriptions and recovery

Scope: candidate PR 14, no installation, live-process change, Telegram send or
merge. The existing finite server session keeps its exact old code and stop policy.

## Reproduced gaps and bounded changes

The catalogue previously ignored Config.max_reconnects and exited after a network
gap. It now honours the existing 0–5 budget (Config default 3), with 1/2/4/8-second
bounded waits inside the original monotonic deadline. No service/supervisor is
introduced. A gap clears readiness and records a diagnostic; the next connection
resubscribes active exact tokens in groups of at most 16, at most one group/second.
Only a new complete native quote can establish a baseline. Durable fired levels,
events and delivery states are retained. Setting max_reconnects=0 still stops on
the first outage. Protocol/handshake refusals and 1008/1009 close codes do not retry.
Storage failures propagate; they are not treated as transient network failures.

Closed markets are explicitly unsubscribed in groups; another city's observation
continues. Date eligibility is checked again after connection setup and before
later subscription groups, preventing an old-day subscription if midnight occurs
during connect. No next-day discovery or market-rule change is introduced.

The full frame limit 262144 B, one writer/queue, PRICE rules, quote freshness
90 seconds, directory budget 256 MiB and prior clock policy stay unchanged.

## Isolated verification

- 121 markets / 242 real-side-shaped synthetic tokens: every token receives its
  baseline and an upward crossing while the next groups are still subscribing.
  Exactly 242 PRICE events and 242 queue entries, with unique token coverage.
- Repeat with a disconnect after the second subscription group: all 242 tokens
  resubscribe, already fired levels do not repeat, totals remain 242/242.
  First connection subscribed 32 tokens; second 242. Database remains below 8 MiB.
- Separate reconnect sequence: pre-gap 50% alert survives; post-gap 81% baseline
  does not invent 55/60/70/80 crossings. Later real 49→61% yields one MULTI-CROSS
  for 55/60 only. Durable queue has exactly two entries.
- Budgets 0 and 2 produce exactly 1 and 3 connection attempts; timeout during
  backoff opens no new connection. Protocol refusal never retries.
- Resolution unsubscribes only the affected two tokens; another market alerts.
  Crossing midnight during connect sends no subscription for the ended day.
- Existing restart/delivery tests verify SENT never repeats, UNKNOWN is not retried,
  sender failure does not block collection, original event+queue transaction and
  SQLite rollback, once-only sides and the 1024-entry queue bound.

These use injected transport faults, not a claim of live outage recovery.

## Saved native data

The original Linux wire captures (London 301 frames, Paris 216, Amsterdam 1336)
were also run through the full catalogue loop, with and without one deliberately
injected midstream outage. The original frames and receipt clocks were preserved;
heartbeat replies and transport fault are explicitly synthetic controls because
those controls are not in the retained market wire files. Backoff elapsed-time
bounds are covered by separate synthetic tests, not this replay.

All six runs completed: zero PRICE events in these particular windows, integrity
ok and zero foreign-key errors. Original database and gzip SHA unchanged. Before-
close data usage was 0.65–0.91 MiB; replay CPU roughly 0.03–0.27 seconds per run
on Mac. These are replay measurements, not live daily traffic/RAM/CPU guarantees.
Evidence: evidence/price-stage1-recovery-20261115/RESULT.json (outside Git).

Earlier real 121-bucket Linux observation and real Telegram cards 362/363 remain
separate supporting evidence. No new live test or send was needed for these
finite-loop changes. Stage 1 clock/order result is in PRICE_CLOCK_ORDER_REVIEW.md.

## Acceptance boundary

READY FOR REVIEW for the stage-1 finite PRICE implementation. PR 12 is accepted; PR 14 is reconciled with the cleaned main.
The retargeted diff excludes deferred weather experiments. Do not merge PR 14
without separate acceptance. This is not permanent-operation readiness or full city
readiness. In an actual disconnected interval unknown market movement cannot be
reconstructed. No guarantee of zero missed signals during a gap is made; baseline
recovery intentionally suppresses inferred crossings across that interval.

A saturated queue preserves events but needs the later per-event delivery audit;
no automatic retention/rollover or unlimited retry is added. The current standalone
server wrapper is not promoted to a production mechanism. Rollback is returning
the old code while keeping the same database; no schema or history replacement.
Old code once again exits at a gap. No source artifacts are removed.
