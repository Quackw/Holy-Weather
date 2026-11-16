# PRICE receive-loop recovery — 16 November 2026

## Evidence and attribution

Read-only inspection of the stopped US observer found four client-native 1011
keepalive timeouts and finite reconnect-budget exhaustion. Last observed native
RTT before the four disconnects was 14.6 / 17.4 / 16.9 / 14.2 seconds. Retained
quote source ages in the preceding minute reached 20–23 seconds. The source
SQLite was intact: 37 PRICE events, 40 fired levels, 25 SENT deliveries,
11 REJECTED receipts, 110 contracts; no outstanding PENDING or UNKNOWN.

This proves backlog symptoms and client timeout, not the exclusive historical
cause. There is no historical socket trace or receive-queue occupancy record.
A remote outage or other processing cost cannot be excluded.

A full-loop socket-pair regression reproduces a local cause with websockets
17.4: a responsive peer sends 75 frames/second while a synthetic delivery scan
costs 40 ms. Per-frame queue scans stop the consumer draining its bounded queue;
the native heartbeat times out. With the correction the same test completes.
Only test heartbeat clocks are accelerated. Production connection arguments are
asserted unchanged in the test. This is a controlled failure test, not live load.

## Minimal correction

The catalogue services delivery housekeeping at most once per 50 ms after the
previous scan completes, instead of once per incoming market frame. The receive
wait also wakes for that deadline. Every received market frame still goes through
the existing router, causal clocks, atomic PRICE/event/evidence/queue transaction
and lifecycle checks. Bid-wait settlement on new samples remains unchanged.

Immediately before each sender tick the code rechecks date/resolution eligibility;
a closed pending market cannot slip through between global activity scans.
Priority 70/80 work has no ordinary ten-second batch delay added. The 50 ms target
is a scheduler cadence, not a latency guarantee: slow SQLite, processing, OS
scheduling and HTTP still matter. Quiet connections may perform 20 short delivery
checks per second. No new thread, queue, service or schema is introduced.

Three scalar delivery counters (ticks, total/max scan time) join the existing
bounded transport snapshot. Existing 10-second checkpoints persist them; there is
no new per-frame diagnostic write.

Unchanged: Decimal midpoint, 50/55/60/70/80 thresholds, once-only/REARM=false,
YES/NO independence, GAP/restart baseline, SENT/UNKNOWN rules, 90-second quote
age, 100 ms source lead, 256 MiB budget, queue bound, receive queue 16, maximum
frame 262144 bytes, native 10/20-second heartbeat, application 25-second limit,
three reconnects and the original finite session deadline. PR #25's separate
notification change is not included.

## Checks and limits

Two new regressions fail before this fix: 1,000 per-frame delivery scans in one
virtual second, and the responsive-peer full-loop timeout. Candidate results are
at most 21 scans and successful completion. Additional tests cover priority
readiness without ordinary grouping and accepted → candidate → accepted code
rollback after new records in both regional configurations. SQLite schema v1,
new events, fired levels and SENT/UNKNOWN/PENDING receipts survive that rollback.
Existing transport, reconnect, resolution, date rollover and queue tests remain.

An offline replay of 3,000 retained original US input payloads uses original
source/receipt clocks, fake socket and fake ACK. PRICE event/evidence/state SHA
matches across accepted and candidate code; both produce five PRICE events and
the same delivery status counts. Delivery scans fall from 3,009 to 559. Retained
per-input payloads cannot reconstruct missing/coalesced wire frames, so this does
not prove sustained live throughput or actual Telegram delivery. Wall time is
machine/load dependent; no guaranteed speedup is claimed.

Real SQLite/streams and reports stay in ignored `evidence/us-websocket-recovery-20261116/`.
Offline replay/source SHA: `4bafeea862acc389b9d4ef80277d89ed420bf13ff744a352d9fffe20484a6e72`.
No observer was restarted, no Telegram was sent and Europe was not changed.

## Install and rollback boundary

Deployment requires one exact package approval. Use a separate US-only code
folder; do not replace shared code used by Europe. Require the original US writer
to be absent, an exclusive data lock, unchanged source SHA, integrity/FK checks,
space and original deadline. Make a consistent SQLite Backup API copy before
resuming on the existing US data. Revalidate today's markets, establish a new
baseline and send only fresh new events, preserving all fired/delivery history.

Code rollback uses accepted code with the **current** database. Never restore an
old SQLite over new records, reset fired levels, resend terminal receipts or
silently extend the exhausted observation. An unknown launch outcome stops
control actions; it does not authorize a second launch. The old code retains the
performance defect. No automatic rollback/restart loop or permanent supervisor
is added; renewed transport exhaustion or storage pressure still stops safely.
