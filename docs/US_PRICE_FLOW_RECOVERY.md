# US receive work and PRICE baseline recovery

Scope: US catalogue only. No installation, service action, timeout change or
European rollout is authorized by this change.

## Observed failure and limits of the evidence

A retained read-only snapshot of installed US `34b19c6` on 19 November 2026,
19:47 Kyiv, showed fresh two-sided Los Angeles depth with no PRICE-ready token.
Other snapshots had ready Los Angeles tokens: this was intermittent, rather than
proof of permanently empty books. Depth freshness and confirmed PRICE freshness
are different checks. An unchanged `price_change` cannot replace a confirmed
book/BBO, and a subscription could previously remain without a new PRICE baseline.

The saved 19:35:07 and 19:44:47 Kyiv disconnects reported native 1011 keepalive
timeouts. Native RTT reached 16.3/18.0 seconds, with paused bounded receive queues
and substantial frame-processing time. These are connection aggregates, not a
precise timeline of the cause of each disconnect. Source receipt delays were also
observed. They support backpressure as a contributor, but do not exclude provider
or network delays. No claim that every live 1011 has been fixed follows from CI.

## Changes

* Once per second, the US observer can request a real WebSocket book for a
  currently subscribed token whose PRICE baseline is absent or older than the
  original 90-second limit, while its observed two-sided top is still fresh.
  Initial grace is five seconds; batches contain at most 16 tokens. A token gets
  at most three unanswered requests per connection, at least 20 seconds apart.
  A successfully received confirmed baseline resets its failure count.
* The token is invalidated durably before unsubscribe/subscribe. Fired thresholds
  and source high-water survive; waiting bid confirmations are refused across the
  gap. The first accepted book/BBO is a baseline, never a reconstructed crossing.
  Missing, stale, closed or invalid books remain unusable. Neither depth nor
  receive time is promoted into a confirmed price.
* US directory-budget checks read each entry's metadata once, preserving the
  exact size check on every transaction and the refusals for non-regular entries,
  links and excess files. No disk budget is increased.
* The US research calculation can reuse its privately decoded state and a bounded
  cache of 4096 validated immutable Decimal prices. Public/default calculation
  retains copy semantics. PRICE midpoint/threshold calculations are unchanged.

The baseline helper adds the `price_baseline_resubscriptions` transport counter.
It does not create events or a new durable schema. Europe keeps its existing
directory checks and does not instantiate the helper. US LAMP/METAR are unchanged.

## Reproduction and compatibility

`tests/test_us_price_flow.py` runs the real finite observer, router and SQLite
with controlled clocks and original-format synthetic frames. A Los Angeles BBO
is followed by unchanged fresh depth beyond 90 seconds. Installed `34b19c6`
never requests a new baseline during the 110-second case; the candidate does,
and the returned book creates zero false crossing events. Other cases cover all
ten US profiles, batching, failures, expiry, the opposite side and source order.

`tests/test_us_receive_guard.py` uses real native WebSocket keepalive and the
unchanged 10/20-second intervals, queue bound 16 and a burst of 920 frames. An
explicit equal per-metadata-read delay constructs filesystem pressure: the old
guard times out with 1011, the new guard drains the burst. Prefix state, events
and queue are compared exactly. The injected delay is synthetic; live filesystem
latency was not measured. This is a conditional reproduction, not attribution of
all historical disconnects to filesystem calls.

`tests/test_us_price_rollback.py` switches old code → candidate → old code on the
same isolated database after new writes, preserving PRICE, LAMP, fired levels,
input references and SENT/UNKNOWN/PENDING. It tests both parent main `667f6e0`
and installed US `34b19c6`. SQLite remains schema v1, with integrity and foreign
keys checked. The installed-US integration also exercises the new recovery and
all local LAMP timing regressions while retaining installed PR #40/#41 functions.

A private replay of 5000 retained US inputs compares unchanged original clocks,
PRICE events, fired state, queue and input evidence. It is a retained subset,
not the full original wire stream. Both versions produced the same 11 PRICE
events and the same semantic hash, with identical input/sample/event tables.
Measurements on this short controlled replay do not predict daily capacity or
guarantee live recovery. Real captures and databases remain outside Git.

Netflix provenance was rechecked against retained `price.mjs`, `monitor.mjs` and
`registry.mjs`: actual token book/BBO only, Decimal midpoint, upward
50/55/60/70/80%, once-only, REARM=false. Subscription operations are documented in
[Polymarket's official WebSocket guide](https://github.com/Polymarket/agent-skills/blob/main/websocket.md).

Rollback changes code on current data; it never restores an old SQLite over new
records. Disabling the new helper restores the prior recovery limitation. An
eventual US package must retain installed #40/#41; plain main is not a replacement
for that installed stack. Before installation: exact package approval, Linux
rehearsal, verified current-data backups, one controlled switch and live checks.
Live disappearance of 1011 and renewed Los Angeles readiness remain separate
qualification. Working services are not changed by this PR.
