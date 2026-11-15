# USA transport review — PR 17

## Historical result: ROOT_CAUSE_UNKNOWN

The 15 November US finite observer stopped at 22:19:54.770 Kyiv with
`RECONNECT_BUDGET_EXHAUSTED`, after three reconnects. Its last error was
`ConnectionClosedError: sent 1011 (internal error) keepalive ping timeout; no close frame received`.
This proves a missing-pong timeout observed by the client, not who caused it.

The old result overwrote earlier error details, did not record message processing
or maintenance durations, native/app PONG clocks, or queue backlog. Only the last
512 ordinary diagnostics survive. Several earlier disconnect reasons are no
longer recoverable. No past network/source/server attribution is justified.

Read-only snapshot of the stopped runtime: 110 contracts, 220 sides, 44,273
samples; integrity `ok`, no FK errors. Contract/side-level intervals are saved in
`evidence/price-final-20261115/USA_CONTRACT_GAPS.json` and `.csv`. The calculation
uses retained full book/BBO confirmations, original source/receipt clocks, 90s
expiry, retained invalidations and gaps. It is not an exact reconstruction of
unpersisted/coalesced frames or rotated old gaps. 58 sides had a retained valid
full confirmation; 162 had none (81 contracts had neither). A one-sided/empty
market is not classified as a network outage from these counts.

Retained aggregate observation interruption: last valid stored quote 22:09:54.605,
recorded disconnect 22:09:55.560, next stored valid quote 22:09:59.777 — 5.172s
between valid stored observations. Final valid stored quote 22:19:53.552; no quote
after the final disconnect. Earlier saved review contains the unsent final Atlanta
YES70 pending record. Nothing was resent, restarted or modified remotely.

## Reproduced local load and minimal correction

The sync websocket library 17.1 has a bounded receive queue (`max_queue=16`). Its
receiver pauses socket reads above that watermark, including reading later PONG
frames. A bounded local socket-pair test reproduces 1011 with a responsive peer:
17 queued frames, the peer receives PING, but the consumer does not drain data.
Accelerated test-only heartbeat clocks do not change runtime settings. This
proves a possible local failure mechanism, **not** the historical US root cause.
The [official keepalive guide](https://websockets.readthedocs.io/en/stable/topics/keepalive.html)
also describes overload-induced latency; installed pinned source and the local
reproduction are the exact-version evidence.

A separate regression proves excessive per-frame global maintenance: 200 feed
messages on the old code made 815 market-activity checks; the candidate performs
fewer than 400. Shared activity scans, unsubscription and queue retirement now run
at most every 250ms. Every routed message still independently checks its own
market's date and resolution before processing; HTTP delivery retains its final
expiry check. No market cutoff, quote limit or detector threshold is relaxed.

## Bounded persistent evidence

`catalogue_transport_v1` in existing `meta` stores counters, a current snapshot
and the last 32 CONNECTING/CONNECTED/DISCONNECTED/RECONNECT_SCHEDULED/STOPPED
records. Error text is bounded to 240 characters. This survives ordinary
diagnostic rotation and restart. It is not an unlimited log.

Snapshots include maximum/total processing time, frame count/maximum bytes,
maximum maintenance duration, subscribed token count, last frame time, distinct
application PING/PONG times, native PONG time and native RTT. Native timestamps
are observed in the library's existing receiver thread; that thread updates only
a scalar clock and never writes SQLite. The existing main thread is the sole
writer. Periodic snapshots are written with existing 10s counter checkpoints and
on transport transitions; there is no per-frame diagnostic write.

The reconnect budget, 10/20s native heartbeat, 25s application heartbeat, 16-frame
queue, 262,144-byte frame bound and original session deadline are unchanged.
Exhaustion persists an explicit STOPPED outcome. No automatic restart is added.
Oversize/protocol refusals continue to stop without a retry. Native PONG and
application text PONG are tested separately; unobserved clocks remain null.

## Tests and real evidence

502 tests pass locally at this stage. Recovery coverage includes resubscription,
first quote after GAP as baseline, once-only flags, finite exhaustion/deadline,
protocol refusal, rollover, queue integrity, timing fields and durable bounded
history after 600 diagnostic rotations. The new activity-scan regression fails
on d7f9647 and passes on this correction.

Two offline runs of 601 original retained US per-input messages, with one explicit
injected disconnect and repeated inputs after restart, preserve identical event
IDs and counts, no duplicate thresholds, schema v1, SQLite integrity and FK
checks. Median processing was 0.76/1.17ms, maximum 2.16/8.81ms on this Mac;
closed test directory 1,478,676 bytes, sampled open peak 2,096,516 bytes. These are
short input-processing measurements, not sustained server throughput. Saved
payloads are per-input messages (largest 2,398 bytes here); the maximum original
live WebSocket batch frame and historical backlog are unknown.

The replay reconstructs a labelled metadata envelope and adapts only the US
registry in the test harness. The previously launched US region-capable package
is a separate pinned bundle; this review does not deploy or replace it with the
Europe catalogue. Source SHA is unchanged. No complete historical transport
replay can manufacture the missing PONG/gap evidence.

## Rollback and scope

No schema change, service, dependency, timeout/retry increase, weather change,
Telegram send or remote process control. Reverting this code restores the old
maintenance frequency/diagnostic limitation; saved history remains inert `meta`
data. Never restore an older database or reset threshold/delivery state.

The PRICE detector is byte-identical to accepted main (SHA256
`6b3e6ddc6df99618c0ab3edd8a70f3cf71c334b77dee11c4b15c8341b1786862`).
