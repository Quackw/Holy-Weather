# US PRICE recovery while LAMP is enabled

The ten-city US catalogue shares its existing SQLite writer and Telegram queue
with LAMP. This change does not alter PRICE rules, reconnect limits, supervisor
budgets, source clocks, service settings or database schema v1. It does not
update the running Europe installation.

## Reproduced defects and bounded repair

On accepted `56f14b0`, a continuous worker that exhausted its WebSocket reconnect
budget entered forecast-only collection until its 1,200-second window ended.
The supervisor could not begin its existing PRICE recovery in that interval.
A controlled socket failure at second one reproduces this through the real
worker ownership and database-resume path. An additional observation bug left
the last published PRICE-ready snapshot visible during the forecast-only wait.

A continuous US worker now drains only the NOAA request already in flight, for
at most 16 seconds, then returns the original recoverable failure to the
supervisor. The drain bound is the existing five-second socket timeout plus the
ten-second scan guard and one second of allowance; neither HTTP nor WebSocket
timeouts are increased. With no pending NOAA work, there is no drain wait.
The existing reconnect budget, supervisor backoff and recovery limits
remain applicable. This is not a promise that usable prices recover in 16 seconds.

No new HTTP request starts during the drain. A request still unfinished at its
end is recorded as `INTERRUPTED / PRICE_RECOVERY_HANDOFF`. Its paid attempt,
retry schedule and last valid forecast survive. A completed response keeps its
original receipt clock; owner-thread availability is recorded separately.
Finite forecast experiments retain their bounded forecast-only behavior.
Access/protocol refusals do not gain another retry path.

Immediately after a market gap, the published snapshot becomes disconnected
with zero currently ready prices. The next connection still needs confirmed
baselines. Neither PRICE crossings nor waiting bid confirmations bridge gaps.

## Feed processing

The idle LAMP collector no longer scans every contract on each delivery tick.
It revalidates contracts when processing a response or starting a scheduled
request. A private state freshly decoded by the SQLite owner does not need a
second recursive copy for legacy research processing. Baseline refusals retain
the old copy path; public `evolve` callers still receive copy isolation.
A repeated Decimal configuration conversion is hoisted out of the point search.
No research/PRICE conditions or stored evidence change.

This removes measured consumer overhead, one source of WebSocket backpressure.
It does not prove that every historical native keepalive timeout had that cause,
or that a remote source can no longer disconnect. The existing offline native
PING/PONG, backpressure, reconnect and subscription tests remain acceptance gates.
See [websockets keepalive](https://websockets.readthedocs.io/en/latest/topics/keepalive.html)
and [buffer behavior](https://websockets.readthedocs.io/en/latest/topics/memory.html).

## Verification and compatible rollback

Offline regressions cover the real owned-worker return, immediate gap reporting,
idle polling, successful and unfinished in-flight NOAA requests, retry debt,
state-copy equivalence and accepted-code rollback after new records. The rollback
runs accepted source → candidate → accepted source in separate interpreters on
one isolated schema-v1 database. PRICE/LAMP events, input references, fired levels
and SENT/UNKNOWN/PENDING remain; the next connection starts a new baseline.
SQLite integrity and foreign keys must pass at each phase.

A retained 5,000-message US payload subset is replayed with original source and
receipt clocks. It is a processing comparison, not the complete original wire
stream or new live alerts. All PRICE events, fired states and delivery rows must
match. CPU measurements are local observations, not a production capacity claim.
Real inputs and profiling reports stay outside Git.

A server update requires its own exact approval and a verified full backup,
including archive/index dependencies and an independent encrypted copy. Switch
only the US code binding through the existing closed-writer procedure. Preserve
the current US LAMP plan, history, recovery accounting and queue. A compatible
rollback uses accepted `56f14b0` against current data through the same verified
rebinding procedure; never restore an old snapshot over newer records.
No installation, merge, new send or runtime recovery is authorized by this PR.
