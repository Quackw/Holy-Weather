# Stage 1

A single finite Python process connects four small components:

`discovery.select/discover → app.process → core.normalize/evolve → storage.Store`

Discovery verifies the exact native bucket, rules, station, local date, timezone,
condition and outcome-to-token order. Gamma receipt bytes and SHA are retained.
The stream subscribes only the selected contract's two assets. Top-of-book
confirmations in price_change are parsed; every changed quote is retained,
unchanged confirmations checkpoint every 15 seconds with explicit coalescing
counters. Raw full-depth churn is not archived. Catalogue notices count toward
input traffic but are not quotes. No CLOB REST market-status dependency or fallback is installed.

`normalize` emits a typed input; `evolve(previous, input, config)` is a pure state
transition. Store inserts raw evidence, sample, detected/suppressed reasons and
updated state in one SQLite transaction. Each event references its price inputs
and the applicable discovery receipt. A failure rolls back the complete change.
An exact repeated input has no new result. Each token has its own state.

## Boundaries

One POSIX process lock owns writes. Another writer is refused. SQLite uses FULL
durability, WAL, foreign keys, a bounded page cache and memory-only SQL temporary
storage. Unknown application/schema/rule identity is refused. A proven empty
owned bootstrap can finish initialization after interruption. There are no
schema migrations or restoration of an older working database.

The database page limit and pre-transaction directory guard reserve up to 8 MiB
inside the 256 MiB budget for WAL, logs and completion. WAL checkpoints at bounded
intervals; technical logs rotate at 512 KiB with two backups. No application
temporary files or automatic VACUUM are created. Exhaustion stops optional
capture and preserves committed inputs/results. External writers filling the
disk are not an application-enforced filesystem quota.

Explicit prune has a dry run. Detected/suppressed events retain 30 days;
unreferenced inputs retain 24 hours. Event evidence and latest snapshots are not
removed. Comparison windows reset before maintenance. SQLite free pages are
reused; physical file shrinking is not promised. Retention deliberately limits
full-tape replay: a pruned database keeps event evidence but isn't advertised as
an intact historical feed. Original external research artifacts are never edited.

State keeps at most 4096 changing ask anchors per token; reaching that boundary
is an explicit comparison reset. The strict 90-second price guard applies at
receipt; current snapshot freshness is recalculated at inspection time. A timed
trade cannot fill in a missing quote or missing depth size.

Connection loss records a gap and performs at most three reconnects with bounded
backoff. First quotes after each connection/process start establish baselines.
Persisted threshold gates/cooldown prevent restart spam. HTTP401/403/429 has no
automatic retry; protocol/invalid-input/storage failures have distinct outcomes.
Signal shutdown finishes current transactions and closes the connection. No
daemon, installer, AI agent, scheduler or administrative mechanism exists.

## Future boundaries

The existing pure input/state functions and versioned SQLite event schema are
the integration boundary. A future notifier can read DETECTED events; a research
module can also read SUPPRESSED inputs. Future weather needs separately typed
source/receipt clocks and its own qualification. No placeholder services,
factories or implementations of those later modules are introduced now.
