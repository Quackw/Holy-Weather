# Finite PRICE recovery — opt-in proposal

This PR depends on #25 and #27. It installs no service, starts no observer and
does not attach to or stop an existing Europe/US observation. Acceptance of code
does not authorize a server transfer, key transfer, send or deployment.

The controller is the parent of one PRICE catalogue child. There is one SQLite
writer; the parent reads only. Each region has its own data and owner-only
control directory. No new database, dependency, weather algorithm, trade or
administrative account is introduced. PRICE thresholds, Decimal midpoint,
once-only/REARM=false, freshness, source clocks and queue policy are unchanged.

## Recovery policy

- Known exhausted transient WebSocket recovery may restart the child after
  backoff: 30, 60, 120, then at most 300 seconds. The existing inner reconnect
  limit is unchanged; the additional process-restart budget is at most three
  per rolling hour and six per rolling 24 hours. Counters and the next permitted
  time are written before launch and survive a controller re-invocation.
- Unexpected SIGKILL/SIGSEGV/SIGABRT/SIGBUS during a reported observation stage
  can recover only after the child has exited and read-only SQLite/WAL checks
  pass. A crash during startup or archive/adoption is not blindly retried.
- SQLite/schema/rule/code drift, storage pressure, refused access, malformed
  results, unknown exit, incomplete archive or uncertain launch fail closed.
  No attempt counter, archive intent or source state is reset to obtain a pass.
- Completion, local-day end, an operator stop and the absolute session deadline
  do not trigger recovery. No new session/date is silently authorized.
- The main loop publishes bounded progress even when no market frames arrive.
  Quiet prices or missing bid/ask alone never mean a process is hung. The parent
  allows 60 seconds without observation-loop progress, 180 for startup/discovery
  and 300 for explicitly authorized archive work. It sends one SIGTERM to its
  own child and waits up to 30 seconds. If the child remains alive, the parent
  records `UNRESPONSIVE_NO_KILL`, never sends SIGKILL and never starts a second
  writer. Only a proven clean `SIGNAL_STOP` after that request may recover.

Every new observer connection remains a baseline. Fired levels and original
evidence stay intact. Pending old PRICE work expires with an explicit reason.
SENT and UNKNOWN are never re-claimed. An uncertain previous HTTP attempt keeps
the sender disabled across child/controller recovery while price collection
continues. This does not claim that a small bid increase proves demand or edge.

### PR #28 review corrections

Planned phase shutdown first forbids new sends, then waits at most six seconds
for the already claimed HTTP request (the request timeout stays five seconds).
The SQLite writer records any result received during that wait before Store
closes. Repeated close calls do not wait again or reopen result processing.
If the bounded wait expires, the claim remains UNKNOWN, including after a late
ACK; it is never reset to PENDING or inferred to be SENT. The six-second drain
fits the controller's 30-second termination grace and 60-second STOPPING limit.

Observer results, worker progress/results, controller state and CLI output expose
`telegram_enabled` and a safe `telegram_disabled_reason`. A previous uncertain
send is reported as `PREVIOUS_SEND_UNCONFIRMED`; a shutdown timeout is
`SEND_RESULT_UNKNOWN_AT_CLOSE`. Collection can remain RUNNING while Telegram is
explicitly disabled. These fields never contain a token or private HTTP payload.

Cancellation/deadline is checked again after preflight and immediately before
spawn. A cancelled launch intent is finalized without an active PID or a charged
restart. A process result is trusted as a normal completion only with exit code
zero: a nonzero exit is classified using the existing crash signal/stage policy
and recovery budget, even when a result file already exists. In particular, a
STOPPING-stage crash cannot become a free ordinary phase or a clean stall stop.

`tests/test_pr28_review.py` reproduces the three original defects before the
correction, using real Store/claim/Sender and delayed synthetic responses through
the normal observer/worker close path. It also covers cancellation inside
preflight/after intent, progress reporting while collection continues, existing
UNKNOWN protection and result/return-code disagreement. No live send is used.

## Data, identities and bounds

The parent checks the ownership marker, available writer lock, matching rule
hash, SQLite v1, integrity, foreign keys, committed WAL and the archived exact
duplicate index before each launch. It never opens a live DB as `immutable`,
copies an active file, performs VACUUM, restores an old snapshot or writes SQLite.
The child records its control binding in existing `meta`; no migration occurs.
Another region, path, build, Python/dependency version or control identity cannot
silently reuse that binding. Code is rechecked before every child launch.

The control directory is owner-only, limited to 1 MiB and 160 total attempts;
progress overwrites one atomic file per attempt rather than appending forever.
No cleanup deletes its past receipts. An unresolved previous launch is not
adopted on controller startup, even if someone supplies an old PID. A stale or
partially written control file causes a refusal instead of another launch.

The 90-second quote limit, 100-ms maximum catalogue source lead, 256-MiB data
limit and queue bound stay unchanged. Optional history maintenance is **off**
when `archive_dir` is absent. Selecting it requires separate exact runtime
authority: it reuses #27's full archive/restore/adopt checks between closed
phases, with the previously qualified 128-MiB trigger, 160-MiB candidate ceiling,
4-GiB/40-part archive bounds and at least 2 GiB filesystem free. Only the two
new verified staging copies may be removed after adoption. Existing archives
and backups remain. Any incomplete part blocks another automatic attempt.

## Finite invocation and credentials

`python -m holy_weather.recovery run --plan /absolute/approved-plan.json
--control-dir /absolute/owned-control` requires an explicit frozen region,
`session_day`, absolute deadline and data path. No environment override chooses
another database. The maximum child phase is 1,200 seconds. The plan cannot run
past the latest supported city's next local midnight. Original station/rule/
token validation is repeated by catalogue discovery. A fixed old-day session
does not consume tomorrow's discovery attempts while later timezones remain open.

### Inherited end-of-session discovery refusals

Older finite launchers could persist `SESSION_LOCAL_DATE_ENDED` as the next
local day's `BLOCKED` discovery even though they never requested that market.
On that actual local day, discovery admits only the exact known legacy record:
`BLOCKED`, attempt 1, one `SESSION_LOCAL_DATE_ENDED` skip, a same-day non-future
timestamp and no HTTP receipt, extra fields or refusal reasons. An ended finite
session still cannot admit the following day.

Before requesting Gamma, the sole SQLite writer atomically retains the original
raw value at `price_catalogue_day_refusal_v1:<city>:<day>` and records the ordinary
`IN_PROGRESS` attempt 2. That historical attempt remains charged against the
existing three-attempt budget. An existing audit entry or unknown `IN_PROGRESS`
attempt cannot be replayed; real access/rule failures remain blocked. All normal
station, date, timezone and token checks still apply. This does not mark a city
ready until its current contracts and fresh WebSocket quotes are verified.

SQLite stays at v1. No PRICE event, fired threshold, delivery receipt or input is
removed by this compatibility path. Exact accepted-main rollback can open the
current database after new records without restoring an old snapshot; its reader
ignores the retained refusal audit. Deployment must separately respect the
controller's code/plan binding and writer lock; do not replace active source or
reset a controller to install this change.

Telegram remains OFF unless explicitly selected in that approved plan. If ON,
the token is read from stdin and passed to the child through an anonymous pipe,
never argv, environment, a control file or logs. Core dumps are disabled by the
entry point. Exception payloads/URLs are not logged; known uppercase refusal
codes and exception classes remain visible. No fallback account/key is used.

This proposal does not recover a stopped controller, survive a host reboot,
persist a bot key, provide a systemd unit or promise permanent operation. Those
require separately reviewed host supervision/credential storage. It cannot add
itself around the currently running unmanaged observer. A future exact install
plan must provide a confirmed handoff without a second writer or forced kill.

## Qualification and rollback

Tests cover persistent backoff/hourly/daily budgets, no restart after normal or
fatal outcomes, quiet-loop progress, graceful stall versus an unresponsive child,
writer exclusion, permissions/identities, UNKNOWN sender isolation, pending-work
expiry, transactional failure and local-day boundaries.

A real isolated test subprocess dies with an uncommitted SQLite transaction.
The controller reads committed WAL, starts a new isolated child, preserves the
old SENT/UNKNOWN/fired levels, treats its first high quote as a baseline, and
records only the subsequent genuinely new threshold. No network or Telegram
call is made. The test then opens the **current** database with exact base #27
code and checks new records, fired state, terminal receipts and SQLite integrity.
Synthetic crash/backoff settings in this fixture are not live-server evidence.

Rollback removes the controller only after confirming its child exited, and
returns to the finite #27 launcher on the **current** DB. Keep #27's archive
reader/files if checkpoints were adopted. Never restore an old SQLite snapshot
over new input, threshold or delivery records. No automatic rollback script or
active-server operation is part of this source PR.

Linux CI must pass the complete suite on Python 3.11 and 3.13.5. Live failure,
stalls, overhead and real delivery under this controller remain NOT TESTED until
a separately approved isolated qualification. Green CI does not prove overnight
or production availability.
