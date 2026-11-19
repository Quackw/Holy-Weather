# Prototype: wait for the US network recovery quota

This is a local prototype, not installation authority. Main is `667f6e0`; PR #48
is separately open at `c23f2ed`. The examined installed US source is `34b19c6`,
including #40/#41 and accepted #47. This change does not include or replace #48.
No Netcup access, real Telegram send, merge or permanent service is needed for
its offline qualification.

## Failing boundary

In installed `34b19c6`, `lifecycle.charge()` sets `RESTART_BUDGET_EXHAUSTED` and
`retry=false` when three hourly or six rolling-day charges already exist.
`recovery.supervise()` then breaks its loop; `service.run()` returns fatal exit
78. `lifecycle.reconcile()` rejects the stored terminal status on later entries,
even after the time window has released capacity. A normal completed worker does
not refund recovery debt. This is independent of PRICE crossing rules.

## Proposed behavior

Only a continuous US plan, with a closed protocol-v1 child and a known recoverable
network result (`RECONNECT_BUDGET_EXHAUSTED` or `NETWORK_GAP_NO_RETRY`), can wait.
The child must have returned 0, no error/reason, a finished timestamp and no active
launch. Other regions, finite runs, refused access, unknown outcomes and crashes
keep the existing policy.

`WAITING_NETWORK_QUOTA` retains the original history and stores an unpaid
`network_quota_wait`: source nonce, network reason, wait start, earliest quota
availability and estimated earliest launch after the ordinary backoff. The slot
calculation handles BOTH rolling windows, including more retained entries than
either current cap. It neither deletes nor rewrites old timestamps.

The actual controller loop continues feeding its watchdog and reports
`collection=PAUSED`, the retry time and the network reason. Waiting adds no helper
thread, source requests, SQLite writer or per-second durable records. The wait
honors the existing stop/deadline and clock-reversal checks.

On wake, code identity, current SQLite/WAL, writer ownership and both quota windows
are checked again. Only then does the existing `charge()` append one timestamp.
The original backoff follows. The paid flag and wait marker are consumed in the
same durable STARTING intent before spawn, preserving the accepted launch-boundary
accounting. One inherited controller lock still owns one observer/writer.

An interrupted controller with no active child can re-enter this particular
known-network wait without spending a second credit. An already paid BACKOFF
retains its original deadline and credit. Unknown wait provenance fails closed.
Manual-stop latches, integrity/version failures, active owners and unresolved
launches do not become network waits. SENT/UNKNOWN are not reset or re-sent;
the first post-GAP book/BBO remains a PRICE baseline.

## Offline evidence

`tests/network_quota_probe.py` and `tests/test_network_quota_probe.py` execute the
same case with exact installed source and the scoped prototype. On Linux they
use actual kernel process identities, inherited locks, a real child, Unix watchdog
datagrams and SQLite. On Mac, the explicitly labeled identity fallback is a model,
not Linux evidence. Time is managed; no real multi-hour wait is claimed.

Before: the quota and backoff have elapsed, but there are zero replacement
launches and no new source data. After: zero launches before admission/backoff,
one launch afterward, one writer PID, a refused second writer, a new confirmed
PRICE baseline and subsequent fresh 80% crossing. A new original-format synthetic
NOAA collective passes the actual streaming loader/decoder and updates the saved
station forecast. The ten blocks and their issue are test data, not real NOAA.

The plan enables Telegram with a conspicuously dummy credential and a test-only
transport. A retained UNKNOWN correctly disables sending while collection
continues. Two SENT receipts, one UNKNOWN, all three old PRICE/LAMP events and
fired levels remain intact; explicit claims of terminal receipts are refused.
No real Telegram HTTP is possible. The probe stops itself after the one proven
worker; this is not uninterrupted live-service qualification.

Two additional subprocess cases exit the controller with test code 91 while
waiting or after the durable charge, then enter from a new process. They verify
the unchanged deadline/history, exactly one added charge/launch and actual source
processing after re-entry. Unit cases exercise both windows, manual stop,
integrity/code drift before and after waiting, occupied locks and unknown state.

The installed-source adapter changes only lifecycle/service and `supervise()`;
its original worker keeps #40's `price_recovery` flag. No PRICE/LAMP algorithm,
queue, schema-v1, freshness, transport timeout or numerical recovery cap changes.

## Region already stopped on old code

The prototype deliberately does NOT auto-unlatch a legacy terminal record.
An old region needs a separately approved closed-region transition: verify dead
owners, exact bindings and both locks; take and verify current recovery backups;
wait for an ordinary quota slot; durably charge ONE recovery with an audit and
the existing backoff; then use the existing current-data `transition.rebind()`
and start the existing US service once. Rebinding must preserve the paid flag,
deadline, complete debt/audit, queue and history. Never recreate control state,
use an uncharged initial launch or put an old SQLite over new records. A US
installation wrapper and exact package still need their own qualification and
approval; a former Europe-only manual exception must not be reused.

## Separate next stage: weather during an outage

During quota waiting this prototype has no observer, so PRICE, NOAA and METAR
collection are paused. Keeping weather alive is a separate change: keep the
existing observer as the sole SQLite writer, continue its NOAA/METAR schedules
while PRICE is in GAP, and make WebSocket retry admission nonblocking within that
ownership. HTTP/receiver threads may return results through bounded queues;
they must not become additional SQLite writers. Controller/worker handoff,
budgets, health and source independence need separate tests. No new service or
database is required or implemented here.
