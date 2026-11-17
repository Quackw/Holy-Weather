# Candidate: controller watchdog and sender visibility

This change is source-only until separately accepted and installed. The dated
[installed-runtime record](OPERATIONS.md) still describes the PR #30 deployment;
no active observer, database or unit is changed by this PR.

## Controller liveness

The existing regional unit gains `WatchdogSec=360s`, `NotifyAccess=main` and an
explicit `WatchdogSignal=SIGABRT`. The controller sends `WATCHDOG=1` from its
actual supervision loop, at most every ten seconds (or one third of the manager's
shorter interval). There is no timer/helper thread that could keep a deadlocked
controller looking alive. Child processes do not inherit the notification
environment and cannot feed the main-only watchdog.

The six-minute bound leaves room for the existing 300-second archive stage and
30-second shutdown grace; it is not a quote TTL. Long blocked controller operations
are detected, even when a worker is still alive. Socket sends are nonblocking;
a notification failure is logged once, retried at the bounded interval and does
not silently claim continued manager contact. The manager's timer remains active.

On timeout systemd aborts the service under its existing cgroup ownership, then
uses its existing restart policy. This can interrupt the SQLite writer and an HTTP
send. Recovery checks current WAL/integrity, process identities and both locks
before admitting another worker. Three/hour and six/rolling-24-hour recovery
limits, durable backoff, manual/fatal-stop latches and `SendSIGKILL=no` remain.
Unreleased owners/locks or unknown state still block a replacement; recovery is
not an unconditional availability guarantee. No old SQLite is restored.

## Telegram health is separate

Controller state already stores sender enabled/reason. The new hook exposes this
through systemd `StatusText` and transition-only `PRICE_SERVICE_HEALTH` journal
records, including startup UNKNOWN, intentionally OFF and unexpectedly DISABLED.
Final service output retains the sender reason too. Quiet markets and missing
crossings are not classified as sender failure. Controller liveness does not
prove fresh PRICE-ready quotes or a Telegram ACK.

A disabled sender does not stop collection or trigger a restart merely to clear
its safety latch. SENT and UNKNOWN remain immutable; an unconfirmed claim keeps
sending disabled across worker/controller recovery. This PR adds no Telegram
health messages, external escalation service, retries or automatic re-enabling.
Diagnosis/resolution of UNKNOWN needs the existing explicit operator procedure.

Notifications use the three already known controller/sender fields; they add no
SQLite tables, queue records, heartbeat files, dependencies or database scans.
Repeated healthy polls do not add journal entries. Existing storage, PRICE rules,
90-second freshness and 100-ms source-clock lead remain unchanged.

## Qualification and rollback

Offline tests cover real Unix datagrams, ownership, notification failure/recovery,
quiet polls/backoff, disabled sender persistence, existing launch/recovery budgets,
locks, receipts and compatible rollback after new data. The existing Linux CI
systemd probe uses an isolated dummy credential, synthetic market/receipts and no
external network. Its watchdog is shortened to five seconds for a finite test:
a blocked controller times out while its worker was alive, recovers after durable
backoff and preserves events, fired thresholds and SENT/UNKNOWN. A PENDING card
expires explicitly under the existing no-backlog policy. Second writers/controllers
are refused; the existing SIGKILL crash and manual-stop tests remain.

These are controlled failure tests, not a live Netcup stall or real delivery.
The accepted prior `9adf061` source is also checked against newly written data.
A future operational rollback must restore the matching old unit **and** code:
the old binary cannot feed the new watchdog. It requires clean ownership, a new
verified reserve and explicit current-data rebind; it never rewinds the database.

Primary contracts: [systemd service/watchdog](https://www.freedesktop.org/software/systemd/man/257/systemd.service.html#WatchdogSec=),
[cgroup stop and no-SIGKILL behavior](https://www.freedesktop.org/software/systemd/man/257/systemd.kill.html),
[watchdog environment/PID](https://www.freedesktop.org/software/systemd/man/257/sd_watchdog_enabled.html).
