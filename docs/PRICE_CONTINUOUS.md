# Continuous PRICE: prepared, not installed

Scope: existing Europe (11 cities) and US (10), one controller/SQLite writer per
region, two systemd instances, shared accepted PRICE/queue code. No weather,
Telegram format, threshold, city or schema changes. Do not install from this guide
without a separate exact package/cutover authorization. PR acceptance is not that
authorization. Current finite observers must remain untouched during development.

## Runtime contract

`Plan.continuous=true`, `session_day=null`, `deadline_ms=null` opts in explicitly.
Old finite plan serialization/hashes are unchanged. Each child still runs for at
most 1,200 seconds; each city uses its own verified local date through the existing
DayDiscovery. New York rolls before Los Angeles. DST changes UTC offsets, never
the identity of an already verified contract. Cold discovery refusals and exhausted
per-day source budgets remain visible; they are not reset at each worker restart.
NO_VERIFIED_CURRENT_MARKETS is degraded collection, not healthy PRICE. Empty/closed
windows are paced; no inferred movement through a reconnect, restart or rollover.

The child inherits the controller flock **before exec**, then commits its owner
receipt before opening SQLite. A parent crash before PID publication cannot let
another controller pass that flock. Linux ownership is boot ID + PID + start ticks
+ UID + executable. A missing owner is resumable only for this exact inherited-lock
protocol and a free controller/writer lock; legacy unresolved launches are refused.
No PID-only adoption or SIGKILL recovery. Unknown launches, unknown exits, integrity,
rule/version drift, clock reversal, incomplete transitions/archives and unresponsive
children are terminal, not an excuse for unlimited service restarts.

The HW controller owns the existing 30-second exponential backoff (cap 300 seconds),
three recoveries/hour and six/rolling 24 hours. Debt is committed before recovery,
survives service/boot/date changes and is not refunded by code transition. systemd
only reopens a crashed controller; fatal exits use 78 and prevent Restart. Four
systemd starts/hour include the initial start, consistent with three recoveries.

ExecStop records intent **before** systemd sends cgroup SIGTERM: normal stop latches
MANUAL_STOP; host manager `stopping` permits the *next boot*, not a same-boot restart;
controller failure remains subject to HW budgets. Fatal exit 78 stays latched.
Unknown manager context is a manual stop. A manual `systemctl restart` intentionally
does not clear this latch. Resumption requires explicit closed-region `rebind`,
which preserves the prior stop/audit and recovery debt; never remove state files.
systemd uses a 45-second graceful stop with SendSIGKILL=no; a hung child can therefore
require intervention. No second writer is allowed while its inherited lock lives.

The sender finishes its existing bounded HTTP close and commits ACK/UNKNOWN before
Store close. Old pending work expires explicitly on recovery. SENT and UNKNOWN
are not repeated. Uncertain delivery disables the sender with a reason while
collection continues. Credentials come only from systemd's read-only credential
directory, not environment/command-line token fallback. Root and the unit UID can
read a running process/credential; encrypted-at-rest does not hide it from root.

## Bounds and operator state

- Quotes: unchanged 90 seconds and at most 100-ms source lead, clocks unmodified.
- Data: unchanged 256 MiB/region including SQLite/WAL/logs, 8-MiB preflight reserve.
- Existing maintenance between closed workers: checkpoint at 128 MiB; candidate
  at most 160 MiB; archives at most 4 GiB/40 parts per region; at least 2 GiB free.
  Only the two SHA-verified redundant scratch copies of an accepted part retire.
- Exact archived dedupe index: unchanged 96-MiB packed cap. Projection now proves
  every removed identity and every old identity, rejecting both missing/extra IDs.
  Archive file **and parent directory** are fsynced before database replacement.
  Restore verifies archive lineage and refuses conflicting immutable events.
- Control: unchanged 1 MiB. At 32 closed attempts, retain the last two active files;
  full closed state/progress/result/owner records commit to existing SQLite `meta`
  before redundant control files retire. Hash-chained audit, lifetime attempt count,
  crash intent and recovery debt survive this compaction. A durable rotation intent
  permits only exact, idempotent retirement after a confirmed SQLite commit.
- No indefinite-retention claim: retained events, control audit, dedupe and archives
  ultimately reach their bounds. Stop explicitly at the limit; do not delete history
  or raise a cap. Operator storage maintenance remains separately authorized.
- Controller `state.json` + active `<nonce>.progress.json` expose current identities,
  stage/heartbeat, sender enabled/reason and per-city contracts/fresh quotes/**PRICE
  ready** counts plus last SENT time. General fresh depth is not a fresh PRICE
  confirmation. NY/Atlanta must pass the separate PRICE-ready field; no promotion
  of a depth sample, stale saved baseline or socket heartbeat to PRICE readiness.
  Observations are snapshots (10 seconds), not guarantees after their timestamp.

## Prepared layout and permissions

| Object | Future path | Ownership / mode |
| --- | --- | --- |
| Immutable release and locked venv | `/opt/holy-weather/releases/<commit>` | root, directories 755; source read-only to runtime |
| Regional data/control/archive | `/var/lib/holy-weather/{europe,us}/{data,control,archive}` | existing `polymeteo-agent`, directories 700/files 600 |
| Plans | `/etc/holy-weather/{europe,us}.json` | root, 644 (no secret) |
| Encrypted token | `/etc/credstore.encrypted/holy-weather.telegram` | root, 600 |
| Unit template | `/etc/systemd/system/holy-weather-price@.service` | root, 644 |
| Transition evidence/backups | `/var/lib/holy-weather/<region>/backups/<unique-operation>` | existing runtime UID, 700/600 |

Netcup read-only preflight found Debian 13/systemd 257, existing UID 1001, no
write access to `/etc/systemd/system` or `/var/lib`, user linger disabled. Choose
system services running as that existing user; administrator installation is a
separate authorized action. No new user, sudo grants, user linger or admin broker.
`/var/tmp` and `/var/lib` were on the same filesystem; repeat that check before any
move. Encrypted credentials use the host key (`systemd-creds --with-key=host`), since
a VPS TPM is not assumed. Preserve that host credential key through host rebuilds;
OS reboot and reinstall are different recovery cases.

References: [systemd service stop/restart policy](https://www.freedesktop.org/software/systemd/man/systemd.service.html),
[encrypted credentials and runtime access](https://www.freedesktop.org/software/systemd/man/systemd.exec.html#Credentials).

## Exact future cutover procedure (NOT executed)

Prepare one immutable package manifest with this PR's final full commit, file SHA256,
locked wheels/Python, rendered files and **fresh** source-region paths/bindings and
process identities. Pin that manifest in the single installation request. Do not
reuse old PID observations or assume another PR's installation approval covers it.
Commands below are the concrete operations; `HW_RELEASE` is that exact approved
release, `HW_REGION` is one of `europe`/`us`, and `HW_STAGE` is its private evidence
directory. No wildcard process stop or retry of an uncertain operation.

1. Stage and hash the approved code/wheels under `/opt/holy-weather/releases/<commit>`.
   Admin creates only the release/config/region/credential directories above; runtime
   keeps its existing UID/primary group. Build the locked venv, run the full Linux
   checks **before** touching a controller. Render without installing:

   ```sh
   "$HW_RELEASE/venv/bin/python" -B deploy/render_price.py --release "$HW_RELEASE" --output "$HW_STAGE/rendered"
   systemd-analyze verify "$HW_STAGE/rendered/holy-weather-price@.service"
   ```

2. Under strict pinned SSH, stream the previously authorized bot secret via stdin
   only to the admin command below; do not put it in shell variables, arguments,
   an EnvironmentFile, plaintext temporary file or a transcript. This transfer and
   the administrator action require the upcoming exact authorization.

   ```sh
   /usr/bin/systemd-creds encrypt --with-key=host --name=telegram - /etc/credstore.encrypted/holy-weather.telegram
   chmod 600 /etc/credstore.encrypted/holy-weather.telegram
   ```

3. Capture each current controller **and every owned child** with `lifecycle.process_identity`,
   plus paths, old `plan.json/state.json` binding, file SHA and archive references.
   The stop must address that verified controller once with SIGTERM, or wait for its
   existing finite deadline. No kill. Confirm both identities are gone, both locks
   are free, no active launch remains, database integrity/FKs and current contents
   match the closed checkpoint. If PID reuse occurs, compare start/boot, not number.
   Unexpected processes, leftover active/WAL ambiguity or incomplete archive => STOP.
   Make a full Backup API reserve of current DB, control/code/config and retain all
   archive files/SHA; verify it and an independent off-server copy. No old snapshot
   may later overwrite current data. Europe first; its failure blocks US cutover.

4. Future source map (recheck, never infer from the existence of a new directory):

   | Region | Data | Control | Archive |
   | --- | --- | --- | --- |
   | Europe | `/var/tmp/holy-weather-all-city-price-20261116/europe/data` | `/var/tmp/holy-weather-price-main-e41bf60-20261117/europe/control` | `/var/tmp/holy-weather-price-main-e41bf60-20261117/europe-archive` |
   | US | `/var/tmp/holy-weather-all-city-price-20261116/us/data` | `/var/tmp/holy-weather-price-main-e41bf60-20261117/us/control` | `/var/tmp/holy-weather-us-history-recovery-20261116/parts` |

   As runtime UID, with the region closed, atomically exchange each existing
   directory into its stable destination. `--kind` is respectively data/control/archive:

   ```sh
   "$HW_RELEASE/venv/bin/python" -B -m holy_weather.transition move --source "$HW_OLD_DATA" --destination "/var/lib/holy-weather/$HW_REGION/data" --kind data
   "$HW_RELEASE/venv/bin/python" -B -m holy_weather.transition move --source "$HW_OLD_CONTROL" --destination "/var/lib/holy-weather/$HW_REGION/control" --kind control
   "$HW_RELEASE/venv/bin/python" -B -m holy_weather.transition move --source "$HW_OLD_ARCHIVE" --destination "/var/lib/holy-weather/$HW_REGION/archive" --kind archive
   ```

   Linux renameat2 exchange preserves original inodes and held locks and leaves a
   symlink tombstone at each old root in the same atomic operation. Old Store and
   controller reject symlink roots, including during migration; no absent-path gap
   can create a fresh empty database. Original directory contents are moved, never
   copied over newer data. Unknown exchange state is a stop for reconciliation.

5. `expected.json` contains the exact captured old binding. `process-proof.json`
   contains the pre-stop boot/PID/start_ticks/UID/executable identities of parent and
   child. Under both locks, explicitly rebind the CURRENT database and old control:

   ```sh
   "$HW_RELEASE/venv/bin/python" -B -m holy_weather.transition rebind --plan "$HW_STAGE/rendered/$HW_REGION.json" --control-dir "/var/lib/holy-weather/$HW_REGION/control" --expected "$HW_STAGE/expected.json" --process-proof "$HW_STAGE/process-proof.json" --backup "/var/lib/holy-weather/$HW_REGION/backups/$HW_OPERATION"
   ```

   The helper refuses live captured owners, nonfree locks, active/foreign binding,
   incomplete rotation/transition or drift. It verifies a Backup API copy, commits
   all old control bytes and binding change in one SQLite transaction, then records
   the new plan/state. Fired levels/events/SENT/UNKNOWN/market/source state remain.
   Every failure keeps reserve + intent; no blind resume or rerun. No binding deletion.
   Approval scope must explicitly include retirement of the now-redundant **control
   files**, already preserved in backup and SQLite audit. Historical market data and
   archive files are not deleted.

6. Admin installs the already verified rendered unit/plans (644), then:

   ```sh
   systemctl daemon-reload
   systemctl enable --now holy-weather-price@europe.service
   systemctl enable --now holy-weather-price@us.service
   ```

   These are explicit future service changes, not authorized or performed now.
   Verify actual `/proc` source/identity and both ownership locks; a folder or green
   CI is not proof that a new process uses it. Stop cutover on any unknown result.

## Tested rollback and boundaries

Disable/stop the new instance only after separate authority; obtain clean parent/
child exit and locks, and a **new** backup of current data including post-upgrade
records. Rebind current stable data/control to an exact compatible finite plan and
separately verified old `code_identity`, using the same helper with `--target-code`.
Run accepted `81d00eed40ee179481822001f693a544d0fa2d29` on that current database:

```sh
"$HW_RELEASE/venv/bin/python" -B -m holy_weather.transition rebind --plan "$HW_STAGE/finite-rollback.json" --control-dir "/var/lib/holy-weather/$HW_REGION/control" --expected "$HW_STAGE/current-binding.json" --process-proof "$HW_STAGE/current-process-proof.json" --backup "/var/lib/holy-weather/$HW_REGION/backups/$HW_ROLLBACK_OPERATION" --target-code "$HW_VERIFIED_OLD_CODE_ID"
```

The old controller cannot read the continuous plan. Rollback is bounded, fixed
`session_day`/deadline with archives **off**, to avoid reinstalling the old unsafe
checkpoint path. Prepare/verify that finite plan using current city dates; if the
region has mixed dates, wait for a verified common date or keep the region stopped
and prepare a separately accepted compatible rolling build. Do not claim identical
continuous availability from the old controller. Do not move data back to temporary
paths or restore any earlier SQLite. Old entrypoints at the stable path need a fresh
explicit authorization; no original wrapper/old expired deadline is reused.

## Acceptance evidence and remaining work

Regression tests exercise per-city midnight/DST, baseline/dedupe, parent/child launch
boundaries, boot/PID reuse, persistent budgets, manual/fatal stop, 200 windows/control
audit, interrupted audit/transition, private credentials, current-data rebinding and
accepted old-source compatibility after new records. Existing sender-close/UNKNOWN
and PRICE/history tests remain mandatory. No database schema or dependency changes.

The isolated Linux CI systemd qualification uses the actual unit/controller/wrapper,
encrypted **dummy** credential and a synthetic child instead of market/Telegram I/O:
actual controller SIGKILL, child graceful cgroup stop, persistent 30-second backoff,
restart and manual-stop refusal. Its file permissions/paths are adapted for the
disposable runner. This is not a real host reboot or Netcup live qualification.

After separately approved cutover, acceptance requires two natural windows per
region, true fresh BBO and PRICE-ready counts (including NY/Atlanta), explicit sender
state/real SENT only on an eligible new signal, no duplicate message IDs/events,
each city's local rollover and then 24 hours of resource/history checks. A separately
approved real Netcup reboot must verify credential decryption, exact loaded source,
no second writer and continued current data. Until then **24/7 = NOT PROVEN**.
Daily data/archive consumption remains NOT MEASURED for this prepared build; no
short synthetic test is extrapolated to guaranteed daily storage.
