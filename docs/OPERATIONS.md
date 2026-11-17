# PRICE operations — dated baseline and open limits

This is a record of the separately approved 17 November 2026 installation,
not a live status endpoint or a new installation/repair authorization. Source:
`8d176109b80628716f745b210d87bca00c5c51ed` (PR #30). Private SHA-bound installation,
restore, process and Telegram receipts remain outside Git.

## What was installed and observed

| Region | PRICE profiles | Service active since UTC | Kyiv time on 17 November |
| --- | --- | --- | --- |
| Europe | 11 | 2026-11-17 13:53:32 | 15:53:32 |
| US | 10 | 2026-11-17 14:10:37 | 16:10:37 |

Both instances were enabled for boot and verified against the actual loaded
source/owner identities, not merely a release directory. The regional runtime
is PRICE-only; it does not collect AWC/LAMP or make trades.

Initial live qualification closed at **16:53:58 Kyiv on 17 November**:

- Two natural worker windows completed per region, with genuine fresh quotes and
  separately reported PRICE-ready tokens. Not every city/token was ready at once.
- Twelve new cards had verified Telegram ACKs, six per region. No TEST or backlog
  was sent; no duplicate SENT events/thresholds or UNKNOWN were found at that cut.
  Explicit non-admission/confirmation refusals remained recorded on source events.
- All previous PRICE events, fired levels, contracts, input/evidence links and
  SENT/UNKNOWN were preserved. SQLite schema v1, integrity and foreign keys passed.
- Full pre-cutover reserves, code/config/control and archive lineage were verified
  on the server and copied to private FileVault Mac storage with matching SHA256.
  This does not prove regular off-server backup of subsequent writes.

These timestamps and results describe that observation. A later health claim
requires new read-only evidence; this documentation update did not probe or alter
the server. Reconnects observed in that qualification took 1.173–2.174 seconds
to CONNECTED. They do not prove all PRICE baselines recovered in that interval.

## Layout and access

| Object | Installed layout | Protection |
| --- | --- | --- |
| Release / pinned Python environment | `/opt/holy-weather/releases/<commit>` | Root-owned; runtime cannot modify source |
| Regional working data | `/var/lib/holy-weather/{europe,us}/data` | Existing runtime UID, directories 700/files 600 |
| Controller records | `/var/lib/holy-weather/{europe,us}/control` | Same private ownership |
| Full history archives / reserves | `/var/lib/holy-weather/{europe,us}/{archive,backups}` | Same private ownership |
| Non-secret plans | `/etc/holy-weather/{europe,us}.json` | Root-owned, 644 |
| Services | `holy-weather-price@europe.service`, `holy-weather-price@us.service` | System units using the existing runtime UID |
| Encrypted Telegram credential | `/etc/credstore.encrypted/holy-weather.telegram` | Root:600; systemd supplies a read-only runtime credential |

The token is not in a plan, environment fallback or command argument. Encryption
at rest does not hide a running credential from root or its service UID. Preserve
the host credential key for recovery after a host rebuild; boot and reinstall are
different cases. Never publish credentials, databases, raw streams or private
backup contents as GitHub evidence.

## Read-only health checks

Read existing systemd state/journal, controller `state.json` and active progress;
compare boot/PID/start ticks/UID/executable, the installed source hashes and
ownership locks. Check heartbeat, completed windows, restart debt and each city's
verified local date/contracts, quote freshness and **PRICE-ready** count. An active
service, heartbeat or fresh depth sample alone does not prove PRICE readiness.

Use bounded SQLite `mode=ro` queries that account for WAL; do not use `immutable=1`
on the active working database. Do not instantiate a writer/Store, checkpoint,
VACUUM, prune or archive to obtain status. Inspect existing sender enabled/reason,
SENT ACK/message IDs, UNKNOWN and per-event refusal/expiry records independently.
Compare working DB/WAL/SHM/logs, archives, dedupe/control and free space with their
separate limits. Never launch a second writer or sender as a diagnostic action.

## Bounds and known gaps

- Quotes remain at most 90 seconds old, catalogue source lead at most 100 ms.
  Each regional data directory remains bounded at 256 MiB with an 8-MiB reserve;
  workers run at most 1,200 seconds. Unresolved queue work is bounded at 1,024.
- Authorized archival is only between closed workers, checked at 128 MiB; a
  candidate is at most 160 MiB. Each region's archives are bounded at 4 GiB/40 parts, packed
  dedupe at 96 MiB, control at 1 MiB and free disk at a minimum 2 GiB. Protective
  limits stop work; history is not deleted or caps raised to continue silently.
- Recovery is persistent: three/hour, six/rolling 24 hours, backoff 30–300 seconds.
  Manual/fatal stops latch. An ordinary service restart is not a general repair;
  resuming a verified manual stop needs explicit matching rebind authority. Never
  reset state/restart debt or restore old SQLite over later writes.
- The unit has no independent controller watchdog or OnFailure notification.
  An ingestion process can remain running after the sender disables itself;
  sender health/ACKs must be checked separately.
- Pending cards can expire during planned worker rotation before their original
  TTL. Events/evidence stay intact; that is a delivery gap, not an ACK. Rotation
  also creates a real market-data gap, followed by a new baseline.
- The historical keepalive-timeout root cause remains unknown. Autonomous
  reconnection is observed, not proof of complete uninterrupted token coverage.
- Regular fresh off-server backups, long-term retention capacity, real Netcup
  reboot, live overnight/DST behavior and full 24-hour reliability remain open.

The first time both instances could have completed 24 hours is **18 November 2026
after 16:10:38 Kyiv (14:10:38 UTC)**. Perform a fresh read-only review then; elapsed
time is not automatic PASS. Daily resource consumption remains **NOT MEASURED**.

See [continuous ownership/transition/rollback](PRICE_CONTINUOUS.md),
[architecture](../ARCHITECTURE.md) and [remaining work/protection](ROADMAP.md).
