# HOLY WEATHER development rules

HOLY WEATHER is independent of POLYMETEO. Reconciled against accepted runtime main
`937d3bc` (16 November 2026, PR #22); inspect fresh GitHub state before acting.
The repository is currently public. Accepted source is not the installed version
of any existing observer.

The product has exactly two independent notification algorithms: PRICE ALERT and
LAMP ALERT, sharing data/queue implementation and one Telegram bot. Current
priority is PRICE qualification. The regional catalogue is PRICE-only: one codebase
for Europe (11 profiles) and US (10), separate data directories and rule identities.
Accepted optional US weather/LAMP in the single-contract observer remains intact;
forecast expansion is deferred.

PRs 1–4, 8–12 and 14–22 are accepted, including queue capacity, local-day rollover,
notification filtering, no-sender timers, regional reconciliation, websockets 17.4
and research-bound isolation. PR #13/European forecast experiments remain deferred
on `codex/europe-weather-experiments-preserved`. PRs #5/#7 are historical/superseded,
not prerequisites to merge. Preserve their branches/evidence and discarded strategy.
See [README](README.md), [ARCHITECTURE](ARCHITECTURE.md) and [ROADMAP](docs/ROADMAP.md).
Old PR reports are historical evidence, not current acceptance gates.

- Use `codex/` branches and small reviewable PRs. Merge only with explicit user
  authorization. No deployment, service/process/settings change or new live run
  is implied by accepted code or an earlier finite observation.
- Never change POLYMETEO, other projects or their working databases. No LOCK,
  own Tmax models, probabilities, trading or additional services.
- Before changing PRICE code, verify actual retained `@holy_nfx_bot` source rules
  and record provenance. Use Decimal midpoint of the real side token bid/ask,
  upward 50/55/60/70/80 crossings, one MULTI-CROSS per update, once per
  market/side/threshold, REARM=false. Missing is not zero. Never substitute ask,
  trade or the opposite token. Legacy 50/70/90 ask research is not Netflix parity.
- First confirmed quote after startup/GAP/restart is a baseline. Preserve fired
  levels, source high-water, raw clocks, input references and explicit refusals.
  Research-only ENGINE_POINT_BOUND cannot reset valid PRICE readiness; retain its
  original sample status. Real invalidity, expiry and gaps still block PRICE.
- LAMP uses original new bulletins independently of price, comparing identical
  remaining hours. First issue is baseline; duplicate downloads do not alert.
  Preserve issue/receipt/availability clocks and the configured 2°F threshold.
  Do not silently expand sources or change thresholds/units.
- PRICE/LAMP state, evidence and dedupe namespaces stay separate. Event, fired
  state, evidence and queue admission commit atomically. Never resend SENT or
  blindly retry UNKNOWN. Telegram failure must not stop collection; SQLite
  failures remain critical. Follow exact preview and current user send authority.
- The filter is opt-in. Preserve priority 70/80 without ordinary batch delay,
  maximum eight-second ask-only wait, explicit fallback/refusal reasons and
  ten-second default ordinary grouping. Delivery never creates PRICE events,
  rearms levels, extends expiry or mixes sides/contracts/dates/tracks.
- One SQLite writer per owned directory. Preserve schema v1, new history,
  thresholds and delivery receipts through compatible code rollback. Never
  restore an old database over new records. No automatic VACUUM, prune, archive
  or cleanup. Explicit maintenance needs matching version/config and authority.
- Keep the 90-second quote limit, maximum 100-ms catalogue source lead, 256-MiB
  per-directory budget and 1024 unresolved-work bound. Terminal receipts remain.
  Bound WAL/logs/memory; do not raise limits or claim indefinite storage.
- Tests are offline/synthetic. Real evidence, credentials, databases and backups
  stay outside Git. Reproduce defects before fixing them; check producer, stored
  state, consumer, restart and rollback. Run pytest/source/compile checks and
  inspect Linux CI (Python 3.11 and 3.13.5). Add only meaningful regressions.
- Stop on permission refusal or unknown write outcome. Do not silently switch
  credentials/identities, bypass denied control actions, reset attempts or retry
  uncertain operations. Do not automatically change GitHub visibility/protection.

## Acceptance boundaries

Keep code/tests, replay, live quotes, actual Telegram ACK and resource evidence
separate. Green CI or HTTP 200 alone is not city readiness. Qualify PRICE
independently of weather. A finite run or historical delivery does not prove the
current build operates autonomously. Catalogue local-day rollover exists within
its finite deadline; no permanent supervisor exists. Qualify current contracts,
live rollover/recovery, delivery and long-run storage before separately authorized
deployment. Documentation changes do not alter these permissions.
