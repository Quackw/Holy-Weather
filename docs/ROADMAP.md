# Roadmap — PRICE reliability first

Runtime baseline: `8d176109b80628716f745b210d87bca00c5c51ed` (PR #30), installed
under separate approval on 17 November 2026. See the dated
[operations record](OPERATIONS.md) for the distinction between source, installed
services, real deliveries and outstanding operational proof.

One project, PRICE ALERT and independent LAMP ALERT, one Telegram bot. No LOCK,
own Tmax/probability model or trading. European forecast expansion remains
deferred and is not a prerequisite for independent PRICE qualification.

## Accepted source

| PR | Accepted implementation |
| --- | --- |
| 1–4 | Finite single-contract monitor, AWC/original LAMP context and US profiles |
| 8–11 | Two-alert concept, PRICE, independent KLGA LAMP and durable Telegram queue |
| 12 | European PRICE foundation with forecast experiments separated |
| 14 | Clock/order policy, grouped subscriptions and finite gap recovery |
| 15 | Unresolved queue capacity, per-event admission/refusal and retained terminal receipts |
| 16 | Local-day contract rediscovery, persistent attempts and new baselines |
| 17 | Opt-in bid confirmation/grouping, fast priority 70/80 and transport evidence |
| 18, 20 | Queue deadlines with Telegram OFF in catalogue and single-contract observer |
| 19 | One regional PRICE codebase, separate Europe/US state and configuration identities |
| 21 | websockets 17.4, Linux compatibility and isolated dependency rollback |
| 22 | Research ENGINE_POINT_BOUND no longer invalidates usable PRICE quotes; raw status retained |
| 23 | Overview documentation reconciled with the then-current PR #22 baseline |
| 24 | Bounded CI traceback-race qualification for the Python 3.13.5 failure |
| 25 | Same-bucket/side delivery grouping and suppression of levels below an already delivered level |
| 26 | Queue servicing no longer starves WebSocket receive handling |
| 27 | Verified history archival, compact working data and restore writer-lock checks |
| 28 | Guarded recovery, persistent budgets and regional ownership |
| 29 | Current-market discovery after legacy session-ended refusals, preserving old audit |
| 30 | Continuous systemd runtime, inherited launch locks, current-data transition and durable recovery charging |

PRs #5/#7 are historical/superseded, not pending prerequisites. Preserve their
branches/evidence. PR #13 and European source experiments remain deferred on
`codex/europe-weather-experiments-preserved`; do not reintroduce them into PRICE.

The runtime baseline's [post-merge CI](https://github.com/Quackw/Holy-Weather/actions/runs/44083496660)
passed 785 tests on each Ubuntu/Python 3.11 and 3.13.5, including source/wheel
checks and rollback. Python 3.11 also exercised actual systemd with dummy
credentials/synthetic I/O. Real streams/ACKs are separate private evidence;
older PR reports retain their original revision/time scope.

## Remaining reliability work

1. **Operational health and escalation.** Verify controller progress and each
   city's PRICE-ready tokens separately from process/socket liveness. The unit
   has no independent controller watchdog or OnFailure notification. Sender
   disabled/UNKNOWN, protective caps and stalled progress need explicit diagnosis;
   any new mechanism requires a separate reviewed change and installation approval.
2. **Storage and independent recovery.** Pre-cutover copies were verified on the
   server and FileVault Mac. Continuing off-server backup of later writes is not
   qualified. Measure real growth and archive/dedupe capacity; prepare retention
   and restore rules preserving all PRICE evidence/thresholds/receipts. Existing
   finite caps are not indefinite storage, and must not be raised silently.
3. **Pending cards at worker rotation.** Recovery preparation expires old pending
   work even when some cards remain inside their TTL. Preserve the event and
   explicit refusal; reproduce and review any delivery fix separately. Never
   backfill stale cards, rearm thresholds or retry UNKNOWN blindly.
4. **Transport and coverage.** Real keepalive failures reconnected, but their
   root cause remains unknown. Connection recovery time is not proof of restored
   PRICE readiness for every token. Qualify missing/stale markets separately
   from genuinely absent crossings; no inferred signals through gaps.
5. **Full lifecycle qualification.** Current-build live overnight/DST behavior,
   a separately authorized real Netcup reboot and a complete 24-hour resource/
   history/delivery review remain open. The first common 24-hour boundary is
   18 November 2026 after 16:10:38 Kyiv; crossing it alone is not PASS. Daily
   consumption is NOT MEASURED until comparable full-period evidence is checked.
6. **Forecast later.** Resume European source research only under a new
   instruction. Keep accepted optional US LAMP; separately qualify stations,
   licensing, causal releases, Celsius thresholds and costs.

This documentation/settings task does not change running observations, send
Telegram, delete data, deploy code or authorize a repair. Merging source never
updates a server automatically. Keep code, replay, live quotes, ACKs and resources
as separate claims.

## GitHub main protection — verified 17 November 2026

The user authorized protection; the classic rule was saved and its edit page
reopened to verify the persisted settings. The branch API also reports
`protected=true`, both required checks and enforcement for everyone.

| Setting | Verified configuration |
| --- | --- |
| Visibility/default branch | Public / main; visibility unchanged |
| Protected branch pattern | Exact `main`, one matching branch |
| Pull request required | Yes |
| Required independent approvals | No; sole maintainer may merge after checks |
| Required checks | `tests (3.11)` and `tests (3.13.5)`, bound to GitHub Actions |
| Up-to-date branch required | Yes |
| Administrator bypass | Disabled; rules apply to administrators |
| Force pushes/deletions | Disabled |

Rulesets are not required in addition to this classic rule. This verification
covers these branch settings, not a complete collaborator, credential or
historical-secret audit. A settings change does not authorize PR merging.
