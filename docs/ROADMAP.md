# Roadmap — PRICE qualification first

Reconciled on 16 November 2026 against accepted main
`937d3bcc3efd66a1bd3f706f859a1e4435f0ba7a` (PR #22). This is accepted source,
not an installed identity or proof of autonomous operation.

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
| 21 | websockets 17.4, Linux compatibility/security and isolated dependency rollback |
| 22 | Research ENGINE_POINT_BOUND no longer invalidates usable PRICE quotes; raw status retained |

PRs #5/#7 are historical/superseded, not pending prerequisites. Preserve their
branches/evidence. PR #13 and European source experiments remain deferred on
`codex/europe-weather-experiments-preserved`; do not reintroduce them into PRICE.

Accepted main's [post-merge CI](https://github.com/Quackw/Holy-Weather/actions/runs/43878233806)
passed 590 tests on each of Ubuntu/Python 3.11 and 3.13.5, including source/wheel
checks and rollback. Saved London/Paris/Amsterdam streams replay with unchanged
SHA/matching outcomes, including explicitly injected outages; those windows
contain zero PRICE crossings. PR #22's 4097-input regression is synthetic,
not an invented live event. Older PR notes/A–D status tables retain their original
revision/time scope and do not override this accepted-source inventory.

## Remaining acceptance gates

1. **Repository protection decision.** This documentation change reconciles the
   four current overview files and records the completed read-only settings audit.
   The recommendations below remain a separate user decision, not enforced rules.
2. **Finite current-build PRICE qualification.** Revalidate current contracts,
   stations/rules/dates/tokens and native bid/ask in an authorized isolated test.
   Start with the small reviewed scope; qualify the broader 11-Europe/10-US
   footprint separately. Preserve gaps and compare filter OFF/ON on identical
   inputs. Zero crossings is normal; a blocked preflight is not a live PASS.
3. **Fresh delivery.** For a genuine eligible event, show its exact preview and
   apply current user send authority. Record event→ready→claim→real Telegram ACK,
   refusals and restart dedupe. Never backfill old cards, retry UNKNOWN blindly
   or add a parallel sender to an active observation.
4. **Live lifecycle/recovery.** Local-day rollover, new-contract checks, DST,
   restart and finite reconnect already exist in code/tests. Observe the current
   build without old cards/threshold repeats. Historical US keepalive failure
   remains ROOT_CAUSE_UNKNOWN; a possible reproduced mechanism is not its cause.
5. **Continuous-operation proposal.** Measure sustained CPU/RAM/traffic,
   SQLite/WAL/log growth and disk reserve; qualify archival/recovery without
   lost evidence. Prepare a bounded supervisor plan after finite checks.
   Permanent deployment needs separate authority. Daily growth/current-build
   24/7 reliability remain NOT MEASURED/UNPROVEN.
6. **Forecast later.** Resume European source research only after PRICE acceptance
   and a new instruction. Retain original US LAMP; qualify models/stations,
   licensing, causal releases, Celsius thresholds and costs separately.

Keep code/tests, replay, live quotes, actual ACK and resources separate. Merging
source never updates an existing server automatically. This documentation task
does not authorize deletion of branches/history/databases/backups, active process
changes, Telegram, deployment or raised limits.

## GitHub protection snapshot — read-only, 16 November 2026

| Setting | Observed result | Evidence |
| --- | --- | --- |
| Visibility | **Public**, default branch main | Repository API and General settings |
| main protection | **Not protected** | Branch API protected=false; Branches settings has no classic rules |
| Repository rulesets | **None** | Empty API list; Rulesets settings shows no created rulesets |
| Required CI enforcement | **Not configured through protection/rulesets** | No main protection/rulesets; green CI alone is not a merge gate |
| CI job names | tests (3.11), tests (3.13.5) | Accepted workflow/check-run results |

The integration returned 403 for the detailed branch-protection endpoint; the
existing authenticated browser supplied read-only Branches/Rulesets views.
No settings, visibility or permissions changed. This snapshot covers visibility
and branch/ruleset enforcement, not collaborators, secrets, tokens or all advanced
security features.

**Proposed only, needs a separate settings decision:** protect main, require PRs
and both named GitHub Actions checks, require an up-to-date base, block force
push/deletion and apply enforcement to administrators without a standing bypass.
Do not require an independent approver by default for a sole-maintainer workflow.
Visibility remains a separate user decision; this PR does not change it.
