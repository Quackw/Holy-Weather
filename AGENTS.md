# HOLY WEATHER development rules

This private project is independent of POLYMETEO. PRs 1–4 and 8–11 are accepted: PRICE, original LAMP and the shared durable Telegram queue exist. Current priority is PRICE acceptance for US and European markets; FORECAST/LAMP expansion is deferred. Stages 0 and 1 are accepted. PR 12 is accepted and merged. Current task: reconcile PR 14 with the accepted PRICE-only main and prepare it for review without changing active observation. Forecast experiments are preserved on codex/europe-weather-experiments-preserved, not included in this PRICE PR. Follow docs/PRICE_ACCEPTANCE_ORDER.md and docs/ROADMAP.md. PR 13 remains deferred; PR 14 targets main and remains unmerged. No automatic merge or production changes. Existing specifically authorized finite observations retain their exact scope; this audit does not stop, expand or redeploy them.

- Use codex/ branches and small reviewable PRs. No automatic merges or deployments.
- No winner/lock strategy, own weather models, probabilities, trading, new services or production changes. Never change POLYMETEO or share its working databases.
- Before PRICE code, verify actual @holy_nfx_bot source rules. Required specification: Decimal midpoint of real side token bid/ask; upward 50/55/60/70/80% crossings; one MULTI-CROSS per update; once per market/side/threshold; REARM=false. First confirmed quote is baseline. Never silently substitute ask/trade or call the existing 50/70/90 engine Netflix parity.
- LAMP events are driven by new bulletins independently of price. Compare identical remaining hours; first issue is baseline; repeated downloads do not alert. Preserve causal clocks, original data, gaps and suppression reasons. Propose/freeze a configured material-change threshold before tests.
- Share data and eventual durable delivery queue, but separate PRICE/LAMP event states, evidence and dedupe keys. Confirmed sends cannot repeat; uncertain sends remain explicit. Telegram failure must not block collection. No new sends without separate preview verification and permission.
- PR #7 is superseded; preserve its branch/evidence and reusable LAMP logic, never merge its discarded strategy. PR #5 stays unmerged until the new notification design is accepted. Follow PR A → B → C → D, each accepted before starting the next.
- Keep bid, ask, trade, source time and receipt time distinct. Missing is not zero.
- New connection/restart is a baseline, never an inferred crossing. Preserve gaps and suppression reasons.
- One SQLite writer. Preserve event evidence and state; bound data, WAL, logs and memory. No automatic VACUUM.
- Tests are offline and synthetic in CI. Real evidence stays outside Git. Record provenance for research replay.
- Run pytest, compile checks and the documented finite Linux/live qualification. Separate code, tests, live observation and scientific claims.
- Stop on permission refusal or unknown write state. No credential/account fallback or silent limit increases.

## Acceptance before declaring a city ready

- Reproduce a reported defect before fixing it; retain an offline regression that fails on the old behavior. Check the producer, stored state, consumer and restart boundary, not just a helper function.
- Keep rule/station/source verification, quote readiness, weather readiness, forecast readiness and delivery evidence separate. Green CI or HTTP 200 alone is not city readiness.
- Qualify PRICE separately from forecasts: verified rules/station, native quotes, clocks, durable queue and restart/deduplication are required. Any authorized city notice must state PRICE-only when forecasts are unqualified; forecast qualification must not block independent PRICE acceptance. Never announce disabled features as working.
- Preserve the small existing architecture. Use bounded tests and the existing writer/queue; no new services, dependencies, storage limits or production rollout as a shortcut to acceptance.
