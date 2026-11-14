# HOLY WEATHER development rules

This private project is independent of POLYMETEO. Accepted runtime PRs 1–4 remain intact. The current product is exactly two independent notification algorithms: PRICE ALERT and LAMP ALERT, sharing inputs and one Telegram bot. See docs/STRATEGY.md. PR A (#8) is accepted. Current task is PR B: opt-in PRICE ALERT with persistence and regression tests; no sends or production changes.

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
