# HOLY WEATHER development rules

This private project is independent of POLYMETEO. Stage 1 is accepted. Implement stage 2 weather context only: one New York/KLGA contract, public prices, observed METAR context and bounded SQLite.

- Use codex/ branches and small reviewable PRs. No automatic merging.
- No production deployment, services, credentials, trading, weather models or Telegram. AWC observations only; no forecasts or probabilities. Never change POLYMETEO or share its working databases.
- Keep bid, ask, trade, source time and receipt time distinct. Missing is not zero.
- New connection/restart is a baseline, never an inferred crossing. Preserve gaps and suppression reasons.
- One SQLite writer. Preserve event evidence and state; bound data, WAL, logs and memory. No automatic VACUUM.
- Tests are offline and synthetic in CI. Real evidence stays outside Git. Record provenance for research replay.
- Run pytest, compile checks and the documented finite Linux/live qualification. Separate code, tests, live observation and scientific claims.
- Stop on permission refusal or unknown write state. No credential/account fallback or silent limit increases.
