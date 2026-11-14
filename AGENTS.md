# HOLY WEATHER development rules

This private project is independent of POLYMETEO. Stages 1–3 are accepted. Current scope: extend the existing AWC/LAMP context to the ten explicitly listed US cities, with station/rule/time checks and no own forecast model.

- Use codex/ branches and small reviewable PRs. No automatic merging.
- No production deployment, services, credentials, trading, own weather models or Telegram. Original AWC/LAMP only; no probabilities. Never change POLYMETEO or share its working databases.
- Keep bid, ask, trade, source time and receipt time distinct. Missing is not zero.
- New connection/restart is a baseline, never an inferred crossing. Preserve gaps and suppression reasons.
- One SQLite writer. Preserve event evidence and state; bound data, WAL, logs and memory. No automatic VACUUM.
- Tests are offline and synthetic in CI. Real evidence stays outside Git. Record provenance for research replay.
- Run pytest, compile checks and the documented finite Linux/live qualification. Separate code, tests, live observation and scientific claims.
- Stop on permission refusal or unknown write state. No credential/account fallback or silent limit increases.
