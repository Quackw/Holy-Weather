# HOLY WEATHER development rules

This private project is independent of POLYMETEO. PRs 1–4 are accepted. Current scope: opt-in Telegram cards to @holy_weather, exact preview approval before a real test send; no production changes.

- Use codex/ branches and small reviewable PRs. No automatic merging.
- No production deployment, services, trading or own weather models. Telegram delivery is opt-in; tokens stay outside Git/logs and require locally configured access. Original AWC/LAMP only; no probabilities. Never change POLYMETEO or share its working databases.
- Keep bid, ask, trade, source time and receipt time distinct. Missing is not zero.
- New connection/restart is a baseline, never an inferred crossing. Preserve gaps and suppression reasons.
- One SQLite writer. Preserve event evidence and state; bound data, WAL, logs and memory. No automatic VACUUM.
- Tests are offline and synthetic in CI. Real evidence stays outside Git. Record provenance for research replay.
- Run pytest, compile checks and the documented finite Linux/live qualification. Separate code, tests, live observation and scientific claims.
- Stop on permission refusal or unknown write state. No credential/account fallback or silent limit increases.
