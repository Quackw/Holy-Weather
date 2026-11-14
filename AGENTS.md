# HOLY WEATHER development rules

This private project is independent of POLYMETEO. PRs 1–4 are accepted. Approved concept: one project, two independent analytical tracks (LOCK ALERT and LAMP ALERT), one shared Telegram bot. See docs/STRATEGY.md. Current task is documentation only; no strategy implementation or deployment is authorized.

- Use codex/ branches and small reviewable PRs. No automatic merging.
- No production deployment, new services, trading or own weather models. Original AWC/LAMP is forecast context, not a probability. Telegram work remains separately scoped in PR #5; do not activate or extend it under this documentation task. Never change POLYMETEO or share its working databases.
- Share source data, but keep LOCK/LAMP triggers, statuses, evidence and notification deduplication independent. Agreement is not a calibrated probability.
- LOCK must distinguish PRICE WATCH, LIKELY LOCK and RESULT VERIFIED. 70¢ is a research interest threshold only. Official verification must follow the exact contract rules; METAR maxima and LAMP are not official settlement proof.
- Preserve causal source/receipt/availability clocks and frozen history for both tracks. Evaluate them separately on new days, including false warnings, gaps, liquidity, spread and costs; missing data never means no market reaction.
- Keep bid, ask, trade, source time and receipt time distinct. Missing is not zero.
- New connection/restart is a baseline, never an inferred crossing. Preserve gaps and suppression reasons.
- One SQLite writer. Preserve event evidence and state; bound data, WAL, logs and memory. No automatic VACUUM.
- Tests are offline and synthetic in CI. Real evidence stays outside Git. Record provenance for research replay.
- Run pytest, compile checks and the documented finite Linux/live qualification. Separate code, tests, live observation and scientific claims.
- Stop on permission refusal or unknown write state. No credential/account fallback or silent limit increases.
