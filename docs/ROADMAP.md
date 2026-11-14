# Roadmap — planning only

The approved direction is [two independent tracks](STRATEGY.md), sharing the
existing monitor and one future Telegram bot. This PR changes documentation only.
PR #5 Telegram remains separate and unmerged; no rollout is implied.

1. **One-city research contract.** Start with New York/KLGA and one verified active
   bucket/date, both sides. Reuse the finite monitor. Freeze LOCK status criteria,
   official evidence requirements, LAMP change thresholds and market-comparison
   windows before evaluating events. Do not assume every city has identical rules.
2. **LOCK in isolation, preview only.** Implement PRICE WATCH first, with explicit
   unknowns. Add LIKELY LOCK and RESULT VERIFIED only with tested evidence rules
   and the contract's official source. Replay causal cases, false warnings,
   corrections, missing quotes, restart and exact duplicates. No Telegram needed
   to validate the logic; no unverified outcome may appear as verified.
3. **LAMP in isolation, preview only.** Trigger on a new bulletin independently of
   price movement. Compare like-for-like remaining hours; distinguish revisions
   from hours expiring. Test bucket boundaries, delayed receipts, missing TMP,
   stale forecasts, duplicates and forecast changes with no new price. Preserve
   UNKNOWN when market coverage cannot support a reaction claim.
4. **Combined one-city finite qualification.** Feed shared sources to both tracks
   while retaining independent events and deduplication. Test restart, gaps,
   source outages, original-data integrity and the combined 256-MiB budget. Replay
   both independently and compare their frozen records. Do not rewrite the
   existing price detector or combine their outputs into a probability.
5. **One shared sender, separately authorized.** After separate review of PR #5,
   route approved previews under 🔒 LOCK ALERT and ☀️ LAMP ALERT. Verify per-track
   duplicate suppression and one track's failure not blocking the other. Obtain
   explicit approval before live test messages or any deployment.
6. **New-day evidence, then expansion.** Evaluate both tracks separately on unseen
   station-days: correctness, false warnings, price response, liquidity, spread
   and hypothetical net results after costs. Expand to the other nine supported
   cities only after one-city acceptance and station/rule checks. Stable server
   operation, date rollover and a bounded 48-hour run require their own scope and
   authorization, with measured storage/WAL/logs, CPU and memory. No dependence
   on Mac/Codex/POLYMETEO should be introduced as a permanent runtime design.

No own Tmax model, probabilities, automatic trading, new services or production
changes are authorized by this plan. Delivery success is not strategy success.
