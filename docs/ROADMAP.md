# Roadmap — PRICE first, independent FORECAST second

One project, two independent alert directions, one Telegram bot. No LOCK,
probability model, automatic orders or additional services. PRICE thresholds
and once-only rules remain fixed. Accepted US LAMP remains intact while forecast
expansion is deferred. See [stage 0 audit](PRICE_ACCEPTANCE_ORDER.md).

## Accepted foundation

PRs 1–4: finite market/weather/LAMP context and US city profiles.
PR 8: two-alert concept; PR 9: PRICE; PR 10: independent KLGA LAMP;
PR 11: shared durable queue, short cards and delivery handling.
PRs 5/7 are preserved historical/superseded work, not pending prerequisites.

## Sequential acceptance gates

0. **Current:** audit main and drafts 12/13/14; repair documentation; prepare
   stage 1. Stop for acceptance. No runtime changes.
1. Verify PRICE rules, bounded clock policy using Mac/Linux source evidence,
   reordered messages, batch subscriptions, gaps, restart and resource limits.
2. Accept the PRICE foundation from PR 12 with forecast research separated;
   qualify London/Paris/Amsterdam, then the other eight profiles individually.
3. Finish PR 14 after PR 12: all verified buckets, one writer/queue, bounded
   subscriptions, per-side state, delivery accounting and measured resources.
4. Accept PRICE Telegram evidence: atomic queue, exact links, fresh delivery,
   restart dedupe, UNKNOWN handling and collection despite sender failure.
   Existing approved deliveries count as evidence; repeat sends need no invention.
5. Separately design daily rollover, recovery, storage and delivery monitoring;
   request authorization for bounded daily qualification before permanent service.
6. Only after PRICE acceptance: resume PR 13 European source research and verify
   original sequential forecasts, station mapping, licensing and resource costs.
7. Implement qualified European FORECAST in the existing second direction;
   retain NOAA LAMP in the US, separate source identity and Celsius thresholds.
8. Qualify both independent directions together, including daily boundaries,
   outages and resource use. No trading.

Each gate reports code/tests, immutable replay, live quotes/events, actual delivery
and resources separately. Stop before PR acceptance; the next gate needs user
acceptance and authorization. Do not merge automatically. Do not delete branches,
source history or evidence. Existing finite session permission is not permission
for production/systemd, tomorrow's contracts or additional Telegram TEST messages.
