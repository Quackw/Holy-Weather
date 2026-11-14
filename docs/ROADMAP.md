# Roadmap — gated PRICE / LAMP delivery

One project, two notification algorithms, one bot. [Strategy](STRATEGY.md) is the
source of product rules. No third winner/lock strategy. No automatic merges,
production changes, deployments, real orders or Telegram sends.

## PR A — concept correction (current task)

Update README, AGENTS, STRATEGY and this roadmap only. Preserve all runtime and
historical evidence. Close superseded PR #7 without deleting its branch; retain
its independent LAMP comparison for later review. Keep PR #5 open and unmerged.
**Stop after PR A and await acceptance.**

## PR B — PRICE ALERT (after A acceptance)

Inspect original @holy_nfx_bot source/version and verify midpoint, threshold and
rearm rules before writing code. Resolve mismatches explicitly. Build the small
independent Decimal detector without changing existing research events. Test
upward 50/55/60/70/80 crossings, MULTI-CROSS, each side's exact token, missing quotes,
baselines, stale/out-of-order data, gaps, duplicates and restart with durable
one-time threshold state/events. Verify SQLite compatibility and rollback. One
New York contract first. No Telegram sends. Await acceptance before PR C.

## PR C — independent LAMP ALERT (after B acceptance)

Reuse original LAMP ingestion and suitable PR #7 comparison logic, without its
removed strategy. Freeze the proposed 2°F peak-change configuration before tests;
verified boundary crossings independently qualify. Test matched remaining hours,
causality, first issue, duplicates, corrections, stale/missing data, restart and
weather-triggered events with unchanged prices. Use genuine saved bulletins;
report absent real changes honestly, separating synthetic cases. No new model.
Await acceptance before PR D.

## PR D — one durable Telegram queue (after C acceptance)

Adapt PR #5 groundwork to 📈 PRICE ALERT / ☀️ LAMP ALERT. Persist both event types,
separate dedupe and reasons; verify pending restart recovery, expiry, confirmed
send suppression and explicit UNKNOWN delivery. Keep collection independent of
sender failures. Show short English previews with exact bucket links; technical
baselines/suppressions remain journal-only. Request separate approval before real
messages. Do not merge PR #5 as-is or assume its recent-event polling is a durable
shared queue.

## After combined one-city acceptance

Consider the other nine supported cities and a bounded live qualification under
separate authorization. Measure storage, gaps, delays and independent algorithm
quality. No permanent service or 48-hour deployment is implied by these PRs.

Each PR report: changes; real versus synthetic evidence; test count; SQLite and
false-alert/restart checks; GitHub CI; PR link; next gated step.
