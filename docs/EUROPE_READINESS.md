# PR12 — PRICE-only European foundation

This PR adds European station/time-zone profiles, strict Celsius contract rules,
independent native YES/NO token validation, complete bid/ask readiness and a
local-date guard before connecting. PRICE thresholds and once-only semantics are
unchanged. NOAA LAMP is explicitly rejected for European stations.

Weather experiments are preserved at commit `16b0aaeacb2b99c85d0ac3c3af54e33ef1c0d839`
on `codex/europe-weather-experiments-preserved`. MOSMIX decoder/tests,
weather polling during WebSocket failure and LAMP timeout recovery are excluded
from this PR. Accepted US weather/LAMP behavior is retained. No new schema,
dependency, service, live send or deployment.

## Three-city real evidence

Saved Linux windows from 15 November 2026 were replayed twice with weather off.
Original Gamma receipts precede every market frame; their bodies/SHA and exact
station/rules/token selection were rechecked. This is historical qualification,
not a claim about current live market freshness.

| City | Station | Bucket | Frames | Stored samples | Fresh full YES / NO | PRICE events |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| London | EGLC | 13°C | 301 | 50 | 20 / 20 | 0 |
| Paris | LFPB | 13°C | 216 | 28 | 14 / 14 | 0 |
| Amsterdam | EHAM | 10°C | 1336 | 391 | 169 / 169 | 0 |

Both replays match exactly. Restart preserves events/queue and requires a new
baseline. Integrity is OK; foreign-key violations zero; user_version remains 1.
Original SQLite and compressed wire SHA are unchanged. No crossing occurred in
these windows; zero cards is the correct outcome. Synthetic tests separately
prove PRICE event → Celsius card → durable queue → restart for all 11 profiles.
Evidence: `evidence/price-foundation-20261115/RESULT.json` and `check.py`.

Rules identify routine AWC METAR at the exact airport, station-local date,
Celsius nearest-integer rounding with exact halves upward. Full reviewed text
is pinned (including SPECI/COR/fallback provisions); changed rules fail closed.
There is no settlement engine or automatic WU fallback. London is not Heathrow;
Paris is not Charles de Gaulle. PRICE qualification needs no weather download.

## Scope and remaining acceptance

All 11 profiles have offline rules/time/queue coverage. Only the three cities
above are qualified here on saved native Linux streams. This does not certify
all markets, uninterrupted delivery or a permanent service.

Stage 1 clock/reordering/group recovery changes live in unmerged PR14, not
silently bundled here. After accepting PR12, reconcile PR14 with this cleaned
base, remove inherited deferred weather changes, retarget and rerun CI. Merely
retargeting PR14 would reintroduce weather code and is not sufficient.

Limits remain 90 seconds and 256 MiB. Active observation is untouched. Reverting
this code must not restore an old database; preserve all evidence/queue files.
Accepted main does not support European configuration, so rollback to main means
stopping this isolated European configuration, not promising continued Europe
operation on old code. No destructive migration is involved.

Local verification: 359 tests passed; source/tests compile and diff whitespace checks passed. Linux CI is recorded on the PR head, separately from these local results.
