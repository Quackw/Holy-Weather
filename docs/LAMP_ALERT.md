# Independent LAMP ALERT — PR C

Default-off, New York/KLGA only. Enable in an existing finite weather/LAMP config:

```toml
weather_context = true
lamp_context = true
lamp_alert_enabled = true
lamp_peak_change_f = "2"
```

A bulletin ingest, not a price update, runs the detector within the existing
SQLite owner transaction. Two valid forecasts are compared at the new bulletin's
actual main-loop availability time, with identical complete sets of future UTC
hours on the contract's local day. First forecast is baseline. Past hours and
next-day hours cannot cause an alert. Different horizons and stale/invalid/partial
forecasts record explicit reasons. Invalid data clears the comparison baseline.

An event requires absolute peak change >= 2°F OR a crossing of the verified
lower/upper raw-temperature boundary in either direction. Both conditions yield
one event. Boundary semantics use the existing half-up contract verification;
no integer-boundary substitution. A one-degree boundary crossing qualifies.
The threshold is pinned in state: changing it in a populated research run refuses
rather than silently changing definitions. First use is configurable before tests.

Original forecasts are retained separately, including issue, receipt, availability,
SHA and hourly values. Event freezes both forecasts, compared hours, current causal
METAR/observed maximum, available side prices and freshness, exact market URL and
boundary evidence. No probability, own Tmax, inferred price reaction or trading.
Later observations cannot modify earlier events. Price data may be unavailable;
that cannot block a genuine forecast-change event. No Telegram formatter or sends
are added here; a downstream PR must filter LAMP_ALERT separately from PRICE_ALERT.

## Persistence and limits

Existing schema version 1, no migration/dependency/service/database. One bounded
meta record stores prior forecast and up to 256 distinct bulletin hashes. Repeated
bodies cannot alert again across restart; returning old data cannot heal a failed
comparison. State, event and evidence links commit atomically. Prior baseline input
is protected with a latest pointer, event inputs retain old/new bulletins and
weather/quote/discovery references. Normal history retention remains unchanged.
Diagnostics stay bounded to 512 rows. The 1-MiB LAMP input ceiling and global
256-MiB directory budget remain unchanged. Ledger exhaustion fails explicitly;
no silent eviction or budget increase. No lifetime-service claim.

Disabling the new flag reopens the same database without erasing evidence; research
rule hash is unchanged. Old-binary deployment rollback has not been qualified.
PR C is not a deployment request. New consumers must distinguish event types.

## Qualification

204 tests passed locally, including ten LAMP ALERT cases: independent no-price
trigger, same-issue revision and newer issue, boundary-only versus >=2°F changes,
downward change, expired peak hour, stale/partial data, duplicate and restart,
clock rejection, pinned configuration and injected SQLite event-write rollback.
Previously accepted PRICE restart/dedupe/coalescing tests remain in the suite.
Compile/source checks pass. Linux CI runs all tests using synthetic responses.

Real saved KLGA 14 November artifacts replayed twice with original SHA checks:
zero LAMP alerts, because only one unique bulletin is present. Counters/event rows
match, SQLite integrity is ok, each closed owned directory is 192,532 bytes.
This is real source ingestion/baseline evidence, not proof of a real forecast
revision. No new provider requests or Telegram/production activity occurred.

Synthetic illustration only: 46°F → 48°F crosses 47.5°F for the 46–47°F bucket
and meets 2°F. One LAMP event is stored, even with zero market quote messages.
Technical baselines and suppressions are diagnostics, not notification events.

Next: review/accept PR C, then separately adapt the shared Telegram queue in PR D.
Do not merge or send automatically. More real saved issues are needed to prove a
real forecast-change example; do not manufacture one from duplicate downloads.
