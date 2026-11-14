# Stage 2 — KLGA weather context, 14 November 2026

Scope: observed weather context for one verified New York contract. No forecasts,
probabilities, Telegram, orders, new service or production installation.

## Interface and causal boundary

[Official AWC API](https://aviationweather.gov/data/api/) and its
[OpenAPI schema](https://aviationweather.gov/data/schema/openapi.yaml) specify
`icaoId`, `rawOb`, `temp` in Celsius and `obsTime` as Unix seconds. The application
retains exact response bodies and raw METAR, checks station/raw clock/temperature,
and records its own HTTP receipt and main-loop availability clocks. Provider
`receiptTime` is retained in the body but never substituted for our receipt time.

Weather enrichment is opt-in (`weather_context=true`) and limited to KLGA. Gamma
still verifies station, date, rules, exact bucket and YES/NO token identities.
Each detected or suppressed event freezes the observations already available
strictly before its local receipt time. The market source clock remains separate.
Late METAR and COR update only future contexts. Duplicates preserve first receipt
and cannot create a new-observation flag. Ambiguous non-COR revisions are excluded
and their conflicting evidence is protected. Maximum is only among received
observations on the station-local market day, never official daily Tmax.

Weather age up to 90 minutes is explicitly labelled FRESH research context;
coverage always remains PARTIAL_RECEIVED_OBSERVATIONS. This is separate from the
unchanged 90-second market freshness limit. A weather/price coincidence is not a
causal claim or a trading signal.

## Real bounded observation

Linux Python 3.13.5 ran 86 tests and one 120-second public observation in an
unprivileged isolated temporary directory. No existing service, production file
or working POLYMETEO database was touched. Standalone source and pinned existing
dependencies were copied to the directory, with no system installation.

Verified market: New York, 14 November 2026, 46–47°F; KLGA / America/New_York.
The run received 693 market frames (462,747 wire bytes) and retained 95 samples:
72 FRESH quotes, 22 AWAITING_BASELINE quotes, one timed trade. There were 1,164
coalesced unchanged confirmations and 12 rejected out-of-order messages. The
existing monitor recorded gaps; those intervals are not claimed as continuous
healthy observation. After a finite run ends the connection is unconfirmed.

AWC HTTP200 supplied 35 reports across the requested 24 hours, including 16
observations on the market day. Receipt: **15:30:27.621 UTC**, main-loop availability
**15:30:28.345 UTC**. Latest observation: **14:51 UTC, 5 °C**; previous: **13:51 UTC,
5 °C**; delta **0 °C**; maximum among received market-day observations **5 °C**.
At a retained quote at **15:32:26.528 UTC**, weather age was **41.44 minutes**,
FRESH with partial coverage. No new observation arrived within that quote window.

The valid YES asks ranged from **68 to 69.7¢**. **Zero configured price events**
were generated. This is a valid no-event result; no live event or warming signal
is fabricated. The real quote-context preview is explicitly not a threshold event.

Closed SQLite: **180,224 bytes (176 KiB)**; measured pre-close data directory:
**780,453 bytes**. Measured process disk-write bytes: **4,947,968**. The full isolated
qualification directory including source/dependencies/test fixtures occupied
**16,530,888 bytes**. These short measurements are not a daily growth forecast.
The global directory budget remains **256 MiB**; weather has a subordinate 4-MiB
compressed-input ceiling. No history was deleted to satisfy a test.

Database SHA256:
`e7d8301c0702c0de13f7e468561ddaf37579fdd5c08ec6b5091ed274ff9a571e`.
Full integrity is ok; foreign-key errors are zero. Every retained raw input was
uncompressed and verified against its recorded SHA. Evidence stays outside Git.

## Reproduction and regression evidence

The final offline suite has **89 tests** (59 stage-1 regressions plus 30 weather
cases). It covers normal receipt, absence, stale/future data, wrong station,
invalid raw clock/temperature, duplicate, COR, ambiguous revision, restart,
transaction rollback, weather budget, slow worker, HTTP204/403/429/500, total AWC
failure with continuing market events, and strict no-future-data evidence.
Linux GitHub Actions verifies the final PR head; CI is entirely synthetic/offline.

Two cold replays of saved real market inputs plus the genuine AWC receipt and
recorded gaps have identical results: zero events, weather OK. Because unchanged
confirmations were coalesced, this is a saved-input replay, not the complete wire
stream; replay sample counts need not equal the original run. No missing frames
are invented. Original database and source SHA remain unchanged.

A separately labelled synthetic stored example exercises the event join:
YES ask **49→60¢**, event received **12:00:00.030 UTC**; latest weather **11 °C**,
previous **10 °C**, delta **+1 °C**, observed maximum **11 °C**. The new observation
was received at **12:00:00.020**, available at **12:00:00.021**, before the event.
A later 15 °C report does not modify that event in the regression test. This is
causality proof from a controlled fixture, not a real market opportunity.

## Compatibility and failures

No SQLite schema change or migration: inputs, latest, meta, events and event_inputs
are reused. Weather-enabled and disabled configurations preserve the stage-1
market rule hash. On a copy of the accepted stage-1 database, enabling weather
preserved all original rows. The actual tagged stage-1 code then opened and wrote
new market samples, and stage-2 code reopened it retaining new weather/market
history. Schema version remains 1; integrity and foreign keys pass. The original
stage-1 database SHA remains
`b4ba74f512ce71c3389a3de589ecb76c92a9213e7b02234c8754b321cea3ecf3`.
Rolling back code retains records; stage-1 code does not enrich subsequent events.

Mac Gamma TLS failed (Python handshake timeout and a bounded curl TLS diagnostic).
AWC on Mac succeeded. The joint live path was therefore qualified on Linux. No
provider deny was bypassed and no alternate identity or API address was used.
A weather error terminates weather attempts for the finite run while market
processing continues. Shared SQLite integrity/write failures remain critical.

Remaining scope limit: no real threshold-crossing event occurred during the
bounded window; the combined event payload is proven by regression, with actual
concurrent source acquisition and deterministic real saved-input replay verified
separately. No scientific or trading advantage is claimed. No automatic merge.
