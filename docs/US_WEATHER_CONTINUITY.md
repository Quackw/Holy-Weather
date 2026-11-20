# US weather through PRICE network outages

This is an opt-in code change, not an installed service change. Start from main
`c32e3ba8d58b8b1b95bd350512a679fbe65a5846` (PR #49). Open PR #48 is excluded;
the already installed US fixes #40/#41 must remain in a later installation build.
Europe and existing defaults retain their behavior.

## Configuration and ownership

Keep `us_lamp_enabled=true` and `catalogue_metar_enabled=true`. New options are:

| Plan option | Default | Meaning |
| --- | --- | --- |
| `us_weather_continuity_enabled` | `false` | Existing controller collects weather while a proven closed PRICE worker waits for network quota/backoff |
| `us_lamp_notice_mode` | `pre_peak` | Opt in to `warming_hourly` for the requested warming-start/hourly city notifications |

Both options require the US and both existing weather collectors. Disabled fields
are omitted from serialized plans; existing plan bytes and PRICE rule hashes
stay unchanged. Enabling these options changes the plan identity and requires
the existing explicit transition/rebinding procedure. Merely merging code does
not change a running plan.

There is one service and the existing SQLite. The PRICE child owns the writer
during ordinary operation, including its bounded weather collection after a
socket failure. After a known closed network result, the controller can own the
same writer during quota/backoff. It runs the existing catalogue with
`weather_only=true`: no market socket, no PRICE crossing or recovery charge.
It closes SQLite and the sender before the existing preflight and child launch.
HTTP helper threads return bounded results; they never open SQLite. The writer
flock rejects a simultaneous owner. No unowned/uncertain child is adopted.

Health distinguishes **PRICE paused / weather collecting** from all collection
paused. Notifications and watchdog calls come from actual controller progress;
no helper feeds a stalled controller's watchdog. The original 3/hour and 6/day
PRICE recovery budgets, ordinary backoff and durable recovery charge remain.
Controller reentry preserves their deadlines and history. Manual stop, corrupt
SQLite, code/plan mismatch and an occupied writer/controller lock still refuse.

NOAA uses one original collective per hour for all ten stations, with the same
publication allowance, timeout, retry and denial policy. Station forecasts are
saved even without a verified Polymarket contract. METAR keeps its existing
staggered five-minute station collection. Discovery/date checks continue; weather
ticks between city requests, without relaxing a blocked discovery request.
An absent or unverified market still prohibits a card, not weather collection.

## Warming start and hourly changes

This opt-in policy implements the user's explicit request for **any temperature
change**. It does not alter the legacy detector's configured 2°F threshold.

* The first issue of a local day is a baseline. Initial notification waits for a
  later fresh issue, complete remaining forecast hours, complete elapsed routine
  METAR coverage, a fresh observation and one verified bucket for that station/day.
* Find the first remaining forecast hour above the latest observed temperature.
  The initial card can be admitted in the hour leading to that warming start,
  including the start of the hour. This is an hourly forecast, not minute-level
  warming precision. It no longer waits until three hours before the peak.
  A daily peak already observed above the future maximum still blocks this card.
* Keep the airport-local 08:00 inclusive–20:00 exclusive sending window. The
  warming-start check runs with incoming source results and a bounded 30-second
  owner-loop review, with at most one-minute reevaluation while awaiting evidence.
* After the first card, review at one-hour intervals, combining NOAA and METAR
  into at most one new city card per review. A change in any identical remaining
  UTC hour counts, even 1°F or a non-peak hour. Any new METAR temperature change
  counts; an intermediate change followed by a return also counts if its report
  was actually collected. Unobserved fluctuations cannot be reconstructed.
* A new issue number, receipt time, wind or pressure alone does not count. Dropped
  past forecast hours do not count. Changed hours/temperature ranges are explicit
  in the card. Changes can produce a daytime follow-up after the peak, including
  cooling; no additional warming is claimed in that case.
* Night reviews update the comparison baseline silently. Missing/stale/incomplete
  source data or an incomparable/old review produces a recorded refusal/baseline,
  not retrospective changes. No synthetic weather or market data is substituted.

The original daily `us_lamp_city_v1:<ICAO>` ledger remains authoritative for the
initial card. Already recorded initial cards are not recreated. A mid-day switch
with an existing daily card establishes a new hourly baseline first. Hourly
state is bounded to one current projection per station; date changes preserve
the old projection as a compressed input in the existing archive path.

Every event freezes NOAA and METAR evidence and the exact verified bucket link.
Event, comparison state and queue admission share one SQLite transaction. No
per-bucket series, fabricated probability or third algorithm is introduced.
Daily bucket remains an estimate, using the larger of observed routine maximum
and remaining forecast maximum, with the existing contract rounding boundaries.

## Delivery, history and rollback

Only fresh, unclaimed PENDING LAMP can cross a weather/PRICE owner boundary.
The contract must still be active for the same local date; original `not_before`
and expiry are preserved. PRICE waiting for a bid still expires across the gap.
SENT and UNKNOWN never requeue; an uncertain result still disables the shared
sender while collection continues. The durable 3100-ms sender pacing survives
ownership changes. Neither a gap nor a weather update creates a PRICE crossing.

Hourly update expiry remains bounded by the original NOAA 2-hour validity,
METAR 90-minute age, 20:00 local and contract date. Only a warming-start card is
also capped by its future peak; a cooling update is not expired at a past peak.
Saved old cards, IDs, text and deadlines are not rewritten.

Schema v1, directory 256 MiB, LAMP inputs 1 MiB, queue 1024 unresolved items and
all existing archive/free-space limits remain. Existing *authorized* closed-owner
checkpoints run between bounded weather windows, under the same locks and history
budgets. No automatic new cleanup or budget increase is authorized by this code.
Old NOAA projections without markets can enter that same verified history under
the configured closed-day retention; UNKNOWN markets still pin evidence.

Rollback disables the two new options in an explicitly rebound plan and uses
compatible previous code on the **current** database. Do not replace it with an
older backup. New events, fired PRICE thresholds and delivery receipts remain.
The previous daily-only notice and weather-pausing behavior then return.

## Offline qualification

`tests/test_us_weather_continuity.py` runs real temporary SQLite and writer flocks,
managed clocks, original-format NOAA fixtures and test AWC responses through the
actual catalogue, ingestion and controller paths. No HTTP source or Telegram is
contacted. In the same 302-second simulated quota/backoff interval:

| Measurement | Existing mode | Continuity enabled |
| --- | ---: | ---: |
| NOAA station projections during waiting | 0 | 10 |
| METAR station projections during waiting | 0 | 10 |
| PRICE launches before quota + paid backoff | 0 | 0 |
| PRICE launches after admission | 1 | 1 |
| Extra recovery charges | 1 | 1 |
| Concurrent SQLite writers | 1 maximum | 1 maximum |

The new price after reconnection is a baseline; previously fired levels remain.
Tests cover interruption in unpaid wait and paid backoff, source collection
without markets, manual stop/version/integrity/lock refusals and unchanged old
plan hashes. Hourly tests cover all ten US cities on ordinary and both DST dates,
one-degree/non-peak changes, METAR changes, cooling, night silence, atomic queue
failure, new records before compatible code rollback and immutable SENT/UNKNOWN.
These are synthetic qualification results, not live NOAA latency, Telegram ACK,
24-hour uptime or a measured daily storage rate. Daily growth: NOT MEASURED.

PRICE detector source is unchanged. Retained Netflix `price.mjs` was rechecked:
SHA256 `4e304a3092ded5cc51ef0fe2a38aaa0a353fc64e3465e5b68e0d3194d068cee8`;
own-side midpoint, 50/55/60/70/80, once-only and REARM=false remain. This is source
provenance, not a claim about another bot's deployed binary.

No Netcup access, service change, real send, merge or installation is part of
this qualification. A later US-only installation must retain #40/#41 and #47/#49,
validate the exact package on Linux, preserve current records/counters/archives,
and obtain one precise installation approval. PR #48 is a separate decision.
