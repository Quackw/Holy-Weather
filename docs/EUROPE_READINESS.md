# Europe: PRICE/METAR first, original forecast qualification

15 November 2026. This is an isolated extension, not a deployed European fleet.
No Telegram sends, services, migrations, new dependencies or budget changes.

## Verified markets

| City | Exact station | Zone | Market units / rules |
| --- | --- | --- | --- |
| London | EGLC London City (not EGLL Heathrow) | Europe/London | Celsius, nearest whole degree, exact halves towards higher integer |
| Paris | LFPB Le Bourget (not LFPG) | Europe/Paris | Same Celsius rounding |

The current-day public Gamma responses identify both stations and AWC routine
METAR as resolution source. Keep SPECI exclusion, COR treatment, local dates and
original source rules. PRICE uses the unchanged own-token Decimal midpoint and
50/55/60/70/80% thresholds. Celsius labels do not change price arithmetic.
Discovery rejects unreviewed unit/source/station combinations. AWC uses the same
bounded source adapter. Both stations returned OK in the native qualification.

## Forecast direction: MOSMIX is not LAMP

NOAA LAMP does not cover these European stations. European LAMP configuration
fails before a request; the ten US city configurations remain supported.
DWD's original MOSMIX_L point forecasts are a promising source for the second
independent direction. The official station catalogue identifies:

- EGLC → **P0478**, LONDON/CITY INTL. 03772 is EGLL and must not substitute.
- LFPB → **07150**, PARIS-LE BOURGET.

A bounded, offline decoder reads one original KMZ, checks the point identity,
UTC issue/valid clocks, strictly hourly sequence and TTT temperatures in Kelvin,
and converts them to Celsius with Decimal. It preserves SHA, issue, request,
receipt and availability times. Missing TTT/temperature is never invented.
Only future hours on the station-local day enter the remaining peak.
No ZIP extraction, dependencies, new schema or background fetching are added.

Real source capture on 15 November: both original forecasts were issued at
09:00 UTC; remaining peaks at receipt were **9.70°C London** and **11.90°C Paris**.
These are original forecast values, not validated outcomes or trading signals.
A newer observed temperature must never clamp these forecasts upwards.
The received files and exact clocks are retained outside Git under
`evidence/europe-readiness-20261115/`.

**Forecast alerts are NOT connected yet.** Decoder returns
`freshness=NOT_QUALIFIED`, `alert_enabled=false`. One issue per point does not
prove new-issue handling, forecast accuracy, a justified freshness policy, or
live delivery. MOSMIX_L update cadence must not be mistaken for hourly LAMP.
The existing LAMP two-hour policy cannot silently be reused for this product.

Next small implementation: validate consecutive original MOSMIX issues, freeze
its source-specific age policy and Celsius material-change threshold, compare
identical remaining hours, then connect the same forecast-direction queue with
an honest source label. Do not introduce a third strategy or rename MOSMIX to
LAMP. PRICE can be qualified separately; no need to fabricate a forecast alert.

## Yesterday's TEST case

Saved test message 359 was Dallas, 14 November, 62–63°F YES, ask 47→50¢,
original LAMP peak 63°F. A fresh Gamma read reports the market closed,
`umaResolutionStatus=resolved`, YES/NO outcome values 1/0. This confirms the
winning outcome, not an executed trade or calibrated forecast skill. The later
synthetic New York TEST messages 360/361 are different cases.

## Primary references

- [NOAA LAMP coverage](https://vlab.noaa.gov/web/mdl/lamp-card-2.7.0)
- [DWD station catalogue](https://www.dwd.de/DE/leistungen/met_verfahren_mosmix/mosmix_stationskatalog.cfg?view=nasPublication&nn=16102)
- [DWD MOSMIX description](https://www.dwd.de/EN/ourservices/met_application_mosmix/mosmix_application_description.pdf?__blob=publicationFile&v=2)
- [DWD point files](https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/single_stations/)

## Full active catalogue expansion

The bounded public Gamma search on 15 November returned 204 results across five
pages and terminated with `hasMore=false`. This is the coverage of that current
search, not a guarantee about future listings. Ten European cities were found;
Ankara is tracked separately as an adjacent Turkish market. Prototype/polyv2
variants are not silently treated as the same contracts.

| City | Contract station | Local zone | Original MOSMIX point |
| --- | --- | --- | --- |
| london | EGLC | Europe/London | P0478 |
| paris | LFPB | Europe/Paris | 07150 |
| amsterdam | EHAM | Europe/Amsterdam | 06240 |
| munich | EDDM | Europe/Berlin | 10870 |
| milan | LIMC | Europe/Rome | 16066 |
| madrid | LEMD | Europe/Madrid | 08221 |
| warsaw | EPWA | Europe/Warsaw | 12375 |
| helsinki | EFHK | Europe/Helsinki | 02974 |
| moscow | UUWW | Europe/Moscow | P0567 |
| istanbul | LTFM | Europe/Istanbul | F9731 |
| ankara | LTAC | Europe/Istanbul | 17128 |

All 121 current-day bucket contracts passed the existing exact rules/station/token
checks. Native AWC requests returned OK for all 11 stations. All 11 original DWD
point files decoded successfully, issued at 09:00 UTC. These are source checks,
not proof of forecast accuracy or forecast-alert readiness. Forecast alerts remain
disabled pending consecutive-issue and freshness qualification.

The bounded observation uses a new confirmed native baseline for each connection,
90-second quote freshness, the existing PRICE algorithm and durable preview queue.
Telegram remains disabled until a real preview receives separate approval. No
service installation, production deployment or change to US alert rules.

Regression: 335 local tests passed, including city-specific PRICE queue/restart,
wrong station/unit, local-day and original forecast-point identity cases.
Original catalogue, receipt clocks and source files remain outside Git under
`evidence/europe-catalog-20261115/`.

## Resolution-source audit and finite live result

All 11 buckets per city have one identical rules SHA for that city. Every airport
ICAO reference agrees with the exact AWC resolution URL and configured station.
Europe discovery now rejects contradictory ICAO references even if the expected
station is mentioned elsewhere. No neighbouring-airport substitution is allowed.

Current rules use routine AWC METAR; SPECI does not count. The T remark supplies
tenths Celsius; absent that, the body temperature supplies whole Celsius. The
maximum is assigned to the nearest integer Celsius with exact halves towards
the higher integer (16.5 -> 17, -8.5 -> -8). Observation date is station-local,
including DST. The source-visible report counts whether or not labelled COR.
Ordinary corrections are considered until the following-day fixing; the rules
also reserve a seven-day exceptional correction/clarification procedure for
clearly erroneous source data.

Weather Underground is a conditional fallback if no qualifying AWC observation
is published by 23:59 ET on the next day. If neither source suffices by that
cutoff, the rules specify the lowest bracket. Do not treat HTTP timeout or stale
live context as authorization to switch to WU. No official settlement engine or
WU fallback is implemented here. Existing observed_max is context among received
observations (including SPECI), not the contract's resolved daily maximum; the
existing conservative conflict/COR handling is also not a final resolver.

Local finite WebSocket selection ran once for each of the 11 cities (15 seconds
per connection, no reconnect). It received real data but none met the selection
requirement of a complete baseline followed by a later valid confirmation within
that window. Several streams had source timestamps 1–19 ms ahead of actual Mac
receipt; these were rejected, not retimed. Other failures included missing top
quotes, old samples and no follow-up updates. No main observation/SQLite alert
queue was started, and zero Telegram messages were sent. An earlier London
Gamma request timed out before WebSocket; the subsequent selection preserved the
already received original catalogue clocks and SHA. The result is PARTIAL, not
an operational European alert fleet. No freshness limit was changed. Further
live qualification needs verified clock agreement and confirmed native updates.
