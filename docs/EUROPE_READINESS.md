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
