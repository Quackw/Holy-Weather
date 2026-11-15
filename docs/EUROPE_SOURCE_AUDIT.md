# Europe forecast-source audit — 15 November 2026

**Verdict: PARTIAL. No European forecast source is production-qualified.**
This is the first, documentation-only PR of the requested source programme.
No sources are activated, no database is migrated, no Telegram is sent. Runtime
PR #12 remains DRAFT. A source being reachable is not a verified forecast alert.

## Repository / independent review

Reviewed main `98ae9d1b40ce1b1cb1e376a343927944cab2d28a`, accepted PRICE PR #9,
LAMP PR #10, queue PR #11, and DRAFT #12 at
`16b0aaeacb2b99c85d0ac3c3af54e33ef1c0d839`. PR #12 has 18 changed files,
836 additions / 10 deletions; do not append the whole five-stage programme to it.
Its 395 offline tests passed again during this audit; source check passed.
Accepted main has no European forecast loader. The current data path already
provides one SQLite writer, original inputs, source/receipt/availability clocks,
independent PRICE/LAMP state, atomic event/queue writes and explicit UNKNOWN sends.
Reuse these; do not create another database service or sender.

Findings requiring qualification before rollout:

1. **Station ID is insufficient.** Original retained MOSMIX_L KML coordinates
   differ materially from AWC for London and Moscow (table below). PR #12's exact
   ICAO/point checks do not detect that. These forecast mappings are BLOCKED,
   pending authoritative explanation or an explicitly reviewed alternative.
   Do not silently change the point ID or relabel another airport.
2. MOSMIX is decode-only. No source-age policy, scheduled ingestion, durable
   issue ledger or European forecast-to-queue path exists. `NOT_QUALIFIED` is
   intentional. The decoder's complete remaining-hour field now distinguishes
   truncated input from a complete peak, but no live consumer is connected.
3. The original LAMP event machinery retains US names/units and a two-hour age
   policy. Reusing it requires a source adapter; renaming old rows is prohibited.
4. Polling survives bounded WS outages in PR #12, but sender ticks still occur
   inside the WS loop. Forecast events can queue during an outage; prompt delivery
   through a prolonged outage is not proven. Slow WS connect can delay main-writer
   consumption up to the connect timeout. Worker HTTP arrival is not availability.
5. The observer stops at local midnight; it does not discover/start the next
   contract. No supervisor or daily autonomous readiness exists.
6. Real three-city Linux evidence establishes PRICE/AWC windows only. Zero events
   means empty-queue restart checks cannot prove real event delivery dedupe.
   Two local replays are deterministic, but replay sample counts differ from live
   when retained GAP diagnostics are also applied. Preserve this limitation.
7. PR #12 description still says 351 tests / old PARTIAL Mac run. Current code
   has 395 tests and a subsequent finite Linux PASS. Read the committed reports;
   update metadata before acceptance rather than treating the description as proof.

## Official-source comparison

Publication time below is never interchangeable with model issue time. Download
latency on our server, CPU, peak RSS and a 24-hour resource rate are **NOT MEASURED**
for these candidates unless explicitly noted. Documentation cadence is not a live
availability measurement. No GRIB or common MOSMIX_S archive was downloaded here.

| Source | Original access / licence evidence | Cadence and payload | Applicable cities / point access | Qualification |
| --- | --- | --- | --- | --- |
| DWD MOSMIX_S | Public directory, no registration [1]; DWD legal notice fetch returned 403, commercial licence verification incomplete | Official description: 24 issues/day, hourly horizon to +240h [2]; current native index has consecutive hourly files around 36 MB | Candidate for all 11 via station catalogue, but actual S station records not decoded; common archive only [3] | PARTIAL access; BLOCKED rollout pending licence, resource and coordinate qualification |
| DWD MOSMIX_L | Public per-station KMZ; same licence gate | Saved 03/09/15/21 UTC issues; original TTT K and hourly valid times; small point files | All 11 IDs decoded previously; London/Moscow coordinate conflict; per-station download supported | PARTIAL; useful low-cost fallback candidate, not wired |
| Met Office UKV | Weather DataHub requires API credentials/subscription [4]; some plans free within quota, product-specific licence/attribution | Original UKV release schedule/payload NOT VERIFIED; site-specific blended output must not be called original UKV | London candidate; exact UKV airport extraction NOT TESTED | BLOCKED original API qualification without account/product selection; not rejected scientifically |
| Météo-France AROME | API requires account; separate public packages exist [5]; package licence Open Licence 2.0 | Hourly valid steps; GRIB2; published packages shown 325.9–773.8 MB, not downloaded | EURW1S40 bounds 37.5–55.4N, 12W–16E: London/Paris/Amsterdam/Munich/Milan/Madrid candidates; grid extraction required | PARTIAL; full packages UNSUITABLE for current 256 MiB directory; API subset requires qualification |
| KNMI HARMONIE | Official anonymous key allows unregistered access with shared quota; registered key recommended [6]; dataset licence still to verify | Hourly cycles, +60h hourly, TAR/GRIB1; full domains only, no per-location/parameter cutouts [7] | EHAM priority; other cities require actual domain/grid match, not the label Europe | PARTIAL documentation; NOT TESTED original files; no silent account/key fallback |
| MET Nordic | MET Norway own service; NLOD2/CC-BY4 attribution [8]; no API key, identifying User-Agent/cache policy | Hourly Nordic updates; coordinate JSON is postprocessed MEPS with later ECMWF horizon [9], not pure original MEPS | EFHK candidate, short-range Nordic fields; original data via THREDDS also documented | PARTIAL; service endpoint available in docs, original consecutive forecasts NOT TESTED |
| DWD ICON-EU | Public GRIB directory [10]; same DWD licence verification outstanding | Directory cycle folders 00/03/06/09/12/15/18/21; file step spacing and actual publish latency NOT VERIFIED | Europe grid, each airport/grid/elevation pairing NOT TESTED; not a station forecast | PARTIAL directory access; NOT TESTED decoding/resources |
| ECMWF IFS | Official free subset CC-BY4 permits commercial redistribution with attribution [11] | GRIB2; 0.25° and newer 0.1° product; official page states finer product delayed two hours relative to lower resolution; exact selected steps/cadence NOT VERIFIED | Global coverage; airport grid/altitude mismatch must be explicit; no point-native station forecast established | PARTIAL documentation, NOT TESTED actual cycle/grid resources |
| Open-Meteo (comparison only) | Free API restricted to non-commercial use; commercial plan required for commercial use [12] | Aggregator response must retain provenance and cannot prove native model publication timing | Potential comparison across cities | NOT TESTED; not chosen as original-source replacement |

## Native MOSMIX_S directory evidence

One bounded local GET returned HTTP 200, 6,854 bytes, 1,522 ms request duration.
This is directory transfer time on Mac, **not** archive delivery latency on server.
Saved evidence: `evidence/source-audit-20261115/INDEX_RECEIPT.json` and original HTML.

| File issue encoded in filename (UTC) | Directory modified time (UTC) | Advertised compressed bytes |
| --- | --- | ---: |
| 2026-11-15 11:00 | 11:39:32 | 36,320,697 |
| 2026-11-15 12:00 | 12:39:17 | 36,199,229 |
| 2026-11-15 13:00 | 13:39:17 | 36,265,889 |
| 2026-11-15 14:00 | 14:39:17 | 36,197,986 |

This supports repeated hourly publication metadata, not measured first appearance.
Archive issue header, TTT, valid times, correction semantics, CRC/SHA, CPU and RSS
remain unmeasured. Directory mtime is not a cryptographic publication receipt.
A 36 MB file downloaded independently for 11 cities is unacceptable duplication.
One immutable source object per issue must feed all relevant station projections.

## Airport-coordinate audit (retained original MOSMIX_L versus AWC)

DWD catalogue coordinates use degrees/minutes, not decimal degrees. The following
comparison uses decimal coordinates embedded in original KML, avoiding that
conversion ambiguity. Heights are metres, native source values; no lapse-rate
correction or nearest-airport substitution is performed.

| City | ICAO / DWD point | KML lat, lon, height | AWC lat, lon, height | Separation km | Forecast mapping |
| --- | --- | --- | --- | ---: | --- |
| London | EGLC / P0478 | 51.48, -0.05, 5 | 51.505, 0.055, 10 | 7.78 | BLOCKED coordinate conflict |
| Paris | LFPB / 07150 | 48.97, 2.43, 67 | 48.967, 2.428, 50 | 0.36 | PARTIAL, height difference explicit |
| Amsterdam | EHAM / 06240 | 52.30, 4.77, -4 | 52.315, 4.790, -2 | 2.15 | PARTIAL |
| Munich | EDDM / 10870 | 48.37, 11.80, 453 | 48.348, 11.813, 445 | 2.63 | PARTIAL |
| Milan | LIMC / 16066 | 45.62, 8.73, 234 | 45.631, 8.728, 221 | 1.23 | PARTIAL |
| Madrid | LEMD / 08221 | 40.45, -3.55, 609 | 40.466, -3.555, 589 | 1.83 | PARTIAL |
| Warsaw | EPWA / 12375 | 52.17, 20.97, 106 | 52.163, 20.961, 107 | 0.99 | PARTIAL |
| Helsinki | EFHK / 02974 | 60.32, 24.97, 51 | 60.327, 24.957, 56 | 1.06 | PARTIAL |
| Moscow | UUWW / P0567 | 56.00, 37.28, 203 | 55.592, 37.261, 195 | 45.38 | BLOCKED coordinate conflict |
| Istanbul | LTFM / F9731 | 41.27, 28.75, 99 | 41.262, 28.740, 99 | 1.22 | PARTIAL |
| Ankara | LTAC / 17128 | 40.12, 32.98, 953 | 40.128, 32.995, 952 | 1.55 | PARTIAL |

Evidence: `evidence/source-audit-20261115/COORDINATE_AUDIT.json`, original
`evidence/europe-catalog-20261115/*-mosmix.kmz` / `*-awc.json`, station catalogue.
Smaller offsets are not a blanket PASS: obtain source metadata justification and
report elevation differences. These L records do not establish S mappings.
Prior rule verification covers 121 retained current-day contracts, Celsius nearest
whole degree with exact halves towards the higher integer. It is not a perpetual
approval for later contracts. Preserve IANA zones and UTC identities, including
23/25-hour days; never use Kyiv's date for an airport market.

## Decision / readiness matrix

**Primary candidate:** MOSMIX_S, conditional on resource, licensing and point
qualification. **Fallback candidate:** MOSMIX_L, visibly distinct cadence/age,
never blended or silently substituted. No primary is operationally approved for
any European city yet. Regional integration only after measured incremental value.

| Path | Status | Direct evidence / limitation |
| --- | --- | --- |
| US PRICE / LAMP code | PASS scoped offline | Accepted PRs; current real US health NOT TESTED here; real sequential LAMP change not proved by old one-issue replay |
| Europe PRICE | PASS finite 3-city windows | London/Paris/Amsterdam Linux real bid/ask and updates; no event; other cities not live-qualified on Linux |
| Europe FORECAST | BLOCKED London/Moscow; PARTIAL others | Decoder exists in DRAFT #12, no operational source/alerts |
| AWC METAR | PASS finite captures | 11 stations previously returned data, 3 fresh Linux contexts; not continuous coverage |
| Telegram queue | PASS tested mechanism | Two historical synthetic TEST deliveries; new FORECAST card/real event delivery NOT TESTED |
| SQLite / resources | PARTIAL | Integrity/FK and bounded short tests pass; common archive CPU/RSS/traffic NOT MEASURED |
| Clocks / DST | PARTIAL | Linux NTP flag and finite quote clocks; 41 calendar tests; no live clock-jump test |
| Day handoff / supervisor | BLOCKED autonomous rollout | Stops at midnight, no verified next-contract transition or permanent supervisor |

**Real European FORECAST ALERT count: 0.** No new Telegram previews are labelled
real events. No production READY claim. Rollback of this documentation-only PR:
revert its docs; runtime, data, schema, cursors and historical event names unchanged.

## Primary references

[1] https://opendata.dwd.de/README.txt

[2] https://www.dwd.de/EN/ourservices/met_application_mosmix/mosmix_application_description.pdf?__blob=publicationFile&v=2
(official indexed description; direct document retrieval previously denied,
not represented as fully read)

[3] https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S/all_stations/kml/

[4] https://datahub.metoffice.gov.uk/docs/getting-started and https://datahub.metoffice.gov.uk/support/faqs

[5] https://www.data.gouv.fr/dataservices/api-modele-arome and https://www.data.gouv.fr/datasets/paquets-arome-resolution-0-025deg

[6] https://developer.dataplatform.knmi.nl/open-data-api

[7] https://english.knmidata.nl/open-data/harmonie

[8] https://api.met.no/doc/License and https://api.met.no/doc/TermsOfService

[9] https://api.met.no/doc/locationforecast/datamodel and https://api.met.no/doc/locationforecast/FAQ

[10] https://opendata.dwd.de/weather/nwp/icon-eu/grib/

[11] https://www.ecmwf.int/en/forecasts/datasets/open-data

[12] https://open-meteo.com/en/terms
