# Ten-city AWC/LAMP qualification

Extends accepted stage 3 (61e7e1f5aaa67019626915a3b274a52682f5c407). No market detector changes, new dependencies, services, models, probabilities, Telegram or production operations. One finite run still observes one explicit contract; all ten city configurations are now supported.

| City | Station | Local timezone |
|---|---|---|
| New York | KLGA | America/New_York |
| Atlanta | KATL | America/New_York |
| Miami | KMIA | America/New_York |
| Chicago | KORD | America/Chicago |
| Dallas | KDAL | America/Chicago |
| Austin | KAUS | America/Chicago |
| Houston | KHOU | America/Chicago |
| Los Angeles | KLAX | America/Los_Angeles |
| San Francisco | KSFO | America/Los_Angeles |
| Seattle | KSEA | America/Los_Angeles |

## Invariants

Gamma must identify the exact local date, station, AWC resolution source, explicit bucket and YES/NO token mapping. Unsupported settlement rules fail closed. Source URLs are generated only for allowlisted stations; receipt URL and raw bulletin/report station must agree with the selected contract. There is no nearby-station fallback.

AWC observation time, receipt time and processing availability remain separate. LAMP nominal issue time is not treated as publication time. Context uses only already received/available inputs. Remaining hourly points must be strictly later than the event and within the station's market local date. Original forecasts are never clamped to the received observed maximum. Exact half rounding towards the higher integer yields finite whole-Fahrenheit bucket boundaries [lower−0.5, upper+0.5).

No schema migration. Existing KLGA inputs and defaults remain readable. Separate city/date contracts must use separate owned directories; rebinding an existing database remains forbidden. The qualification allocates 25 MiB per city and checks combined data against 256 MiB. The quote freshness limit remains 90 seconds.

## Verification

162 local and Linux tests pass, including all previous 121, per-city HTTP routing, station mismatches, local midnight and UTC rollover, event attachment and restart. The initial live bundle ran 152 tests; the final additional HTTP-routing tests were checked in a separate offline Linux run (162 passed), without repeating live collection. Raw artifacts remain outside Git under evidence/ten-city-20261114.

## Real Linux run: 2026-11-14

Ten sequential finite 20-second observations succeeded; no persistent process remains. Gamma contract rules, station and tokens passed validation for all ten. AWC and original LAMP decoded successfully for 10/10, with fresh observation context. This is a finite qualification, not continuous availability.

| City | Bucket °F | Actual unrounded bounds °F | AWC / LAMP | LAMP issue UTC | LAMP receipt UTC |
|---|---|---|---|---|---|
| atlanta (KATL) | 54-55°F | [53.5, 55.5) | OK / OK | 15:30:00 | 16:37:09.118 |
| austin (KAUS) | 68-69°F | [67.5, 69.5) | OK / OK | 15:30:00 | 16:38:54.693 |
| chicago (KORD) | 46-47°F | [45.5, 47.5) | OK / OK | 15:30:00 | 16:38:12.479 |
| dallas (KDAL) | 62-63°F | [61.5, 63.5) | OK / OK | 15:30:00 | 16:38:33.588 |
| houston (KHOU) | 66-67°F | [65.5, 67.5) | OK / OK | 15:30:00 | 16:39:15.858 |
| los-angeles (KLAX) | 70-71°F | [69.5, 71.5) | OK / OK | 15:30:00 | 16:39:36.978 |
| miami (KMIA) | 72-73°F | [71.5, 73.5) | OK / OK | 15:30:00 | 16:37:51.433 |
| new-york (KLGA) | 46-47°F | [45.5, 47.5) | OK / OK | 15:30:00 | 16:36:48.655 |
| san-francisco (KSFO) | 64-65°F | [63.5, 65.5) | OK / OK | 15:30:00 | 16:39:58.153 |
| seattle (KSEA) | 56-57°F | [55.5, 57.5) | OK / OK | 15:30:00 | 16:37:30.238 |

Total 1097 frames / 751417 wire bytes. Closed databases total 1187840 bytes; pre-close data directories including WAL/logs total 5510024 bytes, below 256 MiB. All ten SQLite integrity checks passed.

One Dallas YES upward crossing from 47¢ to 50¢ was detected, with already received KDAL weather and LAMP attached. A reverse crossing was suppressed by existing hysteresis. Thirteen out-of-order samples were marked (NY9, Dallas3, LA1); baseline gating was preserved. No event or movement guarantee is inferred from missing messages. Two cold saved-input replays per city are identical and preserve original SHA. Coalesced unchanged messages are not recreated, so replay frame/baseline counters need not equal the wire run.

An initial Mac Gamma attempt timed out during TLS; it confirmed no rules. No repeated Mac request was made. The planned Linux qualification used public endpoints successfully. A local artifact extractor initially expected the wrong SHA field; parsing the already saved transport output was corrected and each database verified against db_sha256, with no remote rerun.

Compatibility: the accepted stage-3 real KLGA database was copied, processed by the new code, opened and written by the exact prior stage-3 code, then reopened by the new code. New records survived, schema remained version 1 and the original SHA was unchanged. This rollback check covers the previously supported KLGA configuration; old code does not gain support for the seven newly enabled cities.

Live source bundle SHA256: 25f36ee8b0bfe2daab7e7b0eb1a13672ef866fce6da6de06af13d1c6b574cbd2. Final offline Linux bundle SHA256: 36d93d2f74315685b94f937c2c79ebf11d048d0b8cb87780ad661a8de4efd794. Per-city raw rules, weather, forecast, event and database SHA remain in the ignored evidence directory.
