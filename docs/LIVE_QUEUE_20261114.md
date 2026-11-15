# Authorized finite live queue test — 14 November 2026

One run only, using the explicitly approved bundle SHA256
`ca8b5052258b17d3fa36bbd6d9f33e54c422a930bfb67aa4cc6f8c072a6ec097`.
No application-code modification, installation, service or production action.
Telegram was false in the test config and token overrides were removed from the
child environment. No Telegram requests or delivery claims occurred.

## Execution

- Existing pinned Linux test identity on the approved host; unique owned temporary
  directory `/var/tmp/holy-weather-live-queue-j8fpkp50`.
- New York/KLGA, November 14, 46–47°F, both exact tokens, rules and source verified
  by the normal discovery path. Contract open at discovery receipt.
- Start 21:21:33 UTC; observation completed at 21:23:34 UTC. Configured 120 seconds;
  loop elapsed 120.92 seconds, total child lifetime 121.29 seconds including setup
  and shutdown. Exit 0, COMPLETED_WINDOW, empty stderr. No second run.
- Same Gamma URL: HTTP 200, request/receipt interval about 0.0945 seconds on Linux.
  Prior Mac probes connected TCP but failed TLS before HTTP. Linux success
  localizes the observed failure to the Mac/network path; it does not prove the
  underlying cause or that local HTTPS is repaired. No 403 bypass or alternative
  identity was used.

## Observed result and limitations

| Evidence | Result |
| --- | --- |
| WebSocket frames | 16 |
| Stored quote samples | 2 initial side baselines |
| PRICE alerts / LAMP alerts | 0 / 0 |
| Queue records / delivery attempts | 0 / 0 |
| AWC | OK; latest received KLGA METAR 20:51 UTC, 7.8°C / 46.04°F |
| LAMP | OK; original issue 20:30 UTC, remaining hourly peak 45°F |
| SQLite after close | 122,880 bytes |
| Directory size reported before close, including WAL/SHM/log | 483,751 bytes (not a measured peak) |
| Inputs | 5: Gamma, two quote snapshots, AWC receipt, LAMP receipt |
| Integrity / foreign-key errors | ok / 0 |

The two quote source timestamps were 21:20:16.059 UTC, received at 21:21:33.853
UTC: about 77.8 seconds old on arrival. Health initially QUOTES_FRESH, then
PARTIAL_OR_UNUSABLE as the unchanged 90-second source-age limit elapsed. The other
15 frames were ignored depth/control messages according to normal counters; they
were not substituted for fresh BBO evidence. Thus this run does **not** prove
continuous usable quotes or the absence of all market movements. No reconnect or
provider-request loop was added to force fresh prices.

First side values are baselines, never historical threshold alerts. The only
LAMP bulletin is also a baseline, not a demonstrated change. Its original 45°F
future peak remains below the 46.04°F received maximum; no clamping occurred.
No genuine notification was formed, so no short card or send approval was needed.
Final CONNECTION_UNCONFIRMED follows the explicit normal end-of-observation gap;
it is not reported as an ongoing service or current healthy connection.

## Evidence preservation

Copied only this stopped test's SQLite/config/log/run receipts back to ignored
`evidence/gamma-recovery-20261114/`. Verified all copied artifact SHA256s, all
compressed input raw SHA256s, original Gamma body SHA, SQLite integrity and foreign
keys; schema remains 1. No raw source payload or credentials committed to Git.
Temporary Linux artifacts remain; no additional cleanup or live run performed.

Conclusion: public-source access and bounded lifecycle demonstrated on Linux;
price freshness partial, genuine alert generation and delivery not exercised.
The PR remains unmerged. Production and Telegram remain untouched.

Follow-up: the separately authorized bounded diagnostic run and its raw-message
classification are documented in [QUOTE_STREAM_20261114.md](QUOTE_STREAM_20261114.md).
This does not retroactively identify the ignored messages from the first run.
