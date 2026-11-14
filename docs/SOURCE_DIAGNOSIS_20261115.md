# Gamma/WebSocket comparison and AWC timeout recovery

## Confirmed observations

The November 14 Seattle 58–59°F capture contains a genuine discrepancy between
Gamma's indicative fields (bid 0.85 / ask 0.94) and the native WebSocket's books
(YES: no bid / ask 0.01; NO: bid 0.99 / no ask). The condition ID, YES/NO tokens,
contract date and station match. The contract is CTF `v1`, so a protocol-v2
position-ID mix-up does not explain this capture. The original raw book levels
agree with their decoded values. Gamma metadata `updatedAt` is not a quote-source
clock and cannot prove the freshness of its indicative prices.

The 17 native price-change messages had source-to-receipt age 13–21 ms. They
confirmed unchanged one-sided top prices while order sizes/depth changed. They
are not evidence of a usable midpoint or a threshold crossing. The application
did not substitute Gamma prices or last trades for missing quote sides.

An independent CLOB REST book comparison was not captured in that run. Therefore
the upstream cause of the discrepancy remains unproven. A new bounded comparison
must preserve each source's own receipt, timestamp, identity, levels and hash.
The official [order-book API](https://docs.polymarket.com/market-data/prices-order-books)
and [market stream](https://docs.polymarket.com/market-data/realtime-data) describe
the public interfaces used for that comparison. REST is a diagnostic/preflight
source only; it does not replace WebSocket samples or manufacture an alert.

## Minimal AWC correction

The retained AWC receipt proves a `TimeoutError` about five seconds after the
request began. The old code marked all network errors terminal, so one timeout
cancelled every later weather poll in that observation session. The old receipt
does not identify whether the delay occurred during TCP, TLS or response reading;
an exact historical network cause cannot be inferred from it.

`fetch_weather` now distinguishes direct and URL-wrapped socket timeouts. It
retains `NETWORK_OR_DECODE_ERROR`, preserves the request/receipt clocks and
records `NEXT_SCHEDULED_POLL`. The poller keeps its existing five-minute schedule:
there is no immediate retry, new worker, timeout increase or second endpoint.
HTTP refusals, decoding errors and unclassified network errors remain terminal.
The five-second socket timeout, 90-second quote limit, weather age rules and
storage caps are unchanged.

The [AWC API guidance](https://aviationweather.gov/data/api/) requests bounded,
infrequent requests with a custom user agent. A timing diagnostic will make at
most one additional request, record connection/response timings and stop on
401/403/429. It cannot retroactively enrich already saved market events.

## Offline evidence and scope

11 additional regression cases cover direct/wrapped timeout, no immediate retry,
the unchanged socket timeout, permanent failure/refusal behavior, price processing
during the outage, recovery at the original scheduled time, immutable event
contexts and inputs, restart, SQLite integrity and foreign keys. The Gamma/WS
regression reproduces conflicting indicative prices with missing native quote
sides and confirms that no PRICE alert or queue record is fabricated.

No schema migration, new service, dependency, notification rule or probability
model is added. The native PRICE/LAMP algorithms and queue remain independent.
Production and Telegram sending are outside this change.

Live qualification is pending separate payload authorization after automatic
review rejected the new remote diagnostic script. Its intended scope is one
Seattle station-local day, at most three exact rule-verified candidates, two
bounded CLOB books per candidate, and one 120-second observation on the first
candidate whose real books are complete and no older than 90 seconds. No
reconnects or automatic second observation. If no candidate passes, the record
remains pending; no fresh state or signal will be invented.

Existing real artifacts remain in ignored evidence directories with their
original SHA. Synthetic regression success is not a live-source acceptance.
