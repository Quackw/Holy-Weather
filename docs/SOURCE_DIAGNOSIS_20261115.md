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

## Authorized finite qualification

The user approved manifest
`4205d86e96b09fdb078b6ad237d46eb73849e8ea0c73de5f6e570103ef40750d`
after automatic review required exact-payload consent. One execution completed
at 00:25 Kyiv on November 15 (22:25 UTC November 14; Seattle market day November
14), using the existing pinned identity and a fresh owned temporary directory.
All package and dependency pins matched. Production and Telegram were untouched.

Gamma's actual response headers establish HTTP caching: `Age: 149`,
`Cache-Control: public, max-age=300`. These headers prove that this response was
cached and can explain a lag in indicative fields; they do not retroactively
prove the precise cause of every field difference in the previous capture.

Two candidates had indicative two-sided Gamma prices and were checked with four
independent CLOB book reads. Both sides of 60–61°F and 62–63°F had actual bid/ask
levels, but all four book source timestamps were 132.811–132.955 seconds old.
The unchanged 90-second eligibility check rejected them. CLOB responses had
current HTTP Date headers and `cf-cache-status: DYNAMIC`; HTTP receipt time was
not substituted for their older book timestamps.

No candidate passed. The run returned `NO_VERIFIED_FRESH_COMPLETE_CANDIDATE`:
no application observation, WebSocket, SQLite, PRICE event or queue record was
started. There was no reconnect, second observation or different-account/endpoint
fallback. This is a pending fresh-quote acceptance, not proof that a later stream
could not deliver fresh updates.

The single AWC timing request returned HTTP 200 in 0.361 seconds: DNS about
10 ms, TCP about 22 ms, TLS about 40 ms, first byte about 361 ms. Its 13,213-byte
body passed the native decoder: 24 reports, 14 on the Seattle market day; latest
KSEA observation 21:53 UTC, 15.6°C. Thus AWC access and actual data were available
in this diagnostic. The historical timeout's exact network phase remains unknown,
and the native application's post-timeout recovery was not exercised live because
that application was not started. Scheduled recovery remains covered by tests.

The run's 11 copied artifacts total 116,997 bytes. Every artifact and HTTP-body
SHA matches. No SQLite integrity result is claimed for this run because no SQLite
was created. Existing historical artifacts remain untouched. The source fix has
233 passing local tests and successful Linux CI at `7ad0bac`; synthetic tests and
HTTP success do not establish live PRICE readiness or Telegram delivery.

Verdict: **PARTIAL**. Gamma caching is confirmed and AWC availability demonstrated;
fresh complete native quotes and the old cross-source difference remain pending.

Existing real artifacts remain in ignored evidence directories with their
original SHA. Synthetic regression success is not a live-source acceptance.
