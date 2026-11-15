# Readiness repairs — PR 12

15 November 2026. Four offline reproductions identified gaps in the current
finite observer. These repairs do not deploy a service or enable Telegram.

## Changes and acceptance

1. Current quote usability requires both bid and ask. A recent ask without a bid
   is not READY; the opposite side remains independent. Restoring a complete
   PRICE baseline does not invent a crossing.
2. Weather/LAMP polling starts before WebSocket connection and continues during
   bounded reconnect waits. After price recovery is exhausted it continues only
   until the original finite deadline, with the failed price status retained and
   no extra WebSocket retries. One writer owns SQLite. A socket connection attempt
   can still delay consumption of a completed HTTP result up to its bounded
   connection timeout. This is not an always-on supervisor.
3. A LAMP socket timeout permits the next normal 900-second poll. No immediate
   retry; HTTP errors, access denials and decode/integrity failures remain
   terminal. After missing data, recovered guidance establishes a baseline.
4. European discovery checks the complete reviewed Celsius resolution template,
   normalizing only the airport label, explicit date, exact source URL and
   whitespace. The separate checks still require the configured station and AWC
   endpoint. Extra contradictory sentences or changed SPECI, fallback, cutoff,
   correction and rounding terms fail closed. The accepted US discovery path is
   unchanged; the new whole-template pin is scoped to the European extension.

The synthetic fixture uses placeholders for airport, date and source. Original
Gamma/HTTP/WebSocket evidence remains outside Git. A change in the provider's
wording may deliberately require another rules review.

## Evidence

- 351 offline tests pass, including 16 new acceptance cases.
- Full source/compile checks pass.
- Saved real rules: all 121 European/Ankara contracts pass the complete template.
- Two cold replays of the retained real New York stream match: 120 frames,
  28 stored samples, zero alerts; original SQLite and wire SHA unchanged.
- Existing schema version 1 survives new -> previous -> new code reopening;
  inputs, samples, events, evidence links and pending queue are unchanged.
- SQLite integrity OK; foreign-key errors zero.
- Synthetic total WebSocket failure still produces exactly one LAMP event and
  queue entry from two comparable forecasts. No Telegram request is made.

Source receipts and isolated compatibility databases are in
`evidence/readiness-repairs-20261115/`. Live qualification and CI conclusions must
be recorded separately from the offline acceptance count.

## Not a complete city release

The previous European live selection was PARTIAL: source clocks could lead Mac
receipt by 1–19 ms, and some books lacked a usable top or later confirmation.
The 90-second limit and original source/receipt clocks remain unchanged.
MOSMIX is still an offline source decoder, not a connected forecast-alert path.
These facts block city-ready announcements. PRICE and LAMP are independent
algorithms, but forecast queue delivery during a prolonged price outage is not
claimed by these capture tests; sending is still disabled in qualification.

A new 60-second local London 10°C run completed normally: fresh EGLC METAR,
both initial native bid/ask baselines, 32 WebSocket frames, zero events/queue
entries/sends, SQLite 143360 bytes and integrity OK. Subsequent 46 per-token
price changes had source timestamps after actual receipt and were rejected as
FUTURE. Live verdict remains PARTIAL; these fixes do not alter clock policy or
claim to resolve that separate source/receipt-clock discrepancy.
