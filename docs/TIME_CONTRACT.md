# Time contract and qualification — Europe

15 November 2026. Quote clock skew and local calendar boundaries are separate.

## Preserved clocks

Persist source timestamp, receipt timestamp and forecast availability separately.
Never overwrite source/receipt clocks to make a quote pass. UTC instants identify
observations and forecast hours. IANA station zones determine the contract date;
Kyiv or a fixed UTC offset must not determine another city's market day.
Monotonic time controls elapsed runs and retry delays. Wall-clock regressions
remain invalid rather than becoming a new price baseline across the regression.

## Clock evidence

The retained European stream contains source timestamps 1–19 ms later than Mac
receipt. Four read-only Apple SNTP measurements returned positive offsets about
72–73 ms with uncertainties about 34–47 ms. This supports local clock offset as
an explanation but does not measure the exchange clock or prove its exact offset.
No system time, time service, quote timestamp or freshness setting was changed.
The strict future-timestamp rejection and 90-second freshness limit remain.

A running alert host needs measured clock synchronization before live acceptance.
Do not solve this by adding an arbitrary allowance or silently rewriting receipt
time. Any change to clock eligibility needs a separately explicit policy with
uncertainty, bounded drift, forward/backward-jump, restart and causal-replay tests.
Current code has not yet qualified that policy.

## Calendar tests

41 new offline tests cover all 11 European/Ankara profiles on 29 March,
25 October and 15 November 2026. European DST zones have 23/25-hour days;
Moscow and Istanbul zones remain 24 hours. Forecast selection uses distinct UTC
valid times: both occurrences of the repeated autumn hour survive. Observations
at the two occurrences are retained independently. Queue expiry is before the
next station-local midnight; the next day is not inferred as 24 UTC hours later.
The finite observer stops at the contract date boundary. Automated next-day
contract discovery/relaunch is NOT implemented or promised by this PR.

## MOSMIX qualification progress

The official saved directory shows MOSMIX_L issues at 03/09/15/21 UTC and later
publication times. This is not hourly-issued NOAA LAMP and its two-hour freshness
policy must not be copied. Two original London issues (03:00 and 09:00 UTC)
were compared on nine identical future UTC hours: 10.00 -> 9.70 Celsius.
The older issue was retrieved after the newer one; the comparison is retrospective,
not evidence that an alert could have been delivered earlier. Original receipt
clocks/SHA remain in evidence/time-qualification-20261115/.

Before forecast alerts: freeze a source-specific age/material-change policy,
verify full remaining-hour coverage, first/duplicate/regressed/corrected issue
behavior, same-hour comparison, restart and independent durable queue/preview.
MOSMIX stays labelled MOSMIX and disabled for alerts until that qualification.
The DWD descriptive PDF returned HTTP 403 in the browser fetch; it was not retried
or used as evidence. Cadence here is supported by the retained public directory.

## Midnight connection regression

A reproduced gap in `observe` allowed one old-contract WebSocket connection
when the local-date check set the stop flag immediately before connect. The
loop now honours that flag before connecting and records
`MARKET_LOCAL_DATE_ENDED` explicitly, including during reconnect waits. The
regression failed on the previous implementation. This is safe termination,
not automatic next-day activation.

A read-only check of the existing test host `v2202609428928530766` on
15 November at 14:08 UTC returned `NTP=yes`, `NTPSynchronized=yes`, timezone UTC.
This does not itself establish exchange-clock agreement; native-stream
qualification is still required. No clock settings or services were changed.
