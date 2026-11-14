# HOLY WEATHER — two independent analytical tracks

Approved concept: **one project, two tracks, one shared Telegram bot**.
This document specifies research behavior, not implemented strategy or trading
approval. No code, services, models or production settings change with this PR.

## 1. 🔒 LOCK ALERT (Netflix-style)

Find exact weather contracts whose outcome is approaching determination or has
been officially confirmed while the relevant side still trades materially below
$1. Label YES/NO explicitly: a verified bucket outcome determines which side wins.
A price around 70¢ is a research interest threshold, not automatic eligibility,
a recommendation or evidence of certainty. Exact threshold direction, spread and
remaining-upside criteria must be frozen before a future experiment.

| Status | Meaning and required evidence |
| --- | --- |
| PRICE WATCH | Interesting observed price; outcome review still required. Preserve price type, side, quote clocks and freshness. No claim of a lock. |
| LIKELY LOCK | A stated weather/rule hypothesis supports an outcome, but remaining hours, observation coverage, revisions and resolution risk remain explicit. No invented probability. |
| RESULT VERIFIED | An official result matches the exact contract's station, local date, units, rounding and resolution rules. Preserve the official publication, receipt time, provenance and SHA. Distinguish source confirmation from actual market resolution. |

A high price alone cannot promote a status. A received METAR maximum is not
necessarily the official daily Tmax; an hourly LAMP peak is not a settlement
result. Unknown rules or insufficient official evidence prevent RESULT VERIFIED.
Corrections append a new assessment with its reason; they do not rewrite prior
alerts or erase a mistaken likely lock. No official-outcome verifier exists yet.

## 2. ☀️ LAMP ALERT

Independently examine new original LAMP issues and revisions for the verified
station. Record issue, receipt and main-loop availability times, raw bulletin/SHA,
remaining-local-day hourly TMP points and the exact comparison version.

Planned events include a changed future peak and a peak crossing a verified bucket
boundary. Compare both forecasts over matching remaining hours at the comparison
time: a peak disappearing because its hour elapsed is not a forecast revision.
Preserve that distinction, missing hours, stale bulletins and below-observed-maximum
forecasts. Duplicate downloads do not become new forecast issues.

Freeze the observed METAR, received maximum, original future LAMP peak and signed
difference separately. LAMP remains an original forecast, never an own model,
calibrated probability or guarantee. Bucket boundaries follow contract rounding,
not an assumed integer interval.

A possible lag between forecast and price is a research hypothesis. Record quotes
before/at forecast availability and subsequent observed quotes with a predefined
window and movement threshold. Gaps, stale quotes or missing baseline mean UNKNOWN,
not “the market did not react.” Later prices assess the frozen hypothesis; they
cannot be used as evidence that was already available at alert time.

## Shared inputs, independent decisions

Reuse Gamma identity/rules, public WebSocket quotes, AWC/METAR, original LAMP,
bounded SQLite and causal replay. Keep bid, ask, last trade, sizes and their
source/receipt clocks distinct. Metadata and weather must have been available
before the event they support. Late information cannot revise the original card.

Each track independently records event type, reason, status, contract/side,
source evidence, decision time and suppression reason. Future notification keys
must include track identity: duplicate LOCK events suppress LOCK only; a LAMP
event cannot suppress a LOCK event or vice versa. Preserve meaningful revisions
and distinguish them from exact duplicates across restart. Both can use one bot,
with unmistakable headings **🔒 LOCK ALERT** and **☀️ LAMP ALERT**. Their agreement
is not a proven or combined probability.

Use the existing single SQLite writer and storage budget (256 MiB including WAL
and logs), with protected event evidence and explicit exhaustion. No new database,
service or administrative mechanism is proposed. Any future schema need requires
separate compatibility justification, tests and non-destructive recovery.
The market freshness limit stays at 90 seconds. This PR defines no new sends.

## Existing versus planned

| Component | Current state |
| --- | --- |
| Gamma rules/station/date/bucket/YES-NO identity | Implemented in main; exact configured contract per finite run |
| Public stream, typed prices, movement/cross/return events | Implemented in main, including baseline/gap handling; generic price events are not LOCK decisions |
| AWC observations and original LAMP remaining-day context | Implemented in main for ten supported city configurations; not a deployed ten-city fleet |
| SQLite, bounded capture, causal replay, evidence protection | Implemented in main; no perpetual runtime or automatic local-date rollover |
| Shared Telegram delivery adapter | Implemented in separate open PR #5; one explicitly approved test delivered as message 359; not merged or deployed |
| LOCK status engine and contract-specific official verification | Planned |
| Independent forecast-triggered LAMP events and market-lag assessment | Planned; existing LAMP context attached to price events is not this engine |
| Per-track event history, notification deduplication and headings | Planned; current generic Telegram delivery does not establish these semantics |
| Separate new-day evaluation including costs | Planned; neither profitability nor trading advantage is established |

The Telegram test proves one acknowledged delivery, not continuous operation,
strategy accuracy or runtime readiness. PR #5 remains independently reviewable.

## Separate research acceptance

Freeze event definitions and evaluation windows before collecting new days.
For each track report eligible cases, warnings, suppressed events, gaps and
unknowns; official outcome correctness, false warnings and time to confirmation;
subsequent price paths, spread, available sizes/depth and quote ages. Record fees,
slippage and execution assumptions before calculating hypothetical net results.
A best ask is not proof that $10 could execute; unavailable depth leaves that
estimate unknown. Hypothetical results are not actual trades.

Separate LOCK status levels and LAMP event types. Count independent station-days,
not repeated alerts as independent successes. Retain failures and revisions;
report overlap between tracks without double-counting profit or deriving a
probability from agreement. Evaluate warming forecasts separately from price
response and from delivery correctness. No automatic trading follows this work.
