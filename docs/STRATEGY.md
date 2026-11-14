# HOLY WEATHER — PRICE ALERT and LAMP ALERT

Final approved concept: **one project, two independent notification algorithms,
one Telegram bot**. This supersedes PR #6's product concept. LOCK ALERT,
PRICE WATCH as a lock stage, LIKELY LOCK and RESULT VERIFIED are removed from the
product. Historical evidence and commits remain intact; no runtime changes here.

## 📈 PRICE ALERT — Netflix-style price notifications

This is price movement, not winner identification or a promise of reaching 99¢.
Before implementing PR B, inspect the actual original @holy_nfx_bot implementation
and record its version and exact rules. Parity is **not yet verified**. If the
original differs from this specification, report the mismatch before coding;
do not silently choose another metric or describe approximation as exact parity.

Required specification to verify:

- Thresholds: Decimal 0.50, 0.55, 0.60, 0.70, 0.80.
- Canonical metric: `(best_bid + best_ask) / 2` using Decimal and the actual
  token of the selected YES or NO side. Never infer NO from YES or use last trade
  or ask as a midpoint substitute. Missing sides of the book mean no midpoint.
- Only upward crossings: previous < threshold and current >= threshold.
- One MULTI-CROSS event/card when one update crosses several unconsumed levels.
- Each market + side + threshold is notified once. REARM=false. Falling below a
  level does not restore eligibility. Persist consumed levels and pending events.
- First trustworthy value is baseline, not an alert. Gaps, unconfirmed reconnects,
  stale/delayed messages and restart cannot manufacture a crossing. A fresh baseline
  does not erase consumed thresholds. Preserve explicit suppression evidence.

Retain the existing 90-second freshness limit and distinguish source/receipt times,
connection state and quote validity. Existing 50/70/90 ask events with hysteresis
remain a separate research engine, unchanged; they are not this specification.

Illustrative card only, not a recorded event:

> 📈 PRICE ALERT
> Dallas · YES · 62–63°F
> Midpoint: 69.4% → 70.2%
> Crossed 70%
> Event time · contract link · stream state · quote age

## ☀️ LAMP ALERT — independent original forecast notifications

A new received LAMP issue/revision can trigger without any price update. Preserve
original bulletin/SHA, issue/receipt/availability times and previous version.
Use the verified station, local market date and contract rounding boundaries.

An alert requires either a future peak crossing a verified bucket boundary or
an absolute remaining-peak change meeting a configured research threshold.
**Proposed initial threshold: 2°F**, in either direction: native TMP uses whole
Fahrenheit, so this filters a single-degree revision while retaining a two-degree
move. This is a research choice, not validated predictive skill. PR C must freeze
`lamp_peak_change_f = 2` in configuration before testing; a smaller change that
crosses a bucket boundary still qualifies. No configuration/code changes in PR A.

Compare both forecasts on the same complete remaining valid-hour set at the same
comparison time. Expiration of a past peak is not a forecast revision. Incomplete,
stale or incompatible horizons yield a recorded suppression, not invented peaks.
First forecast is baseline; duplicate bulletin downloads never create an alert.
Missing/invalid data and recovery behavior must be tested explicitly.

Keep current METAR, maximum among received observations and future original LAMP
peak separate. Do not clamp LAMP to observed maximum. Late weather cannot revise
an earlier event. Quote gaps mean market reaction UNKNOWN, not “no reaction.”

Illustrative card only, not a recorded event:

> ☀️ LAMP ALERT
> Dallas · 62–63°F
> Previous future peak: 61°F → New future peak: 64°F
> Crossed upper bucket boundary: 63.5°F
> METAR · observed maximum · YES/NO prices and their types/ages
> Forecast issue/receipt time · contract link

This is a forecast warning, not a guaranteed outcome. Baselines and technical
suppression statuses stay in the journal, not in the ordinary Telegram alert feed.

## Shared storage and Telegram

Reuse Gamma, public WebSocket, AWC/METAR, LAMP, causal replay and the existing
single-writer SQLite. No new database, orchestrator or weather model. Preserve
historical evidence, the 256-MiB database/WAL/log budget and current runtime.
Any schema change later needs compatibility, recovery and non-destructive tests.

PRICE and LAMP have independent triggers, threshold/comparison states, reasons,
evidence and namespaced dedupe keys. Agreement does not create a probability.
A shared durable delivery queue must retain events across restart, never resend
confirmed alerts and represent uncertain delivery explicitly. No blind retries
for UNKNOWN outcomes. Define pending-event expiry and honest age display in PR D;
queue persistence does not make an old quote fresh. Telegram failure must not
block weather/price ingestion. Real sends need separate verification and permission.

PR #5 is reusable transport/preview groundwork, not an already proven queue for
these two event types: it records claims before HTTP and polls recent generic
price events. It needs adaptation and restart/expiry/dedupe tests in PR D. Keep
it unmerged until the two-type design is agreed. Earlier one-card permission does
not authorize new sends.

## Current implementation versus plan

| Component | State |
| --- | --- |
| Gamma exact contract/station/date/token checks | Existing main |
| Public prices, original ask research detector, causal replay | Existing main, unchanged |
| AWC/METAR and original LAMP context; ten city configurations | Existing main; finite one-contract runs, not deployed fleet |
| Bounded SQLite and original evidence | Existing main, retained |
| Exact Netflix PRICE algorithm and durable threshold state | Planned PR B; source parity not verified |
| Forecast-triggered LAMP notifications | Planned PR C; reusable offline comparison exists on superseded PR #7 branch |
| Shared two-type durable Telegram queue | Planned PR D; PR #5 transport remains unmerged |
| Profitable strategy, probabilities, automated trading | Not implemented or authorized |

Begin with New York/KLGA and one verified active contract. Expand to ten cities
only after combined qualification. Evaluate the two algorithms separately on new
days: events, gaps, duplicates, delays, forecast changes, subsequent prices,
liquidity, spread and hypothetical costs. Delivery is not trading advantage.
