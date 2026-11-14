# New York offline LOCK / LAMP qualification

Scope: offline saved-input replay only, no live monitor changes, messages, network,
new database schema or services. PR #6 was merged before this branch. PR #5 remains
separate. This is a partial research implementation, not strategy acceptance.

Run with a new owned output directory in a weather+LAMP config:

```sh
PYTHONPATH=src python -m holy_weather.research --config replay.toml --manifest manifest.json > report.json
```

Original Gamma/stream files retain SHA validation before/after replay. Existing
replay opens only a separate owned database. The report freezes track context and
keys separately; at most 1,000 records are permitted. Independent report keys are
deterministic across clean replays, not a production notification ledger. Do not
resume into a populated replay database; repeat into a fresh owned directory.
No history pruning or source writes occur. Existing SQLite integrity/budget checks
apply; no schema changes. Store the report and original SHA-bound inputs together.

## Frozen initial rules

LOCK: first eligible quote per side/contract, 0 < ask <= 70 cents. Only an existing
usable READY quote can produce PRICE WATCH. An interesting but unconfirmed quote
is recorded as WITHHELD, never a signal. No LIKELY LOCK heuristic or official
RESULT VERIFIED verifier is implemented. Both YES and NO can be price watches;
this expresses price screening only, not contradictory outcome predictions.

LAMP: each distinct causal bulletin is examined independently of price changes.
First issue is BASELINE, not an alert of change. Compare peak temperatures on
identical future valid-hour sets at the same decision time. Different horizons or
stale preceding issue are INCOMPARABLE. Invalid input resets comparison baseline.
Duplicate bulletin keys do not create new records. Price reaction is UNKNOWN;
market-lag measurement is intentionally not inferred from sparse saved records.

Observer runs before each replay envelope, and once 1 ms after the final envelope;
thus newly available weather is never consumed before its availability clock.
This is an explicit replay decision clock, not a measured live-delivery latency.
The normal replay path is unchanged when no observer is passed.

## Real saved evidence, 14 November 2026

Two independent *recordings of the same station-day*, not independent scientific
days, replayed twice each. Research records matched exactly between each pair.
Original artifacts passed their SHA checks; new SQLite integrity checks passed.
Source manifests (kept outside Git):

- stage3-20261114/real-replay: LOCK YES 67.7 cents and NO 38.4 cents WITHHELD;
  LAMP baseline peak 46°F.
- ten-city-20261114/new-york-recovery-replay: LOCK NO 33.7 cents WITHHELD;
  LAMP baseline peak 46°F.

All concern the verified KLGA 46–47°F contract. Existing saved replay does not
establish transport connection state: READY cannot be asserted merely from a
recent quote. The replay deliberately retains CONNECTION_UNCONFIRMED rather than
fabricating a connection event. Therefore **zero actionable PRICE WATCH records**
were proven, three diagnostic price candidates were retained. Two LAMP baselines,
**zero real forecast-change events**: each recording contains only one available
bulletin. There is no demonstrated market lead, outcome verification or profit.
No retrospective weather fetch was used to fill missing issues.

Preview examples (historical diagnostics, not live alerts):

> 🔒 LOCK ALERT · New York · WITHHELD  
> 46–47°F · NO ask 33.7¢  
> Outcome unverified · Connection evidence missing

> ☀️ LAMP ALERT · New York · BASELINE  
> 46–47°F · Remaining peak 46°F  
> First available bulletin · Forecast change not yet measurable  
> Market reaction UNKNOWN

Structured records include exact bucket URL, decision clock, market identity,
original weather/forecast context, quote clocks and bulletin SHA. Full raw data is
not committed. Synthetic tests demonstrate a 46→48°F peak revision crossing the
47.5°F upper boundary; this must not be represented as a real recorded revision.

## Verification and remaining work

185 offline regression tests including five new track tests: independent dedupe,
price threshold, causal availability, missing quote confirmation, peak-hour expiry,
invalid bulletin recovery and immutable first-price evidence. Source/compile checks
also run; GitHub Linux CI qualifies the complete suite.

Next evidence needed: a saved interval with explicit connection/baseline records
and at least two causally received LAMP issues. Then test actual PRICE WATCH and
forecast-change records; separately define outcome evidence before LIKELY LOCK or
RESULT VERIFIED. This PR does not authorize new collection or Telegram sending.
