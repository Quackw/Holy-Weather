# PRICE ALERT — opt-in independent detector

Enable `price_alert_enabled = true` in a finite one-contract config. Default is
false. No Telegram, service or trading integration. Original ask research rules
remain unchanged; PRICE_ALERT events are separately typed and namespaced.

## Source comparison

Read original local `holypolyfoundation/netflix/src/price.mjs`, `registry.mjs`,
`monitor.mjs` and README at checkout HEAD
`f6f554c05e3be6a65d2201d151d08fd8a86a455b`.
price.mjs SHA256: `4e304a3092ded5cc51ef0fe2a38aaa0a353fc64e3465e5b68e0d3194d068cee8`.
This verifies saved source behavior, not the currently deployed bot binary.

Matching rules: exact decimal midpoint of own token bid/ask; upward thresholds
50/55/60/70/80; one event containing all newly crossed levels; durable fired flags;
no rearm. Initial quote is baseline. Only full book/BBO advances this detector;
price_change, last trade and Gamma prices do not. One-sided/reversed quotes are
invalid, and BBO 0/1 sentinels require a new valid baseline.

Explicit user-approved difference: Netflix can create SNAPSHOT_AFTER_GAP;
HOLY WEATHER never alerts across a gap or restart. First new confirmed midpoint
is baseline, consumed levels remain consumed. A source-time reversal invalidates
continuity; a previous quote older than 90 seconds cannot support a crossing.
YES and NO have independent token state. Percentages describe prices, not forecast
probabilities or guaranteed outcomes.

## Persistence and compatibility

Existing schema version 1, no migration or new database. `meta` holds one small
`price_alert_v1:<token>` record per side with previous evidence and at most five
fired levels. Input, event, threshold changes and event references commit in the
same existing SQLite transaction. Existing `events`/`event_inputs` retain PRICE
records and discovery/previous/current evidence. A dedicated latest pointer keeps
the pending midpoint input safe from pruning; it is excluded from public quote
snapshots. Gap/restart clears in-memory continuity only, not consumed thresholds.

Flag excluded from the existing research rule hash: disabling it can reopen the
same database and retains new history. Existing schema readers can read records;
PRICE-specific consumers must filter event type. Full old-binary live rollback
has not been run; no deployment is requested. No durable sender is added here.

Unchanged top confirmations are coalesced with the existing 15-second checkpoint
in both modes. Every changed bid/ask is stored. An unconfirmed PRICE baseline
forces a BBO write even when its top matches. In-memory source/receipt high-water
advances on coalesced confirmations; checkpoint inputs retain their own clocks,
not the later confirmation clocks. Original unchanged raw frames remain intentionally
unarchived. The 256-MiB budget is unchanged; real long-run growth is unmeasured.

## Qualification

186 tests passed locally, including six new tests for decimal arithmetic, multi-
cross, both sides, ask-versus-midpoint, exact duplicates, restart and gap baseline,
consumed-level retention, stale/missing/late quotes and protected evidence after
pruning. Disabled-feature reopen and SQLite integrity/foreign keys are checked.
Full Linux CI runs the same suite; all feed inputs there are synthetic.

Saved real New York 14 November stream replay passed source SHA checks and SQLite
integrity. Zero PRICE alerts: saved replay does not establish a confirmed live
connection. No connection was invented to force an example. Real alert triggering
and long-run storage are not proven. No network collection or Telegram sends.

Next: accept this PR before independent LAMP PR C. Any live qualification requires
an explicitly bounded run; real Telegram remains a separate PR D and approval.

## Pre-acceptance review — 14 November

**Historical pre-fix finding (resolved by the coalescing correction below).**
Nine PRICE tests now include all five consumed levels surviving restart and 20
recross updates (each duplicated), high first quote after restart, and an injected
SQLite event-insert failure proving fired flags roll back atomically. No false
or repeated PRICE events occurred in these scenarios. This is synthetic fault
coverage, not a guarantee for every possible feed failure.

Controlled local comparison: identical 6,000 BBO confirmations, unchanged bid/ask,
10 updates/second over 600 simulated seconds, one token and one contract. No real
network, Telegram or production. Original source code under review is unchanged.

| Metric | Flag off | Flag on |
| --- | ---: | ---: |
| Stored inputs including discovery | 41 | 6,001 |
| PRICE events | 0 | 0 |
| Closed directory bytes | 86,036 | 2,711,572 |
| Sampled peak directory bytes, including WAL/SHM | 658,580 | 3,292,596 |
| SQLite integrity | ok | ok |

Peak sampled every 100 inputs, not a guaranteed absolute peak. Closed directory
includes the owned marker/lock. The enabled case used about 31.5 times the closed
space on this quiet feed because coalescing was disabled. This is a synthetic
load comparison, not measured daily production growth. The budget remains 256 MiB;
more frequent writes can reach its stop boundary earlier. Do not extrapolate this
single-token test into a fleet lifetime or production SLA.

Next correction should restore bounded unchanged confirmations while keeping
midpoint continuity, restart baselines and evidence retention correct. Re-run the
same load and regression tests before acceptance. PR remains unmerged. No schema
migration or source-history changes were performed by this review.


## Coalescing correction acceptance

Restored the existing unchanged-top checkpoint path for both flag values. PRICE
requires its own ready baseline before coalescing BBO confirmations. Real changes
are never coalesced. Repeated exact-duplicate diagnostics are sampled at count 1
and each 128th occurrence; the returned runtime Counter retains the exact total.
No SQL diagnostic is written for each unchanged PRICE checkpoint with no event.
Diagnostic table remains bounded at 512 rows. The aggregate counter is per run,
not a new durable lifetime metric.

Exact repeat of the previous 6,000-frame load (600 simulated seconds, one token,
unchanged BBO at 10 Hz):

| Metric | OFF | ON |
| --- | ---: | ---: |
| Samples | 40 | 40 |
| Inputs including discovery | 41 | 41 |
| PRICE events | 0 | 0 |
| Closed directory bytes | 86,036 | 86,036 |
| Sampled peak including WAL/SHM | 658,580 | 666,796 |
| SQLite integrity | ok | ok |

ON/OFF closed-size ratio is 1.00; sampled peak ratio about 1.0125. The same limit
was used. This is measured synthetic quiet-feed behavior, not a daily forecast.
A regression also feeds 6,000 exact duplicates and checks at most 48 diagnostic
rows, exact counters and no new events.

194 regression tests cover bid-only midpoint crossing, MULTI-CROSS, crossing right
after checkpoint, GAP and restart between checkpoints, retained fired thresholds,
missing bid/ask, old clocks, duplicate inputs, SQLite transaction failure and
protected evidence. Two clean saved New York replays produced identical counters
and event records, passed source SHA checks and database integrity; zero real
PRICE events (connection evidence absent). No source history or schema changed.
READY for PR review on this scope, subject to green exact-commit Linux CI. No
merge, production action, AWC/LAMP changes or Telegram sends.
