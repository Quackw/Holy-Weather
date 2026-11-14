# HOLY WEATHER

Private, independent Python monitor. Stage 1 observes **one explicitly selected
temperature contract, both YES/NO tokens**, using Gamma and Polymarket's official
public market stream. It stores typed quotes, raw evidence, events and suppression
reasons in its own SQLite. No account or trading credentials are needed.

## Run

Python 3.11+ on Linux or macOS. One runtime dependency: `websockets==17.1`.
Create a local development environment once:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -c requirements-dev.lock -e '.[test]'
```

Then run the finite experiment with **one command**:

```sh
.venv/bin/holy-weather observe --config config.example.toml
```

The example selects New York's native `46-47°F` contract on the current
LaGuardia local date. Configure an exact bucket before another experiment.
Discovery refuses an absent/ambiguous bucket, wrong station/date, changed rules,
closed market or bad token mapping. It never silently chooses a different bucket.
Market metadata state and quote freshness are separate.

Default duration is 120 seconds; stage-one maximum is one hour. Data directory
and duration may be overridden by `HOLY_WEATHER_DATA_DIR` and
`HOLY_WEATHER_DURATION_SECONDS`. Unknown configuration keys and invalid limits
fail explicitly. No automatic market-date rollover is implemented.

The terminal summary includes contract identity, both typed quote snapshots,
ingest/current freshness, events/suppressions, gaps, bytes, database checks and
Linux process disk writes. Rotating JSON logs are in the owned data directory.
No price movement is a normal result. A missing quote or network failure is a
different state. SIGINT/SIGTERM stop safely; there is no restart daemon.

```sh
.venv/bin/holy-weather stats --config config.example.toml
.venv/bin/holy-weather prune --config config.example.toml
.venv/bin/holy-weather prune --config config.example.toml --apply
```

`stats` and maintenance require the writer to be stopped. Prune defaults to a
preview and touches only this application's owned SQLite. Referenced evidence
and latest snapshots are protected. No source data from other projects is opened.

## Rules and data

* Ask movement at least 10¢ over at most 15 minutes.
* Up/down transitions of `ask >= 50/70/90¢`, with configurable cooldown and 2¢
  rearm distance. Distinct suppressed crossings are retained for research.
* Retrace at least half a significant movement within 15 minutes of its peak.
* Connection/restart/invalid-input recovery establishes a baseline, never a
  guessed crossing across a gap. Rule state and duplicate identities persist.
* Bid, ask, sizes and timed trades stay separate. Snapshot last-trade prices have
  no invented execution clock. BBO messages have unknown sizes.
* A quote older than 90 seconds, future clock, missing ask or reversed source
  time is stored with an explicit status and does not create a price event.

No complete depth-delta history is archived. A price-change message can confirm
the actual top of book. Every changed bid/ask is retained; unchanged confirmations
are coalesced into at most one checkpoint per token per 15 seconds. Coalescing is
counted and never substitutes a heartbeat for a provider quote. Relevant native messages are compressed
and hashed; event records point to their source inputs and the Gamma receipt.
The data budget is 256 MiB including database/WAL/logs. Capture stops before its
reserve is exhausted; the limit is not silently raised. See
[ARCHITECTURE.md](ARCHITECTURE.md) for retention and recovery limits.

## Tests and replay

```sh
.venv/bin/python -B -m pytest -q
.venv/bin/python -B tools/check_source.py
.venv/bin/holy-weather replay --config replay.toml --manifest research-manifest.json
```

CI tests are synthetic/offline on Linux. A replay manifest supplies explicit
SHA-bound Gamma and stream artifacts plus original receipt clocks. No old app,
Mac, shared database, server transport, secret or production component is a
runtime dependency. See [docs/QUALIFICATION.md](docs/QUALIFICATION.md).

Stage 2 optionally adds observed KLGA weather context as described below.
Forecasts, Telegram, production installation and a 48-hour supervisor remain
outside the implementation. Price alerts
establish neither warming accuracy nor trading advantage. No orders are placed.


## Stage 2: observed weather context

```sh
.venv/bin/holy-weather observe --config config.weather.example.toml
```

Only New York/KLGA is supported. Gamma still verifies the station and market
rules before the weather worker starts. The official AWC METAR JSON endpoint
returns up to 24 hours for KLGA. A request runs at most every five minutes in one
bounded HTTP thread; only the main loop writes SQLite. A failed/denied/invalid
weather response disables further weather requests for this finite run, while
market processing continues. Empty results remain explicitly NO_DATA.

Each event's JSON payload freezes `weather`: latest and previous Celsius
observations, temperature difference, observed maximum, observation/receipt/main
loop availability clocks, age, freshness, partial coverage and whether an
observation or correction became available within the movement interval.
The causal cutoff is the **local market receipt clock**, not an assertion about
when other participants knew the weather. No cause of a price move is asserted.
Weather obtained later cannot revise an already committed event.

`FRESH` weather means observation age at most 90 minutes (explicit research
context policy, not a provider SLA). The independent market limit remains
90 seconds. Observed maximum covers only received observations from the market's
local date; it is never an official daily Tmax. COR replaces the same observation
in future contexts; repeated originals cannot undo a correction. Changed reports
without COR are flagged as conflicts and excluded from temperature calculations.

No schema migration: compressed response receipts and duplicate/correction flags
use existing `inputs`; the bounded current-day projection uses `meta`; weather
evidence uses `event_inputs` and existing retention protection. The market rule
hash and event IDs are unchanged. Disabling weather or returning to stage 1
preserves new records; old code simply produces no weather for subsequent events.

Bounds: 128 KiB per HTTP response, 128 current-day observation times, one pending
worker/result, five-second socket timeout, 4 MiB compressed weather-input ceiling
within the unchanged 256-MiB directory budget. The weather ceiling disables only
weather collection; exhausting the global directory budget stops the writer as
before. No automatic deletion, service or schedule is introduced.

Replay may include `kind: weather` envelopes in its existing SHA-bound stream.
Their outer `received_at` is main-loop availability; the nested `receipt` retains
actual HTTP requested/received times, exact body, SHA and source. Envelopes remain
chronological and cannot precede Gamma. Weather is never retroactively fetched
for a historical market event. See [stage 2 qualification](docs/STAGE2_QUALIFICATION.md).
