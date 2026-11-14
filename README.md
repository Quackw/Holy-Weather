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

Weather, forecast/research modules, Telegram, production installation and a
48-hour supervisor belong to later stages and are not implemented. Price alerts
establish neither warming accuracy nor trading advantage. No orders are placed.
