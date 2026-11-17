# HOLY WEATHER

Independent Python monitor of public Polymarket temperature contracts. **One
project, two independent algorithms, one Telegram bot.** The regional catalogue
currently runs PRICE only; optional US AWC/LAMP remains in the single-contract
observer. No trading, probabilities, LOCK strategy or own Tmax model.

Runtime baseline: **`8d176109b80628716f745b210d87bca00c5c51ed`** (17 November 2026,
PR #30). A separately approved installation started permanent Europe/US PRICE
services that day. The dated [operations record](docs/OPERATIONS.md) separates
installed source, real quotes/deliveries and remaining 24/7 qualification. The
repository remains **public**; enforced main protection and accepted work are in
[ROADMAP](docs/ROADMAP.md).

## Implemented capabilities

| Component | Accepted behavior | Qualification boundary |
| --- | --- | --- |
| PRICE ALERT | Real YES/NO token Decimal midpoint; upward 50/55/60/70/80%; once-only, REARM=false | Detector, GAP/restart and rollback tests; genuine events/ACKs on the installed baseline |
| Regional catalogue | One codebase; Europe 11 profiles or US 10 profiles, separate regional directories | Both services installed; per-city/token PRICE readiness must still be checked separately |
| Notification filter | Opt-in bid confirmation, fast priority 70/80, ordinary same-bucket/side grouping | Historical comparison, integration tests and real priority ACKs; explicit refusals retained |
| Shared Telegram queue | Atomic event/enqueue, explicit refusal/outcome, SENT/UNKNOWN restart protection | Real ACKs after worker rotation; pending cards can expire at a planned worker boundary |
| Local-day rollover | Catalogue rediscovery and new baselines; continuous controller opens bounded workers | Date/DST/restart tests; current-build live overnight acceptance remains open |
| Continuous recovery/history | Persistent recovery budgets, inherited ownership locks, bounded archive/control rotation | Linux/systemd tests and two natural windows per installed region; real host reboot/24 hours unproven |
| US weather/LAMP | Optional observations/original hourly context; independent KLGA LAMP ALERT | Accepted finite components; disabled in the regional catalogue |

PRICE rules were checked against the retained original `@holy_nfx_bot` source;
that is not proof of its currently deployed binary. Legacy 50/70/90 ask research
still retains events. Its `ENGINE_POINT_BOUND` no longer invalidates an otherwise
usable PRICE quote. See [research isolation](docs/PRICE_RESEARCH_ISOLATION.md).

## Finite PRICE entry points

Python **3.11+** on Linux/macOS. Runtime dependency **`websockets==17.4`** is pinned
in package metadata and the development lock. Prepare a local environment:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -c requirements-dev.lock -e '.[test]'
```

These finite no-send examples use separate owned test directories. Examples do
not authorize a server installation, new live experiment or Telegram send:

```sh
.venv/bin/python -m holy_weather.catalogue --region europe --data-dir data/europe-test --seconds 120 --price-notification-filter
.venv/bin/python -m holy_weather.catalogue --region us --data-dir data/us-test --seconds 120 --price-notification-filter
```

The catalogue verifies current-local-day contracts, rules, station and real
YES/NO identities. Missing/ambiguous buckets, changed rules or bad identities
fail explicitly. Gamma indicative prices never replace native WebSocket bid/ask.
PRICE and its queue are ON; Telegram is OFF unless `--send` is explicitly used.
The filter is OFF unless `--price-notification-filter` is supplied. Ordinary
batching defaults to 10 seconds; `--price-batch-seconds` accepts 10/15/30.

Profiles are pinned in [core.py](src/holy_weather/core.py):

- US: New York, Atlanta, Seattle, Miami, Chicago, Dallas, Austin, Houston,
  Los Angeles, San Francisco.
- Europe: London, Paris, Amsterdam, Munich, Milan, Madrid, Warsaw, Helsinki,
  Moscow, Istanbul, Ankara.

Each region retains its own SQLite, queue and rule identity; the codebase is
shared, the databases are not merged. One writer owns each directory. The
256-MiB limit is per directory, not a global budget for both regional processes.

The original single-contract entry point remains available:

```sh
.venv/bin/holy-weather observe --config config.example.toml
```

That example enables legacy research only: PRICE, queue and Telegram default OFF.
A matching single-contract config enables PRICE with `price_alert_enabled = true`
and `alert_queue_enabled = true`; filtering is independently opt-in. Select the
exact current native bucket before a run. The single-contract observer stops
at its local-day end; **automatic rediscovery belongs to the catalogue**.
Both direct entry points remain finite: default 120 seconds, configured maximum
one hour. The separate [continuous runtime](docs/PRICE_CONTINUOUS.md) opens workers
of at most 20 minutes under a persistent controller; it requires its own approved
plan, installation and sending authority.

## Quotes, delivery and storage

Full fresh book/BBO establishes PRICE baseline/crossings. Depth, trades, heartbeat
or incomplete prices cannot substitute for a midpoint. Source/receipt clocks
remain separate; the catalogue allows at most 100 ms source lead without rewriting
time or extending the **90-second** freshness limit. GAP/restart requires a new
baseline and preserves consumed levels. Unknown gap movement is not reconstructed.

With filtering enabled, bid-growing 70/80 work is immediately ready; ask-only
work waits at most eight seconds. Eligible unconfirmed priority work carries an
explicit ask-only warning. Ordinary 50/55/60 work groups for ten seconds by
default. Continuity and expiry refusals remain final. Ready time is not Telegram
ACK time. See [notification policy](docs/PRICE_NOTIFICATION_POLICY.md).

The queue bounds unresolved work at 1024, retains terminal receipts and records
per-event admission/refusal. UNKNOWN is not retried. Free queue slots do not
mean free physical disk. SQLite remains schema v1; changed quotes and referenced
evidence are retained. Unchanged confirmations coalesce with 15-second checkpoints.
The **256-MiB** guard includes database/WAL/SHM/logs and stops capture before its
reserve is exhausted. The approved continuous plan checks for archival between
closed workers at 128 MiB, retaining full history under separate 4-GiB/40-part
archive and 96-MiB packed dedupe bounds. These are finite capacities, not indefinite
retention. See [ARCHITECTURE](ARCHITECTURE.md). Additional maintenance requires the
stopped matching writer/configuration and separate authority; a status check must
not invoke maintenance.

## Optional US weather and LAMP

The single-contract observer accepts the existing
[AWC config](config.weather.example.toml) and [LAMP config](config.lamp.example.toml).
Neither example enables Telegram or LAMP ALERT automatically. Context keeps METAR,
maximum among received observations and original future LAMP peak separate.
Late receipts cannot revise old events. Independent KLGA LAMP ALERT is opt-in:
configured 2°F peak change or a verified bucket-boundary crossing, comparing the
same remaining hours. See [LAMP ALERT](docs/LAMP_ALERT.md).

Ten US context profiles exist; they are not a continuously qualified weather
fleet. European forecast experiments/PR #13 remain deferred on
`codex/europe-weather-experiments-preserved`. The regional PRICE catalogue rejects
weather/LAMP flags. Forecast expansion is not a prerequisite for PRICE acceptance.

## Checks and qualification

```sh
.venv/bin/python -B tools/check_source.py
.venv/bin/python -B -m pytest -q
```

The runtime baseline's [post-merge CI](https://github.com/Quackw/Holy-Weather/actions/runs/44083496660)
passed **785 tests** on each of Ubuntu/Python **3.11** and **3.13.5**, including
wheel/source checks and isolated rollback. Its Python 3.11 job also qualified the
actual systemd/controller path with a dummy credential and synthetic child on the
disposable runner. This was not a Netcup reboot. Local platform-specific tests
may skip; dependency rollback needs `HOLY_WEATHER_WEBSOCKETS_ROLLBACK_DIR` pointing
to a separate 17.1 import directory. Exact-source rollback needs retained Git history.

Tests are offline/synthetic. Real captures, receipts, databases, backups and
secrets stay out of Git. SHA-bound replay preserves original availability clocks.
Older PR qualification notes and A–D planning tables describe their original
revisions; the current inventory is [ROADMAP](docs/ROADMAP.md). Green CI does not
prove deployment, current quotes, Telegram delivery, 24/7 operation or trading edge.
