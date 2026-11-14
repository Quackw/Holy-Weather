# PR D — shared PRICE/LAMP queue

Default-off: `alert_queue_enabled = true` enables durable enqueue only.
`telegram_enabled = true` additionally enables the finite run's sender and requires
local `HOLY_WEATHER_TELEGRAM_TOKEN`. Neither is enabled by this PR. No service,
new DB, dependency or production installation. No real sends performed.

Both event producers enqueue within their existing event/state transaction.
A namespaced meta record per event contains its exact immutable short English
card, event ID, PRICE/LAMP type, contract URL, source-bound expiry and target
@holy_weather. Baselines and suppressed diagnostics never enqueue. Generic legacy
research events do not enqueue. PRICE dedupe and LAMP dedupe remain independent.

Queue lifecycle: PENDING → UNKNOWN committed before HTTP → SENT on verified ACK,
or explicit REJECTED/UNKNOWN. Crash before request may lose delivery but cannot
blindly resend an uncertain request. Confirmed and uncertain keys persist across
restart. No exactly-once network claim. Pending events resume when eligible; PRICE
expires at its configured source freshness limit (<=90s), LAMP at issue+2h; both
expire after local contract day. Final expiry is checked again before HTTP. Cards show the approved short format without clock or technical-status lines.
Full clocks, state and evidence remain in originating events and the journal.

One daemon HTTP worker, no SQLite access from that worker. The existing writer
claims and records acknowledgements; collection continues while HTTP waits. A
transport failure stops the sender instance, not ingestion; future pending events
remain. Shutdown records completed results if available, otherwise UNKNOWN.

Reuse from PR #5: bounded HTTPS sendMessage, exact text/link/channel ACK validation,
no token logs and no automatic retries after errors. PR #5 is not merged. This
replaces its recent-event polling with a durable queue. A shared SQLite write
failure remains fatal to the writer, not treated as a Telegram network error.

Bound: 1,024 delivery records, no eviction or silent increase. Full queue explicitly
sets alert_queue_status=FULL; new analytical events still persist but are not
queued. No automatic backfill is promised. Events referenced by delivery records
and their source evidence are protected from normal pruning. Terminal records are
retained for dedupe. Global 256-MiB guard remains; long-run queue growth is not yet
qualified. No migration, schema version remains 1. Old code does not implement the
new prune protection: do not run old maintenance against a pending queue.

## Exact preview and separately approved test

```sh
PYTHONPATH=src python -m holy_weather.telegram preview --config test.toml --key QUEUE_KEY --test
PYTHONPATH=src python -m holy_weather.telegram send --config test.toml --preview-file PREVIEW.json --preview-sha EXACT_SHA
```

Before the send command, show the exact card and obtain explicit permission.
Test mode is visibly marked, allows old/synthetic event data and consumes the
same queue item once. It must use an isolated test database. Previous PR #5 test
permission does not authorize these messages. Never paste a token into chat.

## Qualification

219 tests pass: all existing 207 plus queue persistence, separate types, SENT and
UNKNOWN restart suppression, expiry, exact-preview tampering, atomic enqueue
rollback, retention, queue bounds, worker timeout with continued market ingestion,
verified ACK text/link and pre-HTTP expiry. All Telegram responses are synthetic.
No claim of actual delivery, live latency or production readiness. Exact previews
are saved outside Git from synthetic scenarios, clearly marked TEST. No new real
PRICE/LAMP event was fabricated or attributed to a live market.

Next: review PR D and approve exact test preview(s) before any outbound action.
No automatic merge or deployment.

## Finite live observation

One explicitly approved 120-second Linux run completed with Telegram disabled.
See [LIVE_QUEUE_20261114.md](LIVE_QUEUE_20261114.md): AWC/LAMP and initial quotes
were received; quotes aged out and no genuine PRICE/LAMP event formed. This is
not proof of live delivery or continuously fresh market data.
