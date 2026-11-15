# PR14 reconciliation with accepted PRICE main

PR12 merged as `0a4f3e4d216d3aa6c791a92816b019509177525b`.
Post-merge Linux CI succeeded: run 43742824673 (checks #101).
The existing PR14 history is preserved with a merge from main, no force-push.

The net diff adds only the finite catalogue, bounded PRICE clocks, ordering,
queue/routing and recovery work plus tests/docs. `lamp.py` and `weather.py` match
accepted main exactly. MOSMIX and its tests are absent. App weather polling matches
main; its remaining hunks concern PRICE quote ordering/clocks only. Deferred
weather experiments remain on codex/europe-weather-experiments-preserved.

399 offline tests pass (435 minus 36 deferred weather cases). Source checks and
compile checks pass. Existing clock, grouping, recovery, once-only thresholds,
restart, queue and transaction failure tests all remain included.

Repeated saved Linux streams for London/Paris/Amsterdam and 40,590 retained Mac
messages have identical PRICE events, samples, counters, output sizes and source
SHA to the pre-reconciliation PR14 replay. Two historical Mac PRICE events remain;
none is sent. All three Linux windows contain zero PRICE events.

Six full catalogue replays (three cities, with/without an injected outage) pass.
Source artifacts are unchanged, integrity OK and foreign-key violations zero.
Transport faults/heartbeat are synthetic; market frames and receipt clocks are
original. Evidence: evidence/price-main-reconcile-20261115/ outside Git.

No schema change or database replacement; existing restart/deduplication tests
pass. Accepted main cannot open catalogue-rule-hashed data: rollback to main
means preserving the candidate directory and using the old single-market runner
only with its compatible directory. It is not a guarantee that main can continue
a catalogue run. No service, production change, Telegram send or active observer
operation occurred. PR14 stays unmerged pending separate acceptance.
