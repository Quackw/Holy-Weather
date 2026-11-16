# Unified regional PRICE candidate — 16 November 2026

Based on accepted main 01ef4aac and unmerged PR 18 (1173bbf).
This branch retains PR 18's no-sender queue timer correction. Accept PR 18
before merging this candidate; no production installation is included.

One catalogue implementation selects `--region europe` (default, 11 cities) or
`--region us` (10 cities). The CLI chooses the matching historical configuration
identity: london/europe or new-york/us. Each region keeps its existing separate
data directory and rule hash. This is not a merger of databases or subscriptions.

The regional routing changes are ported from local US build 4dadebd without
replacing current main's queue, transport, rollover or storage implementations.
PRICE detector, thresholds, once-only state, quote age, schema v1, weather,
Telegram transport and dependency versions are unchanged.

Example (finite, no send):
```
python -m holy_weather.catalogue --region us --data-dir /path/to/isolated-us --seconds 120
```
Never point this example at a working directory without an authorized runtime plan.

Validation: main+PR18 passed 540 local tests. Unified code passed 551 tests;
four additional CLI/date tests passed separately (17 targeted cases total).
Two isolated copies of the verified off-server backups each reopened twice with
session=False. All rows of inputs, samples, events, event_inputs, meta, state and
latest remained identical; integrity and foreign keys passed. Backup originals
remained byte-identical. Private evidence: evidence/regional-reconciliation-20261116.
This proves reader/configuration compatibility, not live runtime or deployment.

Rollback: return code/config to the matching prior regional build, retain the
new database and fired/delivery records. Do not restore an older database over
new records. Synthetic restart/no-repeat tests cover all ten US cities; existing
EU, GAP, queue, capacity and rollover tests remain in the full suite.

Linux qualification passed 557 tests on Python 3.13.5 in an isolated directory.
The two subprocess rollback probes used the exact prior EU/US runtime sources
from verified backups: old code writes an event, new code adds another event and
synthetic SENT/UNKNOWN receipts, then old code preserves all event/input/meta rows,
does not repeat consumed thresholds, and records a new 80% crossing. No network
transport or real delivery is used. CI runs the same probes as restart coverage;
the historical source override is private Linux qualification evidence.
Test assembly initially omitted tools/fixtures and the subprocess dependency path;
these packaging errors were corrected before the successful complete run.
GitHub CI must pass on the published head before acceptance. Existing dependency/security and
long-term retention findings are separate work, not silently changed here.
