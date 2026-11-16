# Unified regional PRICE candidate — 16 November 2026

Reconciled with accepted main e3ae46c after PRs 18 and 20. Main's post-merge
Linux CI passed. Both accepted queue timer fixes are retained through a normal
merge from main, without rewriting branch history or duplicating their changes
in this PR's diff. No production installation is included.

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

Validation after reconciliation: 565 local tests passed, including the two
rollback probes with exact historical regional runtime sources from verified
backups. Separate fresh copies of the full off-server backups are compared
table by table and reopened twice with session=False. Integrity, foreign keys,
schema v1, market identities, fired thresholds and queue states are checked.
All rows remained identical and the backup originals retained their SHA256.
Europe/US retain 17/27 PRICE events, 19/29 fired thresholds, 121/110 contracts
and SENT 17/19; US also retains seven EXPIRED and one PENDING delivery.
Private evidence: evidence/pr19-main-reconciliation-20261116.
This proves reader/configuration compatibility, not live runtime or deployment.

Rollback: return code/config to the matching prior regional build, retain the
new database and fired/delivery records. Do not restore an older database over
new records. Synthetic restart/no-repeat tests cover all ten US cities; existing
EU, GAP, queue, capacity and rollover tests remain in the full suite.

The subprocess rollback probes run old code → candidate → old code: the old code
writes an event, the candidate adds another event and synthetic SENT/UNKNOWN
receipts, then old code preserves all event/input/meta rows, does not repeat
consumed thresholds, and records a new 80% crossing. No network transport or real
delivery is used. CI uses the current source for these probes as restart coverage;
the exact historical source override is separately exercised offline. The prior
candidate e9e2cf5 also passed historical-source rollback within 557 Linux tests on
Python 3.13.5; that result is retained separately from the current reconciliation.

GitHub Linux CI must pass on the reconciled published head before acceptance.
Existing dependency/security and long-term retention findings are separate work,
not silently changed here.
