# PR 10 final qualification — 14 November 2026

All load scenarios below are synthetic and use identical OFF/ON inputs. Each pair uses a fresh owned test directory, existing schema, 256-MiB global and 1-MiB LAMP bounds. Sizes are bytes. Peak is sampled after every input and includes SQLite/WAL/SHM/owned files; there are no application logs in this direct-ingest test. It is not an OS-level transient peak. SQL counts are traced INSERT/UPDATE/DELETE statements, not physical disk writes.

| Scenario | Inputs OFF/ON | Saved input rows OFF/ON | Events OFF/ON | SQLite OFF/ON | Peak OFF/ON | SQL writes OFF/ON |
| --- | --- | --- | --- | --- | --- | --- |
| duplicate500 | 500/500 | 501/501 | 0/0 | 380928/389120 | 974068/986404 | 2500/2505 |
| new_stable | 5/5 | 6/6 | 0/0 | 73728/81920 | 288236/370636 | 25/50 |
| sub2 | 2/2 | 3/3 | 0/0 | 73728/81920 | 201716/234676 | 10/20 |
| peak2 | 2/2 | 3/3 | 0/1 | 73728/86016 | 201716/259396 | 10/24 |
| boundary1 | 2/2 | 3/3 | 0/1 | 73728/86016 | 201716/259396 | 10/24 |
| both | 2/2 | 3/3 | 0/1 | 73728/86016 | 201716/259396 | 10/24 |
| down | 2/2 | 3/3 | 0/1 | 73728/86016 | 201716/259396 | 10/24 |
| restart_duplicate | 3/3 | 4/4 | 0/1 | 73728/86016 | 201716/259396 | 15/29 |
| invalid | 4/4 | 5/5 | 0/0 | 73728/81920 | 259396/300596 | 20/37 |

Saved input counts include one discovery row. Every distinct HTTP receipt is retained; repeated bulletin downloads still have distinct receipt clocks. For duplicate500: 499 repeats, zero events; ON adds one baseline diagnostic and only five SQL write statements overall (three affected rows) versus OFF. The payload body hashes and original receipt evidence are not deleted.

ON suppressions: new_stable = baseline + 4 NO_MATERIAL_CHANGE; sub2 = baseline + one NO_MATERIAL_CHANGE; each qualifying change = baseline + exactly one event; restart_duplicate = one baseline, one event and one duplicate with no new diagnostic; invalid = baseline, PARTIAL_TMP, MISSING_TMP, STALE_OR_FUTURE. OFF produces no alert diagnostics. All cases passed integrity and foreign-key checks.

## FIX
A reproduced clock-order defect followed a duplicate bulletin: the lightweight alert state intentionally did not rewrite its last_at for duplicates, allowing an earlier availability time afterward. The ingestion path now checks the existing prior poll_available_ms before recording, only with LAMP ALERT enabled. No additional writes, schema changes or PRICE logic changes. A regression failed before and passes after the fix.

## PROOF
207 tests passed locally, retaining the original 204. Added 500-duplicate write/size regression, reversed-availability-after-duplicate regression, and PRICE independence / frozen-event evidence under a later-received older forecast. Existing tests cover expiry, both conditions, downward changes, restart, partial/stale data and injected SQLite rollback.

Two real saved KLGA replays preserve SHA and produce identical counters/events, with integrity and foreign keys valid. Only one real bulletin exists: **real forecast change remains unconfirmed**. No new collection.

Compatibility: exact accepted main 11f91183dc98a84660442377705800706591cd53 read and wrote a copied ON database. New code reopened it. Inputs, events and event-input references matched before/after byte-for-byte as SQLite values; schema remains 1. This is code/database compatibility, not a production service-switch claim.

## SAFETY AND ACCEPTANCE
No false/duplicate events, lost evidence or damaged databases were found in these bounded scenarios. This is not a universal failure-proof guarantee. CI must pass on the final commit. READY FOR MERGE within tested scope; no automatic merge, Telegram or production actions.
