# PR 4: bounded stream recovery

Scope: part A of the stabilization request. No new runtime module, service, model, Telegram or production change.

## Reproduced defect

`process → reject → gap` treated a data-order rejection as a transport disconnect. The still-open socket could receive valid quotes and populate ready tokens, but `connected` stayed false until a real reconnect. An exact repeated old sample could also enter this rejection path before duplicate detection. Two targeted regression tests failed before the change.

## Classification and recovery

| Input / condition | Action |
|---|---|
| Exact previously seen sample, including repeated packet members | Count and diagnose DUPLICATE before order checks; do not refresh quote time or change readiness. |
| Unknown older source timestamp / reversed receipt clock | Preserve rejected raw input, mark a data GAP for the affected token, reset comparison points; retain transport-connected state and the other token's readiness. This is not assumed to be harmless. |
| Invalid input with uncertain token scope / malformed frame | Invalidate both relevant token baselines; do not invent a transport disconnect. |
| Further reversed messages | Reject below the last confirmed source high-water timestamp; do not let an old full quote re-establish baseline. |
| Confirmed network gap / disconnect | Clear both baselines and connection state; use the existing bounded reconnect loop. |
| New full quote (`book` or `best_bid_ask`) | Establish a fresh baseline only if clocks, ask and 90-second freshness pass; no comparison to the pre-gap price. `price_change` alone cannot establish it. |
| Baseline still absent after five monotonic seconds | Close the connection through its normal context manager and use the same finite reconnect/backoff budget. On exhaustion, exit explicitly with BASELINE_RECOVERY_BUDGET_EXHAUSTED. No endless retry or false healthy state. |

The five-second recovery deadline is independent of the unchanged 90-second freshness limit. It also bounds initial baseline acquisition. Successful baseline receipts clear only their own pending state. Source high-water is an additive JSON state field, retained across gaps/restarts without a schema migration; coalesced confirmations are included when a gap is recorded. Readiness is never persisted as proof of a new connection.

## Proof

180 tests pass locally and on isolated Linux. Tests cover a single old sample, repeated exact packets, multiple reversed timestamps, side independence, fresh baseline without false crossing, a true network gap, restart between gap and baseline, retained inputs/diagnostics, and successful/exhausted runtime reconnect budgets. AWC remains usable across the synthetic market recovery; LAMP failure does not erase market events. The socket/API in these failure tests are simulated.

All ten saved real city streams were replayed twice into new directories. Each pair is identical; original database SHA values are unchanged. Integrity and foreign keys pass. A copy of the accepted KLGA database was read/written with the new code, then the exact prior PR head 80f96514ccee4e0962122fca3291fe688e10d195, then reopened with the new code; new records survived and schema stayed at version 1.

### The original 13 ordering incidents

The original diagnostics contain NY9 / Dallas3 / LA1. They are not established safe duplicates in the retained inputs. The new source high-water checks conservatively reject affected token updates; counts of rejected token updates are not counts of incident packets. In saved replays the counters are NY17 / Dallas6 / LA4, including a later LA update still below the retained high-water mark.

A separate causal reprocessing keeps all original inputs and external gaps but regenerates the old implementation's derived INVALID_INPUT gaps rather than treating those internal resets as network disconnects. New York returns to QUOTES_FRESH after confirmed full quotes. Dallas and Los Angeles end without sufficient baseline confirmation and remain PARTIAL_OR_UNUSABLE. Their recorded stream cannot prove what a new network connection would return; finite reconnect and exhaustion are therefore tested explicitly in the runtime harness, not fabricated in the historical replay.

No new price events appear: Dallas retains one detected crossing and one hysteresis-suppressed crossing; NY and LA have none. Replays preserve recorded transport gaps; coalesced messages cannot be reconstructed, so no claim of full-wire continuity is made.

Raw evidence: ignored directories evidence/stream-recovery-20261114 and evidence/ten-city-20261114. No source files or working databases from POLYMETEO were modified. No new live experiment or persistent process was started for this fix.
