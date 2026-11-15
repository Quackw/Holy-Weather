# PRICE delivery capacity — bounded work, retained receipts

The old 1024 bound counted every delivery receipt, including SENT/EXPIRED/REJECTED.
Once reached, it permanently blocked new enqueue in the same directory. The new
bound counts unresolved work only (PENDING, UNKNOWN and unrecognized states).
Final receipts stay at their original keys; message IDs, content hashes and
restart dedupe remain available. No history, table or source evidence is removed.
This frees queue slots, not physical disk space. The unchanged 256 MiB guard still
stops capture before exhaustion; daily retention/archiving is separate work.
UNKNOWN is not retried or treated as a confirmed delivery and still consumes a slot.

Each newly recorded event gets additive delivery_admission in its existing payload:
QUEUED/ACCEPTED, NOT_QUEUED/QUEUE_FULL or NOT_QUEUED/QUEUE_DISABLED, with receipt time.
This is committed with the event, evidence and fired thresholds. A refused event
cannot later be silently backfilled. Actual sending status remains in the durable
receipt; admission is not delivery confirmation. Historical missing admissions
are not rewritten or guessed. Existing receipts still deduplicate by original key.

Explicit pruning now retains delivery-audited events, including refusals, and their
referenced inputs. Schema/user_version stays 1. No migration, dependency, service,
new weather collection or transport change. Sender pacing/90 seconds/thresholds
are unchanged. The same queue handles accepted LAMP events without activating LAMP.

Validation includes actual 1024-slot saturation, individual refusal, slot reuse,
SENT/REJECTED/EXPIRED versus UNKNOWN, legacy receipts, two-code compatibility,
restart dedupe, atomic rollback on admission failure and evidence retention.
Synthetic capacity records are clearly test-only. Existing real receipts 362–364
are not resent. Active observation and its old installed code are untouched.

Rollback: old code can reopen schema 1 with the same configuration, preserving all
new event fields and receipts. It reintroduces the total-record cap and will report
FULL once total receipts exceed 1024. Do not run old explicit pruning: it does not
recognize refusal admission and can delete such events after 30 days. Never restore
an older database. Code rollback is compatible for observation, not continued
new capacity semantics or old maintenance. No automatic rollback/prune is run.
