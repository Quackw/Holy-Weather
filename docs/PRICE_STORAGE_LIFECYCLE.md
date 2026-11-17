# PRICE storage lifecycle

The opt-in history storage policy uses the existing regional writer, checkpoints,
SQLite schema v1 and service. Installing source alone does not enable the policy.
Existing plans retain their original serialization, hashes and bounds.

## Working data

The live directory remains limited to 256 MiB. A worker closes normally at
160 MiB to permit a checked checkpoint before the hard reserve is reached.
Connection closure remains a real data gap, followed by a new baseline.

Closed continuous checkpoints write a new compact SQLite and an immutable sorted
SHA256 file. The file replaces the cumulative compressed duplicate index in
SQLite. Exact lookup uses bounded reads from disk. New PRICE inputs, fired levels,
source clocks and queue changes still commit in their existing transaction.

Every full original SQLite is archived and restored before adoption. The archive
manifest binds both the materialized index and its sorted incremental file.
The first incremental file contains the complete pre-migration index. Later files
contain only newly archived identities. Adoption independently verifies their
coverage against the source and the candidate.

The current full index stays on the primary disk. Cold history keeps verified
increments. Historical indexes can be reconstructed in a new private directory
with `history_files.materialize_index`, which verifies the original full SHA.
This avoids another full cumulative index in every historical part.

## Closed contracts and audit

Markets older than two completed local days can leave the working projection
when their deliveries are terminal. Their events, evidence, fired states and
receipts remain in the full source archive. The working projection preserves
current markets and unresolved deliveries. UNKNOWN continues disabling the sender.
Retired token identities enter the exact disk index so a later registration
cannot reintroduce them as new contracts.

Historical delivery/event counters and the last SENT time remain visible.
Older closed controller audits move to history, retaining the newest two of each
namespace. A checked manifest chain replaces an ever-growing list of checkpoint
rows in SQLite. Full history restoration follows that chain and applies parts
newest first. Newer state, receipts and records take precedence.

## Primary spool and history disk

The reviewed deployment selects two separate private directories on the existing
additional filesystem, one for Europe and one for US. Each region initially has
the following proposed budget.

| Area | Budget |
| --- | --- |
| Live directory including WAL and logs | 256 MiB |
| Primary disk archive spool and current index | 64 GiB |
| Additional disk historical parts | 192 GiB |
| Free space reserve on each filesystem | 16 GiB |

These larger archive budgets require explicit installation approval. The former
40-part gate is retained for existing plans. The new policy accounts for actual
bytes and free space. At 80% usage it records PRESSURE for operator review.

Copying is paced between closed workers, limited to 512 MiB and 90 seconds per
pass. The history copy is checked file-by-file and made durable before local
payload copies can retire. Local manifests and locators remain. Partial copy
scratch has explicit ownership and restart rules. Original historical data is
retained on the history disk.

An absent, full or inaccessible history disk records SPOOLING while collection
uses the primary disk budget. A configured mountpoint must actually be mounted.
The active full duplicate index remains on the primary filesystem.
The unit's optional writable history path allows primary collection to start
when that path is missing. A new supervisor or transfer service is unnecessary.

Both physical filesystems remain finite. Consumed spool/reserve or storage
corruption still requires protective action. The policy addresses the previous
small archive/index gates and bounds active data. It does not prove indefinite
retention, recovery from every mount failure or a daily data rate.

## Recovery and rollout

The newer reader is required by the external index and manifest chain.
Code rollback must retain the compatible history reader or reconstruct history
in isolation. The current SQLite must never be replaced with an older snapshot.
Existing checkpoints retain their original bytes and SHA.

The server installation needs a separately reviewed package, the latest closed
database/control backup, source verification, isolated restoration, matched
code/unit/plan binding and one graceful regional cutover. Europe and US are
verified independently. Credentials remain in the existing encrypted store.
Real acceptance requires subsequent completed worker windows, fresh city quotes,
queue/ACK checks and measured primary/history disk use.
