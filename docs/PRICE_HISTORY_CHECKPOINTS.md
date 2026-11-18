# Explicit PRICE history checkpoints

This is closed-writer maintenance, not a background archiver or a service.
The existing 256 MiB working-directory limit and eight-MiB reserve remain.
Installing code alone never archives, prunes or adopts a database.

The 16 November US observer reached its storage guard with 380,113 inputs,
354,148 samples and 14,732 events (84 PRICE, the rest legacy research). The stopped
SQLite was intact. Inputs occupied 143.52 MiB, samples 47.62 MiB and the input
identity index 30.26 MiB. There was only one free page: ordinary compaction alone
could not recover substantial headroom. This is retained-history growth, not a
new WebSocket timeout or a lost Telegram receipt.

`python -m holy_weather.history prepare` requires the owned directory, exclusive
writer lock, empty WAL, expected source SHA and schema v1. It never opens Store on
the original, changes its journal mode, checkpoints, prunes, renames or writes it.
It creates an owner-only new destination and a SQLite Backup API copy. Every table
fingerprint is compared with the source. A gzip archive preserves the exact
original file bytes; full decompression, original SHA, integrity, FK and table
checks prove restoration before a candidate is produced. Any failure leaves the
source untouched and the unfinished destination for inspection.

The candidate is a fresh physical copy of the existing schema, not a new runtime
database or a migration. It retains:

- Every PRICE/LAMP/unknown event type and every queue/admission-referenced event.
- Recent legacy CROSS/MOVE/RETURN history (at least the last 15 minutes).
- Every input and sample referenced by retained events, evidence, latest pointers,
  current state or any retained JSON input reference, including confirmations.
- Every existing market identity, configuration hash, fired level, queue receipt,
  source high-water, current state and diagnostic row.
- The maximum input ID, so subsequent inserts cannot reuse archived input IDs.

Only older unreferenced inputs/samples and old unqueued legacy research events
leave the working projection. They remain byte-for-byte available in the full
archive. A packed exact SHA256 index in existing `meta` preserves recognition of
archived duplicates without replaying old messages or introducing another database.
It is bounded to 96 MiB decoded, checked for hash/order/length, and searched in
packed bytes. It never guesses from a probabilistic filter. Corrupt or oversized
state fails closed. No alert rule, formula, time, threshold or expiry changes.

The optional [PRICE storage lifecycle](PRICE_STORAGE_LIFECYCLE.md) replaces
the cumulative inline index with exact disk lookup and verified index increments.
Its separate history-disk policy requires its own approved deployment.

At a closed continuous PRICE catalogue checkpoint, unreferenced market inputs
are retained in the full archive. The successor starts with a new baseline. The
15-minute research-event window and all its evidence stay in the working SQLite,
along with every PRICE/queue/state reference, the input ID high-water and non-market
receipts. The exact duplicate index still covers inputs moved to the archive.
The manifest records this policy. Finite and manual preparation keep their
existing retention policy.

`adopt` is a separate explicit operation. It rechecks source/candidate/archive
SHA, WAL, lock, integrity and headroom, writes a one-time intent and atomically
replaces only the stopped working SQLite. An existing intent forbids a blind retry.
Optional cleanup removes only this newly created part's redundant snapshot and
restore-proof files after their SHA/ownership checks; archives/manifests remain.
It never controls a process. Deployment/maintenance requires separate exact approval.

`restore_history` works only in a new isolated destination with closed input DBs.
An owned source must have its existing, non-symlink writer lock. Restoration
acquires that lock exclusively before checking WAL or creating output, and holds
it until the operation returns or fails. An empty WAL alone is not proof that
the observer stopped. A busy/missing/invalid owned-source lock refuses without
creating a destination or changing the source. Closed archive candidates and
isolated inverse files without a runtime owner remain compatible.
It restores archived inputs, samples, research events and evidence into a copy of
the current DB while retaining current state, fired levels, queue and new records.
Conflicting IDs/identities stop restoration. Multiple parts can be applied newest
first. It never rewinds the working DB, adopts the result, deletes an archive or
promises a restored all-history DB fits the active 256 MiB limit.

## Qualification and rollback boundary

Tests cover exact duplicate recognition, original-byte restoration, preservation
of new records/cursors/receipts, inverse chains, SHA/WAL/writer-lock refusals,
corrupt index/archive, bounded index writes and one-time adoption. The full stopped
US copy prepares to 64,327,680 bytes, with a 130,821,061-byte full archive; original
SHA remains unchanged. These measurements are for that dataset, not a daily rate.

Queue rollback uses the exact accepted queue code while retaining the checkpoint
reader needed by compacted data. Archived state cannot safely be discarded from
code or files after adoption. Whole-history inverse restoration is proven only in
isolation; it is not an automatic rollback over newer records. Any checkpoint or
unknown launch/adoption failure stops the finite run. Returning the old queue also
returns its lower-level notification noise.

A separately approved finite wrapper may call maintenance only between clean,
completed observation phases and record the observation gaps. New connections
remain baselines, never inferred crossings across maintenance. No uninterrupted
market coverage or absence of missed movements during those gaps is claimed.
Archive-count/byte bounds, staging cleanup and phase duration must be pinned in
that exact run manifest. There is no default automatic maintenance, silent limit
increase, permanent supervisor, weather expansion or trading.
