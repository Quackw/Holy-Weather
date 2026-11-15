# Europe PRICE-only catalogue (candidate)

Scope: every verified open v1 temperature bucket of London/EGLC, Paris/LFPB,
Amsterdam/EHAM, Munich/EDDM, Milan/LIMC, Madrid/LEMD, Warsaw/EPWA,
Helsinki/EFHK, Moscow/UUWW, Istanbul/LTFM and Ankara/LTAC. No weather or forecast
HTTP calls. No forecast readiness or trading claim. Depends on draft PR #12.

One bounded process, native WebSocket, SQLite writer and durable Telegram sender.
Existing PRICE thresholds and baseline behavior are reused. Catalogue identities
and exact discovery references use namespaced meta/latest records in schema 1;
no migration, extra database, dependency or service. A catalogue-specific rule
hash refuses opening this database with the old single-market CLI. Old
single-market databases are unchanged. Rollback means stopping this finite
candidate and keeping its database; do not overwrite it with an older snapshot.
The accepted CLI remains available for its own existing single-market directories.

All buckets share the existing 256 MiB directory cap, 1 MiB WAL checkpoint,
1024-delivery queue cap and 512 diagnostic records. At most 176 markets / 352
tokens are accepted, at most 16 buckets per city. One compressed Gamma response
is shared across that city's discovery references. Every event references its
own contract's Gamma receipt. Unchanged top confirmations retain the existing
15-second checkpoint; source/receipt clocks remain distinct.

A single sender starts messages at least 3.1 seconds apart. Expired signals are
not sent; a crowded queue may cause missed delivery, explicitly retained as
EXPIRED or queue FULL. Unknown sends are never blindly retried. The short live
window is not a daily resource forecast. Daily growth: NOT MEASURED.

The CLI defaults to 120 seconds, no Telegram. Maximum window remains 3600 seconds.
Discovery precedes that window (eleven bounded 12-second requests). No automatic
next-day transition, permanent service or recovery supervisor is introduced.
Each market ceases to generate events at its own local midnight or resolution.
A gap invalidates baselines; fired thresholds survive restart.

Run only after reviewing exact scope and permissions:

```
python -m holy_weather.catalogue --data-dir <new-owned-directory> --seconds 120
```

`--send` is opt-in, requires the existing token environment variable and explicit
approval of the shown card / bounded delivery scope. Never put the token in a
command argument, evidence, Git or printed output. Production is not involved.

## Evidence (2026-11-15)

Fresh Gamma read: 121 verified buckets (11 per city), zero rule rejections.
Ignored local evidence: `evidence/europe-price-catalogue-20261115/`.
Initial all-token live attempt closed before a price message; database integrity
passed, zero events / sends. This is not a successful live qualification.
Further results must be recorded before declaring operational readiness.

Regression coverage includes all eleven cities, all 121 synthetic markets,
independent sides, per-contract evidence, shared queue pacing, restart dedupe,
GAP baseline, resolution isolation, midnight, metadata causality, transaction
rollback and 6000 unchanged confirmations. These are synthetic checks, not
proof of real signal quality.

The initial failure was reproduced with close code 1009: a 440107-byte initial
snapshot exceeded the unchanged 262144-byte frame limit. The candidate now opens
with sixteen tokens and adds at most sixteen per second on the same connection.
This uses the official dynamic subscription protocol:
https://docs.polymarket.com/market-data/realtime-data (market stream subscription).
No frame/storage/freshness limit is raised. Until a token receives its native
full quote, it remains without a confirmed baseline and cannot alert.
