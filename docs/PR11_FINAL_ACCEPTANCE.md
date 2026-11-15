# PR 11 final acceptance and read-only disk audit — 15 November 2026

CODE: PASS within the bounded offline and preserved-real-input qualification. Telegram transport has not been exercised with these two TEST cards. PR remains open; do not merge automatically. Exact-head green Linux CI is required before acceptance.

## Reproduced correction

An event claimed just before local midnight could pass the final HTTP freshness check on the next contract day. Both PRICE and LAMP regressions failed before the correction. The queue now caps expiry at the last millisecond of the local contract day. Preview/claim apply the same cap to legacy queue entries without migrating or rewriting their evidence. Existing source-age limits are never extended. All four new/legacy PRICE/LAMP midnight cases now pass. Changed application file: `src/holy_weather/alert_queue.py` only.

No changes to PRICE midpoint, thresholds, fired flags, LAMP comparison or trigger rules, weather, storage schema, budgets, services or production. No diagnostic module is added by this audit; local inventory/measurement drivers and raw results remain ignored evidence.

## Acceptance matrix

| Requirement | Direct check | Result |
| --- | --- | --- |
| PRICE parity | Pinned original Netflix source SHA rechecked: `4e304a3092ded5cc51ef0fe2a38aaa0a353fc64e3465e5b68e0d3194d068cee8`; real own-side Decimal midpoint; upward 50/55/60/70/80%; MULTI-CROSS and durable once-only flags | PASS |
| Independent LAMP | New bulletin is sufficient; matching remaining hours; no market samples needed; duplicate/expired-hour/late guidance tests | PASS |
| Separate dedupe | PRICE token states and LAMP bulletin ledger; separate typed event IDs and queue keys | PASS |
| Atomicity | PRICE and LAMP queue-insert SQLite failures roll back originating event/state/source receipt; retry in the isolated test yields one event | PASS |
| Restart | PENDING survives; SENT and UNKNOWN cannot claim again; first price after gap/restart is baseline and fired thresholds stay consumed | PASS |
| Failure isolation | Timed-out sender stops only itself; new METAR, LAMP event and PRICE event continue to record | PASS |
| Unknown delivery | UNKNOWN is committed before HTTP; only matching channel/text/link ACK permits SENT; no blind retry | PASS |
| Freshness | Stale source inputs do not trigger retrospectively; pre-HTTP expiry and local-day clamp; frozen event evidence | PASS |
| Bounded queue/evidence | 1,024 records, explicit FULL without eviction; source analytical event survives even when no new delivery can be queued; referenced evidence protected from pruning | PASS, finite scope |
| Limits | Quote <=90 seconds, LAMP context <=2h, 1-MiB LAMP journal, 4-MiB AWC journal, 256-MiB owned-data budget, existing reserve/log/WAL guards | UNCHANGED |

272 tests pass locally (266 original + 6 new cases). The added tests cover four midnight variants, LAMP atomic queue rollback and weather/LAMP continuation after a sender timeout. Source checks, in-memory compilation and diff checks pass. Linux CI runs the same complete suite on the final PR head.

Two fresh cold replays of the preserved real 07:48–07:50 UTC KLGA run match each other and live event semantics. Current-day METAR/quote causality remains valid; SQLite integrity is ok, foreign keys have zero errors, schema stays v1, original artifact SHA values stay unchanged. This saved live window contained no PRICE or LAMP alert; actual live alert-triggered delivery is not claimed.

## Current disk measurements

Allocated space is measured with `du -sk`, including directory/symlink allocation; apparent file sizes are also retained in ignored JSON. Overlapping category totals are not added twice. Only known HOLY WEATHER roots were searched; this is not a whole-filesystem discovery guarantee. SQLite files were only stat-ed, never opened during the inventory. No server application, service, installation or cleanup was started.

### Linux test host v2202609428928530766

Measured at **2026-11-15T10:37:42.705379+00:00**. 16 roots. Total **223,014,912 bytes (212.684 MiB)**.

| Object | Allocated bytes | Size |
| --- | ---: | ---: |
| Source / Git | 3,031,040 | 2.891 MiB |
| Python dependencies | 76,455,936 | 72.914 MiB |
| SQLite + WAL/SHM + technical logs | 92,336,128 | 88.059 MiB |
| WebSocket captures | 471,040 | 0.449 MiB |
| Research / archives | 393,216 | 0.375 MiB |
| Other test files, directory blocks and symlinks | 50,327,552 | 47.996 MiB |

Filesystem available: **148,450,328,576 bytes (138.255 GiB)**.

All 16 discovered roots are `/var/tmp/holy-weather-*` test directories, not a production checkout. No matching HOLY WEATHER observer/sender process was present at measurement. Nine `deps` copies occupy 76,455,936 bytes; `test-tmp` subtrees occupy 137,519,104 bytes (these are parts of the total, not additional rows). Test pressure files and synthetic fixture databases explain much of the retained volume. WAL allocation is 0 bytes; SHM totals 393,216 bytes. Symlinks account for the 2,859,008-byte difference between regular-file/directory totals and du.

| Test root | Allocated bytes |
| --- | ---: |
| `/var/tmp/holy-weather-active-selection-8qzd9d7t` | 696,320 |
| `/var/tmp/holy-weather-code-review-vj6cx6b5` | 26,644,480 |
| `/var/tmp/holy-weather-live-queue-j8fpkp50` | 8,945,664 |
| `/var/tmp/holy-weather-metar-quotes-v3qad05s` | 655,360 |
| `/var/tmp/holy-weather-seattle-check-0l3tsr0u` | 167,936 |
| `/var/tmp/holy-weather-seattle-quoted-oww4vpsw` | 151,552 |
| `/var/tmp/holy-weather-source-qualification-22ev0nc0` | 307,200 |
| `/var/tmp/holy-weather-stage2-8wld1e9b` | 19,066,880 |
| `/var/tmp/holy-weather-stage3-ywo9_v0p` | 25,403,392 |
| `/var/tmp/holy-weather-stream-diagnostic-lb_9e2he` | 233,472 |
| `/var/tmp/holy-weather-stream-recovery-k0dt5x20` | 27,381,760 |
| `/var/tmp/holy-weather-telegram-6nu6mydm` | 29,372,416 |
| `/var/tmp/holy-weather-telegram-jis4sncm` | 29,171,712 |
| `/var/tmp/holy-weather-ten-city-3imm8_w9` | 26,296,320 |
| `/var/tmp/holy-weather-ten-city-r1j3e540` | 27,938,816 |
| `/var/tmp/holy-weather-ws-first-0dwhmsq2` | 581,632 |

### Mac

Measured at **2026-11-15T10:51:55.511002+00:00**. 19 roots. Total **141,262,848 bytes (134.719 MiB)**.

| Object | Allocated bytes | Size |
| --- | ---: | ---: |
| Source / Git | 2,695,168 | 2.570 MiB |
| Python dependencies | 31,404,032 | 29.949 MiB |
| SQLite + WAL/SHM + technical logs | 55,816,192 | 53.230 MiB |
| WebSocket captures | 1,482,752 | 1.414 MiB |
| Research / archives | 47,149,056 | 44.965 MiB |
| Other test files, directory blocks and symlinks | 2,715,648 | 2.590 MiB |

Filesystem available: **109,998,096,384 bytes (102.444 GiB)**.

Project locations: `/Users/mac/Projects/Holy-Weather` (main checkout/shared Git) and `/Users/mac/.codex/worktrees/holy-weather-foundation/Holy-Weather` (PR worktree, `.venv`, evidence and new tests), plus related temporary folders. Local process enumeration is **NOT MEASURED**: `/bin/ps` was denied, and that probe was not retried. Directory sizes were measured independently.

Separately discovered legacy experiment `/private/tmp/pm-holy-weather-monitor-20261114.o55EAA`: **256,196,608 bytes**. It is outside this repository and excluded from the independent-project total; its two `history*/journal.jsonl` files each occupy about 108.9 MB. It was not changed. Including that separately named legacy directory, all matched local roots total **397,459,456 bytes**. Full exact per-root/category measurements remain in `DISK_LOCAL_FINAL.json`.

## Controlled resource comparison

All rows below are newly measured synthetic input streams, not a daily real collection. OFF/ON inputs are identical within each pair. Peaks are sampled after each input and include owned SQLite/WAL/SHM files; direct-ingest tests have no application log. These are not guaranteed OS-level transient peaks. SQL counts are statements, not physical I/O.

| Scenario | Messages OFF/ON | Samples OFF/ON | PRICE/LAMP events OFF → ON | Closed SQLite OFF/ON bytes | Sampled peak OFF/ON bytes |
| --- | ---: | ---: | --- | ---: | ---: |
| quiet_price | 6000/6000 | 40/40 | 0/0 → 0/0 | 86,016/86,016 | 654,460/666,796 |
| changing_price | 2000/2000 | 2000/2000 | 0/0 → 1/0 | 1,110,016/1,110,016 | 1,843,260/1,843,260 |
| duplicate_lamp | 500/500 | 0/0 | 0/0 → 0/0 | 380,928/389,120 | 974,068/986,404 |
| revised_lamp | 64/64 | 0/0 | 0/0 → 0/63 | 114,688/684,032 | 679,060/1,314,300 |
| shared_queue | 66/66 | 2/2 | 1/63 → 1/63 | 704,512/770,048 | 1,334,852/1,375,692 |

In the shared-queue pair, OFF/ON toggles only the queue; both analytical detectors stay enabled. Both preserve identical source/analytical events (including two legacy research events that never enqueue). ON creates 64 PRICE/LAMP queue entries: 38,886 bytes of key/value payload, **65,536 bytes extra closed SQLite** and 64 extra SQL write statements. Quiet PRICE coalesces 5,960 unchanged confirmations to 40 saved samples in either mode. LAMP duplicate downloads retain 500 distinct HTTP receipts but add only five alert-state SQL writes versus OFF and no alert. Meaningful 2°F forecast changes correctly cost more because 63 full event/evidence records are retained.

**Real daily growth, daily peak, physical I/O and probability/time to reach 256 MiB: NOT MEASURED.** No extrapolation from a two-minute observation or simulated timestamps. Real changed bid/ask samples are deliberately saved, so intensive streams can consume more disk. The owned data guard stops capture before exhaustion; FULL queue stops new deliveries while preserving analytical events. No silent limit increases or automatic backfill. The 256-MiB budget applies to one owned observer data directory, not the sum of every retained source/dependency/test copy on the server.

## Retention, without deletion

Existing explicit maintenance can remove unreferenced inputs older than 24h and unqueued events older than 30d. Latest state pointers, PRICE comparison evidence, LAMP/observation references and all queue-referenced events plus their evidence remain protected, including SENT/UNKNOWN dedupe records. Diagnostics remain bounded at 512 rows; existing rotating logs retain their caps. SQLite page reuse is possible; physical shrinking/VACUUM is not promised. No pruning or file deletion was run on historical data. Closed test directories are not declared safe to delete merely because they are old; archive/provenance verification and separate permission remain necessary.

## Exact TEST previews and next action

Two new synthetic source-path events produced two frozen PENDING previews using the saved real New York 50–51°F contract identity. Test prices and forecasts are explicitly synthetic. No claim/HTTP send was executed.

PRICE preview SHA256: `9b2a649405edf150b4f64d9d1a81962c6e2f0cbfb450662841b104d698963727`.

```html
TEST · 📈 <b>PRICE ALERT</b>
New York · YES · 50-51°F
Midpoint: <b>48% → 61%</b>
Crossed: 50% · 55% · 60%
<a href="https://polymarket.com/event/highest-temperature-in-nyc-on-november-15-2026/highest-temperature-in-nyc-on-november-15-2026-50-51f">Market ↗</a>
TEST · Preview only; not live market data.
```

LAMP preview SHA256: `1037aabd7469855cc47a91ca91d8f59652cb5b700c4a000f8671102753955300`.

```html
TEST · ☀️ <b>LAMP ALERT</b>
New York · 50-51°F
Forecast peak: <b>48°F → 50°F</b>
Crossed boundary: 49.5°F
<a href="https://polymarket.com/event/highest-temperature-in-nyc-on-november-15-2026/highest-temperature-in-nyc-on-november-15-2026-50-51f">Market ↗</a>
TEST · Preview only; not live market data.
```

NEXT: accept PR #11 after exact-head green CI. Only after separate user approval send these two exact TEST previews, verify Telegram ACK and restart dedupe, then separately authorize one bounded live PRICE/LAMP observation. No automatic merge, ten-city continuous operation, deployment or trading.
