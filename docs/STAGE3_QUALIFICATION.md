# Stage 3 — original KLGA LAMP context

Stage 2 was accepted and merged as f4b9a69a4c70b8fd8b9cae0073e801be4ee30181; main CI 43613437795 passed. Stage 3 adds no market detector changes, dependencies, services, trading, Telegram or own temperature model.

## Source and interpretation

Original NOAA bulletin: https://lamp.mdl.nws.noaa.gov/cgi-bin/getlav.pl?sta=KLGA . This experimental web display is not an availability guarantee. Preserve its original body, SHA, nominal issue time and actual receipt/availability times separately. A nominal bulletin header is not a proven publication timestamp.

Only future hourly points on the contract's New York local date contribute to the remaining peak. Missing TMP, partial TMP, stale forecasts, invalid clocks and missing remaining hours have explicit statuses. A forecast below the received observed maximum remains below it; it is never clamped. The two-hour LAMP age criterion is separate from the unchanged 90-second quote limit.

## Validation on 2026-11-14

- 121 offline tests passed locally and on Linux Python 3.13.5, including causality, missing/invalid/stale data, issue regression, negative temperatures, duplicate receipts, DST, restart, bounded storage and failure isolation.
- A complete simulated LAMP failure leaves AWC and market detection working.
- No SQLite migration: schema remains version 1. On a copy of the accepted stage-2 real database, prior rows survived new LAMP data and read/write rollback to the actual stage-2 code. Integrity and foreign keys passed; original database SHA stayed unchanged.
- Two cold replays of saved real inputs produced identical results, with original artifacts unchanged. Replay covers retained inputs, not the coalesced full wire stream.
- Bounded Linux live observation: 120.897 seconds, 144 frames, 90,540 wire bytes, 14 fresh samples, 272 unchanged confirmations coalesced, zero detected threshold events. AWC and LAMP succeeded. No additional long live run is needed to claim this finite check.
- Closed live database: 118,784 bytes (116 KiB), integrity OK, zero foreign-key errors. Before closing, data directory including WAL/logs was 685,350 bytes; measured process writes 1,040,384 bytes. Entire temporary qualification tree including dependencies/tests: 22,530,060 bytes. This is not a daily growth estimate.
- Global budget remains 256 MiB. Compressed LAMP inputs have a subordinate 1 MiB ceiling; exhaustion disables that context explicitly without deleting evidence.

## Real informational example

Captured 2026-11-14 16:13:30.365 UTC (18:13 Kyiv / 11:13 New York), contract 46–47°F:

- METAR KLGA observation 15:51 UTC: 6.1°C (42.98°F); previous 5°C, change +1.1°C. Received 16:11:29.985 UTC.
- Maximum among received observations: 42.98°F, partial observation coverage, not official daily Tmax.
- Original LAMP header 15:30 UTC; received 16:11:30.485 UTC. Remaining hourly peak 46°F at 15:00–16:00 New York.
- Forecast peak minus observed maximum: +3.02°F. Verified rounded bucket boundaries [45.5, 47.5)°F; observed maximum is 2.52°F below its lower boundary.
- Saved YES best ask 67.7¢, NO best ask 38.3¢; corresponding bids 61.7¢ and 32.3¢. Source time 16:13:13.692 UTC, receipt 16:13:13.706 UTC. Quotes were about 17 seconds old at capture, not last trades or guaranteed executions.

The finite connection has since closed; this is a captured snapshot, not a claim of ongoing live health. No qualifying real price event occurred, so real event-triggered attachment is not claimed; synthetic causal event tests and saved-input replay verify that path. This context establishes neither a probability nor a trading advantage.

## Evidence

Raw evidence is excluded from Git under evidence/stage3-20261114. Linux source bundle SHA256: 8e40bfcbbe60b8f677dee62833ca3a2097c52113eb59846f3060f69d0c845104. Closed live database SHA256: 3f2150ff9d481d329076b98153c37de7a029462807539b1d62073cb4e8d5c34b. Compatibility, transport, replay and card receipts are retained there. POLYMETEO and production were not changed. This PR must not be merged automatically.
