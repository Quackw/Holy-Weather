# European PRICE / source qualification — 15 November 2026

Scope: one explicitly authorized finite diagnostic run on the existing test host
`v2202609428928530766`; no services, production changes or Telegram sends.
Manifest SHA256: `4b6146a1d7ad285ea55d3b2ab2aa97f194993ddd9e46dd383c03c7a76021f418`.
Bundle SHA256: `4e39be95167c3edd365a74a5a69002d27628c7d8fc671a36e4e562c6dd751739`.
It contains the earlier qualified code, not the subsequent midnight and MOSMIX
coverage changes. Those changes are qualified offline/CI and against retained
original MOSMIX files separately.

The test completed in 199.70 seconds. Each city had at most 30 seconds native
selection followed by 60 seconds observation, no reconnect. The reused bootstrap
START metadata retains old generic single-test limits (60/120 seconds/two
connections); the actual three-city script and per-city receipts are authoritative.
No source or receipt clocks were rewritten. Freshness stayed 90 seconds.

| City / station | Verified contracts | Selected bucket | Final YES bid / ask | Final NO bid / ask | METAR | Saved samples | Closed SQLite |
| --- | ---: | --- | --- | --- | --- | ---: | ---: |
| London / EGLC | 11 | 13°C | 7.5 / 14¢ | 86 / 92.5¢ | 11°C, 13:50 UTC, FRESH | 50 | 147456 B |
| Paris / LFPB | 11 | 13°C | 99.3 / 99.5¢ | 0.5 / 0.7¢ | 12°C, 14:00 UTC, FRESH | 28 | 118784 B |
| Amsterdam / EHAM | 11 | 10°C | 51 / 65.8¢ | 34.2 / 49¢ | 7°C, 13:55 UTC, FRESH | 391 | 393216 B |

Windows: London 14:17:27–14:18:28, Paris 14:18:40–14:19:41,
Amsterdam 14:19:43–14:20:44 UTC. These are captured quotes, not current prices or
execution estimates. All six sides had native full baselines and later usable
confirmations. The observation engine reported no FUTURE quotes. Selection in
Amsterdam rejected two stale/out-of-order/clock samples (combined counter).
London rejected ten and Amsterdam 57 out-of-order confirmations; these were not
silently accepted. No source ordering guarantees are inferred from this run.

**Finite PRICE + AWC observation: PASS for these three contracts/windows.**
Zero PRICE events, zero queue entries and zero sends. This does not qualify
real event delivery or nonstop operation. Broad price readiness is not trade
approval. Other European cities were not included in this Linux run.

Original MOSMIX_L was retrieved and decoded for all three stations, issue 09:00
UTC. Available remaining peaks at their respective receipt times: London 9.70°C,
Paris 11.70°C, Amsterdam 9.00°C. They were received after the price windows;
therefore they are not backfilled into earlier events. The latest decoder
confirmed complete remaining-hour coverage on these original files. Forecast
freshness/material-change policy and live forecast notifications remain disabled.

All three SQLite integrity checks returned `ok`, foreign-key errors zero.
Two cold local replays per city were deterministic; original database and stream
SHA remained unchanged. Reopening replay stores retained events and queue and
reset price readiness. These empty-queue reopens do not prove real delivery dedupe.
Replay sample counts differ from live because retained GAP diagnostics are also
applied; exact live sample equality is not claimed. Zero-event results agree.

Evidence (outside Git): `evidence/europe-linux-20261115/` contains the manifest,
source pins, remote receipts, 40 hash-verified artifacts, original streams and
source responses, closed databases and replay results. Test files remain in
`/var/tmp/holy-weather-europe-w5ana0x6`; nothing was deleted or left running by
this finite launcher. No daily disk-growth estimate is inferred.

Remaining: automatic next-contract handoff, qualified European forecast-event
policy/wiring, remaining-city live qualification and separately approved real
alert previews/delivery. Do not announce all cities fully connected yet.
