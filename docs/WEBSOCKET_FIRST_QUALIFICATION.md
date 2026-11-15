# WebSocket-first finite qualification

`tools/qualify_live.py` corrects the source test in PR #11. A stale REST snapshot
must not prevent observing later native quotes. The tool makes **zero CLOB REST
book requests**. Normal Gamma discovery still verifies the exact open contract,
station, local day and outcome IDs; its indicative prices never become quotes.

The ordinary observer still defaults to the accepted five-second baseline
recovery timeout. A new optional Python argument, `baseline_wait_seconds`, lets
this finite qualification wait for a native baseline for the duration of its
explicit observation window. It cannot exceed that window. There is no new
configuration key or CLI behavior in the ordinary monitor. There are no
reconnects in this test. This longer acquisition wait is distinct from the
unchanged **90-second quote freshness limit**.

Old/future/out-of-order input remains unusable. The first accepted full native
quote is a baseline and cannot retroactively fire thresholds. Once-only PRICE
levels, side independence, gap resets, atomic evidence and delivery keys are
unchanged. A qualifying side needs its actual PRICE baseline with real bid and
ask, correct source/receipt clocks and both ages at most 90 seconds. Merely
receiving messages, PONG, a trade or an opposite-side quote is insufficient.
The report lists verified sides separately; one side cannot prove the other.
After a full native baseline, later price-change confirmations may prove the
current top's freshness without changing PRICE event rules. Every proof links to
the preserved frame and baseline. PASS also requires a verified side's source and
receipt to remain within 90 seconds at the end of observation; an early fresh
snapshot followed by five minutes of silence is insufficient. The completed
finite run is not claimed to be a currently connected service.

## Controlled AWC failure

The optional `--controlled-first-awc-timeout` mode injects one local TimeoutError
through the real `fetch_weather` error path. It makes no first HTTP request and
labels the receipt `CONTROLLED_TIMEOUT_NO_HTTP_REQUEST`; this is not presented as
a genuine provider outage. Price ingestion continues. The second poll uses the
real unchanged AWC fetcher on its normal 300-second schedule. No immediate retry,
shorter poll period, larger five-second socket timeout or alternate source is
used. A PASS for recovery requires that the real receipt is stored successfully
by the main SQLite writer, not merely that the worker received HTTP 200.
Overall PASS also requires available, fresh METAR context under its existing
90-minute observation-age rule, separate from the 90-second quote limit.

Use a 330-second observation to allow the scheduled request and its processing:

```toml
city = "seattle"
bucket = "60-61°F" # explicit contract, rechecked by Gamma at run time
duration_seconds = 330
max_reconnects = 0
weather_context = true
lamp_context = true
price_alert_enabled = true
lamp_alert_enabled = false
alert_queue_enabled = true
telegram_enabled = false
quote_age_seconds = 90
storage_bytes = 268435456
```

```sh
PYTHONPATH=src python -B tools/qualify_live.py --config config.toml --output /path/to/new-test --controlled-first-awc-timeout
```

Do not interpret that example bucket as a current market recommendation. It must
be selected from verified metadata before a run. Selecting a contract does not
promise that it will produce a complete, fresh native quote.

## Evidence and bounds

The tool requires a new output directory and refuses enabled Telegram. It reuses
the existing observer, one SQLite writer, weather poller and PRICE event path.
The explicit qualification window replaces a pre-connection price gate; no
producer timestamp is replaced with HTTP receipt time. There are no new services,
dependencies, schemas or POLYMETEO database accesses.

Full native frames and their receipt times are saved in gzip. Capture is bounded
to 8 MiB uncompressed / 1 MiB compressed; the existing SQLite/WAL/log budget stays
256 MiB. Original source times, weather receipts, controlled-fault labels and
event evidence remain available for deterministic replay. Limits are fixed;
exhaustion is not a reason to delete history or silently enlarge them.

The report includes source types, verified bid/ask evidence per side, AWC calls,
whether the scheduled real recovery occurred, PRICE/LAMP event counts, frozen
queue previews, SQLite integrity, foreign keys and artifact sizes/SHA. Actual
alerts must be previewed before a separate send permission. No signal is required
for source qualification; a market with no fresh complete quote stays PARTIAL.

Regression tests reproduce an old 133-second REST diagnostic and stale native
books followed by delayed fresh BBO: the connection opens, old inputs remain
STALE, fresh baselines cause no retroactive cross, and the following valid change
creates only the genuinely crossed threshold. Further checks cover finite wait
bounds, controlled-fault labelling, the unchanged polling interval, no false PASS
from weather alone, no Telegram and no overwrite of existing evidence.

Live proof is reported separately from the offline tests. The previous
`NO_VERIFIED_FRESH_COMPLETE_CANDIDATE` result remains immutable evidence of the
superseded preflight. It is not reclassified as a successful WebSocket observation.

## Authorized Linux observation — November 15, 2026

Manifest `65896398e70521a8f3871dcc59a12524aabbf0b766e7c0267f8c2fb2effad887`
and bundle `45d2dd24776e2043858a01fe7ecdee4156b8008717bc789c94b6f7632c6128d8`
ran once in `/var/tmp/holy-weather-ws-first-0dwhmsq2` on the explicitly approved
host. All source/dependency pins matched. No installation, production operation
or Telegram request occurred. The run stopped normally after its finite window;
no reconnect or second launch was performed.

Normal discovery selected Seattle/KSEA 54–55°F, market day November 14, and
verified the exact open contract, rules and YES/NO tokens. Metadata ranking does
not establish present liquidity: this selected contract did not offer a complete
native bid/ask pair. That observation is not generalized to all Seattle contracts.

The loop ran 331.02 seconds including bounded receive/shutdown overhead (configured
330 seconds). It opened WebSocket directly and completed with `COMPLETED_WINDOW`.
REST book reads: **0**. Native frames: 47; native message objects: 2 initial books,
8 price changes and 38 unrelated new-market announcements. Initial books were
267.012 seconds old and remained STALE. Later native changes had source-to-receipt
age 16–21 ms, but confirmed only YES no-bid / 0.001 ask and NO 0.999 bid / no-ask.
Sentinel 0/1 prices did not become executable quotes or a fabricated midpoint.
No full fresh PRICE baseline was established for either side. PRICE/LAMP events,
queue entries and delivery attempts: **0**.

The controlled AWC timeout is explicitly tagged and made **no first HTTP request**.
The next scheduled fetch started **300.323 seconds** later, used the real AWC
interface, returned HTTP200 in 580 ms and was committed by the main SQLite writer.
Latest KSEA METAR: 03:53 UTC November 15 (19:53 Seattle November 14), **13.9°C**;
at observation end its age was about 9.24 minutes and the existing weather context
was FRESH / provider OK. This proves the controlled timeout-to-real-response path;
it does not identify the network cause of the earlier genuine timeout.

Closed SQLite: **118,784 bytes**; pre-close data-directory accounting: **693,468
bytes**, not a sampled peak. Gzip wire capture: **14,035 bytes**, representing
118,579 uncompressed bytes. The 14 copied artifacts total 374,923 bytes. All
artifact, raw-input, Gamma/AWC/LAMP body and capture SHA checks pass. SQLite
integrity `ok`, foreign-key errors 0, schema 1. Two cold replays agree and match
the original event semantics/counts: no events or queue entries. Original source
SQLite and wire hashes remain unchanged.

Verdict: **WebSocket-first lifecycle PASS; controlled AWC recovery PASS; complete
fresh quote qualification PARTIAL.** No real alert existed to preview. The native
updates were current, but this one-sided contract could not prove a usable
midpoint, a genuine notification or Telegram delivery. No freshness, strategy or
deduplication rule was weakened to manufacture PASS.

Evidence remains outside Git under `evidence/ws-first-20261115/`. Source SQLite
SHA256: `2ef65e05851c5d139d2070b287d468bdf74c026c24502cdab40e02dbe599faeb`;
gzip capture SHA256: `a84a93111de35810f8e8bfc96e0f46cd3e25d98caf4b4a0a21d5ae4b7c27ef86`.

The metadata-only choice used in that completed run is superseded by
[native active-contract selection](ACTIVE_CONTRACT_SELECTION.md). The old capture
and its PARTIAL result are preserved; no different market is substituted into it.
