# Select by native quotes, then qualify the selected contract

`tools/select_active.py` corrects contract selection in PR #11. The previous
metadata-only ranking could choose a one-sided contract with large historical
volume. This selection instead observes actual quotes across the exact event's
rule-verified candidates before committing to one contract.

The selector verifies the normal Gamma contract identity, station, local day,
open status and CTF-v1 outcome IDs. Missing Gamma indicative bid/ask and stale
REST book timestamps are not eligibility filters. There are no CLOB REST reads.
At most 16 candidate markets / 32 outcome tokens share one finite public
WebSocket subscription. Exceeding this bound is explicit, not silently truncated.
No new service, database, dependency, trading rule or persistent scanner is added.

A candidate needs a full native `book` or `best_bid_ask` baseline, real own-token
bid and ask strictly between 0 and 1, non-crossed prices, a current market local
date and both source/receipt ages at most **90 seconds**. Subsequent native
price-change confirmations can maintain current top evidence; they cannot
establish the initial full baseline. Missing sides, sentinel prices, stale/future
or reversed clocks, malformed data, data gaps and resolution invalidate readiness.
Activity also requires a later native confirmation whose source clock advances
after that full baseline. An initial snapshot alone cannot win the selection.
Duplicates cannot refresh timestamps. Unrelated market announcements and trades
are not prices. Every selected quote and baseline links to a captured raw frame.

After the first eligible quote, a one-second settling period allows other
candidate snapshots to arrive. Selection is deterministic: prefer a contract
with both outcome sides eligible, then native midpoint nearer 0.5, then narrower
spread. This only picks an observation subject; it is not a probability or entry
recommendation. Each outcome's readiness remains independent.

Selection observes at most 60 seconds, with application PING/PONG and no retry or
reconnect. If no eligible quote arrives, it returns
`NO_CONFIRMED_ACTIVE_NATIVE_QUOTE` and no main observation begins. A denial or
broken stream returns `STOPPED_NO_RETRY` without a second account, endpoint or
candidate fallback. Source/receipt clocks are never substituted.

The short selected-contract observation starts in a **new connection** through
the existing finite qualifier. Selection evidence is not replayed as a fresh
baseline and no threshold is crossed across the phase boundary. The observation
must independently acquire its actual full baseline and retain a usable side
within 90 seconds of its end. Selection-time readiness cannot force a later PASS.
The ordinary five-second observer default and all PRICE/LAMP/deduplication rules
remain unchanged; only the explicit bounded qualification profile waits for its
baseline within the finite test window.

Both phases retain bounded gzip raw frames, exact receipt times, identities and
hashes. The selector creates no analytical events, queue entries or SQLite.
The actual qualifier reuses the existing SQLite writer and weather sources,
requires Telegram off, preserves genuine event previews and reports integrity,
volume, freshness and any missing evidence. The original 256-MiB data budget
remains; each phase's diagnostic capture is capped at 8 MiB raw / 1 MiB gzip.

The finite live profile uses New York/KLGA on the current station-local day,
one metadata request, at most one 60-second selection connection followed by one
120-second single-contract observation, and no reconnects. AWC and LAMP requests
are native and bounded; the previous controlled AWC recovery proof is preserved
and not re-run or relabelled as a new provider outage.

No genuine alert is required for source qualification. Any actual alert must be
previewed before a separately authorized Telegram send. Failure to find an active
contract remains explicit; no placeholder card or fabricated price is produced.

## Reproduced full-BBO coalescing defect

The pre-live regression also reproduced a missed PRICE event: an original full
baseline midpoint 0.48, then a depth `price_change` confirming 0.61, then its full
`best_bid_ask` message. Previously the depth cache made that first full update look
unchanged and coalesced it before PRICE processed it. The regression failed with
zero PRICE events. PRICE now checks its own last processed bid/ask before a full
BBO can be coalesced. The same sequence creates exactly one event crossing
50/55/60%, and the next unchanged confirmation creates no repeat.

The original Netflix source was rechecked: its PRICE input is full book/BBO,
not depth changes; midpoint thresholds and once-only consumption are unchanged.
No alert is created by the depth message itself. The existing 6,000-message
ON/OFF storage regression still checks 40 saved samples and the same write/size
bounds. This is a reproduced ingestion defect, not a change of strategy.

## Real qualification, 15 November 2026

The user approved exactly manifest
`174fb4b3ba39e0e6225cb68aa8c3ef5ce9a57e530a1a46c96d950f9577f52b60`,
source commit `0473ba5f56d88e0176c911d565bc06960a8f6146`. One isolated run on
`v2202609428928530766` completed without a retry, reconnect, service change,
REST-book request or Telegram request. Its owned directory is
`/var/tmp/holy-weather-active-selection-8qzd9d7t`; the original and returned
artifacts are preserved, not deleted. Raw evidence remains outside Git under
`evidence/active-selection-20261115/`.

| Check | Observed result |
| --- | --- |
| Gamma and rules | HTTP 200; 11 open, verified New York/KLGA candidates for local day 2026-11-15 |
| Native selection | SELECTED in 9.617 seconds, 13 frames; both sides had full baselines followed by a newer native confirmation |
| Selected contract | New York, 50–51°F; condition `0xa0dbe3fee7d4d5150fe546669480a2e72a1a140df99b388469650f0b8f5c1633` |
| Separate observation | 05:52:52.472–05:54:53.506 UTC; a new connection and new baselines; 120-second collection window, 121.013 seconds including shutdown |
| Native messages | 2 full books, 28 depth-update messages, 154 unrelated market announcements |
| YES top | Bid 74¢ / ask 75¢; midpoint 74.5¢ throughout all 29 own-token confirmations |
| NO top | Bid 25¢ / ask 26¢; midpoint 25.5¢ throughout all 29 own-token confirmations |
| Source and receipt clocks | Full initial books arrived 2,226 ms after source time; subsequent confirmations 12–14 ms; last quote source age at end 8,751 ms |
| PRICE events / queue entries / sends | 0 / 0 / 0: neither midpoint crossed an upward threshold; no placeholder notification |
| Economical recording | 10 samples saved; 48 unchanged confirmations coalesced; 13 total inputs |
| AWC | One native HTTP 200 response in 257 ms, no injected timeout and no retry |
| Weather coverage | MISSING for the new market day: all 27 returned observations belong to local 14 November; latest is 04:51 UTC = 23:51 EST on 14 November |
| LAMP context | Native bulletin received; issue 05:30 UTC, remaining-day peak 48°F; observed maximum and remaining warming stay missing |
| Storage | Closed SQLite 106,496 bytes; observation capture 44,909 bytes gzip / 507,822 bytes raw; selection capture 8,599 bytes gzip / 64,984 bytes raw |
| Integrity and provenance | All 16 returned artifact SHA values verified; SQLite integrity `ok`, foreign-key errors 0, schema version 1 unchanged |
| Replay | Two cold selector replays match selection/counters; two cold observation replays match each other and live event semantics; source artifacts unchanged |

The actual quote-selection and refresh objective is **PASS**. The combined
price/weather qualifier honestly remains **PARTIAL**, because an HTTP-successful
AWC response did not yet contain an observation on the new local market day.
The saved response proves that coverage gap; it does not establish why the
provider had not returned a later observation. Previous-day weather is not
substituted as today's observation or maximum. The earlier controlled AWC
timeout/recovery proof remains separate; no new recovery is claimed here.

The stream is closed after this finite test. Its final persisted connection
status is `CONNECTION_UNCONFIRMED`, even though the last prices were younger
than 90 seconds at test end. This is evidence of a completed bounded window,
not a claim of a currently running healthy monitor. There was no real alert
to preview or send, and this run does not prove Telegram delivery or trading
advantage. PRICE and LAMP rules and the 90-second limit are unchanged.

The source commit passed all **266** offline tests and Linux GitHub Actions
[run 43682446529](https://github.com/Quackw/Holy-Weather/actions/runs/43682446529).
This evidence-only update does not change the tested application code. PR #11
remains open and unmerged.
