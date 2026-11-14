# Quote-stream diagnosis — 14 November 2026

The second, explicitly approved 120-second New York observation found no price
decoder failure. After its two initial book snapshots the connection delivered
19 `new_market` broadcasts, all for different conditions. It delivered no
`price_change`, `best_bid_ask` or trade for the selected contract during that
window. Traffic on the connection was therefore not evidence of updated prices.
The previous run did not retain ignored frames; their types cannot be established
retroactively from its aggregate counter.

## Direct evidence

- Same unmodified approved application bundle `ca8b5052258b17d3fa36bbd6d9f33e54c422a930bfb67aa4cc6f8c072a6ec097`.
- Diagnostic authorization manifest `b505352f9697c156536c3a75990c503650f9943605754520c2cfbc12fa5329cc`.
- Independent temporary directory `/var/tmp/holy-weather-stream-diagnostic-lb_9e2he`.
- New York/KLGA, November 14, 46–47°F; normal Gamma discovery verified the
  open contract, rules, local date and exact YES/NO tokens.
- 21:31:35–21:33:36 UTC, exit 0, COMPLETED_WINDOW, no reconnect or error.
- YES bid/ask 0.976/0.991; NO 0.009/0.024. Source time 21:31:19.569 UTC,
  receipt 21:31:35.957 UTC: 16.388 seconds old. Both initially usable.
- No subsequent quote timestamp arrived. The original 90-second source/receipt
  checks correctly stopped calling those quotes fresh as they aged.
- 20 frames, 60,252 wire bytes. Bounded diagnostic capture 64,519 bytes,
  SHA256 `7319e82652c9382164c4e33d25415501afdb132d07b3e46a0d64e64500579822`.
- 2 quote samples, 0 analytical events, 0 queue entries, 0 Telegram requests.
- Closed SQLite 122,880 bytes; integrity `ok`, no foreign-key errors. Every input
  and copied artifact SHA verified. Existing artifacts were read without changes.

## Minimal change

The old `ignored_depth_or_control` aggregate hid the difference between unrelated
market announcements and quotes. Keep it for compatibility, and add fixed counters
for `new_market`, `tick_size_change`, unknown messages and recognized native message
types. No per-message diagnostic writes or unbounded type names are added.
No changes to subscriptions, quote parsing, timestamps, the 90-second limit,
PRICE/LAMP rules, queue semantics, schema, production or Telegram sending.

The native subscription and supported fields match the official
[Polymarket market-stream documentation](https://docs.polymarket.com/market-data/realtime-data).
Receiving a lifecycle broadcast or PONG cannot refresh a selected token's quote.

## Verification

222 tests pass locally (219 existing plus 3 regressions). New checks cover 1,000
unrelated broadcasts with no SQL writes or quote refresh, bounded unknown-message
counters, and real BBO updates after a stale period: a new baseline first, then one
PRICE event at the next eligible threshold. Source and receipt times remain distinct.

Two deterministic replays of the complete new diagnostic capture agree: 2 samples,
19 unrelated announcements and 0 events. Two replays of earlier saved New York
inputs also agree and accept 88 fresh samples; that earlier source coalesced
confirmations and includes recorded gaps, so it is not a complete wire recording
or proof of uninterrupted current quotes. Both source SQLite files remain unchanged;
all replay databases pass integrity and foreign-key checks.

These observations establish fresh initial books and explain the quiet interval.
They do not establish continuously fresh prices in that quiet New York contract or
any trading advantage. All finite processes exited; no background service was added.
