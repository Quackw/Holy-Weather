# PR 17 confirmation review — 15 November 2026

Draft; not deployed. Working Europe was not stopped or changed. No Telegram messages or US restarts.

## Fixed real-history comparison

Local provenance: `evidence/price-confirmation-20261115/COMPARISON.json`, scripts and SHA files. All policies use the same 35 historically SENT events: Amsterdam 16 and US 19. Seven qualification-expired entries and the final US PENDING are excluded equally. Saved subsequent quotes are real; queue replay uses virtual 100ms polling, 200ms ACK and separate 3.1s-paced regional senders. It is not a live delivery test or full raw-stream replay.

| Policy | Amsterdam cards | US cards | Total | Events represented | Not delivered | Expired |
|---|---:|---:|---:|---:|---:|---:|
| Original | 16 | 19 | 35 | 35 | 0 | 0 |
| Previous PR17 permanent ask-only refusal | 5 | 9 | 14 | 15 | 20 | 0 |
| Wait 5s | 9 | 15 | 24 | 24 | 11 | 0 |
| Wait 8s | 9 | 16 | 25 | 25 | 10 | 0 |
| Wait 10s | 9 | 16 | 25 | 25 | 10 | 0 |

All new windows: two high-threshold bid-confirmation outcomes and one explicit high-threshold unconfirmed fallback. Five Amsterdam high-threshold waits end with an explicit continuity failure, rather than an invented confirmation through it. Low-threshold timeouts explain the remaining omissions. Inspect per-event queue results for exact reasons.

Median simulated ACK delay is 30.2s for all new windows due to fixed batching; maximum 33.249s for 5s and 35.748s for 8/10s. These are replay timings, not measured Telegram promises. No duplicate event representation, SQLite integrity/foreign-key failure, or expiry loss in this sample.

The previously reported four fast high-level bid increases are NOT four proven valid confirmations: intervening AWAITING_BASELINE/OUT_OF_ORDER samples invalidate several waits. A bid uptick alone is insufficient.

**Evidence limitation:** diagnostics retain only the latest 512 rows. Missing earlier GAP records mean uninterrupted historical transport cannot be proven for the entire session. Explicit invalid samples are honored; absence of a retained GAP is not evidence that none occurred. Consequently this comparison cannot certify all historic confirmations or profitable movement. Raw evidence and SHA are unchanged.

Amsterdam actual deliveries remain: two buckets, 16 unique events, 16 distinct confirmed Telegram message IDs (362–377), no repeated market/side/threshold. These are actual old-policy receipts; replay cards were not sent.

## USA observer stop — read-only

Remote `/var/tmp/holy-weather-us-price-0w1phxy8`, PID recorded 1929363. Last result: `ConnectionClosedError`, **sent 1011 (internal error) keepalive ping timeout; no close frame received**. Three allowed reconnects were consumed; wrapper ended at **22:19:54.770 Kyiv** with `RECONNECT_BUDGET_EXHAUSTED`. This is the configured finite-budget stop following a transport failure, not an SQLite crash or a newly changed PRICE rule.

This error is the client's missing-pong observation; it does not prove an exchange outage. Saved data cannot distinguish network loss, remote delay, local processing/backpressure or scheduling delay. Earlier exception details were overwritten in the single result field, and the earliest GAP entries rolled out of the bounded diagnostics. All four disconnection root causes therefore cannot be recovered exactly.

Retained reconnect at 22:09:55.560: previous usable stored quote 22:09:54.605; next 22:09:59.777. Stored fresh-observation interval 5.172s, including 4.217s after recorded GAP. This is aggregate across contracts, not proof all 220 sides recovered then.

Final usable stored quote: **22:19:53.552**. Final transport GAP 22:19:54.540; no subsequent quote. At read-only measurement **22:42:14.954**, observer had supplied no new usable stored quote for **22m21.402s**. The snapshot age is not evidence of current market emptiness.

Final pending event: Atlanta YES 54–55°F, 70%, created 22:19:50.733; expiry 22:20:56.163. It has no claimed timestamp or Telegram ACK. Persisted PENDING is now time-expired, not delivered and not UNKNOWN. It was not altered or resent. US receipts: 19 SENT, seven pre-session EXPIRED, one PENDING; no duplicate thresholds in the saved audit.

Read-only integrity: `ok`, foreign-key errors 0; 48,245 inputs, 44,273 samples, 2,026 events (all types), 6,679 evidence links. No restored database or cursor manipulation.

## Acceptance

Delivery regressions cover 5/8/10 boundaries, same-token/later-time checks, bid/midpoint requirements, high-level fallback, low-level refusal, GAP/restart/resolution, expiry, grouping with warning preservation, UNKNOWN, once-only, queue capacity, LAMP independence, SQLite rollback and schema compatibility. See latest CI for exact final count.

**CODE:** candidate tested; **PRICE SIGNAL PRESERVATION:** original events retained and omissions explicit; historical continuity proof PARTIAL. **TELEGRAM NOISE:** 35→25 simulated cards at 8s, no real sends. **USA OBSERVER:** stopped, read-only diagnosis PARTIAL at root-cause level. **VERDICT: BLOCKED for deployment / leave Draft.** Review code/CI and the incomplete historical continuity evidence before acceptance. No silent restart or live test is authorized by this review.
