# PR 17 acceptance review — BLOCKED

Read-only snapshot 2026-11-15 22:23:36 Kyiv. Source evidence is outside Git under evidence/price-policy-review-20261115; raw evidence and SHA are preserved. No observer controls or Telegram sends used in this review.

## Amsterdam separately

Two exact buckets (9°C, 10°C), sixteen distinct PRICE event IDs, sixteen confirmed SENT receipts and sixteen distinct Telegram message IDs (362–377). Zero repeated market/side/threshold combinations or duplicate message IDs. All sixteen European deliveries in this snapshot are Amsterdam. This is delivery duplication evidence, not a claim that every notification was useful.

Eleven of sixteen crossings had no increase in bid. Six of these crossed 70/80%. Four were followed by a higher bid with midpoint still above the same threshold within four seconds:

| Message | Bucket/side | Level | Later bid confirmation | Delay |
|---|---|---|---|---|
| 362 | 10°C NO | 80% | bid .697→.698, ask .935, midpoint .8165 | 3.782s |
| 363 | 9°C YES | 70% | bid .56→.57, ask .84, midpoint .705 | 2.138s |
| 372 | 9°C NO | 80% | bid .75→.76, ask .87, midpoint .815 | .288s |
| 373 | 10°C YES | 70% | bid .619→.62, ask .834, midpoint .727 | .148s |

Message 371 (9°C NO,70%) has later higher-bid evidence after 143.307s, outside the original 90s lifetime. Message 365 (10°C NO,70%) has no matching confirmation in the retained sample snapshot. These are later fresh saved quotes, not proof of uninterrupted connection, profitability or buyer intent. 43,630 retained Amsterdam samples were inspected.

**Acceptance blocker:** the current filter permanently refuses the initial ask-only event while the original detector still consumes its threshold. Later bid confirmation cannot create a replacement notification. Two regression tests reproduce this behavior at 70% and 80%; they document the risk rather than claiming it is fixed. The prior 21→8 result only preserved bid-growing events that already existed; it did not prove preservation of later confirmation opportunities.

## Same-history comparison

Both policies consume the same 35 historically delivered event receipts: 16 Amsterdam and 19 US, in receipt-time order. Seven pre-send qualification events and one undelivered US pending event are excluded from this delivered-card comparison. All are retained in the source snapshot. Queue-only replay, not a full raw-stream replay. Two independent virtual senders use 3.1s pacing and a fixed synthetic 200ms ACK. This is a controlled comparison, not measured future delivery latency.

| Region | Original cards | Filtered cards | Ask-only refusals | Original modeled ACK median/max | Filtered modeled ACK median/max | Expired OFF/ON |
|---|---:|---:|---:|---|---|---|
| Amsterdam |16|5|11|.200 / 5.698s|30.200 / 32.598s|0 / 0|
| US |19|9|9|.200 / 6.271s|30.200 / 32.777s|0 / 0|
| Total |35|14|20|—|—|0 / 0|

All fifteen originally bid-growing events are represented exactly once in fourteen cards. That narrow coverage claim does not include the later confirmation losses above. Observed original Telegram ACK latency: Amsterdam median .1825s/max 6.391s; US median .167s/max 10.698s. No live new-policy sends were performed. SQLite integrity OK and zero foreign-key errors; original source SHA unchanged.

## Activation correction

Main multi-contract CLI now exposes `--price-notification-filter` and forwards it to CatalogueConfig. Default remains OFF. CLI tests cover both settings; real catalogue integration verifies the enabled policy reaches separate per-bucket queues. Existing detached session wrappers do not automatically acquire this flag: a future package must explicitly set it and be qualified. Neither active process was changed.

## Decision

Do not merge or deploy PR 17 yet. First design and test a bounded delivery-confirmation stage for ask-only candidates, preserving original source/receipt clocks, gap handling, expiry, research detector once-only state and Telegram deduplication. A later fresh bid must not resurrect an expired event or be mislabeled as the original crossing. This follow-up is not implemented in this review.

Separate runtime finding: US observer stopped at 22:19:54 Kyiv with RECONNECT_BUDGET_EXHAUSTED; 19 sends and one pending receipt. Europe reported OBSERVING. No restart performed.
