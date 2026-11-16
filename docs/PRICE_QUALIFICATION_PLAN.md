# PR 17 — bounded forward qualification proposal

This is a plan, not authorization or a started process. Merge, a new runtime,
US restart, code/key transfer and Telegram sends need the user's separate exact
approval. Existing observations are left alone.

## Candidate and scope

After acceptance, freeze the exact candidate commit/package and SHA. Use the
previously authorized test Linux host in an isolated owned directory, existing
pinned Python environment and existing finite catalogue entry point. No service,
scheduler, supervisor, new database architecture, weather source or live order.

One run, maximum 300 seconds plus bounded connection/close overhead. Choose one
or two already supported European cities and at most two verified open contracts
(four real side tokens). Use the existing injected discovery interface to limit
the catalogue; verify exact current local date, station/rules, native tokens and
bucket links before subscribing. A stale REST quote must not reject the market
before its WebSocket confirmation. First full fresh quote is baseline.

This small run qualifies real delivery plumbing, not US 220-token peak load or
a whole-session availability guarantee. The existing isolated US region-enabled
bundle is separately pinned; the Europe-only accepted catalogue is not a drop-in
US launcher. Reconcile and test that launcher explicitly before proposing a US
restart. No new city is part of this PR.

## Measurements

Keep 90s freshness, 256MiB data bound, 16-frame queue, original heartbeat settings,
finite reconnect budget and end deadline. Telegram initially OFF. Save source
and receipt times, full quote inputs, PRICE event, admission, ready time, claim,
ACK/UNKNOWN, per-stage lag, queue peak, bounded transport history, CPU/memory and
open/closed SQLite/WAL/SHM/log sizes. Native and application PONG clocks stay
separate. On exhaustion retain an explicit stop; no automatic restart.

No price crossing is a normal result. Do not inject synthetic market movement or
label a replay as live evidence. If no real event occurs, record that real
end-to-end latency remains NOT MEASURED.

## Telegram permission boundary

First show the precise short preview from the genuine event. A separate exact
send approval may authorize a bounded fresh-signal run or individual card; stale
previews cannot be sent as current data. Avoid a parallel sender for the same
contract/threshold, without stopping another observation to obtain the test.
No old queue backlog or source-event reset.

After a permitted send, inspect the API acknowledgement and saved message ID;
unknown outcome remains UNKNOWN without retry. Reopen the same experiment data
and verify no resend of SENT/UNKNOWN. Report actual receipt→event→ready→claim→ACK
latencies separately from source age and the previous synthetic 200ms ACK.

## Exact format examples (historical preview, not for sending)

Priority comes from crossed 70/80 levels; no extra technical header is needed.
These are exact generated historical texts. They are not fresh and are not sent.
HTML and SHA are retained in `evidence/price-final-20261115/PREVIEWS.json`.

### Ordinary (10s group)

📈 **PRICE ALERT**  
Atlanta · NO · 54-55°F  
Midpoint: **49.5% → 50.5%**  
Crossed: 50%  
[Market ↗](https://polymarket.com/event/highest-temperature-in-atlanta-on-november-15-2026/highest-temperature-in-atlanta-on-november-15-2026-54-55f)

### Priority (no batch wait)

📈 **PRICE ALERT**  
Los Angeles · NO · 70-71°F  
Midpoint: **65.5% → 74.5%**  
Crossed: 70%  
[Market ↗](https://polymarket.com/event/highest-temperature-in-los-angeles-on-november-15-2026/highest-temperature-in-los-angeles-on-november-15-2026-70-71f)

### Ask-only priority fallback (at most 8s confirmation wait)

📈 **PRICE ALERT**  
Chicago · YES · 50-51°F  
Midpoint: **64% → 72%**  
Crossed: 70%  
[Market ↗](https://polymarket.com/event/highest-temperature-in-chicago-on-november-15-2026/highest-temperature-in-chicago-on-november-15-2026-50-51f)  
Ask-only · not confirmed by bid

## Acceptance after the separately approved run

Report actual fresh-side coverage, gaps, bounded stop reason, received events,
queue admissions/refusals, message IDs or explicit no-send reasons, duplicates,
measured delays and resource sizes. Do not infer profitability or autonomous
24-hour readiness from this finite test.
