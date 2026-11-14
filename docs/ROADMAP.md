# Roadmap — planning only

1. **After PR 4 acceptance: stable independent ten-city collection.** Use the smallest bounded runtime arrangement, isolate city failures, validate new local-day contracts/tokens, preserve old data and explicit gaps. Remove execution dependence on Mac/Codex/POLYMETEO. Measure combined storage, per-token/source growth, WAL/logs, memory, CPU and peak message rates. Short joint qualification first; then a separately authorized 48-hour acceptance. No new service architecture by default.
2. **Transparent weather research.** Keep raw LAMP as the baseline. Separate current METAR, received maximum, contract-eligible maximum, future LAMP peak, remaining local time, bucket boundaries and source age. Freeze causal hypotheses before observing outcomes; evaluate Tmax MAE/bias and boundary misses/false alarms on unseen station-days, with one station-day not counted as many independent cases. No new model code now.
3. **Weather versus price timing.** On the same future evaluation days, measure whether information preceded price movement and whether a usable quote remained. Include spread, fees and slippage. Delivery or WIN/LOSS alone is not evidence of profit.
4. **Separate adoption decision.** Only a proven out-of-sample improvement can justify proposing deployment; do not replace original forecasts or mutate research history. No automated trading.

Only stream-recovery stabilization is authorized for implementation in the current task. These stages create no code, services, databases or PRs by themselves.
