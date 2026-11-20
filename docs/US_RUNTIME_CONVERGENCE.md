# US runtime convergence and PRICE baseline recovery

The US service already uses the continuous PRICE recovery handoff and the active
delivery index from earlier US work. This change brings those behaviors into
current main while retaining the separate weather-continuity path. Idle LAMP
polls avoid scanning every contract when no request or result is due.

For US markets, a bounded resubscription requests an authoritative book when
fresh depth cannot restore a stale confirmed PRICE baseline. Before requesting,
the affected token is invalidated; the first subsequent valid book is a new
baseline, not a crossing through the gap. Fired levels, source high-water and
delivery receipts remain. The existing one-stat storage guard and bounded Decimal
cache are also used for US. PRICE thresholds, quote limits, schema, sender and
weather source policies are unchanged.

Tests cover bounded requests, invalid or stale depth, both sides, source order,
SQLite failure, idle weather polling, in-flight weather handoff and delivery
rollback. A native socket fixture applies equal synthetic per-metadata latency;
it does not measure production filesystem latency or prove every 1011 is fixed.
A separate existing Europe native-socket pressure case is timing-sensitive on
one Linux host and remains under investigation. Do not interpret test success
as authorization to merge or deploy, or as a claim of uninterrupted live service.
