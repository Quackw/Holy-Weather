# PRICE readiness repairs retained in PR12

- Quote usability requires both bid and ask independently on each side.
- Discovery validates the full reviewed European Celsius rules and exact station.
- The finite observer checks station-local date before a connection attempt;
  it does not subscribe to yesterday's market after midnight.
- Existing PRICE event/queue persistence and first-baseline behavior are reused.

Weather/LAMP reconnect polling, LAMP timeout recovery and MOSMIX are deferred on
`codex/europe-weather-experiments-preserved` at `16b0aae`. Their earlier reports
remain in that branch and original ignored evidence. They are not claimed as
part of this PRICE foundation. See EUROPE_READINESS.md for current verification.
