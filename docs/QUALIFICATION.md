# Stage 1 qualification — 14 November 2026

The implemented path is **Gamma → one temperature contract / two outcome tokens
→ official public market stream → causal event engine → owned SQLite**.
No production application was installed. Weather, Telegram, trading and a
48-hour supervisor are outside this stage.

## Practical result

Linux 3.13.5 executed the standalone source and pinned Python dependencies from
an isolated, unprivileged directory on ext4. Forty-three offline tests passed.
Two consecutive 60-second observations used the same database, with a genuine
process exit and restart between them. The configuration selected New York,
November 14, **46–47°F**, LaGuardia **KLGA**, America/New_York. Actual Gamma rules,
date, station, exact native bucket and both outcome IDs were checked before
each observation. The official public stream matched the condition and assets.

The final retained quotes at 11:42:43 UTC / 13:42:43 Kyiv were:

| Outcome | Best bid | Best ask | Size at checkpoint |
| --- | ---: | ---: | --- |
| YES | 67.2¢ | 70.3¢ | unknown |
| NO | 29.7¢ | 32.8¢ | unknown |

These are individual quote prices, not $10 execution estimates. At the second
window's close their source timestamps were approximately 10 seconds old.
Unknown sizes remained unknown. Native book `last_trade_price` is preserved as
an unverified snapshot attribute, with no invented trade time or verified token
fill. Only an explicit timed trade message can populate the trade fields.

Across the final two windows: **197 input frames / 134,800 incoming bytes**;
**28 fresh quote samples** saved; **342 unchanged top confirmations** coalesced.
All changed bid/ask quotes are retained, including those carried by
`price_change`; unchanged confirmations checkpoint every 15 seconds. No configured
event occurred in these windows. No event is manufactured to qualify the test.

The database was seeded with six inputs / four samples from the earlier isolated
version. The upgraded program and its restart preserved all six original input
rows byte for byte. Final totals were **36 inputs, 32 samples, zero events**.
`PRAGMA integrity_check` returned `ok`; foreign-key errors were zero. The copied
closed database SHA256 is
`b4ba74f512ce71c3389a3de589ecb76c92a9213e7b02234c8754b321cea3ecf3`.
Every compressed input was decoded and its original-data hash checked after
export. No active or historical POLYMETEO database was opened.

## Event engine proof

An immutable, SHA-bound research extract of the earlier real New York stream
contained 90 quotes and seven recrossings around 70¢. Replay produced **one
DETECTED crossing and six SUPPRESSED crossings**, with HYSTERESIS reasons and
original inputs preserved. No earlier research source was changed.

Synthetic tests separately cover movement ≥10¢ within 15 minutes, 50/70/90¢
levels, half returns, cooldown/rearm, absent opposite books, NO token identity,
unknown sizes, stale/future clocks, duplicates, malformed data, reversed source
clock, initial snapshots, gaps, reconnects, process restart, SIGTERM, interrupted
empty initialization, write rollback, foreign directories, budget exhaustion,
retention protection and replay/hash checks. Tests never call a provider or send
a message. Local Python 3.14 and isolated Linux Python 3.13 passed all 43 tests.
Repository CI additionally runs on clean Linux Python 3.11.

## Resources

The closed database grew from **106,496 to 151,552 bytes (148 KiB)** across the
two final observations. The 45,056-byte increase over 121.85 measured seconds
extrapolates to approximately **30.5 MiB/day at this observed quiet traffic**.
That short extrapolation is not a busy-day or 24-hour acceptance result.

Linux `/proc/self/io` measured **1,548,288 process disk-write bytes** during
streaming after discovery. Filesystem growth and write traffic are different
metrics; startup/checkpoint timing affects both. An earlier tmpfs trial reported
zero physical writes and was not used as disk-I/O proof.

The whole final isolated qualification directory, including dependency sources,
tests and test fixtures, occupied **13,848,072 bytes**. The owned runtime data
directory was below 0.7 MiB at the measured pre-close snapshots. Budget remains
**256 MiB**, with reserve, SQLite page bound, bounded WAL and rotating logs.
Exhaustion stops collection; no cap increase or data erasure was used for PASS.

## Failures, scope and recovery

Direct Mac HTTPS to Gamma timed out during TLS. The application returned a
distinct discovery-network error instead of reporting no market event. Live
qualification consequently ran as a self-contained Linux process. Existing
operator transport only transferred the temporary bundle and evidence; it is not
an application dependency. No server role, privilege, service or schedule changed.

The first Linux test bundle omitted `.gitignore`, so its scaffold test stopped
the run before live access (39 passed, one packaging failure). Adding that
omitted source file resolved the known failure; no runtime bound was increased.
The top-of-book confirmation path was then qualified on ext4 with the final code.
No denied provider endpoint was retried or replaced with another identity.

Restart restores committed inputs, event identities and threshold gates. It
records a gap and clears price-comparison windows; the next quote is a baseline.
All writes belonging to one input and its decision are transactional. A failed
write rolls back the input/state/result together. No working database is restored
from an older snapshot. This is process-restart proof, not a machine power-loss
or prolonged production-availability claim.

Source rollback is a Git revert. An unknown database/rule schema is refused;
there is no destructive migration, production installer or automatic repair bot.
No observation process is left running by this qualification.

## GitHub limit

The repository is private. Source review and the offline `tests` workflow are
configured. The owner approved mandatory main protection, but GitHub rejected
the branch-protection API with **HTTP403: upgrade to GitHub Pro or make the
repository public**. Privacy was preserved; no alternate policy endpoint or
paid-plan activation was attempted. Required checks cannot be enforced by GitHub
until the account plan supports private-repository branch protection. Review/CI
remain procedural requirements in AGENTS.md meanwhile. No automatic merge.

The next brick, after review of this stage, is a separately qualified weather
observation input. It is not implemented here.
