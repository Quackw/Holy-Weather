# WebSocket dependency qualification — 16 November 2026

The candidate pins `websockets==17.4` in both package metadata and the development
lock, based on accepted main `1a3f56d` after PR 19. No application, PRICE, weather,
Telegram or SQLite code changes are included.

## Reason and upstream evidence

[GHSA-6m5v-9gmr-5665](https://github.com/python-websockets/websockets/security/advisories/GHSA-6m5v-9gmr-5665)
affects versions before 17.4, including the synchronous client. Empty fragments
could accumulate without a frame-count limit even with a payload-size limit.
[The upstream changelog](https://github.com/python-websockets/websockets/blob/main/docs/project/changelog.rst)
identifies 17.4 as the fixed release, rejecting messages exceeding 4096 frames.
[PyPI metadata](https://pypi.org/project/websockets/17.4/) requires Python >=3.11.
No `WEBSOCKETS_MAX_FRAGMENTS` override is introduced.

## Scope of the proof

All 575 tests passed locally with 17.4. Ten new cases use only finite local socket
pairs and isolated SQLite. The original 565 cases remain in the full suite,
including the real `TimedClientConnection` handshake/native PONG, keepalive
backpressure timeout, application PING/PONG, grouped subscription recovery,
restart, GAP, queue, and PRICE once-only tests.

The new cases cover:

- Complete and fragmented messages at the existing 262144-byte payload limit.
- Rejection of a 262145-byte message with close code 1009.
- Acceptance of a complete 4096-frame message and rejection above that bound.
- Continuation of a fragmented receive after a finite receive timeout.
- An actual socket disconnect followed by a new PRICE baseline: no inferred
  crossing through the gap and no repeated fired threshold.
- Dependency rollback 17.1 → 17.4 → 17.1 for both regional configurations, retaining
  new event/input/meta rows, synthetic SENT/UNKNOWN receipts, fired thresholds and
  a subsequent valid 80% crossing. This reuses the existing subprocess probe.

The finite 4097-piece regression fails on 17.1 because the fragmented message is
accepted, and passes on 17.4 with `1009: too many fragments`. The traced Python
allocation peak in the targeted local test was 860536 bytes; the regression
requires it to remain below 8 MiB. This measures that finite test's allocations,
not total process RSS or a daily operating budget. Payload, queue (16), quote
freshness (90 seconds) and data-directory (256 MiB) limits are unchanged.

Test peers send concurrently with receive calls. Their kernel socket buffers are
set to 1 KiB to force backpressure, avoiding dependence on macOS/Linux buffer
sizes. Sender/receiver workers have finite cleanup. CI jobs have a ten-minute
deadline and dump thread stacks after a test stalls for 30 seconds. These settings
apply to the test harness; application connection timeouts are unchanged.

Linux CI runs the entire suite and a regular wheel build on Python 3.11 and
exactly 3.13.5. It installs 17.1 into a separate temporary import directory solely
for the offline rollback probes; no running environment is downgraded. Those two
cases require `HOLY_WEATHER_WEBSOCKETS_ROLLBACK_DIR`; they are skipped in an ordinary
developer run without it and are mandatory in the configured CI runs.

The new regular wheel builds successfully and declares `websockets==17.4`.
Official PyPI download URL, artifact SHA256, installation report, test output and
allocation evidence are retained outside Git under
`evidence/websockets-upgrade-20261116/`. GitHub CI must pass on the published head
before acceptance. No live market connection, Telegram send or production
deployment is part of this qualification.

## Rollback

Before any future deployment, retain the complete previous release and its
dependency environment. A rollback returns that release's code, metadata and
17.1 dependency together while keeping the current SQLite and all new records.
Never overwrite the current database with an older snapshot or reset thresholds
or delivery states. The subprocess tests prove data compatibility; they do not
exercise a production supervisor or deployment procedure.

Returning to 17.1 also restores the known fragment-memory vulnerability. It is
an emergency compatibility fallback, not the accepted secure operating version.
This PR does not authorize deployment or a production rollback.
