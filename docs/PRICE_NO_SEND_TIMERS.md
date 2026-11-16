# Queue deadlines with Telegram disabled

The finite catalogue must evaluate the existing delivery deadlines even when
Telegram sending is disabled. Previously only `Sender.tick()` advanced them on
a quiet connection. With no sender and no new quote, an ask-only event could
remain `WAITING_BID` beyond its configured eight-second window. A later session
gap would then record a gap refusal instead of the timely delivery outcome.

The catalogue now calls the existing `advance_waits()` on each receive-loop
turn when there is no sender. Sending-enabled runs retain their existing path.
No sender, token, HTTP request, second writer or new timer is introduced. The
existing receive timeout bounds the ordinary scheduling delay; this does not
claim exact wall-clock execution during a blocked process.

The regression drives the actual catalogue and SQLite queue through a synthetic
baseline, an ask-only crossing and a quiet PONG stream. Before the change both
cases failed. With sending disabled, the priority crossing now produces its
labelled unconfirmed fallback after the existing window; the ordinary crossing
records `NO_BID_CONFIRMATION_WITHIN_WINDOW` and is rejected. Construction of a
Telegram sender is forbidden in the test.

PRICE detection, fired thresholds, once-only behavior, quote freshness, the
eight-second maximum, grouping policy, database schema and storage limits are
unchanged. This is an offline regression, not evidence of a real price move or
Telegram delivery. Reverting the single receive-loop branch restores the old
timer behavior without changing stored records or the SQLite schema.

## Single-market observer

`app.observe()` had the same missing no-sender path. Its receive loop now calls
the same `advance_waits()` when Telegram is disabled. Telegram-enabled runs
continue to advance the queue through `Sender.tick()`. This adds no HTTP call,
credential access, new timer or writer.

Eight deterministic offline cases drive the actual observer, PRICE detector and
SQLite queue through complete YES/NO baselines, one ask-only crossing and then
PONG-only traffic or receive timeouts. Both ordinary rejection and priority
fallback are checked within one receive-loop turn of the unchanged eight-second
deadline. Four disabled-sender cases fail on the prior code; all enabled-sender
controls pass. The ON controls exercise the real sender tick with future pacing
and a forbidden transport; no Telegram request is made.

Rollback returns `app.py` to the previous version and keeps the current SQLite,
event history, fired thresholds and delivery records. It restores the previous
disabled-sender deadline defect; no old database should overwrite new records.
