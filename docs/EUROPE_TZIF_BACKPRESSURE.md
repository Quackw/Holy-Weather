# Europe: repeated timezone reads and receive backpressure

The accepted `bb1cee2` European worker recorded native close 1011 on connection
attempt 1 at **19 November 2026, 15:43:34 Kyiv**. Its one-shot capture has SHA-256
`cfe2604c46d40218e7e654f1b8426bb0ea5deba3ae658786f8206f3d6b5e7e77`.
Raw evidence stays outside Git. The 60-second ring has 4,936 records, a complete
footer, zero busy/capacity drops and zero recorder faults; seven records aged
out during serialization, and older records had already left the ring.

## What the capture establishes

| Native ping | Flushed, Kyiv | Pong parsed and processed | Delay |
| --- | --- | --- | --- |
| 2 | 15:42:37.267 | 15:42:40.996 | 3.729 s |
| 3 | 15:42:47.267 | 15:42:54.551 | 7.284 s |
| 4 | 15:42:57.269 | 15:43:13.298 | 16.029 s |
| 5 | 15:43:13.298 | Not observed before native failure at 15:43:33.299 | 20 s timeout |

During this window, 1,411 completed receive pauses totaled 57.701 seconds;
the longest individual pause was 108.933 ms. Unread kernel data reached 611,642
bytes. Receiver stack samples show the library waiting on receive flow control.
The main thread continued working, spending approximately 19.9 seconds in
housekeeping and 36.8 seconds in routing. Delivery calls peaked below 5 ms.
One main-thread stack is inside timezone-file lookup from `DayDiscovery.due`.
Pong parsing and processing occur together when a Pong finally becomes visible.

This establishes sustained consumer backpressure rather than a single 20-second
consumer stall. The capture cannot locate an unparsed Pong within encrypted
network/server buffers or exclude additional remote delays. There is no separate
unloaded, healthy capture; earlier successful pings within this same connection
provide the available comparison.

## Reproduced defect and narrow change

Europe's eleven profiles use ten distinct zones. CPython's
[ZoneInfo implementation](https://github.com/python/cpython/blob/v3.13.5/Modules/_zoneinfo.c#L111)
keeps eight strong references and otherwise uses weak references. Repeated full
date scans evict zones that have no surviving application owner, rereading TZif
files. The accepted code performs 1,006 repeated reads across 100 unchanged
date/market checks. On 5,000 synthetic frames, profiling observed 47,807 reads.

`DayDiscovery` now retains its European ZoneInfo objects for the worker's
lifetime. The existing UTC-to-local conversions, date checks and DST rules run
at their original call sites. The bounded collection contains ten zones for
the existing profiles. The US branch does not retain additional objects.
PRICE arithmetic, thresholds, fired state, sender, queue, schema and timeout/
reconnect/recovery/storage settings are unchanged.

An identical **synthetic** 60,000-frame local publication burst, without injected
I/O latency or consumer sleeps, reproduced native 1011 on accepted code after
47,692 frames. With this change all 60,000 were processed, a native Pong was
acknowledged, and the connection closed normally. CPU was 29.190 versus 4.370
seconds; the successful experiment's 10.155-second wall time includes waiting
for its normal first keepalive. Both cases used native 10s/20s, max_queue=16,
max_size=262144, and the configured reconnect budget. The local peer publishes
under a protocol lock, so control replies wait behind the bounded publication;
application heartbeats in the burst isolate the native timeout. This models a
backlogged publisher and does not reproduce the Polymarket server internally.

CI uses a smaller 4,000-frame version with an explicit 1-ms cost per actual TZif
load, preserving the real 10s/20s keepalive. This makes the regression independent
of runner filesystem speed. Separate tests compare every SQLite table on the
same 2,200-message history, including YES/NO, exact duplicates and GAP, and reopen
accepted code after new PRICE/SENT/UNKNOWN/PENDING records. Existing date/DST,
reconnect, second-writer and systemd qualification remain required.

## Operational boundary and rollback

No running observer or server configuration is changed by this PR. Compatible
code rollback reopens current data; the timezone references have no persistent
representation. Preserve current SQLite, archives, thresholds and receipts and
use the separately approved version-rebinding procedure for any installation.
Do not restore an older database. Elimination of real European 1011s and 24-hour
reliability require separately authorized live qualification.
