# One-shot Europe native WebSocket diagnostic

This PR is stacked on #44 (`68e7d93`). It gathers missing evidence; it does not
claim to fix the remaining live 1011. Installing it requires separate approval.
US and unarmed Europe use the previous connection class. PRICE/LAMP logic,
Decimal midpoint, thresholds, once-only levels, queue admission/delivery,
freshness, reconnect/backoff limits, plan format and SQLite v1 are unchanged.

## Explicit arming and lifecycle

Only the existing Europe catalogue owner checks for the exact regular file
`.ws-diagnostic-v1.arm` in its owned data directory. The service account must own
it with mode 600 and exact bytes `HOLY_WEATHER_EUROPE_WS_DIAGNOSTIC_V1\n`.
No runtime creates this request automatically. An installer may create it only
as part of the separately approved operation, with writer/backup/version guards.
The application never uses an environment or credential fallback to enable it.

Without the request: no recorder, sampler or trace file. US never arms it.
With it: one recorder follows the existing worker's connections. At the next
recorded code 1011, the main owner saves `ws-diagnostic-v1.jsonl` and the existing
DISCONNECTED metadata record gains only the file name, size, SHA and status.
There is no per-frame SQLite diagnostic write or second SQLite writer.
Recording stops after that attempt; later connections use the original class.
A saved output or `.ws-diagnostic-v1.pending` disables arming after restart.
Other errors keep their existing treatment and do not consume a saved capture.

Do not silently rearm, overwrite, remove or truncate either evidence file. An
interrupted/failed publication remains explicit. Preserve and verify the file
before any separately approved move/rearm. The request and trace are ordinary
owned files, so accepted `68e7d93` can read/write the same SQLite with them intact.
Code rollback must use the existing guarded rebind on current data; never restore
an old SQLite over new events. No plan/config migration is necessary.

## Bounds and isolation

- Preallocated **512 KiB** packed ring: 8192 records of 64 bytes. Retain at most
  60 seconds; overwritten capacity and skipped/failed measurements are counted.
  A busy recorder never makes the receiver wait for its lock.
- One final file, **at most 1 MiB including metadata, stacks and footer**. Records
  are streamed newest first without building an unbounded serialized copy.
- Existing **256 MiB** directory cap and SQLite reserve still apply. Atomic
  publication reserves room for both temporary names; the existing **16 GiB**
  filesystem floor is preserved. Insufficient headroom skips the diagnostic.
- One cooperative sampler per armed connection, 100 ms cadence, joined at close;
  no independent observation, source request, service or Telegram call.
- At most three recent stack samples, 16 frames for each of main, receiver and
  keepalive roles, when a queued native Ping is outstanding for at least 15 s.
  Strong Python frame/code references, not a native timed faulthandler walk.
  Filename basenames, function names and line numbers only; no locals, source
  lines, frame representations, bodies, native Ping bytes, environment or secrets.
  A GIL-holding native stall can delay this cooperative sampler; absence is not PASS.
- No timeout or buffer watermark changes: native 10 s / 20 s, high 16 / low 4,
  maximum frame size 262144, unchanged application heartbeat and reconnect budget.

## Reading the evidence

First line: schema/legend, code identity, PID, connection UUIDs, civil/monotonic
anchors, limits, dropped/fault counts and bounded stacks. Final line: completion
and truncation counts. A missing footer or SHA mismatch is partial evidence.
Every middle line is `[monotonic_ns, kind, connection, a, b, c, d, e, f]`.
Unmeasured values are **-1**, never invented zero. Use monotonic times for latency;
civil anchors do not manufacture historical source/receipt timestamps.

| Kind | Values after connection |
|---|---|
| PING_QUEUED / PING_FLUSHED | Opaque per-connection sequence; queued vs successful socket-write completion |
| PONG_PARSED | Matching sequence, or -1 when correlation is missing; decoder timestamp before dispatch |
| PONG_PROCESSED | Matching sequence, number of acknowledged/covered Pings; after library processing |
| READ_PAUSED | Queue depth at flow-control pause |
| READ_RESUMED | Queue depth, measured pause duration in ns |
| SAMPLE | Depth, paused flag, kernel unread bytes, last receiver-local TLS pending bytes, TLS sample age ns, current pause age ns |
| PENDING_PING | Sequence, queued ns, flushed ns, parsed ns, outstanding age ns |
| PHASE_CURRENT | Main-loop phase, entry ns, elapsed ns |
| PHASE_INTERVAL | Phase, completed calls, total ns, **interval maximum ns**, interval start ns, interval end ns |
| RECEIVER_INTERVAL | Interval data frames, interval payload bytes, **connection-cumulative** maximum payload size, interval start ns, interval end ns |
| MAIN_INTERVAL | Interval messages, interval payload bytes, **capture-cumulative** maximum payload size, interval start ns, interval end ns |
| CLIENT_FAIL | Native close code, 1 only for the library's keepalive-timeout reason |
| REMOTE_CLOSE | Received native close code only |

Receiver data-frame counts and main message counts are distinct (fragmentation
and application PONG matter). Byte counts are payload bytes, not TCP/TLS traffic.
`SSL.pending()` is read only on the receiver, not concurrently by the sampler;
the timestamp/age makes stale TLS measurements explicit. Kernel FIONREAD cannot
identify a particular Pong or describe the server's send backlog.

Native correlation uses exact payload equality in a bounded private map but
stores only an opaque sequence. Unmatched trace records, dropped observations or
faults prohibit treating the trace as complete. Text `PING`/`PONG` never confirm
native keepalive and are not assigned fabricated matching IDs.

Long pause/processing intervals with a delayed parsed Pong support a local
bottleneck. Active reading with no matching parsed Pong supports a native
peer/transport delay; it does not alone locate the fault in the remote network
or server. A parsed Pong delayed before acknowledgement identifies client
dispatch/scheduling. Each conclusion needs its own correlated timing evidence.

## Qualification

Offline regressions cover arming/isolation, fixed memory/file limits, storage and
write failures, no overwrite/retry of partial evidence, native correlation and
privacy, actual flow-control pause duration, unchanged full PRICE/queue state,
once-only capture/reconnect limits and actual `68e7d93` rollback after new records.
Two synthetic causes use the real 10/20-second native settings: a responding peer
behind a paused consumer, and withheld native Pong while text PONG continues.
They must produce distinguishable traces, not a claimed reconstruction of the
historical WAN failure. Live evidence remains outside Git. Linux CI on Python
3.11/3.13.5 and the existing systemd qualification are required before review.
