# CI Python 3.13.5 exit 139 — bounded investigation

PR #23 merged as `967eae2`; post-merge Linux CI passed all 590 tests on Python
3.11 and 3.13.5. This change is a separate CI/test-only proposal, not a deployment.

## Evidence and scope

The original PR23 Python 3.13.5 job 133348499792 exited 139 immediately after
`Timeout (0:00:30)!` and a partial native faulthandler stack line. The unchanged
rerun passed. A Python line in that partial dump is not an assertion failure or
proof of corruption in PRICE/SQLite. No original native core/backtrace was retained;
the exact original instruction cannot now be established.

CPython documents a race when its native watchdog walks frames/code being freed:
[issue 140815](https://github.com/python/cpython/issues/140815),
[3.13 backport](https://github.com/python/cpython/pull/140985),
[3.13.10 changelog](https://docs.python.org/3.13/whatsnew/changelog.html#python-3-13-10-final).
A [later related metadata report](https://github.com/python/cpython/issues/158200)
is still open; upgrading a patch version is not treated as universal proof here.

Finite Linux run [43886545975](https://github.com/Quackw/Holy-Weather/actions/runs/43886545975)
reproduced SIGSEGV in 5/5 Python 3.13.5 children with the native watchdog and 0/5
with the watchdog disabled. Python 3.11 showed the same 5/5 versus 0/5 contrast.
These children use only stdlib, dynamic-code churn, accelerated 1-microsecond
dump intervals and debug allocation; no app, SQLite, WebSocket or source data.
This proves a reproducible failure of that diagnostic mechanism and strongly
supports the original incident attribution, not an exact original-core diagnosis
or a production failure rate. Mac 3.13.13's earlier non-debug control did not crash.
On Mac 3.14.7 with debug allocation, native controls hit their four-second cap;
they are not counted as SIGSEGV. Off/new-helper controls each completed 5/5.

## Minimal correction

Keep the same Python 3.11/3.13.5 matrix, complete tests, 30-second snapshot delay
and ten-minute external CI job deadline. Replace only the native no-GIL timed
stack walk with an opt-in pytest helper running in a Python thread. It obtains
strong frame references with `sys._current_frames()` and prints bounded
filename/function/line metadata, never locals, source lines or environment values.
An owned stderr duplicate keeps slow-test snapshots visible despite pytest capture.
The helper covers setup/call/teardown, snapshots once, cancels/joins after each
test, and refuses simultaneous activation of the native timed watchdog.

This is cooperative diagnostics on the tested GIL builds. A C extension holding
the GIL can delay the Python snapshot; the independent ten-minute CI job deadline
still bounds the run. This helper does not kill tests, retry failures, change
application timeouts or claim free-threaded/deadlock-proof introspection.

Five regressions exercise visible slow stacks, cancellation/worker cleanup,
non-disclosure of locals, unchanged assertion-failure exit and conflicting
watchdog rejection. The finite probe additionally compares native/off/new-helper
children. Native negative controls may crash; they are isolated, core dumps are
disabled, at most five trials per mode run, and each child has a four-second cap.
New-helper/off controls must pass; lack of an old crash is not labelled proof.

No runtime source, PRICE/LAMP rule, queue, SQLite schema/history, dependency,
GitHub administrative setting, observer or production file changes. Native fatal-
signal handling remains available; only timed asynchronous CI dumping changes.

## Rollback

Revert the CI/test-only change. No data/configuration/interpreter rollback is
needed. This restores the earlier native-watchdog risk in CI; never restore an
old database or reset delivery state. Linux CI must pass the final published head
before review; no automatic merge or runtime installation is authorized.
