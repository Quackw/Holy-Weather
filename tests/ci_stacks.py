"""CI-only cooperative stack snapshots; never the native no-GIL watchdog."""
from contextlib import contextmanager
import math
import os
import sys
import threading

import pytest

STACK_STREAM = pytest.StashKey()


def dump_stacks(stream, label):
    # Strong Python frame/code references replace faulthandler's racy C walk.
    # No locals, source lines, payloads or environment values are logged.
    frames = sys._current_frames()
    lines = [f'CI_STACK_SNAPSHOT: {label[:500]!r}\n']
    try:
        for ident, frame in list(frames.items())[:100]:
            lines.append(f'Thread {ident:x}:\n')
            for _ in range(100):
                if frame is None:
                    break
                code = frame.f_code
                lines.append(f'  File {code.co_filename[:500]!r}, line {frame.f_lineno} in {code.co_name[:500]!r}\n')
                frame = frame.f_back
        stream.write(''.join(lines))
        stream.flush()
    finally:
        frames.clear()


@contextmanager
def delayed_stacks(seconds, label, stream=None):
    if not seconds:
        yield
        return
    stop = threading.Event()
    def watch():
        if not stop.wait(seconds):
            dump_stacks(stream or sys.__stderr__, label)
    worker = threading.Thread(target=watch, name='ci-stack-snapshot', daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop.set()
        worker.join(1)
        if worker.is_alive():
            raise RuntimeError('CI_STACK_SNAPSHOT_DID_NOT_FINISH')


def pytest_addoption(parser):
    parser.addoption('--ci-stack-seconds', type=float, default=0,
                     help='Cooperative Python stack snapshot for a slow test')


def pytest_configure(config):
    seconds = config.getoption('--ci-stack-seconds')
    if not math.isfinite(seconds) or not 0 <= seconds <= 300:
        raise pytest.UsageError('CI_STACK_SECONDS_BOUND')
    if seconds and config.getini('faulthandler_timeout'):
        raise pytest.UsageError('CI_STACK_CANNOT_COMBINE_WITH_NATIVE_WATCHDOG')
    if seconds:
        # Duplicate before per-test fd capture, like pytest's native plugin.
        config.stash[STACK_STREAM] = os.fdopen(os.dup(sys.stderr.fileno()), 'w',
                                              encoding='utf-8', buffering=1)


def pytest_unconfigure(config):
    if STACK_STREAM in config.stash:
        config.stash[STACK_STREAM].close()
        del config.stash[STACK_STREAM]


@pytest.hookimpl(wrapper=True, trylast=True)
def pytest_runtest_protocol(item):
    with delayed_stacks(item.config.getoption('--ci-stack-seconds'), item.nodeid,
                        item.config.stash.get(STACK_STREAM, None)):
        return (yield)
