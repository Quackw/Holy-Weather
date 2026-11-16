import io
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

from ci_stacks import delayed_stacks, dump_stacks


def test_snapshot_keeps_frame_identity_without_locals():
    secret_value = 'synthetic-local-must-not-be-logged'
    stream = io.StringIO()
    dump_stacks(stream, 'probe')
    assert 'test_snapshot_keeps_frame_identity_without_locals' in stream.getvalue()
    assert 'CI_STACK_SNAPSHOT' in stream.getvalue()
    assert secret_value not in stream.getvalue()


def test_slow_call_gets_one_snapshot_and_joins_worker():
    ready = threading.Event()
    before = {t.ident for t in threading.enumerate() if t.name == 'ci-stack-snapshot'}
    class Output(io.StringIO):
        def write(self, value):
            result = super().write(value)
            ready.set()
            return result
    stream = Output()
    with delayed_stacks(.01, 'slow', stream):
        assert ready.wait(2)
    assert stream.getvalue().count('CI_STACK_SNAPSHOT') == 1
    assert {t.ident for t in threading.enumerate() if t.name == 'ci-stack-snapshot'} == before


def test_fast_call_cancels_without_late_snapshot():
    stream = io.StringIO()
    with delayed_stacks(.05, 'fast', stream):
        pass
    time.sleep(.06)
    assert not stream.getvalue()


def child_pytest(tmp_path, extra):
    sample = tmp_path/'test_sample.py'
    sample.write_text('import time\ndef test_slow(): time.sleep(.08)\ndef test_failure(): assert False, "ORIGINAL_FAILURE"\n')
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).parent),
               PYTEST_DISABLE_PLUGIN_AUTOLOAD='1')
    return subprocess.run([sys.executable, '-B', '-m', 'pytest', '-q', '-p', 'ci_stacks',
                           '--ci-stack-seconds=.01', *extra, str(sample)],
                          cwd=tmp_path, env=env, capture_output=True, text=True, timeout=10)


def test_pytest_hook_preserves_failure_and_emits_slow_stack(tmp_path):
    result = child_pytest(tmp_path, ['-o', 'faulthandler_timeout=0'])
    assert result.returncode == 1, result.stdout+result.stderr
    assert 'ORIGINAL_FAILURE' in result.stdout
    assert '1 failed, 1 passed' in result.stdout
    assert 'CI_STACK_SNAPSHOT' in result.stderr+result.stdout


def test_pytest_rejects_simultaneous_native_watchdog(tmp_path):
    result = child_pytest(tmp_path, ['-o', 'faulthandler_timeout=.01'])
    assert result.returncode == 4
    assert 'CI_STACK_CANNOT_COMBINE_WITH_NATIVE_WATCHDOG' in result.stderr
