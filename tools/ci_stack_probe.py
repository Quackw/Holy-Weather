"""Finite isolated negative/control experiment; no application or database."""
import json
import os
import subprocess
import sys

CHILD = r'''
import faulthandler, json, os, resource, sys, threading, time, types
from pathlib import Path
sys.path.insert(0, str(Path.cwd()/'tests'))
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
if sys.argv[1] == 'safe':
    # Import before CPU churn so all modes measure the one-second experiment,
    # not module-import contention with the busy worker.
    from ci_stacks import dump_stacks
stop = threading.Event()
calls = 0
dumps = 0
def template(): return 1
def churn():
    global calls
    while not stop.is_set():
        code = template.__code__.replace()
        fn = types.FunctionType(code, {})
        fn()
        calls += 1
        del fn, code
worker = threading.Thread(target=churn)
worker.start()
with open(os.devnull, 'w') as output:
    dumper = None
    if sys.argv[1] == 'native':
        faulthandler.dump_traceback_later(0.000001, repeat=True, file=output)
    if sys.argv[1] == 'safe':
        def snapshots():
            global dumps
            while not stop.wait(.000001):
                dump_stacks(output, 'finite-code-churn')
                dumps += 1
        dumper = threading.Thread(target=snapshots)
        dumper.start()
    try:
        time.sleep(1)
    finally:
        faulthandler.cancel_dump_traceback_later()
        stop.set()
        worker.join(1)
        if dumper: dumper.join(1)
assert not worker.is_alive()
assert not dumper or not dumper.is_alive()
assert sys.argv[1] != 'safe' or dumps > 0
assert 'sqlite3' not in sys.modules and not any(n.startswith('holy_weather') for n in sys.modules)
print(json.dumps({'calls': calls, 'dumps': dumps, 'python': sys.version.split()[0]}))
'''

results = []
for mode in ('native', 'off', 'safe'):
    for trial in range(5):
        try:
            p = subprocess.run([sys.executable, '-B', '-c', CHILD, mode],
                               env=dict(os.environ, PYTHONMALLOC='debug'),
                               capture_output=True, text=True, timeout=4)
            results.append(dict(mode=mode, trial=trial, code=p.returncode,
                                output=p.stdout.strip(), stderr=p.stderr[:500]))
        except subprocess.TimeoutExpired:
            results.append(dict(mode=mode, trial=trial, code='TIMEOUT'))
print(json.dumps(results, indent=2))
assert all(r['code'] == 0 for r in results if r['mode'] != 'native')
