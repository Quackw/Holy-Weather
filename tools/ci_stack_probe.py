"""Finite stdlib-only negative/control experiment; no application or database."""
import json
import os
import subprocess
import sys

CHILD = r'''
import faulthandler, json, os, resource, sys, threading, time, types
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
stop = threading.Event()
calls = 0
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
    if sys.argv[1] == 'native':
        faulthandler.dump_traceback_later(0.000001, repeat=True, file=output)
    try:
        time.sleep(1)
    finally:
        faulthandler.cancel_dump_traceback_later()
        stop.set()
        worker.join(1)
assert not worker.is_alive()
print(json.dumps({'calls': calls, 'python': sys.version.split()[0]}))
'''

results = []
for mode in ('native', 'off'):
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
assert all(r['code'] == 0 for r in results if r['mode'] == 'off')
