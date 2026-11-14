"""Tracked-source check only. Never opens a live data store or calls an API."""
import ast
from pathlib import Path
import subprocess

root=Path(__file__).resolve().parents[1]
names=subprocess.check_output(['git','ls-files','-z'],cwd=root).decode().split('\0')
bad=[]
for name in filter(None,names):
    path=root/name;parts=Path(name).parts
    if path.is_symlink() or not path.is_file():
        bad.append((name,'missing or symlink'));continue
    if any(p in ('data','evidence','.venv','__pycache__','.ssh')for p in parts)or path.name.startswith('.env')or name.lower().endswith(('.sqlite3','.sqlite','.db','.jsonl','.gz','.zip','.tar','.tgz','.pyc','.log','.pem','.key','-wal','-shm')):
        bad.append((name,'private data or generated artifact'));continue
    raw=path.read_bytes()
    private_marker=b'-----BEGIN '+b'PRIVATE KEY-----'
    if len(raw)>1024*1024 or b'\0'in raw or private_marker in raw or raw.startswith(b'SQLite format 3'):
        bad.append((name,'binary, credential or excessive source size'));continue
    if name.endswith('.py'):ast.parse(raw,filename=name)
print('Source check:', 'PASS'if not bad else bad)
raise SystemExit(bool(bad))
