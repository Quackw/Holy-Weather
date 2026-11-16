"""Opt-in finite PRICE supervision. No service installation or boot credentials.

The controller never writes SQLite. Its child is the sole writer; source data,
fired levels and terminal receipts stay current across bounded process recovery.
"""
from contextlib import contextmanager
from dataclasses import asdict,dataclass,replace
from datetime import datetime,timedelta,timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from zoneinfo import ZoneInfo
from .catalogue import CatalogueConfig,CatalogueStore,discover_city,observe_catalogue
from .core import canonical,sha
from .history import Seen,check as check_database
from .storage import StorageStop

CONTROL_BYTES=1024*1024
MAX_ATTEMPTS=160
BACKOFF_SECONDS=30
RETRY_STATES={'RECONNECT_BUDGET_EXHAUSTED','NETWORK_GAP_NO_RETRY'}
CRASH_SIGNALS={-int(s) for s in (signal.SIGKILL,signal.SIGSEGV,signal.SIGABRT,signal.SIGBUS)}
STAGE_TIMEOUT={'STARTING':180,'DISCOVERY':180,'OBSERVING':60,'STOPPING':60,'ARCHIVE':300}


class RecoveryStop(RuntimeError):pass


@dataclass(frozen=True)
class Plan:
    config: CatalogueConfig
    session_day: str
    deadline_ms: int
    phase_seconds: int=1200
    archive_dir: str | None=None

    def __post_init__(self):
        cfg=self.config
        if not isinstance(cfg,CatalogueConfig):raise ValueError('RECOVERY_CATALOGUE_ONLY')
        if cfg.city not in cfg.cities:raise ValueError('RECOVERY_REGION_CITY')
        day=datetime.strptime(self.session_day,'%Y-%m-%d')
        if type(self.deadline_ms) is not int or type(self.phase_seconds) is not int or not 1<=self.phase_seconds<=1200:raise ValueError('RECOVERY_PLAN_BOUND')
        ends=[int((day+timedelta(days=1)).replace(tzinfo=ZoneInfo(v[2])).timestamp()*1000) for v in cfg.cities.values()]
        if self.deadline_ms>max(ends) or self.deadline_ms<=min(ends)-26*3600000:raise ValueError('RECOVERY_SESSION_DEADLINE')
        if cfg.date not in (None,self.session_day):raise ValueError('RECOVERY_DATE_DRIFT')
        if not Path(cfg.data_dir).is_absolute() or self.archive_dir is not None and not Path(self.archive_dir).is_absolute():raise ValueError('RECOVERY_ABSOLUTE_PATHS')

    def value(self):return dict(config=asdict(self.config),session_day=self.session_day,deadline_ms=self.deadline_ms,phase_seconds=self.phase_seconds,archive_dir=self.archive_dir)

    @classmethod
    def load(cls,value):
        value=dict(value);cfg=dict(value.pop('config'));cfg['levels']=tuple(cfg.get('levels',('0.50','0.70','0.90')))
        return cls(config=CatalogueConfig(**cfg),**value)


def code_identity():
    import websockets
    return sha(dict(python=list(sys.version_info[:3]),websockets=websockets.__version__,
        files=[(p.name,hashlib.sha256(p.read_bytes()).hexdigest()) for p in sorted(Path(__file__).parent.glob('*.py'))]))


def private(path,directory=False):
    if path.is_symlink() or path.stat().st_uid!=os.getuid() or path.stat().st_mode & 0o077:raise RecoveryStop('RECOVERY_PRIVATE_PATH_REQUIRED')
    if directory!=path.is_dir():raise RecoveryStop('RECOVERY_PATH_TYPE')


def write(root,name,value):
    body=canonical(value).encode()
    total=sum(p.stat().st_size for p in root.iterdir() if p.name not in (name,'.'+name+'.tmp'))
    if total+len(body)>CONTROL_BYTES:raise RecoveryStop('RECOVERY_CONTROL_BUDGET')
    temp=root/('.'+name+'.tmp')
    if temp.exists():raise RecoveryStop('RECOVERY_UNKNOWN_CONTROL_WRITE')
    with temp.open('xb') as f:
        os.chmod(temp,0o600);f.write(body);f.flush();os.fsync(f.fileno())
    os.replace(temp,root/name)
    fd=os.open(root,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


@contextmanager
def owned_control(root,data):
    root=Path(root);data=Path(data)
    if root.is_symlink():raise RecoveryStop('RECOVERY_SYMLINK')
    root=root.resolve();data=data.resolve()
    if not root.is_absolute() or root==data or root in data.parents or data in root.parents:raise RecoveryStop('RECOVERY_CONTROL_DATA_OVERLAP')
    if root.is_symlink():raise RecoveryStop('RECOVERY_SYMLINK')
    root.mkdir(mode=0o700,parents=True,exist_ok=True);private(root,True)
    marker=root/'.price-recovery-owned'
    if not marker.exists():
        if any(root.iterdir()):raise RecoveryStop('RECOVERY_UNOWNED_CONTROL')
        with marker.open('x') as f:
            os.chmod(marker,0o600);f.write('HOLY_WEATHER_PRICE_RECOVERY_V1\n');f.flush();os.fsync(f.fileno())
    if marker.read_text()!='HOLY_WEATHER_PRICE_RECOVERY_V1\n':raise RecoveryStop('RECOVERY_CONTROL_OWNER')
    for p in root.iterdir():private(p)
    lock=root/'.controller.lock'
    with lock.open('a') as f:
        os.chmod(lock,0o600);fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
        yield root


def preflight(plan):
    """Read committed WAL too; never immutable=1, checkpoint or restore old bytes."""
    data=Path(plan.config.data_dir);db=data/'monitor.sqlite3';lock=data/'.writer.lock'
    private(data,True)
    for p in data.iterdir():private(p)
    if (data/'.holy-weather-owned').read_text()!='HOLY_WEATHER_STAGE1\n':raise RecoveryStop('RECOVERY_OWNER')
    if sum(p.stat().st_size for p in data.iterdir())>=plan.config.storage_bytes-min(8*1024*1024,plan.config.storage_bytes//2):raise RecoveryStop('RECOVERY_STORAGE_BOUND')
    if __import__('shutil').disk_usage(data).free<2*1024**3:raise RecoveryStop('RECOVERY_FREE_SPACE')
    if plan.archive_dir:
        archive=Path(plan.archive_dir)
        if archive.exists():
            private(archive,True)
            for part in archive.glob('part-*'):
                if not (part/'ADOPTED.json').is_file():raise RecoveryStop('RECOVERY_UNFINISHED_ARCHIVE')
                for p in part.iterdir():private(p)
                m=json.loads((part/'MANIFEST.json').read_text())
                from .history import digest
                if digest(part/'history.sqlite3.gz')!=m['archive_sha256']:raise RecoveryStop('RECOVERY_ARCHIVE_DRIFT')
    with lock.open('rb') as f:
        fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
        c=sqlite3.connect(db.as_uri()+'?mode=ro',uri=True,timeout=1)
        try:
            c.execute('pragma query_only=ON');c.execute('begin');check_database(c);Seen(c)
            row=c.execute("select value from meta where key='rule_hash'").fetchone()
            if not row or row[0]!=plan.config.rule_hash():raise RecoveryStop('RECOVERY_RULE_DRIFT')
            binding=c.execute("select value from meta where key='price_recovery_control_v1'").fetchone()
            return json.loads(binding[0]) if binding else None
        finally:c.close()


def prepare_resume(store,identity,at):
    """Single writer; expire old PRICE work, keep UNKNOWN/SENT and fired levels."""
    disabled=False
    with store.transaction():
        previous=store.c.execute("select value from meta where key='price_recovery_control_v1'").fetchone()
        if previous and json.loads(previous[0])!=identity:raise RecoveryStop('RECOVERY_CONTROL_BINDING')
        store.c.execute("insert or ignore into meta values('price_recovery_control_v1',?)",(canonical(identity),))
        for key,raw in store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'").fetchall():
            v=json.loads(raw)
            if v['status']=='UNKNOWN' or v.get('claimed_ms') is not None and v['status']!='SENT':disabled=True
            if v['status'] in ('PENDING','WAITING_BID'):
                if v.get('track')!='PRICE_ALERT':raise RecoveryStop('RECOVERY_UNEXPECTED_PENDING_TRACK')
                v.update(status='EXPIRED',reason='NO_BACKLOG_ON_RECOVERY',completed_ms=at)
                store.c.execute('update meta set value=? where key=?',(canonical(v),key))
    return disabled


class Progress:
    def __init__(self,root,nonce):self.root=root;self.nonce=nonce;self.last=-float('inf');self.stage=None
    def __call__(self,stage):
        if stage not in STAGE_TIMEOUT:raise RecoveryStop('RECOVERY_PROGRESS_STAGE')
        mono=time.monotonic()
        if mono-self.last<1 and stage==self.stage:return
        try:write(self.root,self.nonce+'.progress.json',dict(nonce=self.nonce,pid=os.getpid(),stage=stage,monotonic=mono,at_ms=time.time_ns()//1000000))
        except OSError:raise RecoveryStop('RECOVERY_PROGRESS_WRITE') from None
        self.last=mono;self.stage=stage


def checkpoint(plan,progress):
    """Off unless separately authorized in plan; the existing archive API owns it."""
    if plan.archive_dir is None:return
    from .history import prepare,adopt,digest
    data=Path(plan.config.data_dir);db=data/'monitor.sqlite3'
    if db.stat().st_size<128*1024*1024:return
    progress('ARCHIVE');parts=Path(plan.archive_dir)
    if parts==data or data in parts.parents or parts in data.parents:raise RecoveryStop('RECOVERY_ARCHIVE_DATA_OVERLAP')
    parts.mkdir(mode=0o700,parents=True,exist_ok=True);private(parts,True)
    files=list(parts.rglob('*'))
    for p in files:private(p,p.is_dir())
    count=len(list(parts.glob('part-*')));need=5*db.stat().st_size+128*1024*1024
    if count>=40 or sum(p.stat().st_size for p in files if p.is_file())+need>4*1024**3:raise RecoveryStop('RECOVERY_ARCHIVE_BUDGET')
    # After an abrupt writer exit, let SQLite recover committed WAL under its
    # normal single-writer lock before the explicit closed-writer checkpoint.
    with CatalogueStore(plan.config,session=False):pass
    part=parts/('part-'+str(count+1).zfill(3));m=prepare(data,part,expected_sha=digest(db))
    if m['candidate_bytes']>160*1024*1024:raise RecoveryStop('RECOVERY_ARCHIVE_HEADROOM')
    adopt(data,part,cleanup=True)


def frozen_discover(plan,city):
    cfg=plan.config;now=datetime.now(timezone.utc)
    if now.astimezone(ZoneInfo(cfg.cities[city][2])).date().isoformat()!=plan.session_day:return [],{},[{'reason':'SESSION_LOCAL_DATE_ENDED'}]
    markets,receipt,skipped=discover_city(city)
    if any(m['date']!=plan.session_day for m in markets):raise RecoveryStop('RECOVERY_DISCOVERY_DATE_DRIFT')
    return markets,receipt,skipped


def worker(root,nonce,token):
    saved=json.loads((root/'plan.json').read_text());plan=Plan.load(saved['plan'])
    if saved['code']!=code_identity():raise RecoveryStop('RECOVERY_CODE_DRIFT')
    stop=threading.Event()
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:stop.set())
    timer=threading.Timer(max(0,(plan.deadline_ms-time.time_ns()//1000000)/1000),stop.set)
    timer.daemon=True;timer.start()
    progress=Progress(root,nonce);progress('STARTING');outcome='FATAL'
    try:
        remaining=(plan.deadline_ms-time.time_ns()//1000000)//1000
        if remaining<1:outcome='DEADLINE'
        else:
            checkpoint(plan,progress)
            remaining=(plan.deadline_ms-time.time_ns()//1000000)//1000
            if remaining<1 or stop.is_set():outcome='SIGNAL_STOP' if stop.is_set() else 'DEADLINE'
            else:
                cfg=replace(plan.config,duration_seconds=min(plan.phase_seconds,int(remaining)))
                with CatalogueStore(cfg,session=False) as store:
                    disabled=prepare_resume(store,saved['identity'],time.time_ns()//1000000)
                cfg=replace(cfg,telegram_enabled=cfg.telegram_enabled and not disabled)
                result=observe_catalogue(cfg,token=token if cfg.telegram_enabled else None,stop=stop,
                    discover=lambda city:frozen_discover(plan,city),progress=progress,session_day=plan.session_day)
                outcome=result['state']
    except Exception as exc:
        # Exception text can contain private HTTP details. Keep only the class.
        error=type(exc).__name__
        reason=str(exc) if re.fullmatch(r'[A-Z][A-Z0-9_:.+-]{0,96}',str(exc)) else None
    else:error=None;reason=None
    timer.cancel()
    write(root,nonce+'.result.json',dict(nonce=nonce,pid=os.getpid(),state=outcome,error=error,reason=reason,at_ms=time.time_ns()//1000000))


def spawn(root,nonce,token):
    env={'PATH':os.defpath,'LANG':'C.UTF-8','PYTHONDONTWRITEBYTECODE':'1','PYTHONNOUSERSITE':'1','PYTHONPATH':os.pathsep.join(sys.path)}
    child=subprocess.Popen([sys.executable,'-B','-m','holy_weather.recovery','worker',str(root),nonce],
        stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,text=True,env=env,close_fds=True)
    return child


def supervise(plan,control_dir,*,token=None,stop=None,clock=None,monotonic=time.monotonic,wait=None,_spawn=spawn):
    """Finite controller; graceful stop only. No kill, boot install or old PID adoption."""
    if plan.config.telegram_enabled and (not isinstance(token,str) or not re.fullmatch(r'\d+:[A-Za-z0-9_-]{20,}',token)):raise RecoveryStop('RECOVERY_EXPLICIT_TOKEN')
    default_wait=wait is None
    stop=stop or threading.Event();clock=clock or (lambda:time.time_ns()//1000000);wait=wait or stop.wait
    with owned_control(control_dir,plan.config.data_dir) as root:
        if plan.archive_dir:
            archive=Path(plan.archive_dir).resolve()
            if archive==root or root in archive.parents or archive in root.parents:raise RecoveryStop('RECOVERY_CONTROL_ARCHIVE_OVERLAP')
        code=code_identity();identity=dict(control=str(root),plan=sha(plan.value()),code=code)
        state_path=root/'state.json';saved_plan=dict(plan=plan.value(),code=code,identity=identity)
        if state_path.exists():
            state=json.loads(state_path.read_text())
            if state['identity']!=identity:raise RecoveryStop('RECOVERY_IDENTITY_DRIFT')
            if state.get('active'):
                # The previous controller may have died during launch/write.
                # Never guess a missing PID or attach to an unowned process.
                raise RecoveryStop('RECOVERY_PREVIOUS_ATTEMPT_UNRESOLVED')
            if not state.get('retry') and state['status'] not in ('COMPLETED_WINDOW','PREPARING'):return state
        else:state=dict(identity=identity,attempts=[],restarts=[],last_ms=clock(),active=None,status='PREPARING')
        if (root/'plan.json').exists() and canonical(json.loads((root/'plan.json').read_text()))!=canonical(saved_plan):raise RecoveryStop('RECOVERY_PLAN_DRIFT')
        write(root,'plan.json',saved_plan)
        mono_deadline=monotonic()+max(0,(plan.deadline_ms-clock())/1000)
        retry=bool(state.get('retry'));child=None
        def save(status):
            state.update(status=status,last_ms=clock());write(root,'state.json',state)
        def ended():return stop.is_set() or clock()>=plan.deadline_ms or monotonic()>=mono_deadline
        try:
            while not ended():
                now=clock()
                if now<state['last_ms']:raise RecoveryStop('RECOVERY_CLOCK_REVERSED')
                if len(state['attempts'])>=MAX_ATTEMPTS:state['retry']=False;save('ATTEMPT_LIMIT');break
                if retry:
                    recent=[t for t in state['restarts'] if now-t<86400000]
                    if len(recent)>=6 or sum(now-t<3600000 for t in recent)>=3:state['retry']=False;save('RESTART_BUDGET_EXHAUSTED');break
                    next_ms=state.get('next_ms',now+min(BACKOFF_SECONDS*2**len(recent),300)*1000)
                    state.update(retry=True,next_ms=next_ms);save('BACKOFF')
                    while clock()<next_ms and not ended():
                        if clock()<state['last_ms']:raise RecoveryStop('RECOVERY_CLOCK_REVERSED')
                        wait(min(1,(next_ms-clock())/1000))
                    if ended():break
                if code_identity()!=code:raise RecoveryStop('RECOVERY_CODE_DRIFT')
                binding=preflight(plan)
                if binding is not None and binding!=identity:raise RecoveryStop('RECOVERY_CONTROL_BINDING')
                nonce=uuid.uuid4().hex;entry=dict(nonce=nonce,at_ms=clock(),recovery=retry)
                if retry:state['restarts'].append(clock())
                state['attempts'].append(entry);state['active']=nonce;state.pop('next_ms',None);save('STARTING')
                child=_spawn(root,nonce,token);entry['pid']=child.pid;save('RUNNING')
                child.stdin.write((token or '')+'\n');child.stdin.flush();child.stdin.close()
                started=monotonic();requested=None;requested_at=None;progress=None
                while child.poll() is None:
                    p=root/(nonce+'.progress.json');progress=json.loads(p.read_text()) if p.exists() else None
                    if progress and (progress['nonce']!=nonce or progress['pid']!=child.pid or progress['stage'] not in STAGE_TIMEOUT):raise RecoveryStop('RECOVERY_PROGRESS_IDENTITY')
                    if progress and (type(progress['monotonic']) not in (int,float) or not math.isfinite(progress['monotonic']) or progress['monotonic']>monotonic()+.1):raise RecoveryStop('RECOVERY_PROGRESS_CLOCK')
                    age=monotonic()-(progress['monotonic'] if progress else started)
                    limit=STAGE_TIMEOUT[progress['stage']] if progress else STAGE_TIMEOUT['STARTING']
                    if not requested and (ended() or age>limit):
                        requested='STOP' if ended() else 'STALLED';requested_at=monotonic();entry['requested_stop']=requested;save(requested)
                        child.terminate()  # One SIGTERM to our own child. Never SIGKILL.
                    if requested and monotonic()-requested_at>=30:
                        save('UNRESPONSIVE_NO_KILL');return state
                    # Event.wait returns immediately once stopping. Keep the
                    # bounded grace period paced even for an unresponsive child.
                    if requested and default_wait and stop.is_set():time.sleep(.25)
                    else:wait(.25)
                returncode=child.poll();result_path=root/(nonce+'.result.json');result=json.loads(result_path.read_text()) if result_path.exists() else None
                # A short-lived process may have published a newer stage and
                # exited between polls. Never classify it from an older stage.
                p=root/(nonce+'.progress.json')
                progress=json.loads(p.read_text()) if p.exists() else None
                if progress and (progress['nonce']!=nonce or progress['pid']!=child.pid or progress['stage'] not in STAGE_TIMEOUT):raise RecoveryStop('RECOVERY_PROGRESS_IDENTITY')
                if result and (result['nonce']!=nonce or result['pid']!=child.pid):raise RecoveryStop('RECOVERY_RESULT_IDENTITY')
                outcome=result['state'] if result else 'CRASH' if returncode in CRASH_SIGNALS else 'UNKNOWN_EXIT'
                stage=progress['stage'] if progress else 'STARTING'
                retry=(outcome in RETRY_STATES or outcome=='CRASH' and stage=='OBSERVING' or requested=='STALLED' and outcome=='SIGNAL_STOP')
                if result and result.get('error'):retry=False
                entry.update(state=outcome,returncode=returncode,finished_ms=clock(),error=result.get('error') if result else None,reason=result.get('reason') if result else None);state.update(active=None,retry=retry)
                child=None;save(outcome)
                if requested=='STOP':break
                if outcome=='COMPLETED_WINDOW':continue
                if not retry:break
            if ended():save('STOPPED' if stop.is_set() else 'DEADLINE')
            return state
        finally:
            # A control-file/preflight failure never authorizes another launch.
            if child is not None and child.poll() is None and not state['attempts'][-1].get('requested_stop'):
                child.terminate()


def main():
    import argparse
    os.umask(0o077);resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    a=sub.add_parser('run');a.add_argument('--plan',required=True);a.add_argument('--control-dir',required=True)
    a=sub.add_parser('worker');a.add_argument('control_dir');a.add_argument('nonce')
    args=p.parse_args()
    if args.command=='worker':
        if not re.fullmatch('[0-9a-f]{32}',args.nonce):raise RecoveryStop('RECOVERY_NONCE')
        worker(Path(args.control_dir),args.nonce,sys.stdin.readline(4096).strip() or None)
        return 0
    else:
        plan=Plan.load(json.loads(Path(args.plan).read_text()));stop=threading.Event()
        for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:stop.set())
        token=sys.stdin.readline(4096).strip() if plan.config.telegram_enabled else None
        result=supervise(plan,args.control_dir,token=token,stop=stop)
        print(canonical({'status':result['status'],'attempts':len(result['attempts']),'restarts':len(result['restarts'])}))
        return 0 if result['status'] in ('DEADLINE','STOPPED','ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED') else 2


if __name__=='__main__':raise SystemExit(main())
