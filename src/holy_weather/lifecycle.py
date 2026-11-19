"""Persistent ownership and audit for the existing regional PRICE controller.

No process signals, service installation, credential access or network calls.
Closed control files move to schema-v1 SQLite audit before redundant files retire.
"""
from pathlib import Path
from contextlib import contextmanager
import fcntl,json,os,sqlite3,time
from .core import canonical,sha
from .history import check as check_database

PROTOCOL=1
KEEP_ATTEMPTS=2
ROTATE_ATTEMPTS=32
ROTATION='rotation.json'
STOP_REQUEST='service-stop.json'
AUDIT='price_control_audit_v1:'
SAFE_STOP_STATES={'SIGNAL_STOP','COMPLETED_WINDOW','CANCELLED_BEFORE_SPAWN',
                  'NO_VERIFIED_CURRENT_MARKETS','ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED'}
QUOTA_WAIT='WAITING_NETWORK_QUOTA'
NETWORK_RETRY={'RECONNECT_BUDGET_EXHAUSTED','NETWORK_GAP_NO_RETRY'}


def clean_stop(state):
    last=state['attempts'][-1] if state['attempts'] else None
    return not last or bool(last.get('finished_ms') and not last.get('error') and last.get('state') in SAFE_STOP_STATES)


class LifecycleStop(RuntimeError):pass


def process_identity(pid=None):
    """Linux boot plus kernel start ticks; a reused PID is never its old owner."""
    pid=os.getpid() if pid is None else pid
    try:
        boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        root=Path('/proc')/str(pid);stat=(root/'stat').read_text().rsplit(')',1)[1].split()
        if stat[0] in ('Z','X'):return None
        uid=int(next(x.split()[1] for x in (root/'status').read_text().splitlines() if x.startswith('Uid:')))
        return dict(boot=boot,pid=pid,start_ticks=int(stat[19]),uid=uid,exe=os.readlink(root/'exe'))
    except (FileNotFoundError,ProcessLookupError):return None


def alive(identity):
    return bool(identity) and process_identity(identity['pid'])==identity


def own_identity():
    value=process_identity()
    if value is None:raise LifecycleStop('LIFECYCLE_LINUX_IDENTITY_REQUIRED')
    return value


def validate_guard(root,fd):
    """The same open-file description is inherited before child exec starts."""
    if type(fd) is not int or fd<0:raise LifecycleStop('LIFECYCLE_INHERITED_GUARD_REQUIRED')
    a=os.fstat(fd);b=(root/'.controller.lock').stat()
    if (a.st_dev,a.st_ino,a.st_uid)!=(b.st_dev,b.st_ino,os.getuid()):raise LifecycleStop('LIFECYCLE_GUARD_IDENTITY')
    fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)


def claim_child(root,nonce,fd,saved,write):
    """Claim the verified launch and read its PRICE handoff authorization."""
    validate_guard(root,fd)
    state=json.loads((root/'state.json').read_text())
    entry=state['attempts'][-1]
    if state.get('active')!=nonce or entry['nonce']!=nonce or entry.get('guard_protocol')!=PROTOCOL or state['identity']!=saved['identity']:
        raise LifecycleStop('LIFECYCLE_LAUNCH_INTENT_MISMATCH')
    owner=dict(protocol=PROTOCOL,nonce=nonce,binding=saved['identity'],process=own_identity(),controller=state['controller_process'])
    write(root,nonce+'.owner.json',owner)
    return entry.get('carry_pending_price') is True


def charge(state,now):
    """The same 3/hour and 6/day ceiling, persisted before recovery/backoff."""
    recent=[t for t in state['restarts'] if now-t<86400000]
    if len(recent)>=6 or sum(now-t<3600000 for t in recent)>=3:
        state.update(retry=False,status='RESTART_BUDGET_EXHAUSTED')
        return False
    state['restarts'].append(now)
    state.update(retry=True,recovery_charged=True,
        next_ms=max(state.get('next_ms',0),now+min(30*2**len(recent),300)*1000))
    return True


def quota_available(restarts,now):
    """Earliest admission under BOTH unchanged rolling limits; never prune debt."""
    if any(type(t) is not int or not 0<=t<=now for t in restarts):
        raise LifecycleStop('RECOVERY_BUDGET_CLOCK_OR_RECORD')
    due=now
    for window,limit in ((3600000,3),(86400000,6)):
        recent=sorted(t for t in restarts if now-t<window)
        if len(recent)>=limit:due=max(due,recent[len(recent)-limit]+window)
    return due


def network_retry(state,now):
    """Only a known, closed, successful protocol result admits quota waiting."""
    last=state['attempts'][-1] if state['attempts'] else None
    known=bool(not state.get('active') and last and last.get('state') in NETWORK_RETRY
        and type(last.get('returncode')) is int and last['returncode']==0
        and last.get('error') is None and last.get('reason') is None
        and last.get('guard_protocol')==PROTOCOL
        and type(last.get('finished_ms')) is int and last['at_ms']<=last['finished_ms']<=now)
    marker=state.get('network_quota_wait')
    if marker and (not known or marker.get('source_nonce')!=last['nonce'] or marker.get('reason')!=last['state']
            or any(type(marker.get(k)) is not int for k in ('since_ms','quota_available_ms','resume_not_before_ms'))
            or not last['finished_ms']<=marker['since_ms']<=now
            or not marker['since_ms']<=marker['quota_available_ms']<=marker['resume_not_before_ms']):
        raise LifecycleStop('RECOVERY_NETWORK_WAIT_DRIFT')
    return known


def defer_network(state,now):
    """Persist an unpaid wait, not a reusable recovery credit."""
    if not network_retry(state,now) or state.get('recovery_charged'):
        raise LifecycleStop('RECOVERY_NETWORK_WAIT_SOURCE')
    last=state['attempts'][-1];previous=state.get('network_quota_wait')
    if previous and (previous['source_nonce']!=last['nonce'] or previous['reason']!=last['state']):
        raise LifecycleStop('RECOVERY_NETWORK_WAIT_DRIFT')
    due=quota_available(state['restarts'],now)
    recent=sum(due-t<86400000 for t in state['restarts'])
    value=dict(source_nonce=last['nonce'],reason=last['state'],
               since_ms=previous['since_ms'] if previous else now,quota_available_ms=due,
               resume_not_before_ms=due+min(30*2**recent,300)*1000)
    state.update(retry=True,status=QUOTA_WAIT,network_quota_wait=value)


def stop_request(root,state):
    path=root/STOP_REQUEST
    if not path.exists():return None
    value=json.loads(path.read_text())
    if value.get('binding')!=state['identity'] or value.get('controller')!=state.get('stop_request_controller',state.get('controller_process')):
        raise LifecycleStop('LIFECYCLE_STOP_INTENT_MISMATCH')
    if value.get('reason') not in ('MANUAL_STOP','HOST_SHUTDOWN','CONTROLLER_FAILURE','FATAL_SERVICE_EXIT'):
        raise LifecycleStop('LIFECYCLE_STOP_INTENT_UNKNOWN')
    return value


def reconcile(root,state,now,write,preflight,*,wait_network_quota=False):
    """Caller owns inherited-controller lock; no adoption of an ambiguous old run."""
    current=own_identity();previous=state.get('controller_process');request=stop_request(root,state)
    reason=request['reason'] if request else state.get('stop_reason')
    shutdown=reason=='HOST_SHUTDOWN' and previous and previous['boot']!=current['boot']
    if state['status']=='STOPPED' and not clean_stop(state):return False
    if reason in ('MANUAL_STOP','FATAL_SERVICE_EXIT') or state['status'] in (
            'FATAL','UNKNOWN_EXIT','UNRESPONSIVE_NO_KILL','RESTART_BUDGET_EXHAUSTED','ATTEMPT_LIMIT','DEADLINE','NETWORK_REFUSED_NO_RETRY'):
        return False
    if state['status']=='STOPPED' and not shutdown:return False
    waiting=state['status']==QUOTA_WAIT
    if waiting and not (wait_network_quota and state.get('retry') is True
                        and state.get('network_quota_wait') and network_retry(state,now)):
        return False
    allowed={'PREPARING','COMPLETED_WINDOW','BACKOFF','STARTING','RUNNING','STOP','STALLED',
             'NO_VERIFIED_CURRENT_MARKETS','ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED','STOPPED'}
    if state['status'] not in allowed and not state.get('retry'):return False
    if reason=='HOST_SHUTDOWN' and previous and previous['boot']==current['boot']:return False
    if previous and previous!=current and alive(previous):raise LifecycleStop('LIFECYCLE_OLD_CONTROLLER_ALIVE')
    if now<state['last_ms']:raise LifecycleStop('RECOVERY_CLOCK_REVERSED')
    network=wait_network_quota and state.get('retry') is True and network_retry(state,now)
    if network and preflight()!=state['identity']:raise LifecycleStop('RECOVERY_CONTROL_BINDING')
    interrupted=previous is not None and previous!=current
    if state.get('active'):
        entry=state['attempts'][-1];nonce=state['active']
        if entry['nonce']!=nonce or entry.get('guard_protocol')!=PROTOCOL:
            raise LifecycleStop('RECOVERY_PREVIOUS_ATTEMPT_UNRESOLVED')
        owner_path=root/(nonce+'.owner.json');result_path=root/(nonce+'.result.json')
        owner=json.loads(owner_path.read_text()) if owner_path.exists() else None
        final=json.loads(result_path.read_text()) if result_path.exists() else None
        if owner:
            if owner.get('protocol')!=PROTOCOL or owner.get('nonce')!=nonce or owner.get('binding')!=state['identity'] or owner.get('controller')!=previous:
                raise LifecycleStop('LIFECYCLE_CHILD_IDENTITY_MISMATCH')
            if alive(owner['process']):raise LifecycleStop('LIFECYCLE_OLD_CHILD_ALIVE')
        elif final:raise LifecycleStop('LIFECYCLE_RESULT_WITHOUT_OWNER')
        if final and (final.get('nonce')!=nonce or final.get('pid')!=owner['process']['pid']):
            raise LifecycleStop('LIFECYCLE_RESULT_IDENTITY_MISMATCH')
        if final and (final.get('error') or final['state'] not in ('COMPLETED_WINDOW','SIGNAL_STOP','NO_VERIFIED_CURRENT_MARKETS','ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED','RECONNECT_BUDGET_EXHAUSTED','NETWORK_GAP_NO_RETRY','DEADLINE_DURING_GAP')):
            state.update(retry=False,status='FATAL');return False
        # This checks current committed WAL/integrity and holds the writer lock.
        # No old snapshot is restored and no missing OS exit code is invented.
        if preflight()!=state['identity']:raise LifecycleStop('RECOVERY_CONTROL_BINDING')
        entry.update(state='CONTROLLER_INTERRUPTED',finished_ms=now,returncode=None,
                     reason='HOST_BOOT_CHANGED' if previous and previous['boot']!=current['boot'] else 'CONTROLLER_EXIT',
                     child_result_state=final.get('state') if final else None)
        state.update(active=None,retry=True)
        interrupted=True
    if interrupted and not state.get('recovery_charged'):
        if network and quota_available(state['restarts'],now)>now:
            defer_network(state,now)
        else:charge(state,now)
        if not state['retry']:write(root,'state.json',state);return False
    state.update(controller_process=current,stop_reason=None,
                 status=QUOTA_WAIT if state.get('status')==QUOTA_WAIT and not state.get('recovery_charged')
                 else 'BACKOFF' if state.get('retry') else 'PREPARING')
    # Consuming a service-stop intent needs audit/retirement before another start.
    if request:state.update(consume_stop_request=True,stop_request_controller=request['controller'])
    write(root,'state.json',state)
    return True


@contextmanager
def closed_database(plan):
    data=Path(plan.config.data_dir)
    if data.is_symlink() or (data/'monitor.sqlite3').is_symlink() or (data/'.writer.lock').is_symlink():raise LifecycleStop('LIFECYCLE_DATABASE_SYMLINK')
    if (data/'.holy-weather-owned').read_text()!='HOLY_WEATHER_STAGE1\n':raise LifecycleStop('LIFECYCLE_DATABASE_OWNER')
    with (data/'.writer.lock').open('rb') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        c=sqlite3.connect(data/'monitor.sqlite3',timeout=1,isolation_level=None)
        try:
            c.execute('pragma synchronous=FULL');check_database(c)
            yield c
        finally:c.close()


def finish_rotation(root,plan,state,write):
    path=root/ROTATION
    if not path.exists():return state
    intent=json.loads(path.read_text())
    with closed_database(plan) as c:
        row=c.execute('select value from meta where key=?',(intent['audit_key'],)).fetchone()
        if not row or sha(json.loads(row[0]))!=intent['audit_key'][len(AUDIT):]:raise LifecycleStop('LIFECYCLE_CONTROL_AUDIT_MISSING')
        bundle=json.loads(row[0])
        if bundle['binding']!=state['identity']:raise LifecycleStop('LIFECYCLE_CONTROL_AUDIT_BINDING')
        if sha(state) not in (intent['before_sha'],sha(intent['after'])):raise LifecycleStop('LIFECYCLE_ROTATION_STATE_UNKNOWN')
        if sha(state)==intent['before_sha']:write(root,'state.json',intent['after'])
        for name,raw in bundle['files'].items():
            f=root/name
            if f.exists():
                if f.is_symlink() or f.read_text()!=raw:raise LifecycleStop('LIFECYCLE_ROTATION_FILE_DRIFT')
                f.unlink()
        path.unlink()
        fd=os.open(root,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
    return intent['after']


def compact_control(root,plan,state,write,now):
    """Bound active files without deleting audit or resetting recovery budgets."""
    state=finish_rotation(root,plan,state,write)
    if state.get('active'):raise LifecycleStop('LIFECYCLE_ROTATION_ACTIVE_CHILD')
    retire=state['attempts'][:-KEEP_ATTEMPTS] if len(state['attempts'])>=ROTATE_ATTEMPTS else []
    consume=state.get('consume_stop_request')
    if not retire and not consume:return state
    files={}
    for entry in retire:
        if not entry.get('finished_ms'):raise LifecycleStop('LIFECYCLE_ROTATION_UNCLOSED_ATTEMPT')
        for suffix in ('.progress.json','.result.json','.owner.json'):
            f=root/(entry['nonce']+suffix)
            if f.exists():files[f.name]=f.read_text()
    if consume:files[STOP_REQUEST]=(root/STOP_REQUEST).read_text()
    before=sha(state);bundle=dict(version=1,binding=state['identity'],previous=state.get('audit_tip'),state=state,files=files)
    key=AUDIT+sha(bundle);after=dict(state,attempts=state['attempts'][len(retire):],
        archived_attempts=state.get('archived_attempts',0)+len(retire),audit_tip=key,
        restarts=[t for t in state['restarts'] if now-t<86400000])
    after.pop('consume_stop_request',None)
    after.pop('stop_request_controller',None)
    with closed_database(plan) as c:
        binding=c.execute("select value from meta where key='price_recovery_control_v1'").fetchone()
        if not binding or json.loads(binding[0])!=state['identity']:raise LifecycleStop('RECOVERY_CONTROL_BINDING')
        data=Path(plan.config.data_dir)
        if sum(p.stat().st_size for p in data.iterdir())>=plan.config.storage_bytes-8*1024*1024:raise LifecycleStop('RECOVERY_STORAGE_BOUND')
        c.execute('begin immediate')
        try:
            c.execute('insert or ignore into meta values(?,?)',(key,canonical(bundle)))
            if c.execute('select value from meta where key=?',(key,)).fetchone()[0]!=canonical(bundle):raise LifecycleStop('LIFECYCLE_CONTROL_AUDIT_DRIFT')
            c.execute('commit')
        except BaseException:
            if c.in_transaction:c.execute('rollback')
            raise
    # Only after a FULL SQLite commit may file/state retirement be attempted.
    write(root,ROTATION,dict(audit_key=key,before_sha=before,after=after))
    return finish_rotation(root,plan,state,write)
