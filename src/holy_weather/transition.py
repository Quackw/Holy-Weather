"""Explicit closed-region rebinding, never an automatic rollback/installer.

Operator supplies exact old binding and pre-stop Linux process identities. The
current database is backed up and rebound; no snapshot can replace newer rows.
Use only in a separately authorized transition, under both ownership locks.
"""
from pathlib import Path
from contextlib import ExitStack
import ctypes,fcntl,json,os,re,shutil,sqlite3,time
from . import lifecycle
from .core import canonical,sha
from .history import check,digest,fingerprint,sync_dir,sync_file
from .recovery import Plan,RecoveryStop,code_identity,owned_control,preflight,private,write


def backup_record(root,name,value):
    # A full SQLite backup is intentionally outside the 1MiB *control* budget.
    # Bound its metadata separately; never count the database as a control file.
    body=canonical(value).encode()
    if len(body)>4*1024*1024:raise RecoveryStop('TRANSITION_BACKUP_METADATA_BOUND')
    with (root/name).open('xb') as f:
        os.chmod(root/name,0o600);f.write(body);f.flush();os.fsync(f.fileno())
    sync_dir(root)


def move_closed(source,destination,kind):
    """Linux atomic exchange: an old data/control path is NEVER absent.

    The original inode/lock moves to the stable path; a symlink tombstone makes
    old Store/private() refuse a new start, including during the exchange.
    Only for a separately authorized, closed-region filesystem migration.
    """
    source=Path(source);destination=Path(destination)
    if kind not in ('data','control','archive'):raise RecoveryStop('TRANSITION_MOVE_KIND')
    if not source.is_absolute() or not destination.is_absolute() or destination.exists() or destination.is_symlink():raise RecoveryStop('TRANSITION_MOVE_DESTINATION')
    private(source,True);private(destination.parent,True)
    if source==destination or source in destination.parents or destination in source.parents:raise RecoveryStop('TRANSITION_MOVE_OVERLAP')
    if source.stat().st_dev!=destination.parent.stat().st_dev:raise RecoveryStop('TRANSITION_MOVE_SAME_FILESYSTEM_REQUIRED')
    with ExitStack() as stack:
        if kind in ('data','control'):
            marker,content,lock=(('.holy-weather-owned','HOLY_WEATHER_STAGE1\n','.writer.lock') if kind=='data'
                                 else ('.price-recovery-owned','HOLY_WEATHER_PRICE_RECOVERY_V1\n','.controller.lock'))
            if (source/marker).read_text()!=content:raise RecoveryStop('TRANSITION_MOVE_OWNER')
            guard=stack.enter_context((source/lock).open('rb'));fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB)
        else:
            for part in source.glob('part-*'):
                m=json.loads((part/'MANIFEST.json').read_text())
                if not (part/'ADOPTED.json').exists() or digest(part/'history.sqlite3.gz')!=m['archive_sha256']:
                    raise RecoveryStop('TRANSITION_MOVE_ARCHIVE_INCOMPLETE')
        rename=getattr(ctypes.CDLL(None,use_errno=True),'renameat2',None)
        if rename is None:raise RecoveryStop('TRANSITION_MOVE_EXCHANGE_UNAVAILABLE')
        # A self-pointing placeholder occupies only the NEW path before swap.
        destination.symlink_to(destination,target_is_directory=True);sync_dir(destination.parent)
        result=rename(-100,os.fsencode(source),-100,os.fsencode(destination),2)  # AT_FDCWD, RENAME_EXCHANGE
        if result!=0:raise OSError(ctypes.get_errno(),'TRANSITION_MOVE_EXCHANGE_REFUSED')
        sync_dir(source.parent);sync_dir(destination.parent)
        if not source.is_symlink() or not destination.is_dir() or destination.is_symlink():raise RecoveryStop('TRANSITION_MOVE_UNKNOWN')
    return dict(source=str(source),destination=str(destination),kind=kind)


def rebind(plan,control,expected,processes,backup,*,at=None,target_code=None,resume_manual=False):
    """One attempt; any incomplete intent stops future supervision for review."""
    at=time.time_ns()//1000000 if at is None else at
    if target_code is not None and (plan.continuous or not re.fullmatch(r'[0-9a-f]{64}',target_code)):
        raise RecoveryStop('TRANSITION_FINITE_ROLLBACK_CODE_REQUIRED')
    if not processes:raise RecoveryStop('TRANSITION_PROCESS_PROOF_REQUIRED')
    for value in processes:
        if set(value)!={'boot','pid','start_ticks','uid','exe'} or value['uid']!=os.getuid():raise RecoveryStop('TRANSITION_PROCESS_PROOF_INVALID')
        if lifecycle.alive(value):raise RecoveryStop('TRANSITION_OLD_PROCESS_ALIVE')
    backup=Path(backup);data=Path(plan.config.data_dir)
    if backup.exists() or backup.is_symlink() or backup==data or data in backup.parents or backup in data.parents:
        raise RecoveryStop('TRANSITION_NEW_BACKUP_REQUIRED')
    if shutil.disk_usage(backup.parent).free<3*(data/'monitor.sqlite3').stat().st_size+2*1024**3:
        raise RecoveryStop('TRANSITION_FREE_SPACE')
    with owned_control(control,data) as root:
        if (root/'transition.json').exists():raise RecoveryStop('TRANSITION_PREVIOUS_INTENT')
        if preflight(plan)!=expected:raise RecoveryStop('TRANSITION_EXPECTED_BINDING')
        state=json.loads((root/'state.json').read_text())
        if state['identity']!=expected or state.get('active'):raise RecoveryStop('TRANSITION_CONTROL_NOT_CLOSED')
        saved=json.loads((root/'plan.json').read_text())
        if saved['identity']!=expected or sha(saved['plan'])!=expected['plan'] or saved['code']!=expected['code']:
            raise RecoveryStop('TRANSITION_OLD_PLAN_DRIFT')
        # Never carry an unfinished control rotation into a new binding.
        if (root/lifecycle.ROTATION).exists():raise RecoveryStop('TRANSITION_UNFINISHED_AUDIT')
        binding=dict(control=str(root),plan=sha(plan.value()),code=target_code or code_identity())
        if binding==expected and not resume_manual:raise RecoveryStop('TRANSITION_ALREADY_BOUND')
        if resume_manual:
            request=lifecycle.stop_request(root,state)
            if binding!=expected or state['status']!='STOPPED' or not lifecycle.clean_stop(state) or not request or request['reason']!='MANUAL_STOP':
                raise RecoveryStop('TRANSITION_CLEAN_MANUAL_STOP_REQUIRED')
        with lifecycle.closed_database(plan) as c:
            row=c.execute("select value from meta where key='price_recovery_control_v1'").fetchone()
            if not row or json.loads(row[0])!=expected:raise RecoveryStop('TRANSITION_BINDING_RACE')
            # All original control bytes and their ownership evidence remain in
            # this private backup as well as a transactional audit in schema v1.
            files={p.name:p.read_text() for p in root.iterdir() if p.name not in ('.controller.lock',)}
            before=fingerprint(c);backup.mkdir(mode=0o700)
            b=sqlite3.connect(backup/'monitor.sqlite3')
            try:
                c.backup(b);check(b)
                if fingerprint(b)!=before:raise RecoveryStop('TRANSITION_BACKUP_FINGERPRINT')
            finally:b.close()
            (backup/'monitor.sqlite3').chmod(0o600)
            record=dict(version=1,at_ms=at,source=expected,target=binding,processes=processes,files=files,
                        source_fingerprint=before,backup_sha256=digest(backup/'monitor.sqlite3'))
            backup_record(backup,'MANIFEST.json',record)
            sync_file(backup/'monitor.sqlite3');sync_dir(backup);sync_dir(backup.parent)
            key='price_transition_v1:'+sha(record)
            # Intent precedes the one SQLite commit. A partial transition is
            # refused, not silently retried, by every continuous supervisor.
            write(root,'transition.json',dict(audit_key=key,source=expected,target=binding,backup=str(backup)))
            c.execute('begin immediate')
            try:
                c.execute('insert into meta values(?,?)',(key,canonical(record)))
                c.execute("update meta set value=? where key='price_recovery_control_v1'",(canonical(binding),))
                c.execute('commit')
            except BaseException:
                if c.in_transaction:c.execute('rollback')
                raise
            # Archive old control files into the committed audit; only their
            # exact redundant copies may retire. There is no market-data prune.
            new=dict(identity=binding,attempts=[],archived_attempts=state.get('archived_attempts',0)+len(state['attempts']),
                     restarts=state['restarts'],last_ms=at,active=None,status='PREPARING',transition_ref=key)
            # Unexpired recovery debt survives even an explicit code transition.
            if state.get('retry') or state.get('recovery_charged'):
                new.update(retry=True,**{k:state[k] for k in ('recovery_charged','next_ms') if k in state})
            write(root,'plan.json',dict(plan=plan.value(),code=binding['code'],identity=binding))
            write(root,'state.json',new)
            for name,raw in files.items():
                if name in ('state.json','plan.json','.price-recovery-owned'):continue
                p=root/name
                if p.exists():
                    if p.is_symlink() or p.read_text()!=raw:raise RecoveryStop('TRANSITION_CONTROL_DRIFT')
                    p.unlink()
            backup_record(backup,'COMPLETED.json',dict(audit_key=key,target=binding))
            (root/'transition.json').unlink();sync_dir(root)
            check(c)
            return dict(binding=binding,backup=str(backup),backup_sha256=record['backup_sha256'],audit_key=key)


def main():
    import argparse
    os.umask(0o077);p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    a=sub.add_parser('move');a.add_argument('--source',required=True);a.add_argument('--destination',required=True);a.add_argument('--kind',required=True,choices=('data','control','archive'))
    a=sub.add_parser('rebind')
    for name in ('plan','control-dir','expected','process-proof','backup'):a.add_argument('--'+name,required=True)
    a.add_argument('--target-code',help='Separately verified old code identity; finite rollback only')
    a.add_argument('--resume-manual-stop',action='store_true',help='Explicit same-binding clean manual stop acknowledgement; never a fatal reset')
    a=p.parse_args()
    if a.command=='move':result=move_closed(a.source,a.destination,a.kind)
    else:result=rebind(Plan.load(json.loads(Path(a.plan).read_text())),a.control_dir,
                  json.loads(Path(a.expected).read_text()),json.loads(Path(a.process_proof).read_text()),a.backup,target_code=a.target_code,resume_manual=a.resume_manual_stop)
    print(canonical(result))


if __name__=='__main__':main()
