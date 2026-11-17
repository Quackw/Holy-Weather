"""Explicit systemd entry point. No installation, process discovery or signals.

Only the planned units call this. Existing finite runners keep stdin credentials.
No token environment/argument fallback; malformed state fails closed with exit 78.
"""
from pathlib import Path
import json,os,re,resource,signal,stat,subprocess,threading,time
from .core import canonical
from .recovery import Plan,RecoveryStop,private,supervise,write
from .lifecycle import STOP_REQUEST

FATAL_EXIT=78


def credential(directory):
    if not directory:raise RecoveryStop('SERVICE_CREDENTIAL_DIRECTORY_REQUIRED')
    root=Path(directory)
    if root.is_symlink() or not root.is_absolute():raise RecoveryStop('SERVICE_CREDENTIAL_PATH')
    # systemd credentials may be owned by root, readable only by the unit UID.
    path=root/'telegram'
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:
        info=os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_mode&0o007 or info.st_size>4096:
            raise RecoveryStop('SERVICE_CREDENTIAL_PERMISSIONS_OR_BOUND')
        value=os.read(fd,4097).decode().strip()
    finally:os.close(fd)
    if not re.fullmatch(r'\d+:[A-Za-z0-9_-]{20,}',value):raise RecoveryStop('SERVICE_CREDENTIAL_INVALID')
    return value


def record_stop(root,*,environment=None,manager_state=None):
    """ExecStop runs before systemd's cgroup SIGTERM. Never send signals here."""
    root=Path(root);private(root,True)
    path=root/'state.json'
    if not path.exists():return  # A refused start creates no permission to resume.
    state=json.loads(path.read_text());env=os.environ if environment is None else environment
    if manager_state is None:
        try:
            result=subprocess.run(['/usr/bin/systemctl','is-system-running'],capture_output=True,text=True,timeout=3)
            manager_state=result.stdout.strip()
        except (OSError,subprocess.TimeoutExpired):manager_state='unknown'
    if env.get('EXIT_CODE')=='exited' and env.get('EXIT_STATUS')==str(FATAL_EXIT):reason='FATAL_SERVICE_EXIT'
    elif manager_state=='stopping':reason='HOST_SHUTDOWN'
    elif env.get('SERVICE_RESULT') not in (None,'success'):reason='CONTROLLER_FAILURE'
    else:reason='MANUAL_STOP'  # Unknown stop context must not silently resume.
    request=dict(binding=state['identity'],controller=state.get('controller_process'),reason=reason,
                 at_ms=time.time_ns()//1000000,manager_state=manager_state,
                 service_result=env.get('SERVICE_RESULT'),exit_code=env.get('EXIT_CODE'),exit_status=env.get('EXIT_STATUS'))
    previous=root/STOP_REQUEST
    if previous.exists():
        old=json.loads(previous.read_text())
        if old.get('binding')!=request['binding'] or old.get('controller')!=request['controller']:
            raise RecoveryStop('SERVICE_STOP_INTENT_ALREADY_OWNED')
        # Failed start must not replace an earlier intentional stop with retry.
        return old
    write(root,STOP_REQUEST,request)
    return request


def run(plan_path,control):
    plan=Plan.load(json.loads(Path(plan_path).read_text()))
    if not plan.continuous or plan.deadline_ms is not None:raise RecoveryStop('SERVICE_CONTINUOUS_PLAN_REQUIRED')
    stop=threading.Event()
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:stop.set())
    token=credential(os.environ.get('CREDENTIALS_DIRECTORY')) if plan.config.telegram_enabled else None
    result=supervise(plan,control,token=token,stop=stop)
    # All abnormal/uncertain limits are terminal. systemd only recovers a killed
    # controller; worker retry/backoff and budgets are owned durably by HW.
    print(canonical(dict(status=result['status'],active=result.get('active'),
                         attempts=result.get('archived_attempts',0)+len(result['attempts']))))
    return 0 if result['status']=='STOPPED' else FATAL_EXIT


def main(argv=None):
    import argparse
    os.umask(0o077);resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    a=sub.add_parser('run');a.add_argument('--plan',required=True);a.add_argument('--control-dir',required=True)
    a=sub.add_parser('stop-intent');a.add_argument('--control-dir',required=True)
    args=p.parse_args(argv)
    try:
        if args.command=='stop-intent':record_stop(args.control_dir);return 0
        return run(args.plan,args.control_dir)
    except Exception as exc:
        # Never print HTTP exception text, credentials, stack locals or raw env.
        reason=str(exc) if re.fullmatch(r'[A-Z][A-Z0-9_:.+-]{0,96}',str(exc)) else type(exc).__name__
        print(canonical(dict(status='FATAL_SERVICE_EXIT',reason=reason)))
        return FATAL_EXIT


if __name__=='__main__':raise SystemExit(main())
