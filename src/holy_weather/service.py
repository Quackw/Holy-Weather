"""Explicit systemd entry point. No installation, process discovery or signals.

Only the planned units call this. Existing finite runners keep stdin credentials.
No token environment/argument fallback; malformed state fails closed with exit 78.
"""
from pathlib import Path
from datetime import datetime,timezone
import json,os,re,resource,signal,socket,stat,subprocess,threading,time
from .core import canonical
from .recovery import Plan,RecoveryStop,private,supervise,write
from .lifecycle import STOP_REQUEST

FATAL_EXIT=78


class Health:
    """Main-loop notifications only: a timer thread must never hide a deadlock.

    STATUS reports controller/sender state, never quote readiness or delivery ACK.
    No database writes, Telegram sends, retry decisions or credentials here.
    """
    def __init__(self,*,environment=None,monotonic=time.monotonic,notify=None):
        env=os.environ if environment is None else environment
        self.pid=os.getpid();self.thread=threading.get_ident();self.monotonic=monotonic;self.previous=None
        self.next_ping=-float('inf');self.notify_failed=False
        self.address=env.get('NOTIFY_SOCKET');usec=env.get('WATCHDOG_USEC')
        self.armed=False;self.interval=10.
        if usec and env.get('WATCHDOG_PID',str(self.pid))==str(self.pid):
            if not usec.isdecimal() or not 0<int(usec)<=360000000:
                raise RecoveryStop('SERVICE_WATCHDOG_BOUND')
            if not self.address or not self.address.startswith(('/','@')) or '\x00' in self.address:
                raise RecoveryStop('SERVICE_NOTIFY_SOCKET_REQUIRED')
            self.armed=True;self.interval=min(10.,int(usec)/3000000)
        self.notify=notify or self._notify

    def _notify(self,message):
        address='\x00'+self.address[1:] if self.address.startswith('@') else self.address
        with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as s:
            s.setblocking(False);s.sendto(message.encode(),address)

    def __call__(self,value):
        if os.getpid()!=self.pid or threading.get_ident()!=self.thread:raise RecoveryStop('SERVICE_HEALTH_OWNER')
        def safe(v):return v if isinstance(v,str) and re.fullmatch('[A-Z][A-Z0-9_]{0,80}',v) else 'UNREPORTED'
        enabled=value.get('telegram_enabled');reason=value.get('telegram_disabled_reason')
        sender='ENABLED' if enabled is True else 'UNKNOWN' if enabled is not False else (
            'OFF' if reason in ('NOT_ENABLED_IN_PLAN','NOT_ENABLED_IN_CONFIG') else 'DISABLED')
        status=dict(controller=safe(value.get('status')),telegram=sender,
                    reason=safe(reason) if reason is not None else None,watchdog='ARMED' if self.armed else 'OFF')
        if value.get('collection_paused') is True:
            at=value.get('next_attempt_ms');quota=value.get('quota_available_ms')
            valid=lambda t:type(t) is int and 0<=t<253402300800000
            status.update(collection_paused=True,recovery_reason=safe(value.get('recovery_reason')),
                          next_attempt_ms=at if valid(at) else None,
                          quota_available_ms=quota if valid(quota) else None)
        changed=status!=self.previous
        if changed:
            print(canonical(dict(event='PRICE_SERVICE_HEALTH',**status)),flush=True)
            self.previous=status
        if not self.armed:return
        now=self.monotonic()
        if not changed and now<self.next_ping:return
        self.next_ping=now+self.interval
        message='WATCHDOG=1\nSTATUS=PRICE controller='+status['controller']+'; Telegram='+sender
        if status['reason']:message+='; reason='+status['reason']
        if status.get('collection_paused'):
            at=status['next_attempt_ms']
            message+='; collection=PAUSED; retry_at='+(
                datetime.fromtimestamp(at/1000,timezone.utc).isoformat(timespec='seconds') if at is not None else 'UNKNOWN')
            message+='; recovery_reason='+status['recovery_reason']
        try:self.notify(message)
        except OSError:
            if not self.notify_failed:print(canonical(dict(event='PRICE_SERVICE_NOTIFY_UNAVAILABLE')),flush=True)
            self.notify_failed=True  # systemd still times out; no helper feeds it.
        else:
            if self.notify_failed:print(canonical(dict(event='PRICE_SERVICE_NOTIFY_RECOVERED')),flush=True)
            self.notify_failed=False


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
    health=Health();health(dict(status='INITIALIZING'))
    token=credential(os.environ.get('CREDENTIALS_DIRECTORY')) if plan.config.telegram_enabled else None
    result=supervise(plan,control,token=token,stop=stop,on_health=health)
    health(result)
    # All abnormal/uncertain limits are terminal. systemd only recovers a killed
    # controller; worker retry/backoff and budgets are owned durably by HW.
    print(canonical(dict(status=result['status'],active=result.get('active'),
                         attempts=result.get('archived_attempts',0)+len(result['attempts']),
                         **{k:result[k] for k in ('telegram_enabled','telegram_disabled_reason') if k in result})))
    # A newly requested graceful stop succeeds. Reopening an already latched
    # stop is an explicit refusal, not a successful start with no collector.
    return 0 if result['status']=='STOPPED' and stop.is_set() else FATAL_EXIT


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
