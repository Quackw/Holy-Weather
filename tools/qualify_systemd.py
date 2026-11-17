"""Finite OFFLINE systemd qualification on a disposable GitHub runner only.

No public HTTP/WebSocket/Telegram. Uses a synthetic credential and database.
The actual service wrapper, recovery controller, inherited lock, unit and encrypted
credential are exercised; market collection alone is replaced by an offline child.
"""
from pathlib import Path
import json,os,signal,sqlite3,subprocess,sys,tempfile,time

PROBE='''from pathlib import Path
import json,os,signal,subprocess,sys,threading,time
from holy_weather import recovery as r,service
from holy_weather import lifecycle as l
from holy_weather.catalogue import CatalogueStore
if len(sys.argv)>1 and sys.argv[1]=='worker':
 root=Path(sys.argv[2]);nonce=sys.argv[3];fd=int(sys.argv[4])
 saved=json.loads((root/'plan.json').read_text());plan=r.Plan.load(saved['plan'])
 l.claim_child(root,nonce,fd,saved,r.write)
 stop=threading.Event();signal.signal(signal.SIGTERM,lambda *_:stop.set())
 token=sys.stdin.readline().strip();assert token.startswith('123456:')
 with CatalogueStore(plan.config,session=False) as s:
  r.prepare_resume(s,saved['identity'],time.time_ns()//1000000)
  s.set_meta('systemd_offline_probe',{'pid':os.getpid()})
  progress=r.Progress(root,nonce)
  progress.sender_status(dict(telegram_enabled=True,telegram_disabled_reason=None))
  while not stop.wait(.1):progress('OBSERVING')
 r.write(root,nonce+'.result.json',dict(nonce=nonce,pid=os.getpid(),state='SIGNAL_STOP',error=None))
else:
 original=service.supervise
 def spawn(root,nonce,token,fd):
  with (Path(__file__).parent/'offline-child.stderr').open('ab') as log:
   return subprocess.Popen([sys.executable,'-B',__file__,'worker',str(root),nonce,str(fd)],
    stdin=subprocess.PIPE,text=True,pass_fds=(fd,),stdout=subprocess.DEVNULL,stderr=log)
 service.supervise=lambda plan,root,**kw:original(plan,root,_spawn=spawn,**kw)
 raise SystemExit(service.main(sys.argv[1:]))
'''


def command(*args,input=None):
    return subprocess.run(args,input=input,text=True,capture_output=True,check=True,timeout=20).stdout.strip()


def await_state(path,predicate,seconds=55):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        if path.exists():
            value=json.loads(path.read_text())
            if predicate(value):return value
        time.sleep(.2)
    raise RuntimeError('OFFLINE_SYSTEMD_STATE_TIMEOUT')


def main():
    if os.geteuid()!=0 or os.environ.get('GITHUB_ACTIONS')!='true' or Path('/proc/1/comm').read_text().strip()!='systemd':
        raise SystemExit('Disposable GitHub runner with real systemd/root required')
    os.umask(0o077)  # Same private bootstrap as the actual service entry point.
    from dataclasses import replace
    from holy_weather.catalogue import CatalogueConfig,CatalogueStore
    from holy_weather.recovery import Plan
    base=Path(tempfile.mkdtemp(prefix='holy-weather-systemd-',dir=os.environ['RUNNER_TEMP']));base.chmod(0o700)
    root=Path(__file__).resolve().parents[1];unit_name='holy-weather-offline-'+str(os.getpid())+'.service'
    probe=base/'probe.py';probe.write_text(PROBE);probe.chmod(0o600)
    cfg=CatalogueConfig(data_dir=str(base/'data'),telegram_enabled=True,price_notification_filter=True)
    with CatalogueStore(cfg,session=False):pass
    plan=Plan(cfg,None,None,continuous=True)
    (base/'plan.json').write_text(json.dumps(plan.value()));(base/'plan.json').chmod(0o600)
    encrypted=base/'telegram.encrypted'
    command('/usr/bin/systemd-creds','encrypt','--with-key=host','--name=telegram','-',str(encrypted),input='123456:'+'offline_test_only_'*3)
    template=(root/'deploy/holy-weather-price@.service.in').read_text()
    unit=template.replace('@RELEASE@',str(base)).replace('@HISTORY_PATHS@','').replace('User=polymeteo-agent','User=root')
    unit=unit.replace('WorkingDirectory='+str(base),'WorkingDirectory='+str(root))
    unit=unit.replace(str(base)+'/venv/bin/python -B -m holy_weather.service',sys.executable+' -B '+str(probe))
    unit=unit.replace('/etc/holy-weather/%i.json',str(base/'plan.json')).replace('/var/lib/holy-weather/%i/control',str(base/'control'))
    unit=unit.replace('/var/lib/holy-weather/%i',str(base)).replace('RequiresMountsFor=/var/lib/holy-weather','RequiresMountsFor='+str(base))
    unit=unit.replace('/etc/credstore.encrypted/holy-weather.telegram',str(encrypted))
    unit=unit.replace('ProtectHome=yes','ProtectHome=no').replace('PrivateTmp=yes','PrivateTmp=no')
    unit=unit.replace('Environment=PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1',
                      'Environment=PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 PYTHONPATH='+str(root/'src'))
    unit_path=Path('/run/systemd/system')/unit_name;unit_path.write_text(unit)
    report=dict(reboot='NOT TESTED',external_network_calls=0,real_telegram_sends=0)
    try:
        command('/usr/bin/systemd-analyze','verify',str(unit_path));report['unit_verify']='PASS'
        command('/usr/bin/systemctl','daemon-reload');command('/usr/bin/systemctl','start',unit_name)
        path=base/'control/state.json'
        first=await_state(path,lambda s:s['status']=='RUNNING' and s.get('telegram_enabled') is True)
        parent=first['controller_process'];os.kill(parent['pid'],signal.SIGKILL)
        second=await_state(path,lambda s:s['status']=='RUNNING' and s['controller_process']!=parent)
        assert len(second['restarts'])==1 and second['attempts'][-1]['at_ms']>=second['restarts'][0]+30000
        report.update(controller_crash_recovery='PASS',persistent_backoff_ms=second['attempts'][-1]['at_ms']-second['restarts'][0])
        command('/usr/bin/systemctl','stop',unit_name)
        request=json.loads((base/'control/service-stop.json').read_text());assert request['reason']=='MANUAL_STOP'
        # start is intentionally refused, rather than silently undoing manual stop.
        subprocess.run(['/usr/bin/systemctl','start',unit_name],capture_output=True,timeout=20)
        end=time.monotonic()+10
        while time.monotonic()<end:
            status=command('/usr/bin/systemctl','show','-p','ExecMainStatus','--value',unit_name)
            if status=='78':break
            time.sleep(.2)
        assert status=='78';assert json.loads(path.read_text())['status']=='STOPPED'
        c=sqlite3.connect(base/'data/monitor.sqlite3');assert c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not c.execute('pragma foreign_key_check').fetchall();c.close()
        report.update(manual_stop_latch='PASS',encrypted_credential='PASS',sqlite='PASS',result='PASS')
    finally:
        # This disposable probe uses only a fixed dummy credential and no public
        # traffic. Retain bounded failure evidence instead of guessing a timeout.
        if report.get('result')!='PASS':
            for name in ('control/state.json','offline-child.stderr'):
                p=base/name
                if p.exists():print(name+': '+p.read_text()[-16000:])
            print(command('/usr/bin/journalctl','--no-pager','-u',unit_name,'-n','40','-o','cat'))
        subprocess.run(['/usr/bin/systemctl','stop',unit_name],capture_output=True,timeout=50)
        # Only the disposable runner unit, never user data/evidence, is retired.
        unit_path.unlink();subprocess.run(['/usr/bin/systemctl','daemon-reload'],capture_output=True,timeout=10)
        (base/'RESULT.json').write_text(json.dumps(report,sort_keys=True,indent=2))
    print(json.dumps(report,sort_keys=True))


if __name__=='__main__':main()
