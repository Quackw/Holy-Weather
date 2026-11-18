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
from holy_weather.catalogue import CatalogueStore,Router
from holy_weather.alert_queue import preview,claim
from test_catalogue import AT,feed
if len(sys.argv)>1 and sys.argv[1]=='worker':
 root=Path(sys.argv[2]);nonce=sys.argv[3];fd=int(sys.argv[4])
 saved=json.loads((root/'plan.json').read_text());plan=r.Plan.load(saved['plan'])
 l.claim_child(root,nonce,fd,saved,r.write)
 stop=threading.Event();signal.signal(signal.SIGTERM,lambda *_:stop.set())
 token=sys.stdin.readline().strip();assert token.startswith('123456:')
 progress=r.Progress(root,nonce)
 r.checkpoint(plan,progress)
 with CatalogueStore(plan.config,session=False) as s:
  disabled=r.prepare_resume(s,saved['identity'],time.time_ns()//1000000)
  s.set_meta('systemd_offline_probe',{'pid':os.getpid()})
  progress.sender_status(dict(telegram_enabled=not disabled,telegram_disabled_reason='PREVIOUS_SEND_UNCONFIRMED' if disabled else None))
  base=root.parent;ticks=0
  while not stop.wait(.1):
   progress('OBSERVING');ticks+=1
   if ticks%10==0:s.set_meta('systemd_offline_ticks',{'pid':os.getpid(),'ticks':ticks})
   if (base/'inject-unknown').exists() and not (base/'unknown-created').exists():
    # Synthetic, real producer/queue path; deliberately no transport/ACK.
    s.connection_started(AT+10);router=Router(s);m=next(iter(s.markets.values()))
    feed(router,m,'0.79',AT+11);feed(router,m,'0.81',AT+12)
    key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%' and json_extract(value,'$.status')='PENDING'").fetchone()[0]
    assert claim(s,preview(s,key),AT+13)
    feed(router,m,'0.49',AT+14,'NO');feed(router,m,'0.51',AT+15,'NO')
    (base/'unknown-created').touch()
    progress.sender_status(dict(telegram_enabled=False,telegram_disabled_reason='SEND_RESULT_UNKNOWN'))
 r.write(root,nonce+'.result.json',dict(nonce=nonce,pid=os.getpid(),state='SIGNAL_STOP',error=None))
else:
 original=service.supervise
 def spawn(root,nonce,token,fd):
  with (Path(__file__).parent/'offline-child.stderr').open('ab') as log:
   env={k:v for k,v in os.environ.items() if k not in ('NOTIFY_SOCKET','WATCHDOG_USEC','WATCHDOG_PID')}
   return subprocess.Popen([sys.executable,'-B',__file__,'worker',str(root),nonce,str(fd)],
    stdin=subprocess.PIPE,text=True,pass_fds=(fd,),stdout=subprocess.DEVNULL,stderr=log,env=env)
 def supervise(plan,root,**kw):
  notify=kw['on_health'];base=Path(root).parent
  def health(value):
   notify(value)
   if value['status']=='RUNNING' and (base/'block-controller').exists() and not (base/'hung-controller.json').exists():
    (base/'hung-controller.json').write_text(json.dumps(l.own_identity()))
    threading.Event().wait()  # Main loop hangs; live child cannot feed its watchdog.
  kw['on_health']=health
  return original(plan,root,_spawn=spawn,**kw)
 service.supervise=supervise
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


def snapshot(data):
    with sqlite3.connect((data/'monitor.sqlite3').as_uri()+'?mode=ro',uri=True) as c:
        c.execute('pragma query_only=ON');c.execute('begin')
        return dict(events=list(c.execute('select * from events order by id')),
                    price_events=c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0],
                    fired=list(c.execute("select key,value from meta where key like 'price_alert_v1:%' order by key")),
                    delivery=dict(c.execute("select key,value from meta where key like 'alert_delivery_v1:%' order by key")))


def observing(control,state):
    if state['status']!='RUNNING' or not state.get('active'):return False
    path=control/(state['active']+'.progress.json')
    if not path.exists():return False
    value=json.loads(path.read_text())
    return value['nonce']==state['active'] and value['pid']==state['attempts'][-1]['pid'] and value['stage']=='OBSERVING'


def assert_exclusive(cfg,control):
    from holy_weather.catalogue import CatalogueStore
    from holy_weather.storage import StorageStop
    from holy_weather.recovery import owned_control
    try:
        with CatalogueStore(cfg,session=False):pass
    except StorageStop as exc:
        if str(exc)!='WRITER_ALREADY_RUNNING':raise
    else:raise AssertionError('SECOND_WRITER_ADMITTED')
    try:
        with owned_control(control,cfg.data_dir):pass
    except BlockingIOError:pass
    else:raise AssertionError('SECOND_CONTROLLER_ADMITTED')


def main():
    if os.geteuid()!=0 or os.environ.get('GITHUB_ACTIONS')!='true' or Path('/proc/1/comm').read_text().strip()!='systemd':
        raise SystemExit('Disposable GitHub runner with real systemd/root required')
    os.umask(0o077)  # Same private bootstrap as the actual service entry point.
    from dataclasses import replace
    from holy_weather.catalogue import CatalogueStore,Router
    from holy_weather.recovery import Plan
    from holy_weather.history_files import HistoryStorage
    base=Path(tempfile.mkdtemp(prefix='holy-weather-systemd-',dir=os.environ['RUNNER_TEMP']));base.chmod(0o700)
    root=Path(__file__).resolve().parents[1];unit_name='holy-weather-offline-'+str(os.getpid())+'.service'
    probe=base/'probe.py';probe.write_text(PROBE);probe.chmod(0o600)
    # Reuse the same reviewed synthetic market/producer fixture as pytest.
    sys.path.insert(0,str(root/'tests'))
    from conftest import gamma
    from test_price_recovery import seed
    from test_catalogue import AT,feed
    from holy_weather.alert_queue import preview,claim,finish
    finite,m=seed(gamma.__wrapped__(),base)
    cfg=replace(finite.config,telegram_enabled=True);data=Path(cfg.data_dir)
    with CatalogueStore(cfg,session=False) as s:
        s.connection_started(AT);router=Router(s)
        feed(router,m,'0.49',AT+1);feed(router,m,'0.71',AT+2)
        key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        card=preview(s,key);assert claim(s,card,AT+3)
        finish(s,card,dict(status='SENT',message_id=701),AT+4)  # Synthetic receipt only.
    plan=Plan(cfg,None,None,continuous=True,archive_dir=str(base/'archive'),
              history_storage=HistoryStorage(str(base/'absent-history-disk')))
    (base/'plan.json').write_text(json.dumps(plan.value()));(base/'plan.json').chmod(0o600)
    encrypted=base/'telegram.encrypted'
    command('/usr/bin/systemd-creds','encrypt','--with-key=host','--name=telegram','-',str(encrypted),input='123456:'+'offline_test_only_'*3)
    template=(root/'deploy/holy-weather-price@.service.in').read_text()
    unit=template.replace('@RELEASE@',str(base)).replace('@HISTORY_PATHS@','ReadWritePaths=-'+str(base/'absent-history-disk')).replace('User=polymeteo-agent','User=root')
    unit=unit.replace('WorkingDirectory='+str(base),'WorkingDirectory='+str(root))
    unit=unit.replace(str(base)+'/venv/bin/python -B -m holy_weather.service',sys.executable+' -B '+str(probe))
    unit=unit.replace('/etc/holy-weather/%i.json',str(base/'plan.json')).replace('/var/lib/holy-weather/%i/control',str(base/'control'))
    unit=unit.replace('/var/lib/holy-weather/%i',str(base)).replace('RequiresMountsFor=/var/lib/holy-weather','RequiresMountsFor='+str(base))
    unit=unit.replace('/etc/credstore.encrypted/holy-weather.telegram',str(encrypted))
    unit=unit.replace('ProtectHome=yes','ProtectHome=no').replace('PrivateTmp=yes','PrivateTmp=no')
    unit=unit.replace('WatchdogSec=360s','WatchdogSec=5s')  # Test clock only; production stays 360s.
    unit=unit.replace('Environment=PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1',
                      'Environment=PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 PYTHONPATH='+str(root/'src')+':'+str(root/'tests'))
    unit_path=Path('/run/systemd/system')/unit_name;unit_path.write_text(unit)
    report=dict(reboot='NOT TESTED',external_network_calls=0,real_telegram_sends=0,
                production_watchdog_seconds=360,test_watchdog_seconds=5)
    try:
        command('/usr/bin/systemd-analyze','verify',str(unit_path));report['unit_verify']='PASS'
        command('/usr/bin/systemctl','daemon-reload');command('/usr/bin/systemctl','start',unit_name)
        path=base/'control/state.json'
        first=await_state(path,lambda s:observing(base/'control',s) and s.get('telegram_enabled') is True
                          and s.get('storage',{}).get('status')=='SPOOLING')
        report['history_disk_absent_primary_collection']='PASS'
        # Longer than the test watchdog: quiet market is not a controller stall.
        time.sleep(6)
        assert command('/usr/bin/systemctl','is-active',unit_name)=='active'
        (base/'inject-unknown').touch()
        await_state(path,lambda s:s.get('telegram_disabled_reason')=='SEND_RESULT_UNKNOWN')
        end=time.monotonic()+5
        while time.monotonic()<end:
            status=command('/usr/bin/systemctl','show','-p','StatusText','--value',unit_name)
            if 'Telegram=DISABLED' in status and 'SEND_RESULT_UNKNOWN' in status:break
            time.sleep(.1)
        assert 'Telegram=DISABLED' in status and 'SEND_RESULT_UNKNOWN' in status
        assert command('/usr/bin/systemctl','is-active',unit_name)=='active'
        report.update(quiet_controller='PASS',disabled_sender_visible='PASS')
        before=snapshot(data)
        assert before['price_events']==3
        terminal={k:v for k,v in before['delivery'].items() if json.loads(v)['status'] in ('SENT','UNKNOWN')}
        assert len(terminal)==2 and sum(json.loads(v)['status']=='UNKNOWN' for v in terminal.values())==1
        # Both independent locks reject another writer/controller while this child lives.
        assert_exclusive(cfg,base/'control')
        from holy_weather.lifecycle import process_identity,alive
        old_writer=process_identity(first['attempts'][-1]['pid']);assert old_writer
        parent=first['controller_process'];(base/'block-controller').touch()
        await_state(base/'hung-controller.json',lambda s:s['pid']==parent['pid'],seconds=5)
        second=await_state(path,lambda s:observing(base/'control',s) and s['controller_process']!=parent
                           and s.get('telegram_disabled_reason')=='PREVIOUS_SEND_UNCONFIRMED',seconds=85)
        assert not alive(old_writer)
        assert_exclusive(cfg,base/'control')
        assert len(second['restarts'])==1 and second['attempts'][-1]['at_ms']>=second['restarts'][0]+30000
        journal=command('/usr/bin/journalctl','--no-pager','-u',unit_name,'-o','cat')
        assert 'Watchdog timeout' in journal
        assert second['telegram_enabled'] is False
        assert second['telegram_disabled_reason']=='PREVIOUS_SEND_UNCONFIRMED'
        after=snapshot(data)
        assert before['events']==after['events'] and before['fired']==after['fired']
        assert before['delivery'].keys()==after['delivery'].keys()
        assert all(after['delivery'][k]==v for k,v in terminal.items())
        pending,=[k for k,v in before['delivery'].items() if json.loads(v)['status']=='PENDING']
        expired=json.loads(after['delivery'][pending])
        assert expired['status']=='EXPIRED' and expired['reason']=='NO_BACKLOG_ON_RECOVERY'
        assert expired.get('claimed_ms') is None
        assert second['storage']['status']=='SPOOLING'
        report.update(controller_watchdog_recovery='PASS',no_second_writer='PASS',
                      receipts_and_thresholds='PASS',no_duplicate_claims='PASS',
                      watchdog_backoff_ms=second['attempts'][-1]['at_ms']-second['restarts'][0])
        # Keep the earlier abrupt-controller-crash qualification too.
        parent=second['controller_process'];os.kill(parent['pid'],signal.SIGKILL)
        second=await_state(path,lambda s:observing(base/'control',s) and s['controller_process']!=parent,seconds=85)
        assert len(second['restarts'])==2 and second['attempts'][-1]['at_ms']>=second['restarts'][-1]+60000
        final=snapshot(data)
        assert final==after
        report.update(controller_crash_recovery='PASS',persistent_backoff_ms=second['attempts'][-1]['at_ms']-second['restarts'][-1])
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
        c=sqlite3.connect(data/'monitor.sqlite3');assert c.execute('pragma integrity_check').fetchone()[0]=='ok'
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
