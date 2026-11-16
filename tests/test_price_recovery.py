"""Finite supervision only. All subprocesses/data are isolated test fixtures."""
from dataclasses import replace
import io,json,os,signal,sqlite3,subprocess,sys,time,threading
from pathlib import Path
from unittest.mock import patch
import pytest
from holy_weather import recovery as r
from holy_weather.catalogue import CatalogueStore,observe_catalogue
from holy_weather.core import canonical
from holy_weather.alert_queue import preview,claim,finish
from test_catalogue import setup,feed,AT


def private_tree(path):
    for p in [path,*path.rglob('*')]:p.chmod(0o700 if p.is_dir() else 0o600)


def seed(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,1);cfg=replace(cfg,price_notification_filter=True)
    m,receipt=markets[0]
    with CatalogueStore(cfg) as s:s.bind(m,receipt)
    private_tree(Path(cfg.data_dir))
    return r.Plan(cfg,m['date'],AT+3600000,phase_seconds=2),m


class Sim:
    def __init__(self,states,*,hang=False,quiet=False,graceful=True,final_deadline=True):
        self.states=states;self.t=0.;self.calls=[];self.hang=hang;self.quiet=quiet;self.graceful=graceful;self.final_deadline=final_deadline
    def clock(self):return AT+int(self.t*1000)
    def mono(self):return self.t
    def wait(self,seconds):self.t+=seconds
    def spawn(self,root,nonce,token):
        sim=self;state=self.states[len(self.calls)];started=self.t
        class Child:
            pid=12345;stdin=io.StringIO();terminated=0;done=False
            def poll(self):
                if self.done:return 0
                if sim.hang:
                    if sim.quiet:r.write(root,nonce+'.progress.json',dict(nonce=nonce,pid=self.pid,stage='OBSERVING',monotonic=sim.t))
                    if not self.terminated or not sim.graceful:return None
                elif sim.t-started<1:return None
                r.write(root,nonce+'.result.json',dict(nonce=nonce,pid=self.pid,state=state,error=None))
                self.done=True
                if sim.final_deadline and len(sim.calls)==len(sim.states) and state=='COMPLETED_WINDOW':sim.t=4000
                return 0
            def terminate(self):self.terminated+=1
        child=Child();self.calls.append(child)
        r.write(root,nonce+'.progress.json',dict(nonce=nonce,pid=child.pid,stage='OBSERVING',monotonic=self.t))
        return child
    def run(self,plan,root):
        with patch.object(r,'preflight',return_value=None):
            return r.supervise(plan,root,clock=self.clock,monotonic=self.mono,wait=self.wait,_spawn=self.spawn)


def test_network_backoff_global_budget_and_reopen(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);sim=Sim(['RECONNECT_BUDGET_EXHAUSTED']*4)
    value=sim.run(plan,tmp_path/'control')
    assert value['status']=='RESTART_BUDGET_EXHAUSTED' and len(sim.calls)==4 and len(value['restarts'])==3
    assert [x['at_ms']-AT for x in value['attempts']]==[0,31000,92000,213000]
    again=Sim([]);again.t=sim.t
    assert again.run(plan,tmp_path/'control')['status']=='RESTART_BUDGET_EXHAUSTED'
    assert not again.calls


@pytest.mark.parametrize('outcome',['SIGNAL_STOP','FATAL','NETWORK_REFUSED_NO_RETRY','ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED','UNKNOWN_EXIT'])
def test_fatal_or_normal_stop_never_restarts(gamma,tmp_path,outcome):
    plan,m=seed(gamma,tmp_path);sim=Sim([outcome]);value=sim.run(plan,tmp_path/'control')
    assert value['status']==outcome and len(sim.calls)==1
    assert Sim([]).run(plan,tmp_path/'control')['status']==outcome


def test_wait_deadline_and_stop_never_extend(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);plan=replace(plan,deadline_ms=AT+2000)
    sim=Sim(['NETWORK_GAP_NO_RETRY']);value=sim.run(plan,tmp_path/'control')
    assert value['status']=='DEADLINE' and len(sim.calls)==1 and not value['restarts']
    stop=threading.Event();stop.set()
    value=r.supervise(plan,tmp_path/'stopped',stop=stop,clock=lambda:AT,_spawn=lambda *a:pytest.fail('stopped launch'))
    assert value['status']=='STOPPED'


def test_backoff_survives_controller_restart(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);sim=Sim(['NETWORK_GAP_NO_RETRY'],final_deadline=False)
    original=sim.wait
    def interrupted(seconds):
        if sim.t>1:raise RuntimeError('isolated controller interruption')
        original(seconds)
    sim.wait=interrupted
    with pytest.raises(RuntimeError):sim.run(plan,tmp_path/'control')
    before=json.loads((tmp_path/'control/state.json').read_text());assert before['status']=='BACKOFF'
    again=Sim(['COMPLETED_WINDOW']);again.t=2
    value=again.run(plan,tmp_path/'control')
    assert value['attempts'][1]['at_ms']==before['next_ms'] and len(value['restarts'])==1


def test_active_or_unknown_prior_launch_is_not_adopted(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);sim=Sim(['FATAL']);root=tmp_path/'control';sim.run(plan,root)
    v=json.loads((root/'state.json').read_text());v['active']='unresolved';r.write(root,'state.json',v)
    with pytest.raises(r.RecoveryStop,match='UNRESOLVED'):Sim([]).run(plan,root)


def test_hung_child_graceful_only_no_second_writer(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);sim=Sim(['SIGNAL_STOP'],hang=True,graceful=False)
    value=sim.run(plan,tmp_path/'control')
    assert value['status']=='UNRESPONSIVE_NO_KILL' and len(sim.calls)==1
    assert sim.calls[0].terminated==1 and value['active']


def test_quiet_market_progress_is_not_a_stall(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);plan=replace(plan,deadline_ms=AT+70000)
    sim=Sim(['SIGNAL_STOP'],hang=True,quiet=True)
    value=sim.run(plan,tmp_path/'control')
    assert value['status']=='DEADLINE' and sim.calls[0].terminated==1 and not value['restarts']


def test_graceful_stall_can_recover_after_exit(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);sim=Sim(['SIGNAL_STOP','COMPLETED_WINDOW'],hang=True)
    initial=sim.spawn
    def spawn(*args):
        child=initial(*args)
        if len(sim.calls)==2:sim.hang=False
        return child
    sim.spawn=spawn
    value=sim.run(plan,tmp_path/'control')
    assert value['status']=='DEADLINE' and len(sim.calls)==2 and len(value['restarts'])==1
    assert sim.calls[0].terminated==1


def test_real_database_preflight_is_readonly_and_refuses_active_writer(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);db=Path(plan.config.data_dir)/'monitor.sqlite3';before=db.read_bytes()
    assert r.preflight(plan) is None and db.read_bytes()==before
    with CatalogueStore(plan.config,session=False):
        with pytest.raises(BlockingIOError):r.preflight(plan)
    assert db.read_bytes()==before
    with pytest.raises(r.RecoveryStop,match='RULE_DRIFT'):r.preflight(replace(plan,config=replace(plan.config,source_clock_tolerance_ms=0)))


def test_resume_transaction_preserves_receipts_thresholds_and_lamp_terminal(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);identity={'control':'isolated'}
    with CatalogueStore(plan.config) as s:
        from holy_weather.catalogue import Router
        s.connection_started(AT);router=Router(s)
        feed(router,m,'0.49',AT+1);feed(router,m,'0.71',AT+2)
        key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p=preview(s,key);assert claim(s,p,AT+3);finish(s,p,{'status':'SENT','message_id':1},AT+4)
        s.set_meta('alert_delivery_v1:unknown',{'status':'UNKNOWN','track':'PRICE_ALERT','claimed_ms':AT+4})
        s.set_meta('alert_delivery_v1:pending',{'status':'PENDING','track':'PRICE_ALERT'})
        s.set_meta('alert_delivery_v1:lamp',{'status':'SENT','track':'LAMP_ALERT','message_id':99})
        old=dict(s.c.execute("select key,value from meta where key like 'price_alert_v1:%' or key in (?,?)",(key,'alert_delivery_v1:lamp')))
        assert r.prepare_resume(s,identity,AT+5)
        assert all(s.c.execute('select value from meta where key=?',(k,)).fetchone()[0]==v for k,v in old.items())
        assert json.loads(s.c.execute("select value from meta where key='alert_delivery_v1:pending'").fetchone()[0])['status']=='EXPIRED'
        assert json.loads(s.c.execute("select value from meta where key='alert_delivery_v1:unknown'").fetchone()[0])['status']=='UNKNOWN'
        assert r.prepare_resume(s,identity,AT+6)
        with pytest.raises(r.RecoveryStop,match='BINDING'):r.prepare_resume(s,{'different':True},AT+7)
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert s.c.execute('pragma user_version').fetchone()[0]==1


def test_resume_sql_failure_is_atomic(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path)
    with CatalogueStore(plan.config) as s:
        s.set_meta('alert_delivery_v1:p',{'status':'PENDING','track':'PRICE_ALERT'})
        before=list(s.c.execute('select key,value from meta order by key'))
        s.c.execute("CREATE TEMP TRIGGER fail BEFORE UPDATE ON meta BEGIN SELECT RAISE(ABORT,'injected'); END")
        with pytest.raises(sqlite3.IntegrityError):r.prepare_resume(s,{'control':'test'},AT)
        assert list(s.c.execute('select key,value from meta order by key'))==before


def test_code_plan_and_clock_drift_refuse(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);root=tmp_path/'control';sim=Sim(['FATAL']);sim.run(plan,root)
    with pytest.raises(r.RecoveryStop,match='IDENTITY_DRIFT'):Sim([]).run(replace(plan,phase_seconds=3),root)
    with patch.object(r,'code_identity',return_value='changed'):
        with pytest.raises(r.RecoveryStop,match='IDENTITY_DRIFT'):Sim([]).run(plan,root)


def test_archive_recovery_is_explicit_and_incomplete_part_blocks(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path)
    r.checkpoint(plan,lambda stage:pytest.fail('disabled archive'))
    archive=tmp_path/'archive';part=archive/'part-001';part.mkdir(parents=True);private_tree(archive)
    with pytest.raises(r.RecoveryStop,match='UNFINISHED_ARCHIVE'):r.preflight(replace(plan,archive_dir=str(archive)))


def test_recovery_control_budget_and_permissions(tmp_path):
    root=tmp_path/'control';root.mkdir(mode=0o700)
    r.write(root,'a.json',{'a':1});assert (root/'a.json').stat().st_mode&0o077==0
    with pytest.raises(r.RecoveryStop,match='BUDGET'):r.write(root,'b.json',{'b':'x'*r.CONTROL_BYTES})
    (root/'a.json').chmod(0o644)
    with pytest.raises(r.RecoveryStop,match='PRIVATE'):r.private(root/'a.json')


def test_daily_budget_is_separate_from_hourly_budget(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);plan=replace(plan,deadline_ms=AT+12*3600000)
    sim=Sim(['NETWORK_GAP_NO_RETRY']*7);original=sim.spawn
    def spawn(*args):
        child=original(*args);poll=child.poll
        def aged():
            was=child.done;answer=poll()
            if not was and child.done:sim.t+=3601
            return answer
        child.poll=aged;return child
    sim.spawn=spawn;value=sim.run(plan,tmp_path/'control')
    assert len(sim.calls)==7 and len(value['restarts'])==6 and value['status']=='RESTART_BUDGET_EXHAUSTED'


def test_crash_published_during_archive_is_not_retried(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);calls=[]
    def spawn(root,nonce,token):
        class Child:
            pid=12345;stdin=io.StringIO()
            def poll(self):return -signal.SIGKILL
        child=Child();calls.append(child)
        r.write(root,nonce+'.progress.json',dict(nonce=nonce,pid=child.pid,stage='ARCHIVE',monotonic=0.))
        return child
    with patch.object(r,'preflight',return_value=None):
        value=r.supervise(plan,tmp_path/'control',clock=lambda:AT,monotonic=lambda:0,_spawn=spawn)
    assert value['status']=='CRASH' and len(calls)==1 and not value['retry']


def test_controller_does_not_reuse_foreign_or_other_region_control(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);root=tmp_path/'foreign';root.mkdir(mode=0o700);(root/'keep').write_text('other project')
    with pytest.raises(r.RecoveryStop,match='UNOWNED_CONTROL'):Sim([]).run(plan,root)
    assert (root/'keep').read_text()=='other project'
    root=tmp_path/'control'
    with r.owned_control(root,plan.config.data_dir):
        with pytest.raises(BlockingIOError):
            with r.owned_control(root,plan.config.data_dir):pass
    with pytest.raises(r.RecoveryStop,match='OVERLAP'):Sim([]).run(plan,Path(plan.config.data_dir)/'controller')


def test_real_worker_disables_sender_after_unknown_and_preserves_price(gamma,tmp_path,monkeypatch):
    plan,m=seed(gamma,tmp_path);plan=replace(plan,config=replace(plan.config,telegram_enabled=True))
    with CatalogueStore(plan.config) as s:
        s.set_meta('alert_delivery_v1:uncertain',{'status':'UNKNOWN','track':'PRICE_ALERT','claimed_ms':AT})
    private_tree(Path(plan.config.data_dir));root=tmp_path/'control';root.mkdir(mode=0o700);nonce='a'*32;called=[]
    saved=dict(plan=plan.value(),identity={'control':str(root)},code=r.code_identity());r.write(root,'plan.json',saved)
    def observe(cfg,**kwargs):
        called.append(cfg);assert cfg.telegram_enabled is False and kwargs['token'] is None
        from holy_weather.catalogue import Router
        with CatalogueStore(cfg) as s:
            s.connection_started(AT+10);router=Router(s)
            feed(router,m,'0.49',AT+11);feed(router,m,'0.71',AT+12)
            assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
        return {'state':'COMPLETED_WINDOW'}
    monkeypatch.setattr(r.time,'time_ns',lambda:AT*1000000)
    monkeypatch.setattr(r.signal,'signal',lambda *a:None)
    monkeypatch.setattr(r,'observe_catalogue',observe)
    r.worker(root,nonce,'unused-in-memory')
    assert len(called)==1 and json.loads((root/(nonce+'.result.json')).read_text())['state']=='COMPLETED_WINDOW'
    with CatalogueStore(plan.config,session=False) as s:
        assert json.loads(s.c.execute("select value from meta where key='alert_delivery_v1:uncertain'").fetchone()[0])['status']=='UNKNOWN'


def test_observer_reports_main_loop_progress_with_no_market_frames(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path);ticks=[0.];stages=[]
    class WS:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,raw):pass
        def recv(self,timeout):ticks[0]+=.2;return 'PONG'
    receipt={'received_at':datetime_iso(AT)}
    result=observe_catalogue(replace(plan.config,duration_seconds=2),connect=lambda *a,**k:WS(),
        discover=lambda city:([m],receipt,[]) if city=='london' else ([],{},[]),clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0],progress=stages.append)
    assert result['transport']['snapshot']['frames']==0 and stages.count('OBSERVING')>5
    assert 'DISCOVERY' in stages and stages[-1]=='STOPPING'


def datetime_iso(at):
    from datetime import datetime,timezone
    return datetime.fromtimestamp(at/1000,timezone.utc).isoformat()


@pytest.mark.parametrize('region,city',[('europe','london'),('us','new-york')])
def test_region_identity_preflight_and_budget_are_separate(gamma,tmp_path,region,city):
    from holy_weather.catalogue import CatalogueConfig
    cfg=CatalogueConfig(region=region,city=city,data_dir=str(tmp_path/region))
    with CatalogueStore(cfg):pass
    private_tree(Path(cfg.data_dir));plan=r.Plan(cfg,'2026-11-14',AT+3600000)
    assert r.preflight(plan) is None
    result=Sim(['SIGNAL_STOP']).run(plan,tmp_path/('control-'+region))
    assert not result['restarts'] and result['status']=='SIGNAL_STOP'
    with pytest.raises(r.RecoveryStop,match='RULE_DRIFT'):
        other=replace(cfg,region='us' if region=='europe' else 'europe',city='new-york' if region=='europe' else 'london')
        r.preflight(replace(plan,config=other))


def test_worker_storage_failure_is_final_and_does_not_log_exception_payload(gamma,tmp_path,monkeypatch):
    plan,m=seed(gamma,tmp_path);root=tmp_path/'control';root.mkdir(mode=0o700);nonce='c'*32
    saved=dict(plan=plan.value(),identity={'control':str(root)},code=r.code_identity());r.write(root,'plan.json',saved)
    monkeypatch.setattr(r.time,'time_ns',lambda:AT*1000000);monkeypatch.setattr(r.signal,'signal',lambda *a:None)
    def fail(*a,**kw):raise r.StorageStop('STORAGE_BUDGET_CAPTURE_STOPPED')
    monkeypatch.setattr(r,'observe_catalogue',fail);r.worker(root,nonce,None)
    v=json.loads((root/(nonce+'.result.json')).read_text())
    assert v['state']=='FATAL' and v['error']=='StorageStop' and v['reason']=='STORAGE_BUDGET_CAPTURE_STOPPED'
    def secret(*a,**kw):raise OSError('synthetic-private-url-not-for-logs')
    monkeypatch.setattr(r,'observe_catalogue',secret);r.worker(root,'d'*32,None)
    raw=(root/('d'*32+'.result.json')).read_text();assert 'synthetic-private-url' not in raw
    assert json.loads(raw)['state']=='FATAL' and json.loads(raw)['reason'] is None


def test_plan_dates_and_fixed_limits_are_not_silently_extended(gamma,tmp_path):
    plan,m=seed(gamma,tmp_path)
    with pytest.raises(ValueError,match='SESSION_DEADLINE'):replace(plan,deadline_ms=plan.deadline_ms+3*86400000)
    with pytest.raises(ValueError,match='PLAN_BOUND'):replace(plan,phase_seconds=1201)
    with pytest.raises(ValueError,match='REGION_CITY'):replace(plan,config=replace(plan.config,region='us'))
    with pytest.raises(ValueError,match='CONFIG_BOUND'):replace(plan,config=replace(plan.config,quote_age_seconds=91))
    assert r.Plan.load(json.loads(canonical(plan.value())))==plan


def test_finite_old_day_does_not_consume_next_day_discovery(gamma,tmp_path):
    from holy_weather.catalogue import DayDiscovery,DAY_PREFIX
    plan,m=seed(gamma,tmp_path);clock=[AT];calls=[]
    def fetch(city):calls.append(city);return [],{},[]
    with CatalogueStore(plan.config,session=False) as s:
        days=DayDiscovery(s,fetch,lambda:clock[0],session_day=plan.session_day)
        days.refresh();calls.clear();before=dict(s.c.execute('select key,value from meta where key like ?',(DAY_PREFIX+'%',)))
        clock[0]+=86400000;days.refresh()
        assert not calls and dict(s.c.execute('select key,value from meta where key like ?',(DAY_PREFIX+'%',)))==before
        # A separately started next-day session still has its normal first try.
        DayDiscovery(s,fetch,lambda:clock[0]).refresh()
        assert calls and all(json.loads(raw)['attempts']==1 for key,raw in s.c.execute('select key,value from meta where key like ?',(DAY_PREFIX+'%',)))


def test_abrupt_sqlite_writer_crash_wal_resume_and_old_code_rollback(gamma,tmp_path,monkeypatch):
    plan,m=seed(gamma,tmp_path);plan=replace(plan,deadline_ms=AT+10000);root=tmp_path/'control'
    from holy_weather.catalogue import Router
    with CatalogueStore(plan.config) as s:
        s.connection_started(AT);router=Router(s)
        feed(router,m,'0.49',AT+1);feed(router,m,'0.71',AT+2)
        key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p=preview(s,key);assert claim(s,p,AT+3);finish(s,p,{'status':'SENT','message_id':7},AT+4)
        s.set_meta('alert_delivery_v1:uncertain',{'status':'UNKNOWN','track':'PRICE_ALERT','claimed_ms':AT+4})
    private_tree(Path(plan.config.data_dir));started=time.monotonic();calls=[]
    # The test child deliberately dies with an uncommitted SQLite transaction.
    # It runs no sockets or Telegram and never targets a live process.
    script='''
import json,os,signal,sys,time
from pathlib import Path
from holy_weather.recovery import Plan,Progress,prepare_resume,write
from holy_weather.catalogue import CatalogueStore,Router
from holy_weather.core import canonical
root=Path(sys.argv[1]);nonce=sys.argv[2];index=int(sys.argv[3]);sys.stdin.readline()
os.umask(0o077)
saved=json.loads((root/'plan.json').read_text());plan=Plan.load(saved['plan'])
at=1794657600000
with CatalogueStore(plan.config) as s:
 disabled=prepare_resume(s,saved['identity'],at+10);assert disabled
 Progress(root,nonce)('OBSERVING')
 if index==0:
  s.c.execute('begin immediate');s.c.execute("insert into meta values('UNCOMMITTED_TEST','true')")
  os.kill(os.getpid(),signal.SIGKILL)
 s.connection_started(at+20);m=next(iter(s.markets.values()));router=Router(s)
 for n,price in enumerate(['0.81','0.49','0.71','0.79','0.81']):
  router.feed(canonical(dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens']['YES'],timestamp=str(at+21+n),best_bid=price,best_ask=price)),at+21+n)
 assert not s.c.execute("select 1 from meta where key='UNCOMMITTED_TEST'").fetchone()
 assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==2
write(root,nonce+'.result.json',dict(nonce=nonce,pid=os.getpid(),state='ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED',error=None))
'''
    def spawn(root,nonce,token):
        index=len(calls);assert index<2
        p=subprocess.Popen([sys.executable,'-B','-c',script,str(root),nonce,str(index)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        calls.append(p);return p
    monkeypatch.setattr(r,'BACKOFF_SECONDS',.01)
    result=r.supervise(plan,root,clock=lambda:AT+int((time.monotonic()-started)*1000),_spawn=spawn)
    for p in calls:
        if p.returncode not in (0,-signal.SIGKILL):pytest.fail(p.stderr.read())
        p.stdout.close();p.stderr.close()
    assert len(calls)==2 and len(result['restarts'])==1
    assert result['status']=='ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED'
    wal=Path(plan.config.data_dir)/'monitor.sqlite3-wal'
    assert not wal.exists() or wal.stat().st_size==0
    # Base code from before this PR opens the current DB, retaining new records.
    source=tmp_path/'base';source.mkdir()
    ref='59d85f91bce51bb9be538369b96c5b0c39db51c9'
    for name in subprocess.check_output(['git','ls-tree','-r','--name-only',ref,'src']).decode().splitlines():
        if name.endswith('.py'):
            dest=source/Path(name).relative_to('src');dest.parent.mkdir(exist_ok=True,parents=True)
            dest.write_bytes(subprocess.check_output(['git','show',ref+':'+name]))
    check="import sys,json;sys.path.insert(0,sys.argv[1]);from holy_weather.catalogue import CatalogueStore,CatalogueConfig;cfg=CatalogueConfig(**json.loads(sys.argv[2]));s=CatalogueStore(cfg,session=False);assert s.c.execute(\"select count(*) from events where type='PRICE_ALERT'\").fetchone()[0]==2;receipts=[json.loads(x[0]) for x in s.c.execute(\"select value from meta where key like 'alert_delivery_v1:%'\")];assert sum(x['status']=='UNKNOWN' for x in receipts)==1;assert sum(x['status']=='SENT' for x in receipts)==1;assert s.c.execute(\"select sum(json_array_length(json_extract(value,'$.fired'))) from meta where key like 'price_alert_v1:%'\").fetchone()[0]==5;assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok';assert not s.c.execute('pragma foreign_key_check').fetchall();s.close()"
    subprocess.run([sys.executable,'-B','-c',check,str(source),json.dumps(r.asdict(plan.config))],check=True,capture_output=True,text=True)
