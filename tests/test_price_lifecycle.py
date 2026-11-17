"""Offline lifecycle boundaries. Linux /proc tests never touch a real observer."""
from dataclasses import replace
from pathlib import Path
import io,json,os,signal,sqlite3,subprocess,sys,time
from unittest.mock import patch
import pytest
from holy_weather import lifecycle as l,recovery as r
from holy_weather.core import canonical,sha
from holy_weather.catalogue import CatalogueStore
from test_price_recovery import seed,Sim
from test_catalogue import AT

OWNER=dict(boot='synthetic-boot',pid=100,start_ticks=20,uid=os.getuid(),exe='/synthetic/python')


def rolling(plan):return replace(plan,session_day=None,continuous=True)


class ContinuousSim(Sim):
    def spawn(self,root,nonce,token,fd):
        l.validate_guard(root,fd)
        state=json.loads((root/'state.json').read_text())
        cfg=r.Plan.load(json.loads((root/'plan.json').read_text())['plan']).config
        with CatalogueStore(cfg,session=False) as s:r.prepare_resume(s,state['identity'],self.clock())
        child=super().spawn(root,nonce,token)
        r.write(root,nonce+'.owner.json',dict(protocol=1,nonce=nonce,binding=state['identity'],
            controller=state['controller_process'],process=dict(OWNER,pid=child.pid)))
        return child


@pytest.fixture
def identity(monkeypatch):
    monkeypatch.setattr(l,'own_identity',lambda:dict(OWNER))
    monkeypatch.setattr(l,'alive',lambda value:False)


def test_finite_plan_bytes_unchanged_and_rolling_explicit(gamma,tmp_path):
    plan,_=seed(gamma,tmp_path);old=plan.value()
    assert 'continuous' not in old and r.Plan.load(old).value()==old
    new=rolling(plan);assert new.value()['continuous'] and new.session_day is None
    assert replace(new,deadline_ms=None).deadline_ms is None
    with pytest.raises(ValueError,match='ROLLING_DATE'):replace(plan,continuous=True)
    with pytest.raises(ValueError):replace(plan,deadline_ms=None)


def test_more_than_160_windows_bounded_control_preserves_audit(gamma,tmp_path,identity):
    plan,_=seed(gamma,tmp_path);root=tmp_path/'control';sim=ContinuousSim(['COMPLETED_WINDOW']*200)
    state=sim.run(rolling(plan),root)
    assert len(sim.calls)==200 and state['status']=='DEADLINE' and not state['restarts']
    assert state['archived_attempts']+len(state['attempts'])==200
    assert len(state['attempts'])<l.ROTATE_ATTEMPTS
    assert sum(p.stat().st_size for p in root.iterdir())<r.CONTROL_BYTES
    c=sqlite3.connect(Path(plan.config.data_dir)/'monitor.sqlite3')
    bundles={k:json.loads(v) for k,v in c.execute('select key,value from meta where key like ?',(l.AUDIT+'%',))}
    key=state['audit_tip'];retired=[]
    while key:
        b=bundles[key];assert l.AUDIT+sha(b)==key
        retired+=b['state']['attempts'][:-l.KEEP_ATTEMPTS];key=b['previous']
    assert len({a['nonce'] for a in retired})==state['archived_attempts']
    assert c.execute('pragma integrity_check').fetchone()[0]=='ok'
    assert not c.execute('pragma foreign_key_check').fetchall();c.close()


def prepared(tmp_path,plan,*,active=True,status='RUNNING'):
    root=tmp_path/'control'
    with r.owned_control(root,plan.config.data_dir):pass
    binding=dict(control=str(root),plan=sha(plan.value()),code=r.code_identity())
    state=dict(identity=binding,attempts=[],restarts=[],last_ms=AT,active=None,status=status,
               controller_process=dict(OWNER,pid=99))
    if active:
        state['active']='a'*32
        state['attempts']=[dict(nonce=state['active'],at_ms=AT,guard_protocol=1)]
    r.write(root,'state.json',state)
    with CatalogueStore(plan.config,session=False) as s:r.prepare_resume(s,binding,AT)
    return root,state


@pytest.mark.parametrize('boundary',['intent','owner','result','new-boot','pid-reused'])
def test_reconcile_closed_launch_boundaries_and_pid_reuse(gamma,tmp_path,identity,boundary):
    plan,_=seed(gamma,tmp_path);plan=rolling(plan);root,state=prepared(tmp_path,plan)
    nonce=state['active']
    if boundary!='intent':
        process=dict(OWNER,pid=123,start_ticks=1)
        r.write(root,nonce+'.owner.json',dict(protocol=1,nonce=nonce,binding=state['identity'],controller=state['controller_process'],process=process))
    if boundary=='result':r.write(root,nonce+'.result.json',dict(nonce=nonce,pid=123,state='COMPLETED_WINDOW',error=None))
    if boundary=='new-boot':
        state['controller_process']['boot']='old-boot'
        owner=json.loads((root/(nonce+'.owner.json')).read_text());owner['controller']=state['controller_process'];r.write(root,nonce+'.owner.json',owner)
    if boundary=='pid-reused':
        with patch.object(l,'process_identity',return_value=dict(OWNER,pid=123,start_ticks=999)):
            assert not l.alive(process)  # Identity differs although PID is equal.
    assert l.reconcile(root,state,AT+1,r.write,lambda:r.preflight(plan))
    assert state['active'] is None and state['retry'] and len(state['restarts'])==1
    assert state['next_ms']==AT+30001 and state['attempts'][0]['returncode'] is None


def test_live_old_child_and_old_protocol_refuse(gamma,tmp_path,identity):
    plan,_=seed(gamma,tmp_path);root,state=prepared(tmp_path,rolling(plan));nonce=state['active']
    r.write(root,nonce+'.owner.json',dict(protocol=1,nonce=nonce,binding=state['identity'],controller=state['controller_process'],process=dict(OWNER,pid=123)))
    with patch.object(l,'alive',side_effect=lambda v:v['pid']==123):
        with pytest.raises(l.LifecycleStop,match='OLD_CHILD_ALIVE'):l.reconcile(root,state,AT+1,r.write,lambda:pytest.fail('writer admission'))
    state['attempts'][0].pop('guard_protocol')
    with pytest.raises(l.LifecycleStop,match='UNRESOLVED'):l.reconcile(root,state,AT+1,r.write,lambda:None)


@pytest.mark.parametrize('status',['FATAL','UNKNOWN_EXIT','UNRESPONSIVE_NO_KILL','RESTART_BUDGET_EXHAUSTED','NETWORK_REFUSED_NO_RETRY','CRASH','STOPPED'])
def test_terminal_state_is_never_bypassed(gamma,tmp_path,identity,status):
    plan,_=seed(gamma,tmp_path);root,state=prepared(tmp_path,rolling(plan),active=False,status=status)
    assert not l.reconcile(root,state,AT+86400000,r.write,lambda:pytest.fail('terminal launch'))


def test_backoff_charge_survives_restarts_and_midnight(gamma,tmp_path,identity):
    plan,_=seed(gamma,tmp_path);root,state=prepared(tmp_path,rolling(plan),active=False,status='BACKOFF')
    state.update(retry=True,recovery_charged=True,restarts=[AT],next_ms=AT+30000)
    assert l.reconcile(root,state,AT+2000,r.write,lambda:None)
    assert state['restarts']==[AT] and state['next_ms']==AT+30000
    state['restarts']=[AT,AT+100,AT+200];state.pop('recovery_charged')
    assert not l.charge(state,AT+300) and state['status']=='RESTART_BUDGET_EXHAUSTED'
    assert state['restarts']==[AT,AT+100,AT+200]


def test_rotation_interrupted_after_commit_or_state_retires_only_audited_files(gamma,tmp_path,identity,monkeypatch):
    plan,_=seed(gamma,tmp_path);plan=rolling(plan);root,state=prepared(tmp_path,plan,active=False,status='COMPLETED_WINDOW')
    for i in range(32):
        nonce=f'{i:032x}';state['attempts'].append(dict(nonce=nonce,finished_ms=AT+i))
        r.write(root,nonce+'.result.json',dict(nonce=nonce,state='COMPLETED_WINDOW'))
    r.write(root,'state.json',state);before=dict(state)
    original=r.write
    def fail(root,name,value):
        if name=='state.json' and len(value['attempts'])==2:raise OSError('isolated durable rotation interruption')
        original(root,name,value)
    with pytest.raises(OSError):l.compact_control(root,plan,state,fail,AT+40)
    assert len(list(root.glob('*.result.json')))==32
    after=l.finish_rotation(root,plan,before,r.write)
    assert len(after['attempts'])==2 and len(list(root.glob('*.result.json')))==2
    assert not (root/l.ROTATION).exists()
    assert l.finish_rotation(root,plan,after,r.write)==after


@pytest.mark.skipif(sys.platform!='linux',reason='Linux inherited flock and /proc qualification in CI')
def test_inherited_guard_survives_parent_exit_before_pid_is_saved(tmp_path):
    root=tmp_path/'control';root.mkdir(mode=0o700);lock=root/'.controller.lock'
    # Isolated launcher exits without recording its child PID; the inherited FD
    # alone must still exclude a second controller until the child is gone.
    program="""import os,fcntl,subprocess,sys
f=open(sys.argv[1],'a');fcntl.flock(f,fcntl.LOCK_EX)
p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(1)'],pass_fds=(f.fileno(),),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
print(p.pid,flush=True)
"""
    launch=subprocess.run([sys.executable,'-c',program,str(lock)],capture_output=True,text=True,check=True)
    pid=int(launch.stdout);ident=l.process_identity(pid)
    assert ident and ident['pid']==pid and l.alive(ident)
    with lock.open('a') as f:
        import fcntl
        with pytest.raises(BlockingIOError):fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
        limit=time.monotonic()+4
        while l.alive(ident) and time.monotonic()<limit:time.sleep(.05)
        assert not l.alive(ident)
        fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)


def test_worker_requires_guard_before_database_open(gamma,tmp_path,identity):
    plan,_=seed(gamma,tmp_path);plan=rolling(plan);root,state=prepared(tmp_path,plan)
    r.write(root,'plan.json',dict(plan=plan.value(),identity=state['identity'],code=r.code_identity()))
    with pytest.raises(l.LifecycleStop,match='GUARD_REQUIRED'):r.worker(root,state['active'],None)


def test_bad_saved_plan_cannot_rewrite_owner_or_charge_recovery(gamma,tmp_path,identity):
    plan,_=seed(gamma,tmp_path);plan=rolling(plan);root,state=prepared(tmp_path,plan,active=False,status='COMPLETED_WINDOW')
    drift=dict(plan.value(),phase_seconds=3)
    r.write(root,'plan.json',dict(plan=drift,identity=state['identity'],code=state['identity']['code']))
    before=(root/'state.json').read_bytes()
    with pytest.raises(r.RecoveryStop,match='PLAN_DRIFT'):r.supervise(plan,root)
    assert (root/'state.json').read_bytes()==before


def test_continuous_uses_per_city_discovery_and_bounded_window(gamma,tmp_path,identity,monkeypatch):
    plan,_=seed(gamma,tmp_path);plan=replace(rolling(plan),deadline_ms=None);root,state=prepared(tmp_path,plan)
    r.write(root,'plan.json',dict(plan=plan.value(),identity=state['identity'],code=r.code_identity()))
    calls=[]
    monkeypatch.setattr(l,'claim_child',lambda *args:None)
    monkeypatch.setattr(r,'observe_catalogue',lambda cfg,**kw:(calls.append((cfg,kw)) or dict(state='COMPLETED_WINDOW')))
    r.worker(root,state['active'],None)
    cfg,kw=calls[0];assert cfg.duration_seconds==plan.phase_seconds and kw['session_day'] is None
    assert kw['discover'] is r.discover_city
