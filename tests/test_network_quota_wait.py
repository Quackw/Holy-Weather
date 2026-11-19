"""US quota waiting on real isolated SQLite and managed clocks. No source HTTP."""
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import threading

import pytest
from holy_weather import lifecycle as l,recovery as r,service
from holy_weather.catalogue import CatalogueStore
from test_catalogue import AT
from test_price_lifecycle import ContinuousSim,OWNER,prepared
from test_price_recovery import private_tree
from test_us_lamp import setup

HOUR=3600000
DAY=24*HOUR

def history():return [AT-DAY+2000,AT-20*HOUR,AT-16*HOUR,AT-12*HOUR,AT-8*HOUR,AT-4*HOUR]

def case(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path)
    with CatalogueStore(cfg,session=False) as s:s.bind(*markets[0])
    private_tree(Path(cfg.data_dir))
    plan=r.Plan(cfg,None,AT+HOUR,phase_seconds=2,continuous=True)
    root,state=prepared(tmp_path,plan,active=False,status='RECONNECT_BUDGET_EXHAUSTED')
    state.update(retry=True,restarts=history(),attempts=[dict(nonce='a'*32,at_ms=AT-2000,
        finished_ms=AT-1,state='RECONNECT_BUDGET_EXHAUSTED',returncode=0,error=None,reason=None,guard_protocol=1,pid=123)])
    r.write(root,'state.json',state)
    return plan,root,state

def run(plan,root,sim,**kwargs):
    return r.supervise(plan,root,clock=sim.clock,monotonic=sim.mono,wait=sim.wait,_spawn=sim.spawn,**kwargs)

@pytest.fixture
def identity(monkeypatch):
    monkeypatch.setattr(l,'own_identity',lambda:dict(OWNER))
    monkeypatch.setattr(l,'alive',lambda value:False)

@pytest.mark.parametrize('debts,expected',[
    ([AT-1,AT-2,AT-3],AT-3+HOUR),
    (history(),AT+2000),
    ([AT-DAY+2000,AT-12*HOUR,AT-8*HOUR,AT-1,AT-2,AT-3],AT-3+HOUR),
    ([AT-DAY+2000,AT-DAY+3000,AT-12*HOUR,AT-8*HOUR,AT-1,AT-2,AT-3],AT-3+HOUR),
    ([AT-DAY,AT-HOUR,AT-1],AT)])
def test_both_windows_determine_earliest_slot_without_changing_history(debts,expected):
    before=list(debts);assert l.quota_available(debts,AT)==expected;assert debts==before
    assert len([t for t in debts if expected-t<DAY])<6
    assert sum(expected-t<HOUR for t in debts)<3

@pytest.mark.parametrize('debts',[[AT+1],[-1],[True],['123']])
def test_bad_budget_clocks_or_records_refused(debts):
    with pytest.raises(l.LifecycleStop,match='BUDGET_CLOCK_OR_RECORD'):l.quota_available(debts,AT)

def test_wait_then_one_paid_launch_and_health(gamma,tmp_path,identity):
    plan,root,before=case(gamma,tmp_path)
    sim=ContinuousSim(['COMPLETED_WINDOW']);health=[]
    value=run(plan,root,sim,on_health=lambda v:health.append((sim.clock(),v)))
    assert len(sim.calls)==1
    assert value['restarts']==before['restarts']+[AT+2000]
    assert value['attempts'][-1]['at_ms']==AT+302000
    assert not value.get('recovery_charged') and not value.get('network_quota_wait')
    waits=[(at,v) for at,v in health if v['status']==l.QUOTA_WAIT]
    assert waits and all(v['collection_paused'] and v['quota_available_ms']==AT+2000
                         and v['next_attempt_ms']==AT+302000 for _,v in waits)
    assert max(b[0]-a[0] for a,b in zip(waits,waits[1:]))<=1000
    assert all(at<=AT+2000 for at,_ in waits)

def test_hour_limit_can_release_later_than_daily_limit(gamma,tmp_path,identity):
    plan,root,state=case(gamma,tmp_path);plan=replace(plan,deadline_ms=AT+4000000)
    state['identity']['plan']=r.sha(plan.value())
    state['restarts']=[AT-DAY+2000,AT-12*HOUR,AT-8*HOUR,AT-1,AT-2,AT-3]
    r.write(root,'state.json',state)
    with CatalogueStore(plan.config,session=False) as s:
        s.c.execute("update meta set value=? where key='price_recovery_control_v1'",(json.dumps(state['identity']),))
    sim=ContinuousSim(['COMPLETED_WINDOW']);value=run(plan,root,sim)
    due=AT-3+HOUR
    assert len(sim.calls)==1 and value['restarts']==state['restarts']+[due]
    assert value['attempts'][-1]['at_ms']==due+300000

@pytest.mark.parametrize('fault',['code','integrity'])
def test_checks_are_repeated_after_long_wait_before_charging(gamma,tmp_path,identity,monkeypatch,fault):
    plan,root,before=case(gamma,tmp_path);sim=ContinuousSim([])
    real=r.code_identity
    if fault=='code':monkeypatch.setattr(r,'code_identity',lambda:real() if sim.t<2 else 'drift')
    else:
        def corrupt_then_wait(seconds):
            if sim.t>=1:
                with (Path(plan.config.data_dir)/'monitor.sqlite3').open('r+b') as f:f.write(b'CORRUPT DATABASE!')
            sim.t+=seconds
        sim.wait=corrupt_then_wait
    with pytest.raises((r.RecoveryStop,sqlite3.DatabaseError)):run(plan,root,sim)
    stored=json.loads((root/'state.json').read_text())
    assert not sim.calls and stored['restarts']==before['restarts'] and not stored.get('recovery_charged')

@pytest.mark.parametrize('point',['quota','paid_backoff'])
def test_controller_reentry_preserves_deadline_and_exactly_one_charge(gamma,tmp_path,identity,monkeypatch,point):
    plan,root,before=case(gamma,tmp_path);sim=ContinuousSim([])
    def interrupt(seconds):
        state=json.loads((root/'state.json').read_text())
        if state['status']==(l.QUOTA_WAIT if point=='quota' else 'BACKOFF'):
            raise RuntimeError('isolated controller interruption')
        sim.t+=seconds
    sim.wait=interrupt
    with pytest.raises(RuntimeError,match='controller interruption'):run(plan,root,sim)
    stored=json.loads((root/'state.json').read_text());waiting=dict(stored['network_quota_wait'])
    assert len(stored['restarts'])==len(before['restarts'])+(point=='paid_backoff')
    deadline=stored.get('next_ms',waiting['resume_not_before_ms'])
    monkeypatch.setattr(l,'own_identity',lambda:dict(OWNER,pid=101))
    again=ContinuousSim(['COMPLETED_WINDOW']);again.t=sim.t
    result=run(plan,root,again)
    assert len(again.calls)==1 and result['restarts']==before['restarts']+[AT+2000]
    assert result['attempts'][-1]['at_ms']==deadline==AT+302000

def test_manual_stop_in_wait_never_launches_or_spends_credit(gamma,tmp_path,identity):
    plan,root,before=case(gamma,tmp_path);sim=ContinuousSim([]);stop=threading.Event()
    sim.wait=lambda seconds:stop.set()
    result=run(plan,root,sim,stop=stop)
    assert result['status']=='STOPPED' and result['restarts']==before['restarts'] and not sim.calls
    again=ContinuousSim([])
    assert run(plan,root,again)['status']=='STOPPED' and not again.calls

@pytest.mark.parametrize('bad',['writer','integrity','version'])
def test_wait_cannot_mask_integrity_version_or_writer_refusal(gamma,tmp_path,identity,bad):
    plan,root,_=case(gamma,tmp_path);before=(root/'state.json').read_bytes();sim=ContinuousSim([])
    if bad=='version':
        state=json.loads(before);state['identity']['code']='0'*64;r.write(root,'state.json',state)
        before=(root/'state.json').read_bytes()
        with pytest.raises(r.RecoveryStop,match='IDENTITY_DRIFT'):run(plan,root,sim)
    elif bad=='writer':
        with CatalogueStore(plan.config,session=False):
            with pytest.raises(BlockingIOError):run(plan,root,sim)
    else:
        with (Path(plan.config.data_dir)/'monitor.sqlite3').open('r+b') as f:f.write(b'CORRUPT DATABASE!')
        with pytest.raises(sqlite3.DatabaseError):run(plan,root,sim)
    assert not sim.calls and (root/'state.json').read_bytes()==before

def test_wait_keeps_controller_exclusive(gamma,tmp_path,identity):
    plan,root,_=case(gamma,tmp_path);sim=ContinuousSim([])
    with r.owned_control(root,plan.config.data_dir):
        with pytest.raises(BlockingIOError):run(plan,root,sim)
    assert not sim.calls

@pytest.mark.parametrize('after_release',[False,True])
def test_unknown_wait_source_is_never_reused_even_after_quota_releases(gamma,tmp_path,identity,after_release):
    plan,root,state=case(gamma,tmp_path)
    l.defer_network(state,AT);state['network_quota_wait']['source_nonce']='b'*32
    r.write(root,'state.json',state);before=(root/'state.json').read_bytes();sim=ContinuousSim([])
    if after_release:sim.t=3
    with pytest.raises(l.LifecycleStop,match='NETWORK_WAIT_DRIFT'):run(plan,root,sim)
    assert not sim.calls and (root/'state.json').read_bytes()==before

@pytest.mark.parametrize('reason',['UNKNOWN_EXIT','FATAL','NETWORK_REFUSED_NO_RETRY','UNRESPONSIVE_NO_KILL','RESTART_BUDGET_EXHAUSTED'])
def test_legacy_or_non_network_terminal_refusal_remains(gamma,tmp_path,identity,reason):
    plan,root,state=case(gamma,tmp_path);state.update(status=reason,retry=False)
    r.write(root,'state.json',state);sim=ContinuousSim([]);sim.t=DAY/1000+1
    result=run(plan,root,sim)
    assert result['status']==reason and result['restarts']==state['restarts'] and not sim.calls

def test_status_exposes_pause_and_retry_without_repeated_logs_or_secrets(capsys):
    ticks=[0.];sent=[]
    h=service.Health(environment=dict(WATCHDOG_USEC='3000000',NOTIFY_SOCKET='/offline'),
                     monotonic=lambda:ticks[0],notify=sent.append)
    value=dict(status=l.QUOTA_WAIT,collection_paused=True,recovery_reason='RECONNECT_BUDGET_EXHAUSTED',
               quota_available_ms=AT+2000,next_attempt_ms=AT+302000)
    for second in range(20):ticks[0]=second;h(value)
    output=capsys.readouterr().out.splitlines()
    assert len(output)==1 and len(sent)==20
    assert json.loads(output[0])['next_attempt_ms']==AT+302000
    assert all('collection=PAUSED' in x and 'retry_at=' in x for x in sent)
