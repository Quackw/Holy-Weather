"""One controller/one writer handoff through US network quota and backoff."""
from dataclasses import replace
from datetime import datetime, timezone
import functools
import json
from pathlib import Path
import threading
import time

import pytest
from holy_weather import catalogue, catalogue_metar, lifecycle, recovery, us_lamp
from holy_weather.core import US_CITIES, sha
from holy_weather.storage import StorageStop
from test_catalogue import AT, feed
from test_catalogue_metar import observations
from test_network_quota_wait import history
from test_price_lifecycle import ContinuousSim, OWNER, prepared
from test_price_recovery import private_tree
from test_us_lamp import ISSUE, events, receipt, setup


def sources(sim,bindings,calls):
    def forecast(issue):
        calls.append(('NOAA',sim.clock()))
        return dict(status='OK',terminal=False,bytes=100,
                    receipts={s:receipt(s,47,sim.clock(),issue) for _,s,_ in US_CITIES.values()})
    def metar(*,station):
        calls.append((station,sim.clock()))
        _,_,zone=next(v for v in US_CITIES.values() if v[1]==station)
        return observations(dict(station=station,timezone=zone),sim.clock())
    def discovery(city):
        station=US_CITIES[city][1]
        pairs=[(m,r) for m,r in bindings if m['station']==station]
        return [m for m,_ in pairs],pairs[0][1],[]
    return functools.partial(catalogue.observe_catalogue,lamp_fetch=forecast,metar_fetch=metar,
                             discover=discovery,connect=lambda *a,**k:pytest.fail('weather owner opened PRICE socket'))


def scenario(gamma,tmp_path,enabled):
    cfg,bindings=setup(gamma,tmp_path,at=AT-10000)
    cfg=replace(cfg,catalogue_metar_enabled=True,us_weather_continuity_enabled=enabled)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        for m,r in bindings:s.bind(m,r)
        s.connection_started(AT-2000);router=catalogue.Router(s)
        feed(router,bindings[0][0],'0.49',AT-1999);feed(router,bindings[0][0],'0.56',AT-1998)
        before=s.c.execute("select value from meta where key like 'price_alert_v1:%'").fetchone()[0]
    private_tree(Path(cfg.data_dir))
    plan=recovery.Plan(cfg,None,AT+3600000,phase_seconds=1200,continuous=True)
    root,state=prepared(tmp_path,plan,active=False,status='RECONNECT_BUDGET_EXHAUSTED')
    state.update(retry=True,restarts=history(),attempts=[dict(nonce='a'*32,at_ms=AT-1000,
        finished_ms=AT-1,state='RECONNECT_BUDGET_EXHAUSTED',returncode=0,error=None,reason=None,guard_protocol=1,pid=123)])
    recovery.write(root,'state.json',state)
    return plan,root,state,bindings,json.loads(before)['fired']


@pytest.fixture
def identity(monkeypatch):
    monkeypatch.setattr(lifecycle,'own_identity',lambda:dict(OWNER))
    monkeypatch.setattr(lifecycle,'alive',lambda value:False)


@pytest.mark.parametrize('enabled',[False,True])
def test_before_after_quota_wait_collects_both_sources_then_one_price_writer(gamma,tmp_path,identity,enabled,record_property):
    plan,root,before,bindings,fired=scenario(gamma,tmp_path,enabled)
    sim=ContinuousSim(['COMPLETED_WINDOW']);calls=[];health=[];lock_checks=[]
    original_wait=sim.wait
    def wait(seconds):
        if enabled and json.loads((root/'state.json').read_text()).get('weather_collecting'):
            if not lock_checks or sim.clock()-lock_checks[-1]>100000:
                with pytest.raises(StorageStop,match='WRITER_ALREADY_RUNNING'):
                    catalogue.CatalogueStore(plan.config,session=False)
                lock_checks.append(sim.clock())
        time.sleep(.0001);original_wait(seconds)
    sim.wait=wait
    original_spawn=sim.spawn
    def spawn(*args):
        # Opening under the real writer lock proves weather has closed before PRICE.
        with catalogue.CatalogueStore(plan.config,session=False) as s:
            now=sim.clock();s.connection_started(now);router=catalogue.Router(s)
            count=s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]
            feed(router,bindings[0][0],'0.81',now+1)  # Fresh GAP baseline, no retroactive crossing.
            assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==count
        return original_spawn(*args)
    value=recovery.supervise(plan,root,clock=sim.clock,monotonic=sim.mono,wait=sim.wait,_spawn=spawn,
                             _weather_observe=sources(sim,bindings,calls),on_health=lambda v:health.append((sim.clock(),v)))
    assert len(sim.calls)==1 and value['restarts']==before['restarts']+[AT+2000]
    assert value['attempts'][-1]['at_ms']==AT+302000
    with catalogue.CatalogueStore(plan.config,session=False) as s:
        forecasts=list(s.c.execute("select value from meta where key like 'lamp_context_v1:%'"))
        metars=list(s.c.execute("select value from meta where key like 'catalogue_metar_station_v1:%'"))
        assert len(forecasts)==len(metars)==(10 if enabled else 0)
        assert all(json.loads(r[0])['status']=='OK' for r in forecasts+metars)
        saved=json.loads(s.c.execute("select value from meta where key=?",('price_alert_v1:'+bindings[0][0]['tokens']['YES'],)).fetchone()[0])
        assert saved['fired']==fired and s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()
    assert bool(calls)==enabled and bool(lock_checks)==enabled
    if enabled:
        assert sum(s=='NOAA' for s,_ in calls)==1
        assert any(v.get('weather_collecting') and v.get('price_paused') and not v['collection_paused'] for _,v in health)
        assert all(at<AT+302000 for _,at in calls)
    record_property('quota_weather_replay',dict(enabled=enabled,noaa=len(forecasts),metar=len(metars),
                    price_launches=len(sim.calls),writers=1,duplicates=0,charged=len(value['restarts'])-len(before['restarts'])))


def test_noaa_without_any_polymarket_contract_still_ingests_all_stations(tmp_path):
    cfg=catalogue.CatalogueConfig(region='us',city='new-york',data_dir=str(tmp_path),us_lamp_enabled=True,
                                 catalogue_metar_enabled=True,us_weather_continuity_enabled=True)
    sim=ContinuousSim([]);calls=[]
    collector=us_lamp.Collection(lambda issue:dict(status='OK',terminal=False,bytes=100,
        receipts={s:receipt(s,47,AT,issue) for _,s,_ in US_CITIES.values()}))
    with catalogue.CatalogueStore(cfg,session=False) as s:
        collector.tick(s,set(),AT);collector.thread.join(2);collector.tick(s,set(),AT+1)
        assert len(list(s.c.execute("select 1 from meta where key like 'lamp_context_v1:%'")))==10
        assert not events(s) and not list(s.c.execute("select 1 from meta where key like 'alert_delivery_v1:%'"))
        assert not list(s.c.execute('select 1 from events'))


def test_orphan_forecast_days_move_to_verified_history_and_restore(tmp_path):
    from holy_weather import history as archive
    import sqlite3
    cfg=catalogue.CatalogueConfig(region='us',city='new-york',data_dir=str(tmp_path/'data'),us_lamp_enabled=True,
                                 catalogue_metar_enabled=True,us_weather_continuity_enabled=True)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        for _,station,zone in US_CITIES.values():
            descriptor=dict(station=station,timezone=zone,date='2026-11-14')
            s.record_lamp(receipt(station),descriptor,AT,markets=[])
        original=[tuple(r) for r in s.c.execute("select key,value from meta where key like 'lamp_context_v1:%' order by key")]
        s.reject({'isolated':'later history boundary'},'TEST',AT+4*86400000,invalidate_state=False)
    db=Path(cfg.data_dir)/'monitor.sqlite3';part=tmp_path/'part'
    archive.prepare(cfg.data_dir,part,expected_sha=archive.digest(db),compact_price_inputs=True,
                    disk_index=True,retire_at_ms=AT+4*86400000)
    c=sqlite3.connect(part/'candidate.sqlite3')
    try:assert not list(c.execute("select 1 from meta where key like 'lamp_context_v1:%'"))
    finally:c.close()
    archive.restore_history(part/'candidate.sqlite3',part,tmp_path/'restored')
    c=sqlite3.connect(tmp_path/'restored/restored.sqlite3')
    try:assert original==c.execute("select key,value from meta where key like 'lamp_context_v1:%' order by key").fetchall()
    finally:c.close()


@pytest.mark.parametrize('point',['WAITING_NETWORK_QUOTA','BACKOFF'])
def test_interrupted_weather_owner_reentry_keeps_quota_and_one_charge(gamma,tmp_path,identity,monkeypatch,point):
    plan,root,before,bindings,_=scenario(gamma,tmp_path,True)
    sim=ContinuousSim([]);calls=[]
    def interrupt(seconds):
        stored=json.loads((root/'state.json').read_text())
        if stored['status']==point:raise RuntimeError('isolated weather interruption')
        sim.t+=seconds
    sim.wait=interrupt
    with pytest.raises(RuntimeError,match='weather interruption'):
        recovery.supervise(plan,root,clock=sim.clock,monotonic=sim.mono,wait=sim.wait,_spawn=sim.spawn,
                           _weather_observe=sources(sim,bindings,calls))
    saved=json.loads((root/'state.json').read_text());deadline=saved.get('next_ms',saved['network_quota_wait']['resume_not_before_ms'])
    assert not saved.get('weather_collecting') and len(saved['restarts'])==6+(point=='BACKOFF')
    monkeypatch.setattr(lifecycle,'own_identity',lambda:dict(OWNER,pid=101))
    again=ContinuousSim(['COMPLETED_WINDOW']);again.t=sim.t
    value=recovery.supervise(plan,root,clock=again.clock,monotonic=again.mono,wait=again.wait,_spawn=again.spawn,
                            _weather_observe=sources(again,bindings,calls))
    assert value['restarts']==before['restarts']+[AT+2000]
    assert value['attempts'][-1]['at_ms']==deadline and len(again.calls)==1


def test_feature_flags_do_not_change_price_hash_or_old_plan_bytes(tmp_path):
    cfg=catalogue.CatalogueConfig(region='us',city='new-york',data_dir=str(tmp_path))
    off=recovery.Plan(cfg,None,None,continuous=True)
    assert 'us_weather_continuity_enabled' not in off.value()['config'] and 'us_lamp_notice_mode' not in off.value()['config']
    on=replace(cfg,us_lamp_enabled=True,catalogue_metar_enabled=True,
               us_weather_continuity_enabled=True,us_lamp_notice_mode='warming_hourly')
    assert cfg.rule_hash()==on.rule_hash()
    with pytest.raises(ValueError,match='WEATHER_REQUIRES'):replace(cfg,us_weather_continuity_enabled=True)
    with pytest.raises(ValueError):replace(on,region='europe')


@pytest.mark.parametrize('fault',['writer','integrity','version','manual_stop'])
def test_weather_continuity_does_not_bypass_existing_safety(gamma,tmp_path,identity,fault):
    plan,root,state,bindings,_=scenario(gamma,tmp_path,True);sim=ContinuousSim([])
    kwargs=dict(clock=sim.clock,monotonic=sim.mono,wait=sim.wait,_spawn=sim.spawn,
                _weather_observe=lambda *a,**k:pytest.fail('weather bypassed safety refusal'))
    if fault=='writer':
        with catalogue.CatalogueStore(plan.config,session=False):
            with pytest.raises(BlockingIOError):recovery.supervise(plan,root,**kwargs)
    elif fault=='integrity':
        import sqlite3
        with (Path(plan.config.data_dir)/'monitor.sqlite3').open('r+b') as f:f.write(b'CORRUPT')
        with pytest.raises(sqlite3.DatabaseError):recovery.supervise(plan,root,**kwargs)
    elif fault=='version':
        state['identity']['code']='0'*64;recovery.write(root,'state.json',state)
        with pytest.raises(recovery.RecoveryStop,match='IDENTITY_DRIFT'):recovery.supervise(plan,root,**kwargs)
    else:
        state.update(status='STOPPED',retry=False);recovery.write(root,'state.json',state)
        assert recovery.supervise(plan,root,**kwargs)['status']=='STOPPED'
    assert not sim.calls


def test_real_controller_weather_writer_then_real_price_child(gamma,tmp_path,record_property,capsys):
    """On Linux assert kernel writer PIDs in both actual owner processes."""
    import os
    import subprocess
    import sys
    root=Path(__file__).resolve().parents[1];base=tmp_path/'actual';base.mkdir(mode=0o700)
    (base/'gamma.json').write_text(json.dumps(gamma))
    env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',HW_PROBE_WEATHER_CONTINUITY='1',
             PYTHONPATH=str(root/'src')+os.pathsep+str(root/'tests'))
    done=subprocess.run([sys.executable,'-B',str(root/'tests/network_quota_probe.py'),'controller',str(base)],
                        env=env,capture_output=True,text=True,timeout=35,cwd=root)
    assert done.returncode==0,done.stdout+done.stderr
    value=json.loads((base/'RESULT.json').read_text());waiting=value['weather_during_wait']
    assert waiting['noaa_stations']==waiting['metar_stations']==10
    assert value['launches']==1 and value['added_price_events']==1 and value['child']['first_quote_is_baseline']
    assert value['launch_ms']==[value['permitted_ms']] and value['duplicate_sends']==0
    assert value['old_receipts_preserved']==3 and value['old_thresholds_preserved']==8
    assert value['weather_wait_visible'] and value['forecast_after_ms']>value['forecast_before_ms']
    if sys.platform=='linux':
        assert len(waiting['controller_writer_pids'])==len(value['child']['writer_pids'])==1
        assert waiting['controller_writer_pids']!=value['child']['writer_pids']
    with capsys.disabled():print('US_WEATHER_CONTINUITY_PROOF '+json.dumps(value,sort_keys=True))
    record_property('us_weather_real_owner_handoff',value)
