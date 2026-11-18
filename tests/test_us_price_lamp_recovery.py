"""Continuous PRICE handoff and real owner paths; all clocks/transports offline."""
from dataclasses import replace
import json
import os
import signal
import sys
import threading
import time

import pytest

from holy_weather import catalogue, core, lifecycle, recovery, us_lamp
from test_catalogue import AT, feed
from test_price_recovery import private_tree
from test_us_lamp import setup
from test_us_lamp import receipt as forecast_receipt


def test_continuous_lamp_worker_returns_for_price_recovery(gamma,tmp_path,monkeypatch):
    cfg,markets=setup(gamma,tmp_path)
    market,receipt=markets[0]
    cfg=replace(cfg,max_reconnects=0,duration_seconds=1200)
    plan=recovery.Plan(cfg,None,None,continuous=True)
    with catalogue.CatalogueStore(cfg,session=False):pass
    private_tree(tmp_path/'data')
    mono=[0.];calls=[]
    def wait(seconds):
        mono[0]+=seconds;time.sleep(.0001)
    class Socket:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,*a):pass
        def recv(self,timeout):
            time.sleep(.001);mono[0]+=1
            raise OSError('offline controlled stream loss')
    def observe(config,**kwargs):
        calls.append(kwargs.copy())
        return catalogue.observe_catalogue(config,**{**kwargs,
            'connect':lambda *a,**k:Socket(),
            'discover':lambda city:([market],receipt,[]) if city=='new-york' else ([],{},[]),
            'clock':lambda:AT+round(mono[0]*1000),'monotonic':lambda:mono[0],'wait':wait,
            'lamp_fetch':lambda issue:dict(receipts={},status='ACCESS_DENIED',terminal=True,bytes=0)})
    monkeypatch.setattr(recovery,'observe_catalogue',observe)
    owner=dict(pid=os.getpid(),uid=os.getuid(),boot='offline-owner',start_ticks=1,exe=sys.executable)
    monkeypatch.setattr(lifecycle,'own_identity',lambda:owner)
    monkeypatch.setattr(signal,'signal',lambda *a:None)
    control=tmp_path/'control';nonce='a'*32
    with recovery.owned_control(control,cfg.data_dir,with_fd=True) as (root,guard):
        identity=dict(control=str(root),code=recovery.code_identity(),plan=core.sha(plan.value()))
        recovery.write(root,'plan.json',dict(plan=plan.value(),identity=identity,code=identity['code']))
        recovery.write(root,'state.json',dict(identity=identity,active=nonce,controller_process=owner,
            attempts=[dict(nonce=nonce,guard_protocol=lifecycle.PROTOCOL)]))
        recovery.worker(root,nonce,None,guard)
    result=json.loads((control/(nonce+'.result.json')).read_bytes())
    assert result['error'] is None and result['state']=='NETWORK_GAP_NO_RETRY'
    assert mono[0]<20, f'PRICE recovery waited {mono[0]:.1f}s for the LAMP window'
    assert len(calls)==1


def test_lamp_only_wait_publishes_price_gap_immediately(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path);market,receipt=markets[0]
    cfg=replace(cfg,duration_seconds=30,max_reconnects=0)
    mono=[0.];snapshots=[];gap=[False]
    class Progress:
        def __call__(self,stage):pass
        def observation(self,value):snapshots.append(value)
    class Socket:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,*a):pass
        def recv(self,timeout):
            time.sleep(.001);mono[0]+=1
            if mono[0]>=13:
                gap[0]=True;raise OSError('offline gap after a fresh PRICE snapshot')
            return core.canonical(dict(event_type='best_bid_ask',market=market['condition_id'],
                asset_id=market['tokens']['YES'],timestamp=str(AT+round(mono[0]*1000)),best_bid='0.49',best_ask='0.49'))
    def wait(seconds):
        if gap[0]:
            assert snapshots[-1]['connected'] is False
            assert all(c['price_ready']==c['fresh_quotes']==0 for c in snapshots[-1]['cities'].values())
        mono[0]+=seconds;time.sleep(.0001)
    result=catalogue.observe_catalogue(cfg,connect=lambda *a,**k:Socket(),
        discover=lambda city:([market],receipt,[]) if city=='new-york' else ([],{},[]),
        clock=lambda:AT+round(mono[0]*1000),monotonic=lambda:mono[0],wait=wait,progress=Progress(),
        lamp_fetch=lambda issue:dict(receipts={},status='ACCESS_DENIED',terminal=True,bytes=0))
    assert result['state']=='NETWORK_GAP_NO_RETRY' and mono[0]>=30
    assert any(s['connected'] and s['cities']['new-york']['price_ready'] for s in snapshots)


@pytest.mark.parametrize('blocked',[False,True])
def test_idle_lamp_poll_does_not_scan_all_contracts(gamma,tmp_path,monkeypatch,blocked):
    cfg,markets=setup(gamma,tmp_path)
    with catalogue.CatalogueStore(cfg,session=False) as store:
        for market,receipt in markets:store.bind(market,receipt)
        store.set_meta(us_lamp.KEY,dict(next_ms=AT+100000,attempts=1,blocked=blocked))
        calls=[];active=store.active
        monkeypatch.setattr(store,'active',lambda *a:(calls.append(a) or active(*a)))
        collector=us_lamp.Collection(lambda issue:pytest.fail('idle request'))
        for i in range(50):collector.tick(store,set(store.markets),AT+i)
        collector.close()
        assert calls==[]


def test_storage_does_not_recopy_its_owned_research_window(gamma,tmp_path,monkeypatch):
    cfg,markets=setup(gamma,tmp_path);market,receipt=markets[0]
    with catalogue.CatalogueStore(cfg,session=False) as store:
        store.bind(market,receipt);store.connection_started(AT);router=catalogue.Router(store)
        feed(router,market,'0.49',AT+1)
        copies=[];original=core.deepcopy
        def copied(value,*a,**k):
            if isinstance(value,dict) and value.get('points'):copies.append(len(value['points']))
            return original(value,*a,**k)
        monkeypatch.setattr(core,'deepcopy',copied)
        feed(router,market,'0.71',AT+2)
        assert not copies, 'Fresh JSON-owned state was copied again in the feed consumer'
        assert store.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1


@pytest.mark.parametrize('hang',[False,True])
def test_recovery_drains_only_existing_forecast_and_keeps_retry_debt(gamma,tmp_path,hang):
    cfg,markets=setup(gamma,tmp_path);market,receipt=markets[0]
    cfg=replace(cfg,duration_seconds=1200,max_reconnects=0)
    with catalogue.CatalogueStore(cfg,session=False) as store:
        store.bind(market,receipt);store.record_lamp(forecast_receipt(market['station']),market,AT)
    mono=[0.];gate=threading.Event();done=threading.Event();requests=[]
    at=lambda:AT+3600000+round(mono[0]*1000)
    def fetch(issue):
        requests.append(issue);gate.wait()
        try:return dict(receipts={market['station']:forecast_receipt(market['station'],48,at(),issue)},
                        status='OK',terminal=False,bytes=100)
        finally:done.set()
    class Socket:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,*a):pass
        def recv(self,timeout):
            time.sleep(.001);mono[0]+=1;raise OSError('offline PRICE gap while NOAA is in flight')
    def wait(seconds):
        mono[0]+=seconds
        if not hang and mono[0]>=2:gate.set()
        time.sleep(.001)
    try:
        result=catalogue.observe_catalogue(cfg,price_recovery=True,connect=lambda *a,**k:Socket(),
            discover=lambda city:([market],receipt,[]) if city=='new-york' else ([],{},[]),
            clock=at,monotonic=lambda:mono[0],wait=wait,lamp_fetch=fetch)
        assert result['state']=='NETWORK_GAP_NO_RETRY' and mono[0]<=1+us_lamp.HANDOFF_SECONDS
        assert len(requests)==1 and result['price_events']==0
        with catalogue.CatalogueStore(cfg,session=False) as store:
            state=json.loads(store.c.execute('select value from meta where key=?',(us_lamp.KEY,)).fetchone()[0])
            assert state['attempts']==1 and not state['blocked']
            if hang:
                assert state['status']=='INTERRUPTED' and state['reason']=='PRICE_RECOVERY_HANDOFF'
                assert state['next_ms']==AT+3600000+us_lamp.RETRY_MS
                assert store.lamp_state(market)['issue_ms']==AT-1800000
                assert not store.c.execute("select 1 from events where type='LAMP_ALERT'").fetchone()
            else:
                event,=store.c.execute("select payload from events where type='LAMP_ALERT'").fetchall()
                event=json.loads(event[0]);assert event['future_peak_f']=='48' and event['previous_peak_f']=='46'
                assert event['current_forecast']['received_ms']<=event['current_forecast']['available_ms']
            retry=us_lamp.Collection(lambda issue:pytest.fail('a paid request was silently repeated'))
            retry.tick(store,set(store.markets),at());retry.close()
    finally:
        gate.set();assert done.wait(2)


@pytest.mark.parametrize('initial',[None,{}])
def test_owned_evolution_matches_pure_public_rules_and_preserves_input(initial):
    from copy import deepcopy
    cfg=core.Config()
    previous=initial
    for n in range(160):
        sample=dict(kind='best_bid_ask',source_ms=AT+n*1000,received_ms=AT+n*1000,
                    ask=('0.49','0.71','0.48','0.81','0.68')[n%5],input_id=n+1)
        frozen=deepcopy(previous)
        public=core.evolve(previous,sample,cfg)
        owned=core.evolve(deepcopy(previous),sample,cfg,owned=True)
        assert previous==frozen and public==owned
        previous=public[0]


def test_storage_owned_path_matches_copy_path_across_baseline_and_gaps(gamma,tmp_path,monkeypatch):
    from holy_weather import storage
    from holy_weather.history import fingerprint
    cfg,markets=setup(gamma,tmp_path);market,receipt=markets[0];results=[]
    for copy in (False,True):
        with monkeypatch.context() as patch:
            if copy:patch.setattr(storage,'evolve',lambda previous,sample,cfg,**kw:core.evolve(previous,sample,cfg))
            with catalogue.CatalogueStore(replace(cfg,data_dir=str(tmp_path/str(copy))),session=False) as store:
                store.bind(market,receipt);store.connection_started(AT);router=catalogue.Router(store)
                def delta(price,n):
                    router.feed(core.canonical(dict(event_type='price_change',market=market['condition_id'],
                        timestamp=str(AT+n),price_changes=[dict(asset_id=market['tokens']['YES'],best_bid=price,best_ask=price)])),AT+n)
                delta('0.49',1);delta('0.71',2)
                feed(router,market,'0.49',AT+3);feed(router,market,'0.71',AT+4)
                store.gap('OFFLINE',AT+5);store.connection_started(AT+6)
                delta('0.81',7);delta('0.49',8)
                feed(router,market,'0.81',AT+9);feed(router,market,'0.49',AT+10)
                feed(router,market,'0.81',AT+11);feed(router,market,'0.49',AT+10)  # Old source.
                feed(router,market,'0.49',AT+12,'NO');feed(router,market,'0.81',AT+13,'NO')
                results.append(fingerprint(store.c))
    assert results[0]==results[1]


def test_accepted_binary_rollback_keeps_new_price_lamp_and_queue(gamma,tmp_path):
    from pathlib import Path
    import subprocess
    cfg,markets=setup(gamma,tmp_path);market,receipt=markets[0]
    fixture=dict(market=market,receipt=receipt,at=AT,
        lamp_first=forecast_receipt(market['station'],46,AT+6),
        lamp_second=forecast_receipt(market['station'],48,AT+3600000+6,issue=AT+1800000))
    (tmp_path/'fixture.json').write_text(json.dumps(fixture))
    ref='56f14b0898558a84b278cda706ac421bf2a899de';old=tmp_path/'accepted'
    for name in subprocess.check_output(['git','ls-tree','-r','--name-only',ref,'src/holy_weather'],text=True).splitlines():
        if name.endswith('.py'):
            dest=old/Path(name).relative_to('src');dest.parent.mkdir(parents=True,exist_ok=True)
            dest.write_bytes(subprocess.check_output(['git','show',ref+':'+name]))
    current=Path(__file__).resolve().parents[1]/'src';results=[]
    for phase,source in [('seed',old),('candidate',current),('rollback',old)]:
        reply=subprocess.run([sys.executable,'-B',str(Path(__file__).with_name('us_price_lamp_rollback_probe.py')),phase,str(tmp_path)],
            env=dict(PATH=os.defpath,PYTHONPATH=str(source),PYTHONDONTWRITEBYTECODE='1'),
            capture_output=True,text=True,timeout=30)
        assert reply.returncode==0,reply.stderr[-3000:]
        result=json.loads(reply.stdout);results.append(result)
        assert result['integrity']=='ok' and result['schema']==1
    assert results[0]['code']==results[2]['code']!=results[1]['code']
    assert results[-1]['statuses']==['PENDING','PENDING','SENT','UNKNOWN']
