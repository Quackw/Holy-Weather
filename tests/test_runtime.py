from dataclasses import replace
from datetime import datetime, timezone
import json
import signal
import threading
import time
from urllib.error import HTTPError
import pytest
from holy_weather import app
from holy_weather.core import normalize
from holy_weather.storage import Store


def wire(market,p,at):
    return json.dumps({'event_type':'best_bid_ask','asset_id':market['tokens']['YES'],
                       'market':market['condition_id'],'timestamp':str(at),'best_bid':'0.40','best_ask':p})


def test_reconnect_has_gap_and_baseline(cfg,market,monkeypatch):
    stop=threading.Event();market=dict(market,date=datetime.now(timezone.utc).astimezone(__import__('zoneinfo').ZoneInfo(market['timezone'])).date().isoformat())
    monkeypatch.setattr(app,'discover',lambda _: (market,{'synthetic':True}))
    at=app.millis()-1000;actions=[[wire(market,'0.49',at),OSError('synthetic disconnect')],[wire(market,'0.95',at+10),wire(market,'0.96',at+20)]];calls=[]
    class Fake:
        def __init__(self,actions):self.actions=list(actions)
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,value):pass
        def recv(self,timeout):
            if not self.actions:stop.set();raise TimeoutError()
            value=self.actions.pop(0)
            if isinstance(value,Exception):raise value
            return value
    def connect(*a,**k):
        calls.append(1);return Fake(actions[len(calls)-1])
    result=app.observe(replace(cfg,duration_seconds=5),connect=connect,stop=stop)
    assert result['counters']['network_gaps']==1 and result['counters']['reconnects']==1
    assert result['storage']['events']=={} and result['storage']['samples']==3
    assert result['storage']['sqlite_integrity']=='ok'


def test_discovery_denial_has_no_retry(cfg,monkeypatch):
    calls=[]
    def deny(_):calls.append(1);raise HTTPError('https://gamma-api.polymarket.com/',403,'denied',None,None)
    monkeypatch.setattr(app,'discover',deny)
    result=app.observe(cfg,connect=lambda *a,**k:pytest.fail('must not connect'))
    assert result['state']=='DISCOVERY_DENIED' and result['exit_code']==2 and len(calls)==1


def test_bad_message_is_retained_and_does_not_kill_valid_followup(cfg,market):
    from collections import Counter
    at=app.millis()-1000;c=Counter()
    with Store(cfg)as s:
        app.process(wire(market,'0.49',at),at,market,s,c)
        bad=json.loads(wire(market,'0.99',at+1));bad['market']='0x'+'b'*64
        app.process(json.dumps(bad),at+1,market,s,c)
        app.process(wire(market,'0.95',at+2),at+2,market,s,c)
        assert c['invalid_messages']==1 and s.stats()['samples']==2 and s.stats()['events']=={}
        assert s.stats()['inputs']==3


def test_signal_stop_and_safe_close(cfg,market,monkeypatch):
    market=dict(market,date=datetime.now(timezone.utc).astimezone(__import__('zoneinfo').ZoneInfo(market['timezone'])).date().isoformat())
    monkeypatch.setattr(app,'discover',lambda _: (market,{'synthetic':True}))
    stop=threading.Event();old=signal.signal(signal.SIGTERM,lambda *_:stop.set())
    class Fake:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,value):pass
        def recv(self,timeout):
            __import__('os').kill(__import__('os').getpid(),signal.SIGTERM);raise TimeoutError()
    try:
        result=app.observe(cfg,connect=lambda *a,**k:Fake(),stop=stop)
        assert result['state']=='SIGNAL_STOP'and result['storage']['sqlite_integrity']=='ok'
        with Store(cfg)as store:assert store.stats()['foreign_key_errors']==0
    finally:signal.signal(signal.SIGTERM,old)


def test_final_status_after_disconnect_is_not_fresh(cfg,market,monkeypatch):
    market=dict(market,date=datetime.now(timezone.utc).astimezone(__import__('zoneinfo').ZoneInfo(market['timezone'])).date().isoformat())
    monkeypatch.setattr(app,'discover',lambda _: (market,{'synthetic':True}))
    at=app.millis();yes=json.loads(wire(market,'0.6',at));no=dict(yes,asset_id=market['tokens']['NO'])
    class Fake:
        def __init__(self):self.actions=[json.dumps([yes,no]),OSError('disconnected')]
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,value):pass
        def recv(self,timeout):
            value=self.actions.pop(0)
            if isinstance(value,Exception):raise value
            return value
    result=app.observe(replace(cfg,max_reconnects=0),connect=lambda *a,**k:Fake())
    assert result['state']=='NETWORK_RECONNECT_BUDGET_EXHAUSTED'
    assert result['data_state']=='CONNECTION_UNCONFIRMED'
    assert all(r['freshness_now']=='FRESH'for r in result['storage']['latest'])
