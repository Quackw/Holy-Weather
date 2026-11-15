"""Failure-first acceptance: honest health and independent finite context."""
from collections import Counter
from dataclasses import replace
from datetime import datetime,timezone
import json
import time
from urllib.error import HTTPError,URLError
import pytest
from holy_weather import app,lamp,weather
from holy_weather.storage import Store
from test_lamp import AT,ISSUE,bulletin,receipt


def test_missing_bid_is_unusable_independently_then_recovers(cfg,market,make_raw):
    cfg=replace(cfg,price_alert_enabled=True)
    with Store(cfg) as s:
        s.bind(market,{});s.connection_started(AT)
        for side,token in market['tokens'].items():
            app.process(json.dumps(make_raw(at=AT,token=token,bid=None if side=='YES' else '0.4',ask='0.6')),AT,market,s,Counter())
        status=s.observation_snapshot(now_ms=AT)['observation']
        assert status['sides']=={'YES':'QUOTE_UNUSABLE','NO':'READY'}
        assert status['state']=='PARTIAL_OR_UNUSABLE'
        app.process(json.dumps(make_raw(at=AT+1,bid='0.8',ask='0.9')),AT+1,market,s,Counter())
        assert s.observation_snapshot(now_ms=AT+1)['observation']['state']=='QUOTES_FRESH'
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==0


@pytest.mark.parametrize('wrapped',[False,True])
def test_lamp_timeout_waits_full_interval_and_recovers(cfg,market,wrapped):
    clock=[0.0];calls=[]
    def fetch():
        calls.append(clock[0])
        if len(calls)==1:
            def fail(*a,**k):raise URLError(TimeoutError()) if wrapped else TimeoutError()
            return lamp.fetch_lamp(fetch=fail,clock=lambda:AT)
        return receipt(bulletin(),AT+900000)
    poller=lamp.LampPoller(fetch,monotonic=lambda:clock[0])
    cfg=replace(cfg,weather_context=True,lamp_context=True,lamp_alert_enabled=True)
    with Store(cfg) as s:
        s.bind(market,{})
        poller.tick(s,market,AT);poller.thread.join(1)
        poller.tick(s,market,AT+1)
        assert not poller.stopped and s.lamp_status()=='NETWORK_OR_DECODE_ERROR'
        clock[0]=899.99;poller.tick(s,market,AT+899990);assert len(calls)==1
        clock[0]=900;poller.tick(s,market,AT+900000);poller.thread.join(1)
        poller.tick(s,market,AT+900001)
        assert calls==[0,900] and s.lamp_status()=='OK'
        assert s.c.execute("select count(*) from events where type='LAMP_ALERT'").fetchone()[0]==0
        assert s.stats()['sqlite_integrity']=='ok'
    poller.close()


@pytest.mark.parametrize('code',[401,403,429,500])
def test_lamp_http_error_remains_terminal(code):
    def fail(*a,**k):raise HTTPError('https://example.invalid',code,'synthetic',None,None)
    r=lamp.fetch_lamp(fetch=fail)
    assert r['terminal'] and r['http']==code


def test_forecast_event_queue_survives_total_ws_failure(cfg,market,monkeypatch):
    class Frozen(datetime):
        @classmethod
        def now(cls,tz=None):return datetime.fromtimestamp(AT/1000,timezone.utc).astimezone(tz)
    wall=[AT];calls=[];ws=[]
    monkeypatch.setattr(app,'datetime',Frozen)
    monkeypatch.setattr(app,'millis',lambda:wall[0])
    monkeypatch.setattr(weather,'millis',lambda:wall[0])
    monkeypatch.setattr(app,'discover',lambda _: (market,{}))
    monkeypatch.setattr(lamp.LampPoller,'interval',.1)
    def forecast():
        wall[0]+=1000;calls.append(wall[0])
        return receipt(bulletin([46 if len(calls)==1 else 48]*25),wall[0])
    def awc():return dict(source=weather.SOURCE,status='NO_DATA',requested_ms=AT,received_ms=AT,terminal=True)
    def fail(*a,**k):ws.append(1);raise OSError('SYNTHETIC_WS_FAILURE')
    cfg=replace(cfg,duration_seconds=1,max_reconnects=0,weather_context=True,lamp_context=True,lamp_alert_enabled=True,alert_queue_enabled=True)
    r=app.observe(cfg,connect=fail,weather_fetch=awc,lamp_fetch=forecast)
    assert ws==[1] and len(calls)>=2
    assert r['state']=='NETWORK_RECONNECT_BUDGET_EXHAUSTED'
    assert r['data_state']=='CONNECTION_UNCONFIRMED'
    with Store(cfg) as s:
        assert s.c.execute("select count(*) from events where type='LAMP_ALERT'").fetchone()[0]==1
        assert s.c.execute("select count(*) from meta where key like 'alert_delivery_v1:%'").fetchone()[0]==1
        assert s.stats()['sqlite_integrity']=='ok'
