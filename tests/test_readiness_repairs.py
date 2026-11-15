"""PRICE readiness and ended-contract guards; weather experiments deferred."""
from collections import Counter
from dataclasses import replace
from datetime import datetime,timezone
import json
from holy_weather import app
from holy_weather.storage import Store
from test_lamp import AT

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

def test_midnight_before_connect_never_subscribes_old_contract(cfg,market,monkeypatch):
    from datetime import timedelta
    class NextDay(datetime):
        @classmethod
        def now(cls,tz=None):
            return (datetime.fromtimestamp(AT/1000,timezone.utc)+timedelta(days=1)).astimezone(tz)
    monkeypatch.setattr(app,'datetime',NextDay)
    monkeypatch.setattr(app,'discover',lambda _: (market,{}))
    connects=[]
    def connect(*a,**k):
        connects.append(1)
        raise OSError('OLD_CONTRACT_MUST_NOT_CONNECT')
    result=app.observe(replace(cfg,duration_seconds=1,max_reconnects=0),connect=connect)
    assert not connects
    assert result['state']=='MARKET_LOCAL_DATE_ENDED'
    assert result['storage']['sqlite_integrity']=='ok'
