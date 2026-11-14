from collections import Counter
from dataclasses import replace
import json
import pytest
from holy_weather import app
from holy_weather.storage import Store
from test_confirmations import delta

AT=1794650000000

def send(s,m,raw,at,c):app.process(json.dumps(raw),at,m,s,c)
def start(s,m,make_raw,c):
    s.bind(m,{'synthetic':True});s.connection_started(AT)
    for side in ('YES','NO'):send(s,m,make_raw('0.49',AT,token=m['tokens'][side]),AT,c)


def test_exact_old_packet_does_not_destroy_confirmed_state(cfg,market,make_raw):
    c=Counter()
    with Store(cfg)as s:
        start(s,market,make_raw,c)
        old=make_raw('0.49',AT)
        send(s,market,make_raw('0.51',AT+10),AT+10,c)
        before=s.stats(now_ms=AT+10)['events']
        send(s,market,old,AT+20,c)
        assert s.stats(now_ms=AT+20)['observation']['state']=='QUOTES_FRESH'
        assert s.stats()['events']==before and c['DUPLICATE']==1


def test_uncertain_reverse_clock_recovers_side_without_false_cross(cfg,market,make_raw):
    c=Counter()
    with Store(cfg)as s:
        start(s,market,make_raw,c)
        send(s,market,make_raw('0.99',AT-1),AT+10,c)
        assert s.connected
        assert s.observation_snapshot(now_ms=AT+10)['observation']['sides']=={'YES':'AWAITING_BASELINE','NO':'READY'}
        send(s,market,delta(market,AT+20,ask='0.95'),AT+20,c)
        send(s,market,make_raw('0.69',AT+30),AT+30,c)
        assert not s.stats()['events']
        send(s,market,make_raw('0.71',AT+40),AT+40,c)
        assert s.stats()['events']=={'DETECTED':1}
        assert s.observation_snapshot(now_ms=AT+40)['observation']['state']=='QUOTES_FRESH'


def test_multiple_late_full_quotes_cannot_reestablish_old_baseline(cfg,market,make_raw):
    c=Counter()
    with Store(cfg)as s:
        start(s,market,make_raw,c)
        for i in range(1,4):send(s,market,make_raw('0.4',AT-i),AT+i,c)
        assert market['tokens']['YES'] not in s.ready_tokens
        send(s,market,make_raw('0.9',AT+10),AT+10,c)
        assert s.stats()['events']=={}
        assert market['tokens']['YES'] in s.ready_tokens


def test_restart_between_invalid_message_and_baseline_preserves_history(cfg,market,make_raw):
    c=Counter()
    with Store(cfg)as s:
        start(s,market,make_raw,c)
        send(s,market,make_raw('0.99',AT-1),AT+1,c)
        original=[tuple(r) for r in s.c.execute('select * from inputs')]
        reasons=[tuple(r) for r in s.c.execute('select * from diagnostics')]
    with Store(cfg)as s:
        s.connection_started(AT+2)
        send(s,market,make_raw('0.4',AT-2),AT+3,c)
        assert not s.ready_tokens
        send(s,market,make_raw('0.9',AT+4),AT+4,c)
        assert not s.stats()['events']
        assert all(r in [tuple(x) for x in s.c.execute('select * from inputs')] for r in original)
        assert all(r in [tuple(x) for x in s.c.execute('select * from diagnostics')] for r in reasons)
        assert s.stats()['sqlite_integrity']=='ok'


def test_repeated_packets_do_not_duplicate_events(cfg,market,make_raw):
    c=Counter()
    with Store(cfg)as s:
        start(s,market,make_raw,c)
        packet=[make_raw('0.51',AT+10),make_raw('0.71',AT+20)]
        send(s,market,packet,AT+20,c);before=s.stats()['events']
        for n in range(5):send(s,market,packet,AT+30+n,c)
        assert s.stats()['events']==before and c['DUPLICATE']==10
        assert s.connected and len(s.ready_tokens)==2


def test_coalesced_high_water_survives_gap(cfg,market,make_raw):
    c=Counter()
    with Store(cfg)as s:
        start(s,market,make_raw,c)
        send(s,market,make_raw('0.49',AT+100),AT+100,c)
        assert c['coalesced_unchanged_top_confirmations']==1
        s.gap('TRUE_TRANSPORT_GAP',AT+101);s.connection_started(AT+102)
        send(s,market,make_raw('0.45',AT+50),AT+103,c)
        assert market['tokens']['YES'] not in s.ready_tokens
        send(s,market,make_raw('0.9',AT+104),AT+104,c)
        assert not s.stats()['events']


@pytest.mark.parametrize('recover',[True,False])
def test_runtime_recovery_has_finite_budget_and_keeps_weather(cfg,market,monkeypatch,recover):
    import threading,time
    from datetime import datetime,timezone
    from zoneinfo import ZoneInfo
    from holy_weather import weather,lamp
    from test_weather import report,receipt
    from test_lamp import bulletin,receipt as lamp_receipt
    stop=threading.Event();calls=[];at=app.millis()-1000
    market=dict(market,date=datetime.now(timezone.utc).astimezone(ZoneInfo(market['timezone'])).date().isoformat())
    monkeypatch.setattr(app,'discover',lambda _: (market,{'synthetic':True}))
    monkeypatch.setattr(app,'BASELINE_RECOVERY_SECONDS',.05)
    def raw(p,t,side='YES'):
        return dict(event_type='best_bid_ask',market=market['condition_id'],asset_id=market['tokens'][side],timestamp=str(t),best_bid='0.4',best_ask=p)
    weather_calls=[]
    def awc():weather_calls.append(1);return receipt([report(5,(at//60000)*60000)],app.millis())
    def lamp_error():return dict(source=lamp.SOURCE,requested_ms=at,received_ms=app.millis(),status='NO_DATA',terminal=True)
    class Socket:
        def __init__(self,n):
            self.n=n;self.frames=[raw('0.49',at),raw('0.49',at,'NO'),raw('0.99',at-1)] if n==0 else ([raw('0.9',at+100,'YES'),raw('0.9',at+100,'NO')] if recover else [])
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def send(self,v):pass
        def recv(self,timeout):
            time.sleep(.01)
            if self.frames:return json.dumps(self.frames.pop(0))
            if self.n==1 and recover:stop.set()
            raise TimeoutError()
    def connect(*args,**kwargs):calls.append(1);return Socket(len(calls)-1)
    result=app.observe(replace(cfg,duration_seconds=5,max_reconnects=1,weather_context=True,lamp_context=True),connect=connect,stop=stop,weather_fetch=awc,lamp_fetch=lamp_error)
    assert len(calls)==2 and result['counters']['reconnects']==1
    assert result['storage']['events']=={} and result['storage']['weather_status']=='OK'
    assert result['storage']['sqlite_integrity']=='ok' and weather_calls==[1]
    assert result['state']==('SIGNAL_STOP' if recover else 'BASELINE_RECOVERY_BUDGET_EXHAUSTED')
    if recover:assert result['counters']['FRESH']==4
