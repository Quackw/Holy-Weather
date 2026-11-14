from collections import Counter
from dataclasses import replace
from datetime import datetime, timezone
import json
import sqlite3
import threading
import time
from urllib.error import HTTPError
import pytest
from holy_weather import app, weather
from holy_weather.core import canonical, normalize, sha
from holy_weather.storage import Store

AT=int(datetime(2026,11,14,12,tzinfo=timezone.utc).timestamp()*1000)


def report(temp=10, observed=AT-60000, *, station='KLGA',cor=False):
    dt=datetime.fromtimestamp(observed/1000,timezone.utc)
    whole=f'{abs(round(temp)):02d}';whole=('M' if temp<0 else '')+whole
    return dict(icaoId=station,obsTime=observed//1000,temp=temp,metarType='METAR',
                rawOb=f'{station} {dt:%d%H%M}Z '+('COR ' if cor else '')+f'00000KT 10SM CLR {whole}/05 A3000')


def receipt(rows, received=AT, status='OK'):
    raw=json.dumps(rows)
    return dict(source=weather.SOURCE,requested_ms=received-100,received_ms=received,
                status=status,raw_body=raw,sha256=sha(raw.encode()))


def enabled(cfg):return replace(cfg,weather_context=True)


def feed(store,market,make_raw,price,at):
    return store.apply(normalize(make_raw(price,at,bid='0.4'),at,market))


def test_raw_temperature_units_and_receipt(cfg,market):
    with Store(enabled(cfg)) as s:
        s.record_weather(receipt([report(-3)]),market,AT+1)
        state=s.weather_state();r=state['records'][0]
        assert r['station']=='KLGA' and r['temperature_c']=='-3' and r['units']=='C'
        assert r['received_ms']==AT and r['available_ms']==AT+1
        assert r['observed_ms']==AT-60000 and 'M03/05' in r['raw_metar']
        assert s.c.execute('pragma user_version').fetchone()[0]==1


def test_event_weather_frozen_and_strictly_causal(cfg,market,make_raw):
    with Store(enabled(cfg)) as s:
        s.bind(market,{'synthetic':True})
        s.record_weather(receipt([report(9,AT-3600000),report(10)],AT),market,AT+1)
        feed(s,market,make_raw,'0.49',AT+10)
        s.record_weather(receipt([report(11,AT)],AT+20),market,AT+21)
        events=feed(s,market,make_raw,'0.60',AT+30)['events']
        ctx=events[0]['weather']
        assert ctx['latest']['temperature_c']=='11' and ctx['previous']['temperature_c']=='10'
        assert ctx['temperature_change_c']=='1' and ctx['observed_max_c']=='11'
        assert ctx['new_observation_in_interval'] and ctx['official_daily_tmax'] is None
        assert ctx['freshness']=='FRESH' and ctx['completeness']=='PARTIAL_RECEIVED_OBSERVATIONS'
        before=s.c.execute('select * from events order by id').fetchall()
        # Old observation delivered late must not revise the saved event.
        s.record_weather(receipt([report(15,AT-120000)],AT+100),market,AT+101)
        assert s.c.execute('select * from events order by id').fetchall()==before
        for e in events:
            for ref in e['weather']['input_ids']:
                assert s.c.execute('select received_ms from inputs where id=?',(ref,)).fetchone()[0]<AT+30
        assert s.stats()['foreign_key_errors']==0


def test_duplicate_and_correction_do_not_invent_new_observation(cfg,market,make_raw):
    with Store(enabled(cfg)) as s:
        s.record_weather(receipt([report(9,AT-3600000),report(10)]),market,AT+1)
        feed(s,market,make_raw,'0.49',AT+10)
        s.record_weather(receipt([report(10)],AT+20),market,AT+21)
        ctx=feed(s,market,make_raw,'0.60',AT+30)['events'][0]['weather']
        assert not ctx['new_observation_in_interval']
        assert len(s.weather_state()['records'])==2
        import zlib
        row=s.c.execute("select raw from inputs where identity like 'AWC:%' order by id desc limit 1").fetchone()
        assert json.loads(zlib.decompress(row[0]))['report_flags'][0]['duplicate']
        s.record_weather(receipt([report(8,cor=True)],AT+40),market,AT+41)
        state=s.weather_state()
        ctx=weather.context(state,AT+50,AT+30)
        assert ctx['observed_max_c']=='9' and ctx['latest']['temperature_c']=='8'
        assert ctx['correction_in_interval'] and not ctx['new_observation_in_interval']
        s.record_weather(receipt([report(10)],AT+60),market,AT+61)
        assert s.weather_state()['records'][-1]['temperature_c']=='8'


@pytest.mark.parametrize('case', ['station','raw_station','future','raw_clock','nan','raw_temp','sha','too_many','source','receipt'])
def test_bad_weather_explicit_not_market_failure(cfg,market,make_raw,case):
    r=report();rec=receipt([r])
    if case=='station':r['icaoId']='KJFK'
    elif case=='raw_station':r['rawOb']=r['rawOb'].replace('KLGA','KJFK')
    elif case=='future':r['obsTime']=AT//1000+100
    elif case=='raw_clock':r['obsTime']-=3600
    elif case=='nan':r['temp']=float('nan')
    elif case=='raw_temp':r['temp']=50
    if case in ('station','raw_station','future','raw_clock','nan','raw_temp'):rec=receipt([r])
    elif case=='sha':rec['sha256']='bad'
    elif case=='too_many':rec=receipt([r]*129)
    elif case=='source':rec['source']='https://other.invalid/'
    elif case=='receipt':rec['requested_ms']=AT+200
    with Store(enabled(cfg)) as s:
        s.record_weather(rec,market,AT+1)
        assert s.weather_status()=='INVALID_DATA'
        feed(s,market,make_raw,'0.49',AT+10)
        events=feed(s,market,make_raw,'0.60',AT+20)['events']
        assert events and events[0]['weather']['freshness']=='MISSING'


def test_stale_no_data_and_previous_day_are_distinct(cfg,market):
    with Store(enabled(cfg)) as s:
        s.record_weather(receipt([report(20,AT-86400000),report(10,AT-7200000)]),market,AT+1)
        ctx=weather.context(s.weather_state(),AT+10,AT)
        assert ctx['freshness']=='STALE' and ctx['observed_max_c']=='10'
        s.record_weather(receipt([],AT+20),market,AT+21)
        ctx=weather.context(s.weather_state(),AT+30,AT)
        assert ctx['provider_status']=='NO_DATA' and ctx['freshness']=='STALE'


def test_conflicting_noncor_report_is_not_silently_replaced(cfg,market):
    with Store(enabled(cfg)) as s:
        s.record_weather(receipt([report(10)]),market,AT+1)
        s.record_weather(receipt([report(11)],AT+20),market,AT+21)
        ctx=weather.context(s.weather_state(),AT+30,AT)
        assert ctx['unverified_conflicts'] and ctx['freshness']=='MISSING'


def test_restart_preserves_weather_and_event_refs(cfg,market,make_raw):
    cfg=enabled(cfg)
    with Store(cfg) as s:
        s.record_weather(receipt([report()]),market,AT+1)
        state=s.weather_state()
    with Store(cfg) as s:
        assert s.weather_state()==state
        feed(s,market,make_raw,'0.49',AT+10)
        feed(s,market,make_raw,'0.60',AT+20)
        s.prune(AT+2*86400000,apply=True)
        assert not s.c.execute('pragma foreign_key_check').fetchall()
        assert s.weather_state()==state


def test_weather_write_rollback_keeps_market_history(cfg,market,make_raw):
    with Store(enabled(cfg)) as s:
        feed(s,market,make_raw,'0.49',AT)
        before=s.c.execute('select * from inputs').fetchall()
        s.c.execute("create trigger fail_weather before insert on meta when new.key='weather_context_v1' begin select raise(abort,'synthetic');end")
        with pytest.raises(sqlite3.IntegrityError):s.record_weather(receipt([report()]),market,AT+1)
        assert s.c.execute('select * from inputs').fetchall()==before
        assert not s.weather_state()
        s.c.execute('drop trigger fail_weather')
        assert feed(s,market,make_raw,'0.60',AT+20)['events']


@pytest.mark.parametrize('status',[204,403,429,500])
def test_http_statuses_bounded_no_retry(status):
    calls=[]
    class Response:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def read(self,n):return b''
    Response.status=status
    def fetch(*a,**k):
        calls.append(1)
        if status!=204:raise HTTPError(weather.SOURCE,status,'test',None,None)
        return Response()
    result=weather.fetch_weather(fetch=fetch,clock=lambda:AT)
    assert len(calls)==1
    assert result['status']=='NO_DATA' if status==204 else result['terminal']


def test_strict_weather_availability_boundary(cfg,market):
    with Store(enabled(cfg)) as s:
        s.record_weather(receipt([report()],AT),market,AT+100)
        assert weather.context(s.weather_state(),AT+100,AT)['latest'] is None
        assert weather.context(s.weather_state(),AT+101,AT)['latest'] is not None


def test_market_loop_survives_complete_awc_failure(cfg,market,make_raw,monkeypatch):
    now=app.millis();stop=threading.Event();calls=[]
    market=dict(market,date=datetime.now(timezone.utc).astimezone(__import__('zoneinfo').ZoneInfo(market['timezone'])).date().isoformat())
    monkeypatch.setattr(app,'discover',lambda _: (market,{'synthetic':True}))
    def failed():
        calls.append(1)
        return dict(source=weather.SOURCE,requested_ms=now,received_ms=app.millis(),status='NETWORK_OR_DECODE_ERROR',terminal=True)
    class Socket:
        def __init__(self):self.i=0
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,v):pass
        def recv(self,timeout):
            time.sleep(.01);self.i+=1
            if self.i>5:stop.set();raise TimeoutError()
            return json.dumps(make_raw('0.49' if self.i<4 else '0.60',app.millis(),bid='0.4'))
    result=app.observe(enabled(cfg),connect=lambda *a,**k:Socket(),stop=stop,weather_fetch=failed)
    assert len(calls)==1 and result['storage']['weather_status']=='NETWORK_OR_DECODE_ERROR'
    assert result['storage']['events']['DETECTED']>=1 and result['storage']['sqlite_integrity']=='ok'


def test_slow_weather_does_not_block_or_spawn_more_workers(cfg,market):
    release=threading.Event();entered=threading.Event();calls=[]
    def slow():
        calls.append(1);entered.set();release.wait(2)
        return dict(source=weather.SOURCE,requested_ms=AT,received_ms=AT,status='NO_DATA',terminal=True)
    poller=weather.WeatherPoller(slow,monotonic=lambda:1000)
    try:
        with Store(enabled(cfg)) as s:
            before=time.monotonic();poller.tick(s,market,AT)
            assert time.monotonic()-before<.5 and entered.wait(.5)
            for _ in range(10):poller.tick(s,market,AT)
            assert len(calls)==1 and poller.queue.maxsize==1
    finally:
        poller.close();release.set();poller.thread.join(1)


def test_real_time_source_and_receipt_are_never_substituted():
    row=report(10,AT-3600000)
    r=receipt([row]);decoded=weather.decode(r,{'station':'KLGA','timezone':'America/New_York'},AT+100)[0]
    assert decoded['observed_ms']==AT-3600000 and decoded['received_ms']==AT and decoded['available_ms']==AT+100


def test_conflict_evidence_survives_new_poll_and_explicit_retention(cfg,market,make_raw):
    with Store(enabled(cfg)) as s:
        s.record_weather(receipt([report(10)]),market,AT+1)
        s.record_weather(receipt([report(11)],AT+10),market,AT+11)
        conflict_id=s.weather_state()['records'][0]['conflict_input_id']
        s.record_weather(receipt([],AT+20),market,AT+21)
        s.prune(AT+2*86400000,apply=True)
        assert s.c.execute('select 1 from inputs where id=?',(conflict_id,)).fetchone()
        ctx=weather.context(s.weather_state(),AT+30,AT)
        assert conflict_id in ctx['input_ids'] and ctx['unverified_conflicts']


def test_weather_journal_cap_leaves_market_processing_alive(cfg,market,make_raw):
    with Store(enabled(cfg)) as s:
        # Represent already occupied compressed weather history without inflating the directory budget.
        s.c.execute('insert into inputs(identity,received_ms,raw,raw_sha) values(?,?,?,?)',('AWC:capacity-fixture',AT,b'x'*(4*1024*1024),'synthetic'))
        s.record_weather(receipt([report()]),market,AT+1)
        assert s.weather_status()=='WEATHER_STATE_BOUND'
        feed(s,market,make_raw,'0.49',AT+10)
        assert feed(s,market,make_raw,'0.60',AT+20)['events']
        assert s.usage()<cfg.storage_bytes


def test_future_receipt_error_does_not_enter_event_evidence(cfg,market,make_raw):
    with Store(enabled(cfg)) as s:
        s.record_weather(receipt([report()],AT+1000),market,AT+1)
        feed(s,market,make_raw,'0.49',AT+10)
        ctx=feed(s,market,make_raw,'0.60',AT+20)['events'][0]['weather']
        assert ctx['latest'] is None and ctx['input_ids']==[]
        assert ctx['provider_status']=='NOT_YET_AVAILABLE'
