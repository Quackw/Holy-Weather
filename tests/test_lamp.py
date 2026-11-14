from dataclasses import replace
from datetime import datetime,timedelta,timezone
from decimal import Decimal
import json
import sqlite3
from urllib.error import HTTPError
import pytest
from holy_weather import lamp,weather
from holy_weather.core import sha,normalize
from holy_weather.storage import Store
from test_weather import AT,report,receipt as weather_receipt

ISSUE=AT-1800000  # 11:30 UTC; first valid 12:00 UTC


def enabled(cfg):return replace(cfg,weather_context=True,lamp_context=True)


def bulletin(values=None,issue=ISSUE,station='KLGA',count=25):
    issued=datetime.fromtimestamp(issue/1000,timezone.utc)
    first=issued.replace(minute=0)+timedelta(hours=1)
    hours=[(first+timedelta(hours=i)).hour for i in range(count)]
    values=values if values is not None else [46]*count
    return '<PRE>\n '+station+' NEW YORK GFS LAMP '+issued.strftime('%H%M UTC  %m/%d/%Y')+'\n UTC '+''.join(f' {h:02d}'for h in hours)+'\n TMP '+''.join(f'{t:3d}'for t in values)+'\n</PRE>'


def receipt(body=None,received=AT):
    body=bulletin() if body is None else body
    return dict(source=lamp.SOURCE,requested_ms=received-100,received_ms=received,status='OK',raw_body=body,sha256=sha(body.encode()))


def observed(maximum='5',latest='5'):
    return dict(observed_max_c=maximum,latest={'temperature_c':latest},freshness='FRESH',completeness='PARTIAL_RECEIVED_OBSERVATIONS')


def parse(body,market,at=AT):return lamp.decode(receipt(body,at),market,at+1)


def test_original_tmp_not_clamped_to_observed_maximum(market):
    state=parse(bulletin([40]*25),market)
    f=lamp.context(state,observed('10','9'),market,AT+2)
    assert f['future_peak_f']=='40' and f['observed_max_f']=='50.0'
    assert f['peak_minus_observed_max_f']=='-10.0' and f['lamp_below_observed_max']
    assert f['metar_temperature_f']=='48.2'


def test_remaining_hours_excludes_past_and_next_local_day(market):
    values=[100]+[46]*16+[99]*8
    state=parse(bulletin(values),market)
    f=lamp.context(state,observed(),market,AT+2)
    assert f['future_peak_f']=='46'
    assert all(h['valid_ms']>AT+2 and h['local_time'][:10]==market['date']for h in f['hours'])
    assert len(f['hours'])==16  # 08:00 through 23:00 local


@pytest.mark.parametrize('case,status',[('missing','MISSING_TMP'),('partial','PARTIAL_TMP'),('stale','STALE'),('empty_day','NO_REMAINING_HOURS')])
def test_no_invented_peak(market,case,status):
    text=bulletin();at=AT+2
    if case=='missing':text='\n'.join(line for line in text.splitlines()if 'TMP'not in line)
    if case=='partial':text=bulletin([46,999]+[46]*23)
    state=parse(text,market)
    if case=='stale':at=ISSUE+lamp.MAX_AGE_MS+1
    if case=='empty_day':market=dict(market,date='2026-11-13')
    result=lamp.context(state,observed(),market,at)
    assert result['status']==status and result['future_peak_f']is None and result['peak_minus_observed_max_f']is None


def test_fixed_width_negative_and_three_digit_values(market):
    state=parse(bulletin([-10,100,101,999]+[46]*21),market)
    assert [h['temperature_f']for h in state['hours'][:4]]==['-10','100','101',None]


@pytest.mark.parametrize('case',['station','future_issue','bad_hour','short_row','duplicate_tmp','sha','clock','multiple','source'])
def test_invalid_bulletin_is_explicit(market,case):
    body=bulletin()
    if case=='station':body=bulletin(station='KJFK')
    elif case=='future_issue':body=bulletin(issue=AT+3600000)
    elif case=='bad_hour':body=body.replace('UTC  12 13','UTC  12 15')
    elif case=='short_row':body=bulletin([46]*24)
    elif case=='duplicate_tmp':body=body.replace('</PRE>',' TMP '+' 46'*25+'\n</PRE>')
    elif case=='multiple':body+=body
    r=receipt(body)
    if case=='sha':r['sha256']='bad'
    if case=='clock':r['received_ms']=AT+10
    if case=='source':r['source']='https://other.invalid/'
    state=lamp.update({},r,market,AT+1)
    assert state['status']=='INVALID_DATA'
    assert lamp.context(state,observed(),market,AT+100)['future_peak_f']is None


def test_receipt_availability_and_issue_clocks_independent(market):
    state=lamp.decode(receipt(),market,AT+100)
    assert state['issue_ms']==ISSUE and state['received_ms']==AT
    assert lamp.context(state,observed(),market,AT+100)['status']=='NOT_YET_AVAILABLE'
    assert lamp.context(state,observed(),market,AT+101)['future_peak_f']=='46'


def test_duplicate_does_not_reset_forecast_receipt_or_age(market):
    first=lamp.update({},receipt(),market,AT+1)
    again=lamp.update(first,receipt(received=AT+10000),market,AT+10001)
    assert again['duplicate'] and again['received_ms']==AT and again['available_ms']==AT+1
    assert lamp.context(again,observed(),market,ISSUE+lamp.MAX_AGE_MS+1)['status']=='STALE'


def test_issue_regression_after_error_cannot_select_old_cycle(market):
    s=lamp.update({},receipt(),market,AT+1)
    error=dict(source=lamp.SOURCE,requested_ms=AT,received_ms=AT+10,status='NO_DATA')
    s=lamp.update(s,error,market,AT+11)
    s=lamp.update(s,receipt(bulletin(issue=ISSUE-3600000),AT+20),market,AT+21)
    assert s['status']=='ISSUE_REGRESSION'


def test_utc_rollover_and_dst_fold(market):
    issue=int(datetime(2026,11,1,3,30,tzinfo=timezone.utc).timestamp()*1000)
    state=parse(bulletin(issue=issue),dict(market,date='2026-11-01'),issue+1000)
    local=[h['local_time']for h in state['hours']]
    assert '2026-11-01T01:00:00-04:00'in local and '2026-11-01T01:00:00-05:00'in local
    assert all(b['valid_ms']-a['valid_ms']==3600000 for a,b in zip(state['hours'],state['hours'][1:]))


def test_bucket_real_thresholds_and_signed_distances(market):
    b=lamp.bucket_context(market,'41','46')
    assert (b['lower_f'],b['upper_f'])==('45.5','47.5')
    assert b['lower_minus_observed_f']=='4.5' and b['upper_minus_peak_f']=='1.5'
    assert b['lower_inclusive'] and not b['upper_inclusive']
    assert lamp.bucket_context(dict(market,rules='unknown'),'41','46')['status']=='UNVERIFIED_RULES'


def test_event_context_frozen_before_late_forecast_and_restart(cfg,market,make_raw):
    cfg=enabled(cfg)
    with Store(cfg)as s:
        s.bind(market,{'synthetic':True})
        s.record_weather(weather_receipt([report(5)]),market,AT+1)
        s.record_lamp(receipt(),market,AT+2)
        for p,t in [('0.49',AT+10),('0.60',AT+20)]:result=s.apply(normalize(make_raw(p,t),t,market))
        assert result['events'][0]['lamp']['future_peak_f']=='46'
        before=s.c.execute('select * from events order by id').fetchall()
        s.record_lamp(receipt(bulletin([55]*25),AT+30),market,AT+31)
        assert s.c.execute('select * from events order by id').fetchall()==before
        state=s.lamp_state()
    with Store(cfg)as s:
        assert s.lamp_state()==state and s.stats()['sqlite_integrity']=='ok'
        assert s.forecast_card(AT+40)['lamp']['future_peak_f']=='55'
        s.prune(AT+2*86400000,apply=True)
        assert s.stats()['foreign_key_errors']==0


def test_lamp_rollback_and_budget_keep_market_data(cfg,market,make_raw):
    with Store(enabled(cfg))as s:
        s.bind(market,{'synthetic':True})
        s.c.execute("create trigger fail_lamp before insert on meta when new.key='lamp_context_v1' begin select raise(abort,'synthetic');end")
        before=s.c.execute('select * from inputs').fetchall()
        with pytest.raises(sqlite3.IntegrityError):s.record_lamp(receipt(),market,AT+1)
        assert s.c.execute('select * from inputs').fetchall()==before and not s.lamp_state()
        s.c.execute('drop trigger fail_lamp')
        s.c.execute('insert into inputs(identity,received_ms,raw,raw_sha)values(?,?,?,?)',('LAMP:capacity-fixture',AT,b'x'*1048576,'synthetic'))
        s.record_lamp(receipt(),market,AT+1)
        assert s.lamp_status()=='LAMP_STATE_BOUND'
        for p,t in [('0.49',AT+10),('0.60',AT+20)]:result=s.apply(normalize(make_raw(p,t),t,market))
        assert result['events'] and s.usage()<cfg.storage_bytes


@pytest.mark.parametrize('code',[204,403,429,500])
def test_lamp_http_failure_no_retry(code):
    calls=[]
    class Response:
        status=204
        def read(self,n):return b''
        def __enter__(self):return self
        def __exit__(self,*a):pass
    def get(*a,**k):
        calls.append(1)
        if code!=204:raise HTTPError(lamp.SOURCE,code,'test',None,None)
        return Response()
    result=lamp.fetch_lamp(fetch=get,clock=lambda:AT)
    assert len(calls)==1 and (result['status']=='NO_DATA'if code==204 else result['terminal'])


def test_opt_in_preserves_previous_rule_hash(cfg):
    assert enabled(cfg).rule_hash()==cfg.rule_hash()
    with pytest.raises(ValueError,match='REQUIRES'):replace(cfg,lamp_context=True)


def test_lamp_failure_does_not_stop_awc_or_prices(cfg,market,make_raw,monkeypatch):
    import threading,time
    from holy_weather import app
    now=app.millis();stop=threading.Event();calls=[]
    market=dict(market,date=datetime.now(timezone.utc).astimezone(__import__('zoneinfo').ZoneInfo(market['timezone'])).date().isoformat())
    monkeypatch.setattr(app,'discover',lambda _: (market,{'synthetic':True}))
    def failed():
        calls.append('lamp')
        return dict(source=lamp.SOURCE,requested_ms=now,received_ms=app.millis(),status='NETWORK_OR_DECODE_ERROR',terminal=True)
    def awc():
        calls.append('awc');return weather_receipt([report(5,(now//60000-1)*60000)],app.millis())
    class Socket:
        def __init__(self):self.i=0
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,v):pass
        def recv(self,timeout):
            time.sleep(.01);self.i+=1
            if self.i>6:stop.set();raise TimeoutError()
            return json.dumps(make_raw('0.49'if self.i<5 else'0.60',app.millis(),bid='0.4'))
    result=app.observe(enabled(cfg),connect=lambda *a,**k:Socket(),stop=stop,weather_fetch=awc,lamp_fetch=failed)
    assert calls.count('lamp')==calls.count('awc')==1
    assert result['storage']['weather_status']=='OK'
    assert result['storage']['events']['DETECTED']>=1
    assert result['storage']['forecast_card']['lamp']['status']=='NETWORK_OR_DECODE_ERROR'


def test_forecast_card_prices_are_not_read_from_future(cfg,market,make_raw):
    with Store(enabled(cfg))as s:
        s.bind(market,{'synthetic':True})
        s.apply(normalize(make_raw('0.49',AT+100),AT+100,market))
        card=s.forecast_card(AT+50)
        assert card['prices']['YES']['ask']is None
        assert card['prices']['NO']['ask']is None
        assert 'н/д' in lamp.format_card(card)


def test_future_invalid_receipt_not_linked_to_event(cfg,market,make_raw):
    with Store(enabled(cfg))as s:
        s.bind(market,{'synthetic':True})
        s.record_lamp(receipt(received=AT+100),market,AT+1)
        for price,t in [('0.49',AT+10),('0.60',AT+20)]:r=s.apply(normalize(make_raw(price,t),t,market))
        assert r['events'][0]['lamp']['input_ids']==[]
        assert r['events'][0]['lamp']['future_peak_f']is None
