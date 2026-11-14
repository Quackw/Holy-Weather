from dataclasses import replace
from html import unescape
import json
import threading
import time
import pytest
from holy_weather import telegram as tg
from holy_weather.core import normalize
from holy_weather.storage import Store
from test_weather import AT,report,receipt as awc
from test_lamp import receipt as lamp


@pytest.fixture
def prepared(cfg,market,make_raw):
    cfg=replace(cfg,weather_context=True,lamp_context=True)
    with Store(cfg)as s:
        s.bind(market,{'synthetic':True});s.connection_started(AT)
        s.record_weather(awc([report(5)]),market,AT+1);s.record_lamp(lamp(),market,AT+2)
        for side in ('YES','NO'):
            s.apply(normalize(make_raw('0.49',AT+10,token=market['tokens'][side]),AT+10,market))
        s.apply(normalize(make_raw('0.71',AT+20),AT+20,market))
        ids=[r[0]for r in s.c.execute("select id from events where status='DETECTED'")]
        yield s,ids


def test_english_exact_bucket_format_and_no_disclaimer(prepared):
    s,ids=prepared;p=tg.prepare(s,ids[0],at_ms=AT+21)
    text=p['html']
    assert 'Latest METAR:' in text and 'Observed max:' in text and 'Remaining LAMP peak:' in text
    assert 'Information only' not in text and 'trade recommendation' not in text
    assert '46-47°F · YES' in text and 'href="https://polymarket.com/event/' in text
    assert '49¢ → 71¢' in text and '&lt;1s' in text and '<1s' not in text
    assert p['target']=='@holy_weather'


def test_one_update_multiple_event_types_one_attempt(prepared):
    s,ids=prepared;assert len(ids)>=2
    a=tg.prepare(s,ids[0],at_ms=AT+21);b=tg.prepare(s,ids[1],at_ms=AT+21)
    assert a['key']==b['key']
    assert tg.claim(s,a,now_ms=AT+21)
    assert not tg.claim(s,b,now_ms=AT+22)
    record=json.loads(s.c.execute('select value from meta where key=?',(a['key'],)).fetchone()[0])
    assert record['status']=='UNKNOWN'


def test_unknown_survives_restart(prepared):
    s,ids=prepared;p=tg.prepare(s,ids[0],test=True,at_ms=AT+21)
    assert tg.claim(s,p,now_ms=AT+21);cfg=s.cfg;s.close()
    with Store(cfg,session=False)as again:
        assert not tg.claim(again,p,now_ms=AT+22)
        assert again.stats()['sqlite_integrity']=='ok'


def test_stale_or_disconnected_no_normal_card(prepared):
    s,ids=prepared
    with pytest.raises(ValueError):tg.prepare(s,ids[0],at_ms=AT+100000)
    s.gap('DISCONNECTED',AT+21)
    with pytest.raises(ValueError,match='CONFIRMED'):tg.prepare(s,ids[0],at_ms=AT+22)
    p=tg.prepare(s,ids[0],test=True,at_ms=AT+100000)
    assert 'TEST ·' in p['html'] and 'Recorded event' in p['html']


def test_preview_tampering_and_delayed_send_refused(prepared):
    s,ids=prepared;p=tg.prepare(s,ids[0],at_ms=AT+21)
    with pytest.raises(ValueError,match='PREVIEW_CHANGED'):tg.claim(s,dict(p,html='wrong'),now_ms=AT+22)
    with pytest.raises(ValueError,match='EXPIRED'):tg.claim(s,p,now_ms=AT+100000)
    assert not s.c.execute("select 1 from meta where key like 'telegram_delivery:%'").fetchone()


@pytest.mark.parametrize('case',['success','timeout','reject','bad_ack','wrong_chat','oversize','wrong_text','wrong_link'])
def test_http_one_attempt_and_redacted_failure(prepared,case):
    s,ids=prepared;p=tg.prepare(s,ids[0],test=True,at_ms=AT+21);calls=[];secret='123:DO_NOT_LEAK_SECRET_VALUE'
    class Response:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def read(self,n):
            if case=='oversize':return b'x'*65537
            return json.dumps({'ok':case!='reject','result':{'text':unescape(__import__('re').sub(r'<[^>]+>','',p['html'])) if case!='wrong_text' else 'wrong', 'entities':[{'type':'text_link','url':__import__('re').search(r'<a href="([^"]+)">',p['html'])[1] if case!='wrong_link' else 'https://example.com'}], 'date':AT//1000,'message_id':1 if case!='bad_ack' else None,'chat':{'id':-123,'username':'holy_weather' if case!='wrong_chat' else 'wrong','type':'channel'}}}).encode()
    def fetch(request,**kwargs):
        calls.append(1);payload=json.loads(request.data)
        assert payload['chat_id']=='@holy_weather' and payload['parse_mode']=='HTML'
        if case=='timeout':raise TimeoutError(secret)
        return Response()
    result=tg.post(secret,p,fetch=fetch)
    assert len(calls)==1 and secret not in json.dumps(result)
    assert result['status']==('SENT' if case=='success' else 'REJECTED' if case=='reject' else 'UNKNOWN')


def test_ack_and_delivery_delay_saved(prepared):
    s,ids=prepared;p=tg.prepare(s,ids[0],at_ms=AT+21)
    tg.claim(s,p,now_ms=AT+21)
    result=tg.finish(s,p,{'status':'SENT','message_id':7},AT+50)
    assert result['event_to_completion_ms']==30 and result['message_id']==7
    assert result['attempt_to_completion_ms']==29
    assert not tg.claim(s,p,now_ms=AT+51)


def test_background_sender_does_not_write_sqlite_or_block(prepared):
    s,ids=prepared;release=threading.Event();entered=threading.Event();calls=[]
    def transport(token,p):calls.append(1);entered.set();release.wait(2);return {'status':'SENT','message_id':8}
    sender=tg.Sender('synthetic',transport)
    try:
        started=time.monotonic();sender.tick(s,AT+21)
        assert time.monotonic()-started<.5 and entered.wait(.5)
        for _ in range(5):sender.tick(s,AT+22)
        assert calls==[1]
        release.set();sender.thread.join(1);sender.tick(s,AT+23)
        assert sender.status=='SENT' and calls==[1]
        assert s.stats()['sqlite_integrity']=='ok'
    finally:release.set();sender.close()


def test_unknown_delivery_stops_sender_without_retry(prepared):
    s,ids=prepared;calls=[]
    def transport(token,p):calls.append(1);raise TimeoutError('redacted')
    sender=tg.Sender('synthetic',transport);sender.tick(s,AT+21);sender.thread.join(1);sender.tick(s,AT+22)
    assert sender.stopped and sender.status=='UNKNOWN' and calls==[1]


def test_sender_opt_in_preserves_rule_hash(cfg):
    enabled=replace(cfg,weather_context=True,lamp_context=True,telegram_enabled=True)
    assert enabled.rule_hash()==cfg.rule_hash()
    with pytest.raises(ValueError):replace(cfg,telegram_enabled=True)


def test_missing_token_never_prints_value(monkeypatch):
    monkeypatch.setenv('HOLY_WEATHER_TELEGRAM_TOKEN','hidden-invalid-secret')
    with pytest.raises(ValueError,match='^TELEGRAM_TOKEN_NOT_CONFIGURED$'):tg.token_from_env()


def test_event_cannot_be_prepared_before_receipt(prepared):
    s,ids=prepared
    with pytest.raises(ValueError,match='EVENT_NOT_YET_RECEIVED'):tg.prepare(s,ids[0],test=True,at_ms=AT+19)


def test_ledger_cap_disables_sender_without_damaging_database(prepared,monkeypatch):
    s,ids=prepared;monkeypatch.setattr(tg,'MAX_DELIVERIES',0)
    sender=tg.Sender('synthetic',lambda *_:pytest.fail('no request'))
    sender.tick(s,AT+21)
    assert sender.stopped and sender.status=='TELEGRAM_LEDGER_BOUND'
    assert s.stats()['sqlite_integrity']=='ok'


def test_full_monitor_continues_after_telegram_timeout(cfg,market,make_raw,monkeypatch):
    from datetime import datetime,timezone
    from zoneinfo import ZoneInfo
    from holy_weather import app,weather,lamp
    monkeypatch.setenv('HOLY_WEATHER_TELEGRAM_TOKEN','123:'+'A'*24)
    market=dict(market,date=datetime.now(timezone.utc).astimezone(ZoneInfo(market['timezone'])).date().isoformat())
    monkeypatch.setattr(app,'discover',lambda _: (market,{'synthetic':True}))
    stop=threading.Event();calls=[]
    def transport(token,p):calls.append(p);return {'status':'UNKNOWN','reason':'TimeoutError'}
    def empty(source):
        t=app.millis();return dict(source=source,requested_ms=t,received_ms=t,status='NO_DATA',terminal=True)
    class Socket:
        def __init__(self):self.i=0
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def send(self,v):pass
        def recv(self,timeout):
            time.sleep(.01);self.i+=1;t=app.millis()
            if self.i>20:stop.set();raise TimeoutError()
            side='NO' if self.i==2 else 'YES'
            return json.dumps(make_raw('0.49' if self.i<4 else '0.71',t,token=market['tokens'][side]))
    result=app.observe(replace(cfg,weather_context=True,lamp_context=True,telegram_enabled=True),connect=lambda *a,**k:Socket(),stop=stop,
                       weather_fetch=lambda:empty(weather.SOURCE),lamp_fetch=lambda:empty(lamp.SOURCE),telegram_transport=transport)
    assert len(calls)==1 and result['telegram_status']=='UNKNOWN'
    assert result['counters']['frames']==20 and result['storage']['sqlite_integrity']=='ok'
    assert result['storage']['events']['DETECTED']>=1


def test_quote_expiring_before_worker_http_is_not_sent(prepared):
    s,ids=prepared;p=tg.prepare(s,ids[0],at_ms=AT+21)
    result=tg.post('synthetic',p,fetch=lambda *a,**k:pytest.fail('expired request'),clock=lambda:AT+100000)
    assert result=={'status':'REJECTED','reason':'QUOTE_EXPIRED_BEFORE_HTTP'}
