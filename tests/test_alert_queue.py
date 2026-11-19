from dataclasses import replace
import json,time
from html import unescape
import re
import pytest
from holy_weather.storage import Store
from holy_weather.alert_queue import PREFIX,preview,claim,finish
from holy_weather.telegram import Sender,post
from test_price_alert import send,AT
from test_lamp_alert import put


def config(cfg):return replace(cfg,weather_context=True,lamp_context=True,lamp_alert_enabled=True,price_alert_enabled=True,alert_queue_enabled=True)
def keys(s):return [r[0]for r in s.c.execute('select key from meta where key like ? order by key',(PREFIX+'%',))]
def seed(s,m,make):
    s.bind(m,{});s.connection_started(AT)
    send(s,m,make('0.49',AT,bid='0.47'),AT)
    send(s,m,make('0.62',AT+1,bid='0.60'),AT+1)
    put(s,m,[46]*25,AT+2);put(s,m,[48]*25,AT+3)


def test_two_types_restart_and_unknown_no_retry(cfg,market,make_raw):
    cfg=config(cfg)
    with Store(cfg)as s:
        seed(s,market,make_raw);kk=keys(s);assert len(kk)==2
        cards=[preview(s,k)for k in kk]
        assert any('PRICE ALERT' in p['html']for p in cards)
        assert any('☀️ <b>LAMP · Нью-Йорк' in p['html']for p in cards)
        assert claim(s,cards[0],AT+4)
        finish(s,cards[0],{'status':'SENT','message_id':1},AT+5)
        assert claim(s,cards[1],AT+4) # simulate crash after claim
    with Store(cfg)as s:
        assert not claim(s,cards[0],AT+6)
        assert not claim(s,cards[1],AT+6)
        assert len(keys(s))==2
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'

def test_pending_survives_restart_and_expiry(cfg,market,make_raw):
    cfg=config(cfg)
    with Store(cfg)as s:seed(s,market,make_raw)
    with Store(cfg)as s:
        for k in keys(s):
            p=preview(s,k)
            assert not claim(s,p,AT+86400000)
            assert json.loads(s.c.execute('select value from meta where key=?',(k,)).fetchone()[0])['status']=='EXPIRED'

def test_exact_test_preview_and_tampering(cfg,market,make_raw):
    with Store(config(cfg))as s:
        seed(s,market,make_raw);p=preview(s,keys(s)[0],test=True)
        assert p['html'].startswith('TEST')
        with pytest.raises(ValueError,match='PREVIEW_CHANGED'):claim(s,dict(p,html='changed'),AT+5)
        assert claim(s,p,AT+86400000)

def test_event_and_queue_atomic(cfg,market,make_raw):
    import sqlite3
    with Store(config(cfg))as s:
        s.bind(market,{});s.connection_started(AT)
        send(s,market,make_raw('0.49',AT,bid='0.47'),AT)
        s.c.execute("CREATE TEMP TRIGGER abort_queue BEFORE INSERT ON meta WHEN NEW.key LIKE 'alert_delivery_v1:%' BEGIN SELECT RAISE(ABORT,'queue'); END")
        with pytest.raises(sqlite3.IntegrityError):send(s,market,make_raw('0.62',AT+1,bid='0.60'),AT+1)
        assert not keys(s)
        assert not s.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()

def test_pending_event_retention(cfg,market,make_raw):
    with Store(config(cfg))as s:
        seed(s,market,make_raw)
        s.prune(AT+40*86400000,apply=True)
        assert s.c.execute("select count(*) from events where type in ('PRICE_ALERT','LAMP_ALERT')").fetchone()[0]==2
        assert not s.c.execute('pragma foreign_key_check').fetchall()

def test_worker_failure_does_not_block_collection(cfg,market,make_raw):
    calls=[]
    def fail(token,p):calls.append(1);raise TimeoutError()
    with Store(config(cfg))as s:
        seed(s,market,make_raw);sender=Sender('unused',fail)
        sender.tick(s,AT+5);sender.thread.join(1);sender.tick(s,AT+6)
        send(s,market,make_raw('0.72',AT+7,bid='0.70'),AT+7)
        assert len(calls)==1 and len(keys(s))==3
        sender.tick(s,AT+8);assert len(calls)==1

def test_transport_ack_and_wrong_content(cfg,market,make_raw):
    class Response:
        def __init__(self,x):self.x=x
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def read(self,n):return json.dumps(self.x).encode()
    with Store(config(cfg))as s:
        seed(s,market,make_raw);p=preview(s,keys(s)[0],test=True)
        link=unescape(re.search(r'<a href="([^"]+)">',p['html'])[1])
        ack={'ok':True,'result':{'message_id':1,'date':1794657600,'chat':{'id':-1,'username':'holy_weather','type':'channel'},'text':unescape(re.sub(r'<[^>]+>','',p['html'])),'entities':[{'type':'text_link','url':link}]}}
        assert post('unused',p,fetch=lambda *a,**k:Response(ack))['status']=='SENT'
        ack['result']['text']='wrong'
        assert post('unused',p,fetch=lambda *a,**k:Response(ack))['status']=='UNKNOWN'

def test_sender_claim_before_http(cfg,market,make_raw):
    with Store(config(cfg))as s:
        seed(s,market,make_raw);calls=[]
        def fake(token,p):calls.append(p);return {'status':'SENT'}
        sender=Sender('unused',fake);sender.tick(s,AT+5);sender.thread.join(1);sender.tick(s,AT+6)
        assert calls
        sender.close(s)

def test_queue_bound_keeps_event_and_price_processing(cfg,market,make_raw,monkeypatch):
    import holy_weather.alert_queue as q
    monkeypatch.setattr(q,'LIMIT',1)
    with Store(config(cfg))as s:
        seed(s,market,make_raw)
        assert len(keys(s))==1
        assert s.c.execute("select count(*) from events where type in ('PRICE_ALERT','LAMP_ALERT')").fetchone()[0]==2
        assert json.loads(s.c.execute("select value from meta where key='alert_queue_status'").fetchone()[0])['status']=='FULL'

def test_pending_can_be_claimed_after_restart(cfg,market,make_raw):
    cfg=config(cfg)
    with Store(cfg)as s:seed(s,market,make_raw)
    with Store(cfg)as s:
        p=preview(s,keys(s)[0]);assert claim(s,p,AT+5)
        finish(s,p,{'status':'SENT'},AT+6)
    with Store(cfg)as s:assert not claim(s,p,AT+7)

def test_transport_refuses_expiry_without_http(cfg,market,make_raw):
    with Store(config(cfg))as s:
        seed(s,market,make_raw);p=preview(s,keys(s)[0]);calls=[]
        r=post('unused',p,clock=lambda:p['expires_ms']+1,fetch=lambda *a,**k:calls.append(1))
        assert r['status']=='REJECTED' and not calls

def test_short_cards_without_clock_or_technical_status(cfg,market,make_raw):
    with Store(config(cfg))as s:
        seed(s,market,make_raw)
        for k in keys(s):
            html=preview(s,k)['html']
            if 'PRICE ALERT' in html:
                assert 'Market ↗' in html and len(html.splitlines())==5
            else:
                assert 'Открыть рынок 46–47°F ↗' in html and len(html.splitlines())==7
            assert 'UTC' not in html and 'Kyiv' not in html
            assert 'READY' not in html and 'N/A' not in html


@pytest.mark.parametrize('track',['PRICE_ALERT','LAMP_ALERT'])
@pytest.mark.parametrize('legacy_expiry',[False,True])
def test_claim_before_midnight_cannot_send_on_next_contract_day(cfg,market,make_raw,track,legacy_expiry):
    from datetime import datetime,timedelta
    from zoneinfo import ZoneInfo
    midnight=int((datetime.fromisoformat(market['date'])+timedelta(days=1)).replace(
        tzinfo=ZoneInfo(market['timezone'])).timestamp()*1000)
    with Store(config(cfg))as s:
        s.bind(market,{})
        if track=='PRICE_ALERT':
            at=midnight-1000;s.connection_started(at-1)
            send(s,market,make_raw('0.49',at-1,bid='0.47'),at-1)
            send(s,market,make_raw('0.62',at,bid='0.60'),at)
        else:
            at=midnight-3600001
            put(s,market,[46]*25,at-1,issue=midnight-7200000)
            put(s,market,[48]*25,at,issue=midnight-5400000)
        key=keys(s)[0]
        if legacy_expiry:
            value=json.loads(s.c.execute('select value from meta where key=?',(key,)).fetchone()[0])
            value['expires_ms']=midnight+90000
            s.c.execute('update meta set value=? where key=?',(json.dumps(value),key))
        p=preview(s,key)
        assert json.loads(s.c.execute('select value from meta where key=?',(key,)).fetchone()[0])['track']==track
        assert claim(s,p,midnight-1)
        calls=[]
        result=post('unused',p,clock=lambda:midnight,fetch=lambda *a,**k:calls.append(1))
        assert result['status']=='REJECTED'
        assert not calls


def test_lamp_event_queue_and_forecast_rollback_together(cfg,market):
    import sqlite3
    with Store(config(cfg))as s:
        s.bind(market,{});put(s,market,[46]*25,AT)
        before=s.c.execute('select count(*) from inputs').fetchone()[0]
        s.c.execute("CREATE TEMP TRIGGER abort_lamp_queue BEFORE INSERT ON meta WHEN NEW.key LIKE 'alert_delivery_v1:%' BEGIN SELECT RAISE(ABORT,'queue'); END")
        with pytest.raises(sqlite3.IntegrityError):put(s,market,[48]*25,AT+1)
        assert s.c.execute('select count(*) from inputs').fetchone()[0]==before
        assert not keys(s)
        assert not s.c.execute("select 1 from events where type='LAMP_ALERT'").fetchone()
        s.c.execute('DROP TRIGGER abort_lamp_queue');put(s,market,[48]*25,AT+1)
        assert len(keys(s))==1
        assert not s.c.execute('pragma foreign_key_check').fetchall()


def test_sender_failure_does_not_block_weather_or_lamp(cfg,market,make_raw):
    from test_weather import receipt,report
    def fail(token,p):raise TimeoutError()
    with Store(config(cfg))as s:
        seed(s,market,make_raw);sender=Sender('unused',fail)
        sender.tick(s,AT+5);sender.thread.join(1);sender.tick(s,AT+6)
        assert sender.stopped
        s.record_weather(receipt([report(11)],received=AT+10),market,AT+11)
        put(s,market,[50]*25,AT+20)
        send(s,market,make_raw('0.72',AT+30,bid='0.70'),AT+30)
        assert s.weather_status()=='OK'
        assert len(keys(s))==4
        assert s.c.execute("select count(*) from events where type='LAMP_ALERT'").fetchone()[0]==2
        assert any(json.loads(r[0])['status']=='UNKNOWN' for r in s.c.execute('select value from meta where key like ?',(PREFIX+'%',)))
        sender.close(s)
