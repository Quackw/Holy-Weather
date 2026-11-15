"""Pending capacity is independent of retained delivery history (no HTTP)."""
import json
from dataclasses import replace
import sqlite3
import pytest
from holy_weather.storage import Store
from holy_weather.alert_queue import PREFIX,LIMIT,enqueue,preview,claim,finish
from holy_weather.telegram import Sender
from test_price_alert import send,AT


def seed(s,m,make):
    s.bind(m,{});s.connection_started(AT)
    send(s,m,make('0.49',AT,bid='0.47'),AT)
    send(s,m,make('0.62',AT+1,bid='0.60'),AT+1)
    return dict(s.c.execute("select * from events where type='PRICE_ALERT'").fetchone())


def copy_event(s,base,eid,m):
    # Explicit synthetic load copies, not detector-generated market evidence.
    p=json.loads(base['payload']);p.pop('delivery_admission',None)
    v=dict(base,id=eid,payload=json.dumps(p))
    s.c.execute('insert into events values(?,?,?,?,?,?,?,?)',tuple(v[k] for k in ('id','input_id','token','received_ms','type','status','reason','payload')))
    enqueue(s,eid,p,m,AT+2)


def test_real_limit_reclaims_final_slots_retains_history_restart(cfg,market,make_raw):
    cfg=replace(cfg,price_alert_enabled=True,alert_queue_enabled=True)
    with Store(cfg) as s:
        base=seed(s,market,make_raw)
        with s.transaction():
            for i in range(1,LIMIT+1):copy_event(s,base,f'load-{i}',market)
        assert s.c.execute('select count(*) from meta where key like ?',(PREFIX+'%',)).fetchone()[0]==LIMIT
        refused=json.loads(s.c.execute('select payload from events where id=?',(f'load-{LIMIT}',)).fetchone()[0])
        assert refused['delivery_admission']['reason']=='QUEUE_FULL'
        original=s.c.execute("select key from meta where key like ? order by key limit 1",(PREFIX+'%',)).fetchone()[0]
        p=preview(s,original);assert claim(s,p,AT+3)
        finish(s,p,{'status':'SENT','message_id':987},AT+4)
        with s.transaction():copy_event(s,base,'after-final',market)
        assert s.c.execute('select count(*) from meta where key like ?',(PREFIX+'%',)).fetchone()[0]==LIMIT+1
        assert not s.c.execute("select 1 from meta where key='alert_queue_status'").fetchone()
        with s.transaction():enqueue(s,f'load-{LIMIT}',refused,market,AT+5)
        assert not s.c.execute("select 1 from meta where key like ? and json_extract(value,'$.event_id')=?",(PREFIX+'%',f'load-{LIMIT}')).fetchone()
        # Explicit old-history pruning must preserve refusal and its evidence too.
        s.prune(AT+40*86400000,apply=True)
        assert s.c.execute('select count(*) from events where type=\'PRICE_ALERT\'').fetchone()[0]==LIMIT+2
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()
        assert s.usage()<16*1024*1024
    with Store(cfg) as s:
        assert not claim(s,p,AT+6) # SENT cannot repeat after reopening.
        assert json.loads(s.c.execute('select value from meta where key=?',(original,)).fetchone()[0])['message_id']==987
        assert json.loads(s.c.execute('select payload from events where id=?',(f'load-{LIMIT}',)).fetchone()[0])==refused


@pytest.mark.parametrize('status',['SENT','REJECTED','EXPIRED','UNKNOWN'])
def test_final_statuses_free_slot_unknown_does_not(cfg,market,make_raw,monkeypatch,status):
    import holy_weather.alert_queue as q
    monkeypatch.setattr(q,'LIMIT',1)
    with Store(replace(cfg,price_alert_enabled=True,alert_queue_enabled=True)) as s:
        base=seed(s,market,make_raw);key=s.c.execute('select key from meta where key like ?',(PREFIX+'%',)).fetchone()[0]
        p=preview(s,key);assert claim(s,p,AT+3)
        finish(s,p,{'status':status},AT+4)
        with s.transaction():copy_event(s,base,'next',market)
        d=json.loads(s.c.execute("select payload from events where id='next'").fetchone()[0])['delivery_admission']
        assert d['reason']==('QUEUE_FULL' if status=='UNKNOWN' else 'ACCEPTED')
        assert not claim(s,p,AT+5)


def test_disabled_has_reason_and_cannot_later_backfill(cfg,market,make_raw):
    with Store(replace(cfg,price_alert_enabled=True)) as s:
        base=seed(s,market,make_raw)
        assert json.loads(base['payload'])['delivery_admission']['reason']=='QUEUE_DISABLED'
        s.cfg=replace(s.cfg,alert_queue_enabled=True)
        with s.transaction():enqueue(s,base['id'],json.loads(base['payload']),market,AT+2)
        assert not s.c.execute('select 1 from meta where key like ?',(PREFIX+'%',)).fetchone()


def test_admission_failure_rolls_back_event_queue_and_fired(cfg,market,make_raw):
    with Store(replace(cfg,price_alert_enabled=True,alert_queue_enabled=True)) as s:
        s.bind(market,{});s.connection_started(AT)
        send(s,market,make_raw('0.49',AT,bid='0.47'),AT)
        s.c.execute("CREATE TEMP TRIGGER reject_admission BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT,'admission'); END")
        with pytest.raises(sqlite3.IntegrityError):send(s,market,make_raw('0.62',AT+1,bid='0.60'),AT+1)
        assert not s.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()
        assert not s.c.execute('select 1 from meta where key like ?',(PREFIX+'%',)).fetchone()
        state=json.loads(s.c.execute('select value from meta where key=?',('price_alert_v1:'+market['tokens']['YES'],)).fetchone()[0])
        assert state['fired']==[]


def test_legacy_receipt_without_admission_still_deduplicates(cfg,market,make_raw,monkeypatch):
    import holy_weather.alert_queue as q
    monkeypatch.setattr(q,'LIMIT',1)
    with Store(replace(cfg,price_alert_enabled=True,alert_queue_enabled=True)) as s:
        base=seed(s,market,make_raw);p=json.loads(base['payload']);p.pop('delivery_admission')
        s.c.execute('update events set payload=? where id=?',(json.dumps(p),base['id']))
        key=s.c.execute('select key from meta where key like ?',(PREFIX+'%',)).fetchone()[0]
        pv=preview(s,key);assert claim(s,pv,AT+3);finish(s,pv,{'status':'SENT','message_id':123},AT+4)
        with s.transaction():
            enqueue(s,base['id'],p,market,AT+5)
            copy_event(s,base,'next-legacy',market)
        assert s.c.execute('select count(*) from meta where key like ?',(PREFIX+'%',)).fetchone()[0]==2
        assert not claim(s,pv,AT+6)
        assert s.c.execute('pragma user_version').fetchone()[0]==1
