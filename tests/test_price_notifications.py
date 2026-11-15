from dataclasses import replace
import json
import sqlite3
import pytest
from holy_weather.storage import Store
from holy_weather.alert_queue import enqueue,preview,claim,finish
from holy_weather.core import canonical
from test_price_alert import send,AT
from test_alert_queue import keys


def cfg_on(cfg):return replace(cfg,price_alert_enabled=True,alert_queue_enabled=True,price_notification_filter=True)
def feed(s,m,make,bid,ask,at,side='YES'):
    send(s,m,make(ask,at,bid=bid,token=m['tokens'][side]),at)
def value(s,k):return json.loads(s.c.execute('select value from meta where key=?',(k,)).fetchone()[0])
def seed(s,m,make):
    s.bind(m,{});s.connection_started(AT)
    feed(s,m,make,'0.48','0.50',AT)


def test_ask_only_retained_not_sent(cfg,market,make_raw):
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.48','0.64',AT+1)
        assert keys(s)==[]
        e=json.loads(s.c.execute("select payload from events where type='PRICE_ALERT'").fetchone()[0])
        assert e['delivery_admission']['reason']=='ASK_ONLY_CROSSING'
        assert e['thresholds']==['0.50','0.55']
        assert s.c.execute('select count(*) from event_inputs').fetchone()[0]>0


def test_bid_growth_batches_sides_restart_and_finality(cfg,market,make_raw):
    cfg=cfg_on(cfg)
    with Store(cfg) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.51','0.53',AT+1)
        feed(s,market,make_raw,'0.59','0.61',AT+1000)
        feed(s,market,make_raw,'0.48','0.50',AT+2000,'NO')
        feed(s,market,make_raw,'0.51','0.53',AT+21000,'NO')
        assert len(keys(s))==1;k=keys(s)[0];v=value(s,k)
        assert len(v['event_ids'])==3 and v['not_before_ms']==AT+30001
        p=preview(s,k);assert 'Both sides moved' in p['html']
        assert not claim(s,p,AT+29000)
    with Store(cfg) as s:
        p=preview(s,k);assert claim(s,p,AT+30001)
        finish(s,p,{'status':'SENT','message_id':1},AT+30002)
    with Store(cfg) as s:
        assert not claim(s,preview(s,k),AT+30003)
        s.prune(AT+40*86400000,apply=True)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==3
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()


def test_merge_failure_rolls_back_event_and_batch(cfg,market,make_raw):
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.51','0.53',AT+1)
        k=keys(s)[0];before=value(s,k)
        s.c.execute("CREATE TEMP TRIGGER bad BEFORE UPDATE ON meta WHEN NEW.key LIKE 'alert_delivery_v1:%' BEGIN SELECT RAISE(ABORT,'fail'); END")
        with pytest.raises(sqlite3.IntegrityError):feed(s,market,make_raw,'0.59','0.61',AT+2)
        assert value(s,k)==before
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1


def test_window_not_extended_and_expired_group_not_reused(cfg,market,make_raw):
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.51','0.53',AT+1)
        feed(s,market,make_raw,'0.59','0.61',AT+30002)
        assert len(keys(s))==2
        for k in keys(s):assert not claim(s,preview(s,k),AT+200000)
        assert all(value(s,k)['status']=='EXPIRED' for k in keys(s))


def test_legacy_hash_and_invalid_config(cfg):
    assert cfg_on(cfg).rule_hash()==replace(cfg,price_alert_enabled=True,alert_queue_enabled=True).rule_hash()
    with pytest.raises(ValueError):replace(cfg,price_notification_filter=1)


def test_sender_wait_does_not_block_lamp(cfg,market,make_raw):
    from holy_weather.telegram import Sender
    from test_lamp_alert import put
    cfg=replace(cfg_on(cfg),weather_context=True,lamp_context=True,lamp_alert_enabled=True)
    calls=[]
    with Store(cfg) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.51','0.53',AT+1)
        put(s,market,[46]*25,AT+2);put(s,market,[48]*25,AT+3)
        sender=Sender('unused',lambda token,p:(calls.append(p) or {'status':'SENT','message_id':1}))
        sender.tick(s,AT+4);sender.thread.join(1);sender.tick(s,AT+5)
        assert len(calls)==1 and 'LAMP ALERT' in calls[0]['html']
        sender.tick(s,AT+30001);sender.thread.join(1);sender.close(s)
        assert len(calls)==2 and 'PRICE ALERT' in calls[1]['html']


def test_unknown_frozen_no_merge_and_restart_no_resend(cfg,market,make_raw):
    cfg=cfg_on(cfg)
    with Store(cfg) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.51','0.53',AT+1)
        k=keys(s)[0];assert claim(s,preview(s,k,test=True),AT+2)
        before=value(s,k)
        feed(s,market,make_raw,'0.59','0.61',AT+3)
        assert len(keys(s))==2 and value(s,k)==before
    with Store(cfg) as s:assert not claim(s,preview(s,k),AT+40000)


def test_capacity_merges_without_losing_history(cfg,market,make_raw,monkeypatch):
    import holy_weather.alert_queue as q
    monkeypatch.setattr(q,'LIMIT',1)
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.51','0.53',AT+1)
        feed(s,market,make_raw,'0.59','0.61',AT+2)
        assert len(keys(s))==1 and len(value(s,keys(s)[0])['event_ids'])==2
        feed(s,market,make_raw,'0.71','0.73',AT+31000)
        reasons=[json.loads(r[0])['delivery_admission']['reason'] for r in s.c.execute("select payload from events where type='PRICE_ALERT'")]
        assert sorted(reasons)==['ACCEPTED','BATCHED','QUEUE_FULL']


def test_disabled_policy_can_read_existing_group_without_duplicate(cfg,market,make_raw):
    cfg=cfg_on(cfg)
    with Store(cfg) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.51','0.53',AT+1)
        feed(s,market,make_raw,'0.59','0.61',AT+2)
        k=keys(s)[0];p=preview(s,k)
        assert claim(s,p,AT+30001);finish(s,p,{'status':'SENT','message_id':1},AT+30002)
    with Store(replace(cfg,price_notification_filter=False)) as s:
        assert not claim(s,preview(s,k),AT+31000)
        assert len(value(s,k)['event_ids'])==2
        assert s.c.execute('pragma user_version').fetchone()[0]==1


@pytest.mark.parametrize('bid,ask,allowed',[('0.47','0.65',False),('0.52','0.54',True),('0.60','0.60',True)])
def test_demand_growth_vs_ask_only(cfg,market,make_raw,bid,ask,allowed):
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,bid,ask,AT+1)
        assert bool(keys(s))==allowed


def test_wait_never_extends_price_expiry(cfg,market,make_raw):
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.51','0.53',AT+1)
        k=keys(s)[0];first=value(s,k)
        feed(s,market,make_raw,'0.59','0.61',AT+20000)
        assert value(s,k)['expires_ms']==first['expires_ms']
        assert not claim(s,preview(s,k),AT+90002)
        assert value(s,k)['status']=='EXPIRED'
