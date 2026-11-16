from dataclasses import replace
import json
import sqlite3
import pytest
from test_price_notifications import cfg_on,feed,value
from test_price_alert import AT
from test_alert_queue import keys
from holy_weather.storage import Store
from holy_weather.alert_queue import advance_waits,preview,claim,finish


def high(s,m,make):
    s.bind(m,{});s.connection_started(AT)
    feed(s,m,make,'0.56','0.80',AT)
    feed(s,m,make,'0.56','0.85',AT+100)
    return keys(s)[0]


@pytest.mark.parametrize('seconds,wait_ms',[(5,5000),(8,8000),(10,8000)])
def test_windows_confirm_exact_deadline(cfg,market,make_raw,seconds,wait_ms):
    with Store(replace(cfg_on(cfg),price_confirmation_seconds=seconds)) as s:
        k=high(s,market,make_raw)
        feed(s,market,make_raw,'0.57','0.84',AT+100+wait_ms)
        assert value(s,k)['confirmation']=='CONFIRMED'
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
        assert not s.c.execute('pragma foreign_key_check').fetchall()


@pytest.mark.parametrize('seconds,wait_ms',[(5,5000),(8,8000),(10,8000)])
def test_high_timeout_explicit_warning_and_no_late_upgrade(cfg,market,make_raw,seconds,wait_ms):
    with Store(replace(cfg_on(cfg),price_confirmation_seconds=seconds)) as s:
        k=high(s,market,make_raw);advance_waits(s,AT+101+wait_ms)
        v=value(s,k)
        assert v['status']=='PENDING' and v['confirmation']=='UNCONFIRMED'
        assert 'Ask-only · not confirmed by bid' in preview(s,k)['html']
        feed(s,market,make_raw,'0.57','0.84',AT+102+wait_ms)
        assert value(s,k)['confirmation']=='UNCONFIRMED'
        assert not claim(s,preview(s,k),AT+91000)
        assert value(s,k)['status']=='EXPIRED'


@pytest.mark.parametrize('bad',['same_bid','below_level','other_token','same_source'])
def test_confirmation_requires_later_same_token_bid_and_level(cfg,market,make_raw,bad):
    with Store(cfg_on(cfg)) as s:
        k=high(s,market,make_raw)
        if bad=='same_source':
            from test_price_alert import send
            send(s,market,make_raw('0.84',AT+100,bid='0.57'),AT+101)
        else:feed(s,market,make_raw,'0.56' if bad=='same_bid' else '0.57','0.80' if bad=='below_level' else '0.85',AT+200,'NO' if bad=='other_token' else 'YES')
        assert value(s,k)['confirmation']!='CONFIRMED'


def test_gap_and_restart_never_resurrect(cfg,market,make_raw):
    cfg=cfg_on(cfg)
    with Store(cfg) as s:
        k=high(s,market,make_raw)
    with Store(cfg) as s:
        assert value(s,k)['reason'].startswith('CONFIRMATION_GAP:')
        s.connection_started(AT+200)
        feed(s,market,make_raw,'0.57','0.84',AT+300)
        assert value(s,k)['status']=='REJECTED'
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1


def test_gap_invalidate_atomic_confirmation(cfg,market,make_raw):
    with Store(cfg_on(cfg)) as s:
        k=high(s,market,make_raw);before=value(s,k)
        s.c.execute("CREATE TEMP TRIGGER fail BEFORE UPDATE ON meta WHEN NEW.key LIKE 'alert_delivery_v1:%' BEGIN SELECT RAISE(ABORT,'failed'); END")
        with pytest.raises(sqlite3.IntegrityError):feed(s,market,make_raw,'0.57','0.84',AT+300)
        assert value(s,k)==before
        assert not s.c.execute("select 1 from samples where received_ms=?",(AT+300,)).fetchone()
        s.c.execute('DROP TRIGGER fail')
        s.invalidate('TEST',AT+400,market['tokens']['YES'])
        assert value(s,k)['status']=='REJECTED'


def test_waiting_unknown_dedup_restart(cfg,market,make_raw):
    cfg=cfg_on(cfg)
    with Store(cfg) as s:
        k=high(s,market,make_raw);feed(s,market,make_raw,'0.57','0.84',AT+300)
        advance_waits(s,AT+30100);p=preview(s,k);assert claim(s,p,AT+30100)
    with Store(cfg) as s:
        advance_waits(s,AT+30200)
        assert value(s,k)['status']=='UNKNOWN'
        assert not claim(s,preview(s,k),AT+30200)
        finish(s,p,{'status':'SENT','message_id':42},AT+30300)
        assert not claim(s,preview(s,k),AT+30400)


def test_wait_counts_capacity_and_low_timeout_releases(cfg,market,make_raw,monkeypatch):
    monkeypatch.setattr('holy_weather.alert_queue.LIMIT',1)
    with Store(cfg_on(cfg)) as s:
        s.bind(market,{});s.connection_started(AT)
        feed(s,market,make_raw,'0.48','0.50',AT)
        feed(s,market,make_raw,'0.48','0.54',AT+1)
        feed(s,market,make_raw,'0.48','0.64',AT+2)
        es=[json.loads(r[0]) for r in s.c.execute("select payload from events where type='PRICE_ALERT' order by received_ms")]
        assert es[-1]['delivery_admission']['reason']=='QUEUE_FULL'
        advance_waits(s,AT+5001)
        assert value(s,keys(s)[0])['status']=='REJECTED'
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'


def test_confirmed_and_unconfirmed_group_preserves_evidence(cfg,market,make_raw):
    with Store(cfg_on(cfg)) as s:
        k=high(s,market,make_raw)
        feed(s,market,make_raw,'0.56','0.99',AT+200) # still below 80
        feed(s,market,make_raw,'0.63','0.99',AT+300) # bid-driven 80
        advance_waits(s,AT+5100)
        rows=[value(s,k) for k in keys(s)]
        pending=[v for v in rows if v['status']=='PENDING']
        assert len(pending)==1 and len(pending[0]['event_ids'])==2
        assert len(set(pending[0]['event_ids']))==2
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==2


def test_resolution_cancels_wait_without_crossing(cfg,market,make_raw):
    from test_price_alert import send
    with Store(cfg_on(cfg)) as s:
        k=high(s,market,make_raw)
        send(s,market,{'event_type':'market_resolved','market':market['condition_id']},AT+200)
        assert value(s,k)['reason']=='CONFIRMATION_GAP:MARKET_RESOLVED'
        advance_waits(s,AT+5100)
        assert value(s,k)['status']=='REJECTED'


def test_unconfirmed_warning_survives_same_side_group(cfg,market,make_raw):
    with Store(cfg_on(cfg)) as s:
        k=high(s,market,make_raw)
        advance_waits(s,AT+5100)
        feed(s,market,make_raw,'0.75','0.87',AT+5200)
        advance_waits(s,AT+5300)
        pending=[value(s,k) for k in keys(s) if value(s,k)['status']=='PENDING']
        assert len(pending)==1 and len(pending[0]['event_ids'])==2
        assert 'not confirmed by bid' in pending[0]['html']
        assert 'UNCONFIRMED' in pending[0]['delivery_notes'].values()


def test_wait_does_not_block_lamp(cfg,market,make_raw):
    from holy_weather.telegram import Sender
    from test_lamp_alert import put
    cfg=replace(cfg_on(cfg),weather_context=True,lamp_context=True,lamp_alert_enabled=True)
    calls=[]
    with Store(cfg) as s:
        k=high(s,market,make_raw)
        put(s,market,[46]*25,AT+200);put(s,market,[48]*25,AT+300)
        sender=Sender('unused',lambda token,p:(calls.append(p) or {'status':'SENT','message_id':1}))
        sender.tick(s,AT+400);sender.thread.join(timeout=1)
        assert len(calls)==1 and 'LAMP ALERT' in calls[0]['html']
        assert value(s,k)['status']=='WAITING_BID'


def test_rollback_option_keeps_unknown_and_waiting_finality(cfg,market,make_raw):
    cfg=cfg_on(cfg)
    with Store(cfg) as s:k=high(s,market,make_raw)
    with Store(replace(cfg,price_notification_filter=False)) as s:
        advance_waits(s,AT+5100)
        assert value(s,k)['status']=='REJECTED'
        assert not claim(s,preview(s,k),AT+30100)
        assert s.c.execute('pragma user_version').fetchone()[0]==1
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
