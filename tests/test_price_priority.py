"""Priority changes delivery readiness only; quote detector is untouched."""
import json
from dataclasses import replace
import pytest
from test_price_notifications import cfg_on,feed,value,seed
from test_price_alert import AT
from test_alert_queue import keys
from test_price_confirmation import high
from holy_weather.storage import Store
from holy_weather.alert_queue import preview,claim,finish,advance_waits
from holy_weather.telegram import Sender


@pytest.mark.parametrize('peak,levels',[('0.71',['0.50','0.55','0.60','0.70']),('0.81',['0.50','0.55','0.60','0.70','0.80'])])
def test_bid_confirmed_high_is_immediately_claimable(cfg,market,make_raw,peak,levels):
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,peak,peak,AT+1)
        k=keys(s)[0];v=value(s,k)
        assert claim(s,preview(s,k),AT+1)
        assert v['priority']==1 and v['not_before_ms']==AT+1
        assert not claim(s,preview(s,k),AT+2)
        assert json.loads(s.c.execute("select payload from events where type='PRICE_ALERT'").fetchone()[0])['thresholds']==levels


def test_ask_high_confirmation_has_no_batch_wait(cfg,market,make_raw):
    with Store(replace(cfg_on(cfg),price_confirmation_seconds=8)) as s:
        k=high(s,market,make_raw)
        assert not claim(s,preview(s,k),AT+101)
        feed(s,market,make_raw,'0.57','0.84',AT+2100)
        assert value(s,k)['confirmation']=='CONFIRMED'
        assert claim(s,preview(s,k),AT+2100)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1


def test_ask_high_fallback_ready_at_eight_seconds(cfg,market,make_raw):
    with Store(replace(cfg_on(cfg),price_confirmation_seconds=8)) as s:
        k=high(s,market,make_raw);advance_waits(s,AT+8099)
        assert value(s,k)['status']=='WAITING_BID'
        advance_waits(s,AT+8100)
        assert value(s,k)['confirmation']=='UNCONFIRMED'
        assert 'Ask-only · not confirmed by bid' in preview(s,k)['html']
        assert claim(s,preview(s,k),AT+8100)


@pytest.mark.parametrize('batch',[10,15,30])
def test_regular_group_window_explicit_and_fixed(cfg,market,make_raw,batch):
    with Store(replace(cfg_on(cfg),price_batch_seconds=batch)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.51','0.53',AT+1)
        feed(s,market,make_raw,'0.59','0.61',AT+1000)
        k=keys(s)[0]
        assert len(value(s,k)['event_ids'])==2
        assert not claim(s,preview(s,k),AT+batch*1000)
        assert claim(s,preview(s,k),AT+1+batch*1000)


def test_old_regular_work_cannot_delay_same_side_high(cfg,market,make_raw):
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.51','0.53',AT+1)
        feed(s,market,make_raw,'0.70','0.72',AT+100)
        advance_waits(s,AT+101)
        regular=[k for k in keys(s) if not value(s,k)['priority']];urgent=[k for k in keys(s) if value(s,k)['priority']]
        assert len(regular)==len(urgent)==1
        assert claim(s,preview(s,urgent[0]),AT+101)
        assert not claim(s,preview(s,regular[0]),AT+101)


def test_high_precedes_ready_regular_but_honors_shared_spacing(cfg,market,make_raw,monkeypatch):
    calls=[];at=[AT+30001]
    monkeypatch.setattr('holy_weather.telegram.millis',lambda:at[0])
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.51','0.53',AT+1)
        feed(s,market,make_raw,'0.70','0.72',AT+2)
        sender=Sender('unused',lambda token,p:(calls.append(p) or {'status':'SENT','message_id':len(calls)}),minimum_interval_ms=3100)
        sender.tick(s,at[0]);sender.thread.join(1)
        assert '70%' in calls[0]['html'] and '55%' in calls[0]['html']
        sender.tick(s,at[0]+1);assert len(calls)==1
        at[0]+=3100;sender.tick(s,at[0]);sender.thread.join(1)
        assert len(calls)==2 and 'Crossed: 50%' in calls[1]['html']
        sender.close(s)


@pytest.mark.parametrize('state',['waiting','confirmed','fallback'])
def test_gap_prevents_ask_only_delivery_even_after_settlement(cfg,market,make_raw,state):
    with Store(replace(cfg_on(cfg),price_confirmation_seconds=8)) as s:
        k=high(s,market,make_raw)
        if state=='confirmed':feed(s,market,make_raw,'0.57','0.84',AT+200)
        if state=='fallback':advance_waits(s,AT+8100)
        at=AT+8200 if state=='fallback' else AT+201
        s.gap('TEST_NETWORK',at);s.connection_started(at+1)
        feed(s,market,make_raw,'0.58','0.84',at+2)
        assert value(s,k)['status']=='REJECTED'
        assert value(s,k)['reason']=='CONFIRMATION_GAP:TEST_NETWORK'
        assert not claim(s,preview(s,k),at+3)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1


def test_full_queue_refuses_high_explicitly_and_preserves_event(cfg,market,make_raw,monkeypatch):
    monkeypatch.setattr('holy_weather.alert_queue.LIMIT',1)
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.51','0.53',AT+1)
        feed(s,market,make_raw,'0.70','0.72',AT+2)
        es=[json.loads(r[0]) for r in s.c.execute("select payload from events where type='PRICE_ALERT' order by received_ms")]
        assert '0.70' in es[-1]['thresholds'] and es[-1]['delivery_admission']['reason']=='QUEUE_FULL'
        assert len(es)==2 and len(keys(s))==1
        assert not s.c.execute('pragma foreign_key_check').fetchall()


@pytest.mark.parametrize('bad',[0,5,11,True,60])
def test_batch_configuration_bounded(cfg,bad):
    with pytest.raises(ValueError,match='PRICE_BATCH_WINDOW'):replace(cfg,price_batch_seconds=bad)


@pytest.mark.parametrize('result_status',['UNKNOWN','REJECTED'])
def test_sender_failure_does_not_stop_price_collection(cfg,market,make_raw,monkeypatch,result_status):
    at=[AT+10];monkeypatch.setattr('holy_weather.telegram.millis',lambda:at[0])
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.70','0.72',AT+1)
        calls=[];sender=Sender('unused',lambda token,p:(calls.append(p) or {'status':result_status,'reason':'SYNTHETIC_FAILURE'}))
        sender.tick(s,at[0]);sender.thread.join(1);sender.tick(s,at[0]+1)
        assert sender.stopped and len(calls)==1
        feed(s,market,make_raw,'0.81','0.83',AT+12)
        sender.tick(s,AT+13)
        assert len(calls)==1
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==2
        assert sorted(value(s,k)['status'] for k in keys(s))==sorted([result_status,'PENDING'])
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'


def test_high_own_freshness_not_shortened_by_old_regular(cfg,market,make_raw):
    from test_price_alert import send
    with Store(cfg_on(cfg)) as s:
        s.bind(market,{});s.connection_started(AT+84000)
        send(s,market,make_raw('0.50',AT-1,bid='0.48'),AT+84000)
        send(s,market,make_raw('0.53',AT,bid='0.51'),AT+85000)
        feed(s,market,make_raw,'0.70','0.72',AT+85001)
        advance_waits(s,AT+90001)
        urgent=next(k for k in keys(s) if value(s,k)['priority'])
        assert claim(s,preview(s,urgent),AT+90001)
        assert value(s,urgent)['expires_ms']==AT+175001


def test_local_midnight_expires_ask_wait_before_fallback(cfg,market,make_raw):
    from datetime import datetime,timedelta
    from zoneinfo import ZoneInfo
    midnight=int((datetime.fromisoformat(market['date'])+timedelta(days=1)).replace(tzinfo=ZoneInfo(market['timezone'])).timestamp()*1000)
    with Store(cfg_on(cfg)) as s:
        s.bind(market,{});s.connection_started(midnight-2000)
        feed(s,market,make_raw,'0.56','0.80',midnight-1000)
        feed(s,market,make_raw,'0.56','0.85',midnight-500)
        k=keys(s)[0];advance_waits(s,midnight)
        assert value(s,k)['status']=='EXPIRED'
        assert not claim(s,preview(s,k),midnight)


def test_exact_duplicate_high_does_not_repeat_queue_or_evidence(cfg,market,make_raw):
    from test_price_alert import send
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);raw=make_raw('0.72',AT+1,bid='0.70')
        send(s,market,raw,AT+1)
        before=s.c.execute('select count(*) from event_inputs').fetchone()[0]
        send(s,market,raw,AT+2)
        assert len(keys(s))==1
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
        assert s.c.execute('select count(*) from event_inputs').fetchone()[0]==before


def test_high_sides_independent_in_one_update(cfg,market,make_raw):
    from test_price_alert import send
    with Store(cfg_on(cfg)) as s:
        seed(s,market,make_raw);feed(s,market,make_raw,'0.48','0.50',AT,'NO')
        messages=[make_raw('0.72',AT+1,bid='0.70',token=market['tokens'][side]) for side in ('YES','NO')]
        send(s,market,messages,AT+1)
        assert len(keys(s))==2
        assert {value(s,k)['side'] for k in keys(s)}=={'YES','NO'}
        assert all(claim(s,preview(s,k),AT+1) for k in keys(s))


@pytest.mark.parametrize('settled',['CONFIRMED','UNCONFIRMED'])
@pytest.mark.parametrize('invalid',['missing_ask','missing_bid','sentinel'])
def test_invalid_quote_cancels_settled_ask_only_before_send(cfg,market,make_raw,settled,invalid):
    from test_price_alert import send
    with Store(replace(cfg_on(cfg),price_confirmation_seconds=8)) as s:
        k=high(s,market,make_raw)
        if settled=='CONFIRMED':feed(s,market,make_raw,'0.57','0.84',AT+200)
        else:advance_waits(s,AT+8100)
        assert value(s,k)['confirmation']==settled
        at=AT+8200
        bid,ask={'missing_ask':('0.57',None),'missing_bid':(None,'0.85'),'sentinel':('0','1')}[invalid]
        send(s,market,make_raw(ask,at,bid=bid),at)
        assert value(s,k)['status']=='REJECTED'
        assert value(s,k)['reason'].startswith('CONFIRMATION_INVALID_QUOTE:')
        assert not claim(s,preview(s,k),at+1)
