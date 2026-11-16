from dataclasses import replace
import json
import sqlite3
import pytest
from holy_weather.storage import Store
from holy_weather.alert_queue import advance_waits, claim, finish, preview
from test_price_notifications import feed, value
from test_price_alert import AT
from test_alert_queue import keys


def config(cfg):
    return replace(cfg,price_alert_enabled=True,alert_queue_enabled=True,
                   price_notification_filter=True,price_batch_seconds=10)


def start(s,market,make,bid='0.53',ask='0.55'):
    s.bind(market,{});s.connection_started(AT)
    feed(s,market,make,bid,ask,AT)


def delivered(s,key,at,*,test=False):
    advance_waits(s,at)
    p=preview(s,key,test=test);assert claim(s,p,at)
    finish(s,p,{'status':'SENT','message_id':1},at+1)


def test_real_seattle_shape_55_then_50_is_not_another_card(cfg,market,make_raw):
    with Store(config(cfg)) as s:
        start(s,market,make_raw)
        feed(s,market,make_raw,'0.55','0.57',AT+1)
        first=keys(s)[0];delivered(s,first,AT+10001)
        feed(s,market,make_raw,'0.40','0.58',AT+11000)
        feed(s,market,make_raw,'0.51','0.59',AT+12000)
        events=[json.loads(r[0]) for r in s.c.execute("select payload from events where type='PRICE_ALERT' order by received_ms")]
        assert len(events)==2 and events[-1]['thresholds']==['0.50']
        assert events[-1]['delivery_admission']['reason']=='DELIVERED_HIGHER_LEVEL'
        assert events[-1]['delivery_admission']['covered_by']['queue_key']==first
        assert len(keys(s))==1 and value(s,first)['status']=='SENT'
        fired=json.loads(s.c.execute("select value from meta where key=?",('price_alert_v1:'+market['tokens']['YES'],)).fetchone()[0])['fired']
        assert set(fired)=={'0.50','0.55'}


def test_pending_lower_is_blocked_after_priority_ack(cfg,market,make_raw):
    with Store(config(cfg)) as s:
        start(s,market,make_raw,'0.48','0.50')
        feed(s,market,make_raw,'0.51','0.53',AT+1)
        low=keys(s)[0];old_preview=preview(s,low)
        feed(s,market,make_raw,'0.71','0.73',AT+2)
        high=next(k for k in keys(s) if k!=low)
        delivered(s,high,AT+3)
        assert not claim(s,old_preview,AT+10001)
        assert value(s,low)['reason']=='DELIVERED_HIGHER_LEVEL'
        assert value(s,high)['status']=='SENT'


def test_ordinary_grouping_keeps_all_references_and_priority_is_immediate(cfg,market,make_raw):
    with Store(config(cfg)) as s:
        start(s,market,make_raw,'0.48','0.50')
        feed(s,market,make_raw,'0.51','0.53',AT+1)
        feed(s,market,make_raw,'0.56','0.58',AT+2000)
        feed(s,market,make_raw,'0.61','0.63',AT+3000)
        low=keys(s)[0]
        assert len(value(s,low)['event_ids'])==3
        assert value(s,low)['not_before_ms']==AT+10001
        feed(s,market,make_raw,'0.71','0.73',AT+3001)
        high=next(k for k in keys(s) if k!=low)
        assert claim(s,preview(s,high),AT+3001)
        assert value(s,high)['event_ids']
        assert s.c.execute('select count(*) from event_inputs').fetchone()[0]>=8


def test_new_priority_survives_lower_ack_and_partial_multi_cross(cfg,market,make_raw):
    with Store(config(cfg)) as s:
        start(s,market,make_raw)
        feed(s,market,make_raw,'0.55','0.57',AT+1)
        delivered(s,keys(s)[0],AT+10001)
        feed(s,market,make_raw,'0.48','0.50',AT+11000)
        feed(s,market,make_raw,'0.71','0.73',AT+12000)
        high=next(k for k in keys(s) if value(s,k)['status']=='PENDING')
        p=preview(s,high)
        assert 'Crossed: 60% · 70%' in p['html'] and '50%' not in p['html']
        assert claim(s,p,AT+12000)
        finish(s,p,{'status':'SENT','message_id':2},AT+12001)
        feed(s,market,make_raw,'0.81','0.83',AT+12002)
        top=next(k for k in keys(s) if value(s,k)['status']=='PENDING')
        assert '80%' in preview(s,top)['html'] and claim(s,preview(s,top),AT+12002)


@pytest.mark.parametrize('legacy',[False,True])
@pytest.mark.parametrize('test_send',[False,True])
def test_restart_and_test_receipts_do_not_mask_real_priority(cfg,market,make_raw,test_send,legacy):
    cfg=config(cfg)
    with Store(cfg) as s:
        start(s,market,make_raw,'0.77','0.81')
        feed(s,market,make_raw,'0.81','0.83',AT+1)
        delivered(s,keys(s)[0],AT+2,test=test_send)
        if legacy:
            k=keys(s)[0];v=value(s,k);v.pop('test');v.pop('notification_thresholds')
            s.c.execute('update meta set value=? where key=?',(json.dumps(v),k))
    with Store(cfg) as s:
        s.connection_started(AT+3)
        feed(s,market,make_raw,'0.67','0.71',AT+4)
        feed(s,market,make_raw,'0.71','0.73',AT+5)
        e=json.loads(s.c.execute("select payload from events order by received_ms desc limit 1").fetchone()[0])
        if test_send:
            assert e['delivery_admission']['status']=='QUEUED'
        else:
            assert e['delivery_admission']['reason']=='DELIVERED_HIGHER_LEVEL'
            assert e['delivery_admission']['covered_by']['threshold']=='0.80'


def test_unknown_higher_does_not_claim_delivery_of_lower_priority(cfg,market,make_raw):
    with Store(config(cfg)) as s:
        start(s,market,make_raw,'0.77','0.81')
        feed(s,market,make_raw,'0.81','0.83',AT+1)
        old=keys(s)[0];p=preview(s,old);assert claim(s,p,AT+2)
        feed(s,market,make_raw,'0.67','0.71',AT+3)
        feed(s,market,make_raw,'0.71','0.73',AT+4)
        new=next(k for k in keys(s) if k!=old)
        assert value(s,old)['status']=='UNKNOWN' and claim(s,preview(s,new),AT+4)


def test_suppression_failure_rolls_back_new_event_threshold_and_input(cfg,market,make_raw):
    with Store(config(cfg)) as s:
        start(s,market,make_raw)
        feed(s,market,make_raw,'0.55','0.57',AT+1)
        delivered(s,keys(s)[0],AT+10001)
        feed(s,market,make_raw,'0.40','0.58',AT+11000)
        before={t:[tuple(r) for r in s.c.execute('select * from '+t+' order by rowid')] for t in ('events','inputs','samples','meta')}
        s.c.execute("CREATE TEMP TRIGGER bad BEFORE UPDATE ON events WHEN json_extract(NEW.payload,'$.delivery_admission.reason')='DELIVERED_HIGHER_LEVEL' BEGIN SELECT RAISE(ABORT,'fail'); END")
        with pytest.raises(sqlite3.IntegrityError):feed(s,market,make_raw,'0.51','0.59',AT+12000)
        assert before=={t:[tuple(r) for r in s.c.execute('select * from '+t+' order by rowid')] for t in before}


def test_market_side_date_and_track_are_independent(gamma,tmp_path):
    from test_catalogue import setup,feed as push
    from holy_weather.catalogue import CatalogueStore,Router
    cfg,markets=setup(gamma,tmp_path,2);cfg=config(cfg)
    a,b=[m for m,_ in markets]
    with CatalogueStore(cfg) as s:
        for m,r in markets:s.bind(m,r)
        s.connection_started(AT);router=Router(s)
        push(router,a,'0.54',AT+1);push(router,a,'0.56',AT+2)
        delivered(s,keys(s)[0],AT+10002)
        for m,side in ((a,'NO'),(b,'YES')):
            push(router,m,'0.49',AT+11000,side);push(router,m,'0.51',AT+12000,side)
        assert len(keys(s))==3
        assert sum(value(s,k)['status']=='PENDING' for k in keys(s))==2
        # Even a matching old URL/token cannot let a different contract date suppress today.
        k=next(k for k in keys(s) if value(s,k)['status']=='SENT')
        v=value(s,k);v['market_date']='2026-11-13'
        s.c.execute('update meta set value=? where key=?',(json.dumps(v),k))
        push(router,a,'0.49',AT+13000);push(router,a,'0.51',AT+14000)
        assert len(keys(s))==4


def test_pending_preview_is_regenerated_if_only_lower_batch_levels_are_covered(cfg,market,make_raw):
    with Store(config(cfg)) as s:
        start(s,market,make_raw,'0.48','0.50')
        feed(s,market,make_raw,'0.51','0.53',AT+1)
        low=keys(s)[0]
        feed(s,market,make_raw,'0.61','0.63',AT+2)
        original=preview(s,low)
        # A retained compatible receipt from before this queued batch: high=55.
        from holy_weather.alert_queue import card
        v=value(s,low)
        e=dict(s.c.execute('select * from events where id=?',(v['event_id'],)).fetchone())
        payload=json.loads(e['payload']);payload['thresholds']=['0.55']
        payload['previous'].update(bid='0.53',ask='0.55',midpoint='0.54')
        payload['current'].update(bid='0.55',ask='0.57',midpoint='0.56')
        e.update(id='compatible-sent55',payload=json.dumps(payload))
        s.c.execute('insert into events values(?,?,?,?,?,?,?,?)',tuple(e[k] for k in ('id','input_id','token','received_ms','type','status','reason','payload')))
        v.update(status='SENT',event_id=e['id'],event_ids=[e['id']],notification_thresholds=['0.55'],html=card(payload,market,AT+3)[0])
        other='alert_delivery_v1:compatible-proof'
        s.c.execute('insert into meta values(?,?)',(other,json.dumps(v)))
        v['preview_sha256']=preview(s,other)['preview_sha256']
        s.c.execute('update meta set value=? where key=?',(json.dumps(v),other))
        assert not claim(s,original,AT+10001)
        changed=preview(s,low)
        assert 'Crossed: 60%' in changed['html'] and '50%' not in changed['html']
        assert claim(s,changed,AT+10002)


def test_accepted_queue_rollback_preserves_new_history_and_no_backfill(cfg,market,make_raw,monkeypatch):
    import subprocess,sys,types
    from pathlib import Path
    import holy_weather.alert_queue as candidate
    old=types.ModuleType('holy_weather.accepted_queue');old.__package__='holy_weather'
    root=Path(__file__).resolve().parents[1]
    exec(subprocess.check_output(['git','show','7327f03d5402894fadef66657df940f4dfa7e176:src/holy_weather/alert_queue.py'],cwd=root),old.__dict__)
    cfg=config(cfg)
    with Store(cfg) as s:
        start(s,market,make_raw)
        feed(s,market,make_raw,'0.55','0.57',AT+1);delivered(s,keys(s)[0],AT+10001)
        feed(s,market,make_raw,'0.40','0.58',AT+11000);feed(s,market,make_raw,'0.51','0.59',AT+12000)
        first=[tuple(r) for r in s.c.execute('select * from events order by id')]
    with monkeypatch.context() as patch:
        patch.setitem(sys.modules,'holy_weather.alert_queue',old)
        with Store(cfg) as s:
            s.connection_started(AT+13000)
            feed(s,market,make_raw,'0.67','0.71',AT+14000);feed(s,market,make_raw,'0.71','0.73',AT+15000)
            k=next(k for k in keys(s) if value(s,k)['status']=='PENDING')
            p=old.preview(s,k);assert old.claim(s,p,AT+15000);old.finish(s,p,{'status':'SENT','message_id':2},AT+15001)
            assert all(r in [tuple(x) for x in s.c.execute('select * from events')] for r in first)
    with Store(cfg) as s:
        s.connection_started(AT+16000)
        feed(s,market,make_raw,'0.57','0.61',AT+17000);feed(s,market,make_raw,'0.61','0.63',AT+18000)
        assert len(keys(s))==2
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==4
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()


@pytest.mark.parametrize('region',['europe','us'])
def test_current_catalogue_pump_groups_covers_and_preserves_highs_on_restart(gamma,tmp_path,monkeypatch,region):
    """Full finite observer with PR26's delivery cadence; offline quotes/ACK only."""
    from collections import deque
    from holy_weather.catalogue import CatalogueConfig,CatalogueStore,observe_catalogue
    from holy_weather.core import Config
    from holy_weather.discovery import select
    from holy_weather.telegram import Sender
    from test_catalogue import setup
    from test_catalogue_recovery import message
    from conftest import NOW
    if region=='europe':
        cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0];city='london'
    else:
        city='new-york';cfg=CatalogueConfig(region='us',city=city,data_dir=str(tmp_path/'catalogue'))
        m=select(gamma,Config(city=city,bucket='46-47°F'),now=NOW)
        receipt={'received_at':NOW.isoformat(),'raw_body':json.dumps(gamma)}
    cfg=replace(config(cfg),duration_seconds=16,telegram_enabled=True)
    ticks=[0];senders=[];sent=[]
    class TrackedSender(Sender):
        def __init__(self,*a,**kw):super().__init__(*a,**kw);senders.append(self)
    monkeypatch.setattr('holy_weather.telegram.Sender',TrackedSender)
    monkeypatch.setattr('holy_weather.telegram.millis',lambda:AT+ticks[0])
    pending=deque([(10,'YES','0.49'),(15,'NO','0.79'),(20,'YES','0.51'),
        (25,'YES','0.56'),(30,'YES','0.61'),(35,'NO','0.81'),
        (1000,'NO','0.69'),(1010,'NO','0.71'), # Lower after a delivered NO80.
        (11000,'YES','0.49'),(11010,'YES','0.71'),(11020,'YES','0.81')])
    class WS:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def send(self,raw):pass
        def recv(self,timeout):
            if senders[-1].thread:senders[-1].thread.join(1)
            assert 0<timeout<=.051
            ticks[0]+=5
            if pending and pending[0][0]<=ticks[0]:
                _,side,p=pending.popleft()
                return message(m,m['tokens'][side],p,AT+ticks[0])
            return 'PONG'
    def run(seconds):
        return observe_catalogue(replace(cfg,duration_seconds=seconds),
            discover=lambda c:([m],receipt,[]) if c==city else ([],{},[]),
            connect=lambda *a,**kw:WS(),clock=lambda:AT+ticks[0],monotonic=lambda:ticks[0]/1000,
            token='offline',transport=lambda token,p:(sent.append((p,ticks[0])) or {'status':'SENT','message_id':len(sent)}))
    r=run(16)
    assert r['price_events']==7 and r['deliveries']=={'SENT':3}
    assert len(sent)==3 and 'Crossed: 80%' in sent[0][0]['html'] and sent[0][1]<=100
    assert 'Crossed: 50% · 55% · 60%' in sent[1][0]['html']
    assert 'Crossed: 70% · 80%' in sent[2][0]['html']
    assert 0<sent[2][1]-11010<4000 # Existing 3.1s pacing, no ordinary batch wait.
    with CatalogueStore(cfg,session=False) as s:
        events=[json.loads(x[0]) for x in s.c.execute("select payload from events where type='PRICE_ALERT' order by received_ms")]
        assert events[4]['thresholds']==['0.70']
        assert events[4]['delivery_admission']['reason']=='DELIVERED_HIGHER_LEVEL'
        assert events[4]['delivery_admission']['covered_by']['threshold']=='0.80'
        before={k:value(s,k) for k in keys(s)}
        assert sorted(len(v['event_ids']) for v in before.values())==[1,2,3]
        represented=[i for v in before.values() for i in v['event_ids']]
        assert len(represented)==len(set(represented))==6
    pending.extend([(ticks[0]+10,'YES','0.49'),(ticks[0]+15,'NO','0.69'),
                    (ticks[0]+20,'YES','0.81'),(ticks[0]+25,'NO','0.81')])
    r=run(1)
    assert r['price_events']==7 and r['deliveries']=={'SENT':3} and len(sent)==3
    with CatalogueStore(cfg,session=False) as s:
        assert {k:value(s,k) for k in keys(s)}==before
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()
