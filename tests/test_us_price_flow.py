"""US-only receive and authoritative PRICE baseline recovery; offline only."""
from collections import Counter
from dataclasses import replace
import json

import pytest
from holy_weather import catalogue
from holy_weather.core import Config, US_CITIES, canonical
from holy_weather.discovery import select
from conftest import NOW
from test_catalogue import AT, feed


def us_market(gamma, tmp_path, city='los-angeles', number=None):
    slug, station, _ = US_CITIES[city]
    doc = json.loads(json.dumps(gamma).replace('nyc', slug).replace('KLGA', station))
    if number is not None:
        doc[0]['markets'][0].update(conditionId='0x'+f'{number:064x}',
                                   clobTokenIds=json.dumps([str(2*number),str(2*number+1)]))
    market = select(doc, Config(city=city), now=NOW)
    cfg = catalogue.CatalogueConfig(region='us', city=city, data_dir=str(tmp_path/'us'))
    return cfg, market, dict(received_at=NOW.isoformat(), raw_body=canonical(doc))


def depth(market, at, bid='0.49', ask='0.49', side='YES'):
    return canonical(dict(event_type='price_change', market=market['condition_id'], timestamp=str(at),
        price_changes=[dict(asset_id=market['tokens'][side], price=bid, size='3', side='BUY',
                            best_bid=bid, best_ask=ask)]))


def test_los_angeles_fresh_depth_without_bbo_requests_authoritative_baseline(gamma,tmp_path):
    cfg, market, receipt = us_market(gamma,tmp_path)
    ticks = [0.0]
    sent = []
    baseline_requested = [False]
    want_pong = [False]
    class WS:
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def send(self,raw):
            if raw == 'PING':
                want_pong[0]=True;return
            value=json.loads(raw);sent.append((ticks[0],value))
            if value.get('operation') == 'subscribe' and market['tokens']['YES'] in value['assets_ids']:
                baseline_requested[0] = True
        def recv(self,timeout):
            ticks[0] += 1
            at=AT+int(ticks[0]*1000)
            if want_pong[0]:
                want_pong[0]=False;return 'PONG'
            if ticks[0] == 1:
                return canonical(dict(event_type='best_bid_ask',market=market['condition_id'],
                    asset_id=market['tokens']['YES'],timestamp=str(at),best_bid='0.49',best_ask='0.49'))
            if baseline_requested[0]:
                baseline_requested[0] = False
                return canonical(dict(event_type='book',market=market['condition_id'],
                    asset_id=market['tokens']['YES'],timestamp=str(at),
                    bids=[dict(price='0.71',size='2')],asks=[dict(price='0.71',size='2')]))
            return depth(market,at)
    result = catalogue.observe_catalogue(replace(cfg,duration_seconds=110),
        discover=lambda city:([market],receipt,[]) if city==cfg.city else ([],{},[]),
        connect=lambda *args,**kwargs:WS(),clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0])
    requests=[(t,v) for t,v in sent if v.get('operation')=='subscribe']
    assert requests, 'Fresh depth must recover a stale PRICE baseline through a real WS book'
    assert requests[0][0] > 90 and requests[0][0] <= 97
    assert len(requests) <= 3
    assert result['price_events'] == 0, 'Recovery book is baseline, not a crossing through the gap'
    assert result['state'] == 'COMPLETED_WINDOW'


class Socket:
    def __init__(self):self.sent=[]
    def send(self,raw):self.sent.append(json.loads(raw))


@pytest.mark.parametrize('bad',['missing_bid','stale','empty','closed'])
def test_recovery_never_requests_from_unusable_or_closed_top(gamma,tmp_path,bad):
    cfg,m,r=us_market(gamma,tmp_path)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        s.bind(m,r);s.connection_started(AT);router=catalogue.Router(s)
        if bad!='empty':router.feed(depth(m,AT+1,bid='0' if bad=='missing_bid' else '0.49'),AT+1)
        if bad=='closed':router.feed(canonical(dict(event_type='market_resolved',market=m['condition_id'])),AT+2)
        socket=Socket();recovery=catalogue.USPriceRecovery(0)
        assert recovery.tick(s,socket,set(m['tokens'].values()),AT+(90002 if bad=='stale' else 6000),6)==0
        assert not socket.sent and not s.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()


def test_unanswered_recovery_is_bounded_until_a_real_baseline(gamma,tmp_path):
    cfg,m,r=us_market(gamma,tmp_path)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        s.bind(m,r);s.connection_started(AT);router=catalogue.Router(s)
        socket=Socket();recovery=catalogue.USPriceRecovery(0)
        for second in [6,7,26,27,46,47,66,86]:
            router.feed(depth(m,AT+second*1000),AT+second*1000)
            recovery.tick(s,socket,set(m['tokens'].values()),AT+second*1000,second)
        requests=[x for x in socket.sent if x['operation']=='subscribe']
        assert len(requests)==3 and all(x['assets_ids']==[m['tokens']['YES']] for x in requests)
        assert not s.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()
        feed(router,m,'0.71',AT+87000)
        recovery.tick(s,socket,set(m['tokens'].values()),AT+88000,88)
        assert not recovery.failures
        router.feed(depth(m,AT+179000,bid='0.71',ask='0.71'),AT+179000)
        assert recovery.tick(s,socket,set(m['tokens'].values()),AT+179000,179)==1
        assert len([x for x in socket.sent if x['operation']=='subscribe'])==4


def test_recovery_preserves_other_side_fired_sent_unknown_and_source_high_water(gamma,tmp_path):
    from holy_weather import alert_queue
    cfg,m,r=us_market(gamma,tmp_path)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        s.bind(m,r);s.connection_started(AT);router=catalogue.Router(s)
        for side in ['YES','NO']:
            feed(router,m,'0.49',AT+1,side);feed(router,m,'0.71',AT+2,side)
        queue=s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' order by key").fetchall()
        for i,(key,_) in enumerate(queue):
            p=alert_queue.preview(s,key);assert alert_queue.claim(s,p,AT+3)
            if i==0:alert_queue.finish(s,p,dict(status='SENT',message_id=1),AT+4)
        frozen=dict(s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'"))
        feed(router,m,'0.71',AT+91000,'NO')
        router.feed(depth(m,AT+92000,bid='0.71',ask='0.71'),AT+92000)
        socket=Socket();recovery=catalogue.USPriceRecovery(0)
        assert recovery.tick(s,socket,set(m['tokens'].values()),AT+92000,92)==1
        assert socket.sent[-1]['assets_ids']==[m['tokens']['YES']]
        assert m['tokens']['NO'] in s.price_alert_ready
        # An older snapshot cannot replace the high-water established by depth.
        feed(router,m,'0.81',AT+91999)
        assert m['tokens']['YES'] not in s.price_alert_ready
        feed(router,m,'0.81',AT+93000)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==2
        feed(router,m,'0.49',AT+94000);feed(router,m,'0.81',AT+95000)
        events=[json.loads(x[0]) for x in s.c.execute("select payload from events where type='PRICE_ALERT' order by received_ms")]
        assert events[-1]['thresholds']==['0.80'] and len(events)==3
        assert all(s.c.execute('select value from meta where key=?',(k,)).fetchone()[0]==v for k,v in frozen.items())
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()


def test_sqlite_failure_prevents_any_resubscription(gamma,tmp_path):
    import sqlite3
    cfg,m,r=us_market(gamma,tmp_path)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        s.bind(m,r);s.connection_started(AT);router=catalogue.Router(s)
        feed(router,m,'0.49',AT+1)
        router.feed(depth(m,AT+92000),AT+92000)
        before=[tuple(x) for x in s.c.execute('select * from state')]
        socket=Socket();recovery=catalogue.USPriceRecovery(0)
        s.c.execute("create temp trigger fail before update on state begin select raise(abort,'FAIL_RECOVERY'); end")
        with pytest.raises(sqlite3.IntegrityError,match='FAIL_RECOVERY'):
            recovery.tick(s,socket,set(m['tokens'].values()),AT+92000,92)
        assert not socket.sent and not recovery.failures
        assert [tuple(x) for x in s.c.execute('select * from state')]==before


def test_recovery_uses_bounded_groups_for_all_ten_us_cities(gamma,tmp_path):
    cfg,_,_=us_market(gamma,tmp_path)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        bindings=[us_market(gamma,tmp_path,city,n+1)[1:] for n,city in enumerate(US_CITIES)]
        for m,r in bindings:s.bind(m,r)
        s.connection_started(AT);router=catalogue.Router(s)
        for m,_ in bindings:
            for side in ['YES','NO']:router.feed(depth(m,AT+1,side=side),AT+1)
        socket=Socket();recovery=catalogue.USPriceRecovery(0);tokens=set(s.tokens)
        assert recovery.tick(s,socket,tokens,AT+6000,6)==16
        assert recovery.tick(s,socket,tokens,AT+6500,6.5)==0
        assert recovery.tick(s,socket,tokens,AT+7000,7)==4
        requests=[x for x in socket.sent if x['operation']=='subscribe']
        assert len(requests)==2 and sorted(map(lambda x:len(x['assets_ids']),requests))==[4,16]
        assert set().union(*(set(x['assets_ids']) for x in requests))==tokens
        assert not s.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()
