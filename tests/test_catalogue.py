from dataclasses import replace
from datetime import datetime,timezone
import json
import sqlite3
import pytest
from holy_weather.catalogue import CatalogueConfig,CatalogueStore,Router
from holy_weather.storage import Store,StorageStop
from holy_weather.discovery import select
from holy_weather.core import EUROPE_CITIES,Config
from test_europe import european
from conftest import NOW

AT=int(NOW.timestamp()*1000)


def setup(gamma,tmp_path,count=11):
    cfg=CatalogueConfig(data_dir=str(tmp_path/'catalogue'))
    markets=[]
    for n,city in enumerate(list(EUROPE_CITIES)[:count]):
        doc=european(gamma,city)
        item=doc[0]['markets'][0]
        item['conditionId']='0x'+f'{n+1:064x}'
        item['clobTokenIds']=json.dumps([str(n*2+1),str(n*2+2)])
        m=select(doc,replace(Config(),city=city,bucket='13°C'),now=NOW)
        markets.append((m,dict(received_at=NOW.isoformat(),raw_body=json.dumps(doc))))
    return cfg,markets


def feed(router,m,value,at,side='YES',kind='best_bid_ask'):
    router.feed(json.dumps(dict(event_type=kind,market=m['condition_id'],asset_id=m['tokens'][side],timestamp=str(at),best_bid=value,best_ask=value)),at)


def test_all_cities_independent_queue_and_discovery(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path)
    with CatalogueStore(cfg) as s:
        for m,r in markets:s.bind(m,r)
        s.connection_started(AT);router=Router(s)
        for m,_ in markets:
            feed(router,m,'0.49',AT+1);feed(router,m,'0.71',AT+2)
        events=s.c.execute("select * from events where type='PRICE_ALERT'").fetchall()
        assert len(events)==11
        assert s.c.execute("select count(*) from meta where key like 'alert_delivery_v1:%'").fetchone()[0]==11
        for event in events:
            m=s.market_for_token(event['token']);e=json.loads(event['payload'])
            assert e['market_url']==m['url'] and e['thresholds']==['0.50','0.55','0.60','0.70']
            refs=[r[0] for r in s.c.execute('select input_id from event_inputs where event_id=?',(event['id'],))]
            assert s.discovery_reference(event['token'])[0] in refs
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()
        assert s.c.execute('pragma user_version').fetchone()[0]==1


def test_restart_gap_resolution_and_yes_no(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,2);a,b=[x[0] for x in markets]
    with CatalogueStore(cfg) as s:
        for m,r in markets:s.bind(m,r)
        s.connection_started(AT);router=Router(s)
        feed(router,a,'0.49',AT+1);feed(router,a,'0.56',AT+2)
    with CatalogueStore(cfg) as s:
        s.connection_started(AT+3);router=Router(s)
        feed(router,a,'0.61',AT+4) # restart baseline, no crossing
        feed(router,a,'0.49',AT+5);feed(router,a,'0.56',AT+6) # fired once
        feed(router,a,'0.49',AT+7,'NO');feed(router,a,'0.56',AT+8,'NO')
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==2
        router.feed(json.dumps(dict(event_type='market_resolved',market=a['condition_id'])),AT+9)
        s.expire_closed_deliveries(AT+9)
        assert not s.active(a['condition_id'],AT+10)
        assert s.active(b['condition_id'],AT+10)
        feed(router,b,'0.49',AT+11);feed(router,b,'0.51',AT+12)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==3
        assert s.c.execute("select count(*) from meta where key like 'alert_delivery_v1:%' and json_extract(value,'$.status')='EXPIRED'").fetchone()[0]==2
        s.gap('TEST',AT+13);s.connection_started(AT+14)
        feed(router,b,'0.81',AT+15)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==3


def test_bind_rollback_collision_and_single_market_refusal(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,2)
    with CatalogueStore(cfg) as s:
        s.bind(*markets[0])
        bad=dict(markets[1][0],tokens=markets[0][0]['tokens'])
        with pytest.raises(StorageStop,match='COLLISION'):s.bind(bad,markets[1][1])
        assert len(s.markets)==1
        s.c.execute("CREATE TEMP TRIGGER fail BEFORE INSERT ON meta BEGIN SELECT RAISE(ABORT,'test'); END")
        with pytest.raises(sqlite3.IntegrityError):s.bind(*markets[1])
        assert len(s.markets)==1
    with pytest.raises(StorageStop,match='CONFIG_RULE_DRIFT'):
        Store(Config(city='london',bucket='catalogue',data_dir=cfg.data_dir))


def test_price_only_and_date_end(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,1)
    with pytest.raises(ValueError):replace(cfg,weather_context=True)
    with CatalogueStore(cfg) as s:
        s.bind(*markets[0]);s.connection_started(AT);r=Router(s)
        feed(r,markets[0][0],'0.49',AT+1)
        feed(r,markets[0][0],'0.71',AT+86400000)
        assert not s.c.execute('select 1 from events').fetchone()


def test_router_causality_and_selected_scope(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,2)
    with CatalogueStore(cfg) as s:
        for m,r in markets:s.bind(m,r)
        s.connection_started(AT);r=Router(s,{markets[0][0]['condition_id']})
        feed(r,markets[0][0],'0.49',AT-1)
        assert r.counters['before_metadata_receipt']==1
        feed(r,markets[1][0],'0.49',AT+1)
        assert r.counters['unsubscribed_messages']==1
        assert not s.c.execute('select 1 from samples').fetchone()


def test_event_queue_rollback_and_checkpoint(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0]
    with CatalogueStore(cfg) as s:
        s.bind(m,receipt);s.connection_started(AT);r=Router(s)
        feed(r,m,'0.49',AT+1);feed(r,m,'0.49',AT+16001)
        s.c.execute("CREATE TEMP TRIGGER fail BEFORE INSERT ON meta WHEN NEW.key LIKE 'alert_delivery_v1:%' BEGIN SELECT RAISE(ABORT,'test'); END")
        with pytest.raises(sqlite3.IntegrityError):feed(r,m,'0.61',AT+16002)
        assert not s.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()
        s.c.execute('DROP TRIGGER fail')
        feed(r,m,'0.61',AT+16002)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1


def test_catalogue_finite_ws_and_no_weather(gamma,tmp_path,monkeypatch):
    from holy_weather.catalogue import observe_catalogue
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0]
    calls=[];ticks=[0.0]
    class WS:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def send(self,p):calls.append(json.loads(p))
        def recv(self,timeout):ticks[0]+=1;return 'PONG'
    def discover(city):return (markets[0:1][0:0],receipt,[]) if city!='london' else ([m],receipt,[])
    result=observe_catalogue(replace(cfg,duration_seconds=2),discover=discover,connect=lambda *a,**k:WS(),clock=lambda:AT,monotonic=lambda:ticks[0])
    assert result['state']=='COMPLETED_WINDOW'
    assert calls[0]['assets_ids']==list(m['tokens'].values())
    assert result['price_events']==0 and result['deliveries']=={}
    assert result['weather_enabled'] is False and result['sqlite_integrity']=='ok'


def test_6000_confirmations_coalesced(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0]
    with CatalogueStore(cfg) as s:
        s.bind(m,receipt);s.connection_started(AT);r=Router(s)
        for n in range(6000):feed(r,m,'0.49',AT+n)
        assert s.c.execute('select count(*) from samples').fetchone()[0]==1
        assert r.per_market[m['condition_id']]['coalesced_unchanged_top_confirmations']==5999
        assert s.usage()<1024*1024


def test_shared_sender_pacing_and_restart_no_duplicate(gamma,tmp_path):
    from holy_weather.telegram import Sender
    from holy_weather.alert_queue import preview,claim,finish
    cfg,markets=setup(gamma,tmp_path,2);calls=[]
    with CatalogueStore(cfg) as s:
        for m,r in markets:s.bind(m,r)
        s.connection_started(AT);router=Router(s)
        for m,_ in markets:feed(router,m,'0.49',AT+1);feed(router,m,'0.51',AT+2)
        sender=Sender('test',lambda token,p:(calls.append(p) or {'status':'SENT'}),minimum_interval_ms=3100)
        sender.tick(s,AT+3);sender.thread.join(1);sender.tick(s,AT+4)
        assert len(calls)==1
        sender.tick(s,AT+3103);sender.thread.join(1);sender.tick(s,AT+3104)
        assert len(calls)==2;sender.close(s)
    with CatalogueStore(cfg) as s:
        sender=Sender('test',lambda *a:pytest.fail('duplicate send'))
        sender.tick(s,AT+4000)
        assert s.c.execute("select count(*) from meta where key like 'alert_delivery_v1:%' and json_extract(value,'$.status')='SENT'").fetchone()[0]==2


def test_all_121_markets_share_receipts_and_bound(gamma,tmp_path):
    cfg,cities=setup(gamma,tmp_path);markets=[]
    with CatalogueStore(cfg) as s:
        for city_index,(city,receipt) in enumerate(cities):
            for bucket in range(11):
                n=city_index*11+bucket+1
                m=dict(city,condition_id='0x'+f'{n:064x}',tokens={'YES':str(1000+n*2),'NO':str(1001+n*2)},bucket=f'{bucket}°C')
                s.bind(m,receipt);markets.append(m)
        assert len(s.markets)==121
        assert s.c.execute('select count(*) from inputs').fetchone()[0]==11
        s.connection_started(AT);r=Router(s)
        for m in markets:
            for side in ('YES','NO'):
                feed(r,m,'0.49',AT+1,side);feed(r,m,'0.81',AT+2,side)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==242
        assert s.usage()<8*1024*1024
        assert not s.c.execute('pragma foreign_key_check').fetchall()


def test_subscription_chunks_preserve_frame_bound(gamma,tmp_path):
    from holy_weather.catalogue import observe_catalogue
    cfg,markets=setup(gamma,tmp_path);by_city=dict(zip(EUROPE_CITIES,markets))
    ticks=[0.0];sent=[]
    class WS:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def send(self,p):
            if p!='PING':sent.append(json.loads(p))
        def recv(self,timeout):ticks[0]+=1;return 'PONG'
    def connect(*a,**kw):
        assert kw['max_size']==262144
        return WS()
    result=observe_catalogue(replace(cfg,duration_seconds=3),discover=lambda city:([by_city[city][0]],by_city[city][1],[]),
                             connect=connect,clock=lambda:AT,monotonic=lambda:ticks[0])
    assert result['state']=='COMPLETED_WINDOW'
    assert [len(x['assets_ids']) for x in sent]==[16,6]
    assert sent[0]['type']=='market' and sent[1]['operation']=='subscribe'
    assert len({t for x in sent for t in x['assets_ids']})==22
