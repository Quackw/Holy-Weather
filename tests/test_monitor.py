from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import sqlite3
import pytest
from holy_weather.core import Config,normalize,sha
from holy_weather.discovery import select,discover
from holy_weather.storage import Store,StorageStop
from conftest import NOW


def feed(store,market,raw,lag=0):
    s=normalize(raw,int(raw['timestamp'])+lag,market)
    return store.apply(s)


def test_exact_token_order(gamma,cfg):
    gamma[0]['markets'][0]['outcomes']='["No","Yes"]'
    m=select(gamma,cfg,now=NOW)
    assert m['tokens']=={'NO':'12345','YES':'67890'}


@pytest.mark.parametrize('change,code',[
    ('closed','EVENT_NOT_OPEN'),('station','STATION'),('date','DATE'),('rules','RULES'),
    ('tokens','TOKEN'),('duplicate','AMBIGUOUS'),('accepting','NOT_OPEN')])
def test_discovery_refusals(gamma,cfg,change,code):
    m=gamma[0]['markets'][0]
    if change=='closed':gamma[0]['closed']=True
    elif change=='station':m['resolutionSource']='https://aviationweather.gov/api/data/metar?ids=KJFK'
    elif change=='date':cfg=replace(cfg,date='2026-11-13')
    elif change=='rules':m['description']='unreviewed'
    elif change=='tokens':m['clobTokenIds']='["1","1"]'
    elif change=='duplicate':gamma[0]['markets'].append(dict(m))
    elif change=='accepting':m['acceptingOrders']=False
    with pytest.raises(ValueError,match=code):select(gamma,cfg,now=NOW)


def test_causal_discovery_receipt(gamma,cfg):
    class Response:
        status=200
        def read(self,n):return json.dumps(gamma).encode()
        def __enter__(self):return self
        def __exit__(self,*a):pass
    m,r=discover(cfg,clock=lambda:NOW,fetch=lambda *a,**k:Response())
    assert m['tokens']['YES']=='12345' and r['sha256']==sha(json.dumps(gamma).encode())


def test_no_initial_event_and_no_opposite_requirement(cfg,market,make_raw):
    with Store(cfg)as s:
        r=feed(s,market,make_raw('0.95',bid=None));assert not r['events']
        assert s.stats()['samples']==1 and s.stats()['latest'][0]['bid'] is None


def test_own_no_price_and_exact_duplicate(cfg,market,make_raw):
    with Store(cfg)as s:
        a=make_raw('0.49',token=market['tokens']['NO']);feed(s,market,a)
        b=make_raw('0.60',1794650001000,token=market['tokens']['NO']);r=feed(s,market,b)
        assert any(x['type']=='MOVE' for x in r['events'])
        assert feed(s,market,b)['status']=='DUPLICATE'
        assert s.stats()['samples']==2 and s.stats()['latest'][0]['ask']=='0.60'


def test_exact_10_cents_and_all_levels(cfg,market,make_raw):
    with Store(cfg)as s:
        feed(s,market,make_raw('0.49'))
        result=feed(s,market,make_raw('0.91',1794650900000))
        assert len([e for e in result['events']if e['type']=='CROSS'])==3
        assert any(e['type']=='MOVE'for e in result['events'])


def test_move_window_expires(cfg,market,make_raw):
    with Store(cfg)as s:
        feed(s,market,make_raw('0.20',bid='0.19'))
        assert not feed(s,market,make_raw('0.30',1794650900001,bid='0.29'))['events']


@pytest.mark.parametrize('prices', [('0.20','0.40','0.30'),('0.40','0.20','0.30')])
def test_returns(cfg,market,make_raw,prices):
    with Store(cfg)as s:
        for i,price in enumerate(prices):r=feed(s,market,make_raw(price,1794650000000+i*60000,bid=None))
        assert any(e['type']=='RETURN' for e in r['events'])


def test_chatter_preserved_but_suppressed(cfg,market,make_raw):
    with Store(cfg)as s:
        for i,price in enumerate(['0.699','0.701','0.699','0.702','0.699','0.702']):feed(s,market,make_raw(price,1794650000000+i*1000))
        assert s.stats()['events']=={'DETECTED':1,'SUPPRESSED':4}
        assert s.c.execute("select count(*)from events where reason='HYSTERESIS'").fetchone()[0]==4


def test_rearm_requires_distance_and_cooldown(cfg,market,make_raw):
    with Store(cfg)as s:
        for p,t in [('0.699',0),('0.701',1),('0.67',2),('0.701',3)]:r=feed(s,market,make_raw(p,1794650000000+t*1000,bid=None))
        assert any(e.get('reason')=='COOLDOWN'for e in r['events'])
        feed(s,market,make_raw('0.67',1794650400000,bid=None))
        assert any(e['type']=='CROSS'and e['status']=='DETECTED'for e in feed(s,market,make_raw('0.701',1794650401000))['events'])


@pytest.mark.parametrize('lag,status', [(90001,'STALE'),(-1,'FUTURE')])
def test_bad_clocks_and_new_baseline(cfg,market,make_raw,lag,status):
    with Store(cfg)as s:
        feed(s,market,make_raw('0.49'))
        assert feed(s,market,make_raw('0.95',1794650001000),lag)['status']==status
        assert not feed(s,market,make_raw('0.96',1794650100000))['events']


def test_gap_and_restart_do_not_infer_crossing(cfg,market,make_raw):
    with Store(cfg)as s:
        feed(s,market,make_raw('0.49'));s.gap('NETWORK',1794650001000)
        assert not feed(s,market,make_raw('0.95',1794650002000))['events']
    with Store(cfg)as s:
        assert not feed(s,market,make_raw('0.10',1794650003000,bid=None))['events']
        assert s.stats()['samples']==3
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()


def test_trade_never_becomes_quote(cfg,market,make_raw):
    with Store(cfg)as s:
        raw=make_raw(kind='last_trade_price');raw.update(price='0.95',size='50',side='BUY')
        assert not feed(s,market,raw)['events']
        row=s.stats()['latest'][0];assert row['ask']is None and row['trade']=='0.95'


def test_snapshot_trade_has_unknown_clock_and_sizes(cfg,market,make_raw):
    with Store(cfg)as s:
        raw=make_raw(kind='book');raw['last_trade_price']='0.88';feed(s,market,raw)
        row=s.stats()['latest'][0];assert row['snapshot_trade']=='0.88'and row['trade']is None
        assert row['ask_size']=='20'
        feed(s,market,make_raw('0.50',1794650001000));assert s.stats()['latest'][0]['ask_size']is None


def test_atomic_write_failure_and_safe_retry(cfg,market,make_raw):
    with Store(cfg)as s:
        feed(s,market,make_raw('0.49'))
        s.c.execute("create trigger fault before insert on events begin select raise(abort,'synthetic write failure');end")
        raw=make_raw('0.60',1794650001000)
        with pytest.raises(sqlite3.IntegrityError):feed(s,market,raw)
        assert s.stats()['inputs']==1
        s.c.execute('drop trigger fault');feed(s,market,raw)
        assert s.stats()['inputs']==2


def test_one_writer_and_foreign_data_refusal(cfg,tmp_path):
    with Store(cfg):
        with pytest.raises(StorageStop,match='WRITER'):Store(cfg)
    foreign=tmp_path/'foreign';foreign.mkdir();(foreign/'anything').write_text('keep')
    with pytest.raises(StorageStop,match='UNOWNED'):Store(replace(cfg,data_dir=str(foreign)))
    assert (foreign/'anything').read_text()=='keep'


def test_interrupted_empty_bootstrap_and_unknown_schema(cfg):
    path=Path(cfg.data_dir);path.mkdir();(path/'.holy-weather-owned').write_text('HOLY_WEATHER_STAGE1\n')
    sqlite3.connect(path/'monitor.sqlite3').close()
    with Store(cfg)as s:assert s.stats()['sqlite_integrity']=='ok'
    c=sqlite3.connect(path/'monitor.sqlite3');c.execute('pragma user_version=2');c.close()
    with pytest.raises(StorageStop,match='UNKNOWN_SCHEMA'):Store(cfg)


def test_budget_stops_before_mutation(cfg,market,make_raw):
    cfg=replace(cfg,storage_bytes=8*1024*1024)
    with Store(cfg)as s:
        feed(s,market,make_raw())
        pressure=s.root/'pressure.hw-tmp';pressure.write_bytes(b'x'*(4*1024*1024))
        with pytest.raises(StorageStop,match='BUDGET'):feed(s,market,make_raw('0.60',1794650001000))
        assert s.c.execute('select count(*)from inputs').fetchone()[0]==1
        assert s.usage()<cfg.storage_bytes


def test_retention_preserves_event_evidence_and_latest(cfg,market,make_raw):
    with Store(cfg)as s:
        for i,p in enumerate(['0.49','0.60','0.61']):feed(s,market,make_raw(p,1794650000000+i*1000))
        before=s.stats()['inputs'];plan=s.prune(1794650000000+2*86400000)
        assert not plan['applied']and s.stats()['inputs']==before
        s.prune(1794650000000+2*86400000,apply=True)
        assert not s.c.execute('pragma foreign_key_check').fetchall()
        assert s.c.execute('select count(*)from event_inputs').fetchone()[0]>0


@pytest.mark.parametrize('kwargs',[{'move':'0'},{'quote_age_seconds':91},{'storage_bytes':268435457},{'max_reconnects':6},{'city':'unknown'},{'levels':('0.5','0.5')}])
def test_config_bounds(kwargs):
    with pytest.raises(ValueError):Config(**kwargs)
