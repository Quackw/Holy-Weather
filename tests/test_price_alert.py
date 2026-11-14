from dataclasses import replace
import json
from collections import Counter
from holy_weather.core import normalize
from holy_weather.storage import Store
from holy_weather.app import process
from holy_weather.price_alert import midpoint

AT=1794657600000

def test_decimal():
    assert midpoint('0.694','0.710')=='0.702' or midpoint('0.694','0.710')=='0.702'
    assert midpoint(None,'0.7') is None
    assert midpoint('0.8','0.7') is None

def send(s,m,raw,at):
    process(json.dumps(raw),at,m,s,Counter())

def alerts(s):return [json.loads(r[0])for r in s.c.execute("select payload from events where type='PRICE_ALERT'")]

def test_cross_restart_gap_dedupe(cfg,market,make_raw):
    cfg=replace(cfg,price_alert_enabled=True)
    with Store(cfg)as s:
        s.bind(market,{});s.connection_started(AT)
        send(s,market,make_raw('0.49',AT,bid='0.47'),AT)
        r=make_raw('0.62',AT+1,bid='0.60');send(s,market,r,AT+1);send(s,market,r,AT+2)
        assert len(alerts(s))==1 and alerts(s)[0]['thresholds']==['0.50','0.55','0.60']
        s.gap('test',AT+3);s.connection_started(AT+4)
        send(s,market,make_raw('0.82',AT+5,bid='0.80'),AT+5)
        assert len(alerts(s))==1
    with Store(cfg)as s:
        s.connection_started(AT+6)
        send(s,market,make_raw('0.49',AT+7,bid='0.47'),AT+7)
        send(s,market,make_raw('0.82',AT+8,bid='0.80'),AT+8)
        assert len(alerts(s))==2
        assert alerts(s)[1]['thresholds']==['0.70','0.80']
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
    with Store(replace(cfg,price_alert_enabled=False))as s:
        assert len(alerts(s))==2

def test_sides_and_midpoint_not_ask(cfg,market,make_raw):
    with Store(replace(cfg,price_alert_enabled=True))as s:
        s.bind(market,{});s.connection_started(AT)
        for side in ('YES','NO'):
            token=market['tokens'][side]
            send(s,market,make_raw('0.6',AT,token=token,bid='0.2'),AT)
            send(s,market,make_raw('0.7',AT+1,token=token,bid='0.2'),AT+1)
        assert not alerts(s)
        send(s,market,make_raw('0.7',AT+2,token=market['tokens']['NO'],bid='0.5'),AT+2)
        assert alerts(s)[0]['side']=='NO' and len(alerts(s))==1

def test_stale_previous_and_invalid_do_not_cross(cfg,market,make_raw):
    with Store(replace(cfg,price_alert_enabled=True))as s:
        s.bind(market,{});s.connection_started(AT)
        send(s,market,make_raw('0.49',AT,bid='0.47'),AT)
        send(s,market,make_raw('0.82',AT+91000,bid='0.80'),AT+91000)
        assert not alerts(s)
        send(s,market,make_raw('0.49',AT+92000,bid=None),AT+92000)
        send(s,market,make_raw('0.82',AT+93000,bid='0.80'),AT+93000)
        assert not alerts(s)

def test_late_input_requires_baseline(cfg,market,make_raw):
    with Store(replace(cfg,price_alert_enabled=True))as s:
        s.bind(market,{});s.connection_started(AT)
        send(s,market,make_raw('0.49',AT,bid='0.47'),AT)
        send(s,market,make_raw('0.48',AT-1,bid='0.46'),AT+1)
        send(s,market,make_raw('0.82',AT+2,bid='0.80'),AT+2)
        assert not alerts(s)

def test_retention_protects_midpoint_evidence(cfg,market,make_raw):
    with Store(replace(cfg,price_alert_enabled=True))as s:
        s.bind(market,{});s.connection_started(AT)
        send(s,market,make_raw('0.49',AT,bid='0.47'),AT)
        first=s.c.execute("select input_id from latest where kind='price_alert'").fetchone()[0]
        s.prune(AT+2*86400000,apply=True)
        assert s.c.execute('select 1 from inputs where id=?',(first,)).fetchone()
        send(s,market,make_raw('0.62',AT+1,bid='0.60'),AT+1)
        assert len(alerts(s))==1
        assert not s.c.execute('pragma foreign_key_check').fetchall()

def test_all_fired_survive_restart_and_recross(cfg,market,make_raw):
    cfg=replace(cfg,price_alert_enabled=True)
    with Store(cfg)as s:
        s.bind(market,{});s.connection_started(AT)
        send(s,market,make_raw('0.49',AT,bid='0.47'),AT)
        send(s,market,make_raw('0.82',AT+1,bid='0.80'),AT+1)
        assert len(alerts(s))==1 and len(alerts(s)[0]['thresholds'])==5
    with Store(cfg)as s:
        s.connection_started(AT+2)
        for i in range(20):
            ask,bid=('0.49','0.47') if i%2==0 else ('0.82','0.80')
            r=make_raw(ask,AT+3+i,bid=bid)
            send(s,market,r,AT+3+i);send(s,market,r,AT+3+i)
        assert len(alerts(s))==1

def test_restart_high_first_quote_is_not_cross(cfg,market,make_raw):
    cfg=replace(cfg,price_alert_enabled=True)
    with Store(cfg)as s:
        s.bind(market,{});s.connection_started(AT)
        send(s,market,make_raw('0.49',AT,bid='0.47'),AT)
    with Store(cfg)as s:
        s.connection_started(AT+1)
        send(s,market,make_raw('0.82',AT+2,bid='0.80'),AT+2)
        assert not alerts(s)

def test_cross_transaction_failure_preserves_unfired(cfg,market,make_raw):
    import sqlite3
    import pytest
    with Store(replace(cfg,price_alert_enabled=True))as s:
        s.bind(market,{});s.connection_started(AT)
        send(s,market,make_raw('0.49',AT,bid='0.47'),AT)
        s.c.execute("CREATE TEMP TRIGGER fail_price BEFORE INSERT ON events WHEN NEW.type='PRICE_ALERT' BEGIN SELECT RAISE(ABORT,'injected'); END")
        with pytest.raises(sqlite3.IntegrityError):send(s,market,make_raw('0.82',AT+1,bid='0.80'),AT+1)
        state=json.loads(s.c.execute("select value from meta where key like 'price_alert_v1:%'").fetchone()[0])
        assert state['fired']==[] and not alerts(s)
        s.c.execute('DROP TRIGGER fail_price')
        send(s,market,make_raw('0.82',AT+1,bid='0.80'),AT+1)
        assert len(alerts(s))==1

def test_6000_confirmations_and_duplicate_write_bound(cfg,market,make_raw):
    from pathlib import Path
    results=[]
    for enabled in (False,True):
        local=replace(cfg,data_dir=str(Path(cfg.data_dir)/str(enabled)),price_alert_enabled=enabled)
        counts=Counter()
        with Store(local)as s:
            s.bind(market,{});s.connection_started(AT)
            for i in range(6000):
                r=make_raw('0.49',AT+i*100,bid='0.47')
                process(json.dumps(r),AT+i*100,market,s,counts)
            assert s.c.execute('select count(*) from samples').fetchone()[0]==40
            assert counts['coalesced_unchanged_top_confirmations']==5960
            assert not alerts(s)
            for i in range(6000):process(json.dumps(r),AT+600000,market,s,counts)
            assert counts['DUPLICATE']==6000
            assert s.c.execute("select count(*) from diagnostics where category='DUPLICATE'").fetchone()[0]<=48
            assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        results.append(sum(p.stat().st_size for p in Path(local.data_dir).iterdir() if p.is_file()))
    assert results[1]<=2*results[0]

def test_bid_only_cross_after_checkpoint(cfg,market,make_raw):
    with Store(replace(cfg,price_alert_enabled=True))as s:
        s.bind(market,{});s.connection_started(AT)
        for dt in (0,100,14999,15000):
            send(s,market,make_raw('0.70',AT+dt,bid='0.28'),AT+dt)
        assert s.c.execute('select count(*) from samples').fetchone()[0]==2
        send(s,market,make_raw('0.70',AT+15001,bid='0.52'),AT+15001)
        e=alerts(s)[0]
        assert e['thresholds']==['0.50','0.55','0.60']
        assert e['previous']['source_ms']==AT+15000
        assert e['current']['ask']=='0.70' and e['current']['bid']=='0.52'

def test_gap_between_checkpoints_preserves_highwater(cfg,market,make_raw):
    with Store(replace(cfg,price_alert_enabled=True))as s:
        s.bind(market,{});s.connection_started(AT)
        send(s,market,make_raw('0.49',AT,bid='0.47'),AT)
        send(s,market,make_raw('0.49',AT+1000,bid='0.47'),AT+1000)
        s.gap('gap',AT+1001);s.connection_started(AT+1002)
        send(s,market,make_raw('0.82',AT+500,bid='0.80'),AT+1003)
        assert not alerts(s)
        send(s,market,make_raw('0.82',AT+1004,bid='0.80'),AT+1004)
        assert not alerts(s)

def test_missing_ask_and_bid_restore_baseline(cfg,market,make_raw):
    with Store(replace(cfg,price_alert_enabled=True))as s:
        s.bind(market,{});s.connection_started(AT)
        send(s,market,make_raw('0.49',AT,bid='0.47'),AT)
        send(s,market,make_raw(None,AT+1,bid='0.47'),AT+1)
        send(s,market,make_raw('0.82',AT+2,bid='0.80'),AT+2)
        assert not alerts(s)
        send(s,market,make_raw('0.82',AT+3,bid=None),AT+3)
        send(s,market,make_raw('0.82',AT+4,bid='0.80'),AT+4)
        assert not alerts(s)

def test_restart_between_checkpoints_retains_fired(cfg,market,make_raw):
    cfg=replace(cfg,price_alert_enabled=True)
    with Store(cfg)as s:
        s.bind(market,{});s.connection_started(AT)
        send(s,market,make_raw('0.49',AT,bid='0.47'),AT)
        send(s,market,make_raw('0.62',AT+1,bid='0.60'),AT+1)
        send(s,market,make_raw('0.62',AT+100,bid='0.60'),AT+100)
        assert len(alerts(s))==1
    with Store(cfg)as s:
        s.connection_started(AT+101)
        send(s,market,make_raw('0.82',AT+102,bid='0.80'),AT+102)
        assert len(alerts(s))==1
        send(s,market,make_raw('0.49',AT+103,bid='0.47'),AT+103)
        send(s,market,make_raw('0.82',AT+104,bid='0.80'),AT+104)
        assert alerts(s)[-1]['thresholds']==['0.70','0.80']
