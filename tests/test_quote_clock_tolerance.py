"""Bounded clock disagreement does not rewrite time or extend freshness."""
from dataclasses import replace,asdict
from collections import Counter
import json
import pytest
from holy_weather.core import Config,quote_clock_status,sha
from holy_weather.storage import Store,StorageStop
from holy_weather.app import process
from holy_weather.alert_queue import preview,claim
from test_price_alert import AT


@pytest.mark.parametrize('lead,expected',[(0,'FRESH'),(1,'FRESH'),(54,'FRESH'),(100,'FRESH'),(101,'FUTURE'),(1000,'FUTURE'),(-90000,'FRESH'),(-90001,'STALE')])
def test_explicit_clock_boundary(lead,expected):
    cfg=Config(source_clock_tolerance_ms=100)
    assert quote_clock_status(cfg,AT+lead,AT,AT)==expected
    if lead>0:assert quote_clock_status(Config(),AT+lead,AT,AT)=='FUTURE'


def test_receipt_age_and_clock_reversal_still_fail():
    cfg=Config(source_clock_tolerance_ms=100)
    assert quote_clock_status(cfg,AT+100,AT,AT+90001)=='STALE'
    assert quote_clock_status(cfg,AT,AT+1,AT)=='CLOCK_UNVERIFIED'
    with pytest.raises(ValueError):replace(cfg,source_clock_tolerance_ms=101)
    with pytest.raises(ValueError):replace(cfg,source_clock_tolerance_ms=-1)


def test_pipeline_original_times_checkpoint_expiry_restart(cfg,market,make_raw):
    cfg=replace(cfg,source_clock_tolerance_ms=100,price_alert_enabled=True,alert_queue_enabled=True)
    with Store(cfg) as s:
        s.bind(market,{});s.connection_started(AT);counts=Counter()
        def feed(price,receipt,lead=54):
            process(json.dumps(make_raw(price,receipt+lead,bid=price)),receipt,market,s,counts)
        feed('0.49',AT)
        for i in range(1,6000):feed('0.49',AT+i)
        assert s.c.execute('select count(*) from samples').fetchone()[0]==1
        feed('0.49',AT+15001) # retained checkpoint
        feed('0.61',AT+15002)
        e=json.loads(s.c.execute("select payload from events where type='PRICE_ALERT'").fetchone()[0])
        assert e['current']['source_ms']==AT+15056 and e['current']['received_ms']==AT+15002
        assert e['clock_policy']['current_source_lead_ms']==54
        assert e['thresholds']==['0.50','0.55','0.60']
        snapshot=s.observation_snapshot(now_ms=AT+15002)
        q=next(q for q in snapshot['latest'] if q['token']==market['tokens']['YES'])
        assert q['quote_usable_now'] and q['source_lead_ms']==54
        key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p=preview(s,key);assert p['expires_ms']==AT+15002+90000
        assert not claim(s,p,AT+15002+90001)
        feed('0.71',AT+15003,lead=101)
        assert s.c.execute('select status from samples order by input_id desc limit 1').fetchone()[0]=='FUTURE'
        feed('0.81',AT+15004) # recovery baseline
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
    with Store(cfg) as s:
        s.connection_started(AT+16000)
        process(json.dumps(make_raw('0.85',AT+16054,bid='0.85')),AT+16000,market,s,Counter())
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
    with pytest.raises(StorageStop,match='CONFIG_RULE_DRIFT'):
        Store(replace(cfg,source_clock_tolerance_ms=0))


def test_zero_tolerance_preserves_accepted_rule_hash():
    cfg=Config()
    excluded=('data_dir','duration_seconds','max_reconnects','weather_context','lamp_context','price_alert_enabled','lamp_alert_enabled','lamp_peak_change_f','alert_queue_enabled','telegram_enabled','source_clock_tolerance_ms')
    assert cfg.rule_hash()==sha({k:v for k,v in asdict(cfg).items() if k not in excluded})


def test_same_source_order_and_backward_receipt_are_not_tolerated(cfg,market,make_raw):
    cfg=replace(cfg,source_clock_tolerance_ms=100,price_alert_enabled=True,alert_queue_enabled=True)
    with Store(cfg) as s:
        s.bind(market,{});s.connection_started(AT);c=Counter()
        process(json.dumps(make_raw('0.49',AT+90,bid='0.49')),AT,market,s,c)
        process(json.dumps(make_raw('0.61',AT+89,bid='0.61')),AT+1,market,s,c)
        assert c['OUT_OF_ORDER']==1
        process(json.dumps(make_raw('0.49',AT+200,bid='0.49')),AT+110,market,s,c)
        process(json.dumps(make_raw('0.61',AT+201,bid='0.61')),AT+109,market,s,c)
        assert c['RECEIPT_CLOCK_REVERSED']==1
        assert not s.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()
