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
