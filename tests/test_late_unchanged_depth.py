"""Late depth confirmation cannot erase a newer identical confirmed top."""
from collections import Counter
from dataclasses import replace
import json
import pytest
from holy_weather.app import process
from holy_weather.storage import Store
from test_price_alert import AT
from test_confirmations import delta

@pytest.mark.parametrize('next_source',[AT+10,AT+22])
def test_same_top_late_depth_preserves_crossing(cfg,market,make_raw,next_source):
    cfg=replace(cfg,price_alert_enabled=True,alert_queue_enabled=True)
    with Store(cfg) as s:
        s.bind(market,{});s.connection_started(AT);c=Counter()
        def feed(raw,at):process(json.dumps(raw),at,market,s,c)
        feed(make_raw('0.49',AT+10,bid='0.49'),AT+20)
        late=delta(market,AT+9,ask='0.49');late['price_changes'][0]['best_bid']='0.49'
        feed(late,AT+21)
        assert market['tokens']['YES'] in s.price_alert_ready
        n=s.c.execute('select count(*) from inputs').fetchone()[0]
        feed(late,AT+22)
        assert s.c.execute('select count(*) from inputs').fetchone()[0]==n
        feed(make_raw('0.51',next_source,bid='0.51'),AT+23)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
        assert c['LATE_UNCHANGED_DEPTH']==2
        # The ignored packet must not replace original clocks or high-water.
        assert s.seen_tops[market['tokens']['YES']]['source_ms']==next_source
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'

@pytest.mark.parametrize('case',['changed','missing','stale','receipt_back','gap'])
def test_uncertain_late_depth_still_resets(cfg,market,make_raw,case):
    cfg=replace(cfg,price_alert_enabled=True,alert_queue_enabled=True)
    with Store(cfg) as s:
        s.bind(market,{});s.connection_started(AT);c=Counter()
        process(json.dumps(make_raw('0.49',AT+10,bid='0.49')),AT+20,market,s,c)
        late=delta(market,AT+9,ask='0.49');late['price_changes'][0]['best_bid']='0.49';receipt=AT+21
        if case=='changed':late['price_changes'][0]['best_ask']='0.48'
        if case=='missing':late['price_changes'][0]['best_bid']='0'
        if case=='stale':receipt=AT+100000
        if case=='receipt_back':receipt=AT+19
        if case=='gap':s.gap('REAL_GAP',AT+21);s.connection_started(AT+22);receipt=AT+23
        process(json.dumps(late),receipt,market,s,c)
        assert not c['LATE_UNCHANGED_DEPTH']
        assert market['tokens']['YES'] not in s.price_alert_ready

def test_sqlite_failure_cannot_mutate_quote_and_restart_is_baseline(cfg,market,make_raw):
    import sqlite3
    cfg=replace(cfg,price_alert_enabled=True,alert_queue_enabled=True)
    with Store(cfg) as s:
        s.bind(market,{});s.connection_started(AT);c=Counter()
        process(json.dumps(make_raw('0.49',AT+10,bid='0.49')),AT+20,market,s,c)
        previous=dict(s.seen_tops[market['tokens']['YES']])
        late=delta(market,AT+9,ask='0.49');late['price_changes'][0]['best_bid']='0.49'
        s.c.execute("CREATE TEMP TRIGGER fail BEFORE INSERT ON inputs BEGIN SELECT RAISE(ABORT,'failure'); END")
        with pytest.raises(sqlite3.IntegrityError):process(json.dumps(late),AT+21,market,s,c)
        assert s.seen_tops[market['tokens']['YES']]==previous
        assert market['tokens']['YES'] in s.price_alert_ready
        s.c.execute('DROP TRIGGER fail')
        process(json.dumps(late),AT+22,market,s,c)
        assert s.seen_tops[market['tokens']['YES']]==previous
    with Store(cfg) as s:
        s.connection_started(AT+30)
        process(json.dumps(make_raw('0.71',AT+31,bid='0.71')),AT+32,market,s,Counter())
        assert not s.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()
        assert not s.c.execute('pragma foreign_key_check').fetchall()
