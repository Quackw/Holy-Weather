from dataclasses import replace
import json
import sqlite3
import pytest
from holy_weather.storage import Store
from test_lamp import AT,ISSUE,bulletin,receipt


def enabled(cfg):return replace(cfg,weather_context=True,lamp_context=True,lamp_alert_enabled=True)
def events(s):return [json.loads(r[0]) for r in s.c.execute("select payload from events where type='LAMP_ALERT' order by received_ms")]
def put(s,m,values,at,issue=ISSUE):s.record_lamp(receipt(bulletin(values,issue=issue),received=at),m,at)


def test_independent_change_boundary_duplicate_restart(cfg,market):
    cfg=enabled(cfg)
    with Store(cfg)as s:
        s.bind(market,{})
        put(s,market,[46]*25,AT)
        assert not events(s)
        put(s,market,[48]*25,AT+1000)
        assert len(events(s))==1
        e=events(s)[0]
        assert e['crossed_boundaries_f']==['47.5'] and e['change_f']=='2'
        assert not s.c.execute('select * from samples').fetchall() # no market update needed
    with Store(cfg)as s:
        put(s,market,[48]*25,AT+2000)
        assert len(events(s))==1
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()

@pytest.mark.parametrize('a,b,count,reason',[(44,45,0,None),(45,46,1,'BOUNDARY_CROSS'),(49,51,1,'PEAK_CHANGE'),(51,49,1,'PEAK_CHANGE')])
def test_threshold_or_boundary(cfg,market,a,b,count,reason):
    with Store(enabled(cfg))as s:
        s.bind(market,{});put(s,market,[a]*25,AT);put(s,market,[b]*25,AT+1000)
        assert len(events(s))==count
        if count:assert events(s)[0]['reason']==reason

def test_expired_hour_not_revision(cfg,market):
    with Store(enabled(cfg))as s:
        s.bind(market,{})
        put(s,market,[99,99]+[46]*23,AT)
        put(s,market,[46]*25,AT+3600001)
        assert not events(s)

def test_missing_reset_and_late_clock(cfg,market):
    with Store(enabled(cfg))as s:
        s.bind(market,{});put(s,market,[46]*25,AT)
        s.record_lamp(receipt(bulletin().replace(' TMP ',' XXX '),AT+1),market,AT+1)
        put(s,market,[48]*25,AT+2)
        assert not events(s)
        with pytest.raises(ValueError,match='CLOCK_REVERSED'):put(s,market,[50]*25,AT+1)
        assert not events(s)

def test_transaction_failure_and_threshold_pin(cfg,market):
    cfg=enabled(cfg)
    with Store(cfg)as s:
        s.bind(market,{});put(s,market,[46]*25,AT)
        s.c.execute("CREATE TEMP TRIGGER fail_lamp BEFORE INSERT ON events WHEN NEW.type='LAMP_ALERT' BEGIN SELECT RAISE(ABORT,'injected'); END")
        with pytest.raises(sqlite3.IntegrityError):put(s,market,[48]*25,AT+1)
        assert not events(s)
        s.c.execute('DROP TRIGGER fail_lamp');put(s,market,[48]*25,AT+1)
        assert len(events(s))==1
    with Store(replace(cfg,lamp_peak_change_f='3'))as s:
        with pytest.raises(ValueError,match='THRESHOLD_CHANGED'):put(s,market,[51]*25,AT+2)
    with Store(replace(cfg,lamp_alert_enabled=False))as s:assert len(events(s))==1

def test_new_issue_same_remaining_hours(cfg,market):
    with Store(enabled(cfg))as s:
        s.bind(market,{});put(s,market,[46]*25,AT)
        put(s,market,[48]*25,AT+3600001,issue=ISSUE+3600000)
        assert len(events(s))==1
        assert events(s)[0]['current_forecast']['issue_ms']-events(s)[0]['previous_forecast']['issue_ms']==3600000

def test_stale_previous_and_partial_hours(cfg,market):
    with Store(enabled(cfg))as s:
        s.bind(market,{});put(s,market,[46]*25,AT)
        put(s,market,[50]*25,AT+7200001,issue=ISSUE+7200000)
        assert not events(s)
        put(s,market,[999]*25,AT+7200002,issue=ISSUE+7200000)
        assert not events(s)
