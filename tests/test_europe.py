"""Celsius contracts share PRICE/METAR; unsupported LAMP fails before HTTP."""
from dataclasses import replace
from datetime import datetime,timezone
import json
from pathlib import Path
import pytest
from holy_weather.core import Config,EUROPE_CITIES
from holy_weather.discovery import select
from holy_weather import app,lamp,weather
from holy_weather.storage import Store
from conftest import NOW


def european(gamma,city,bucket='13°C'):
    slug,station,zone=EUROPE_CITIES[city]
    doc=json.loads(json.dumps(gamma).replace('nyc',slug).replace('KLGA',station))
    m=doc[0]['markets'][0];m['groupItemTitle']=bucket
    m['description']=(Path(__file__).parent/'fixtures/europe_rules_template.txt').read_text().replace('{AIRPORT}','Synthetic Airport ('+station+')').replace('{DATE}',"14 Nov '26").replace('{SOURCE}',m['resolutionSource'])
    m['slug']=m['slug'].replace('46-47f','13c')
    return doc


@pytest.mark.parametrize('city',EUROPE_CITIES)
def test_europe_price_queue_restart(city,gamma,cfg,make_raw):
    cfg=replace(cfg,city=city,bucket='13°C',weather_context=True,price_alert_enabled=True,alert_queue_enabled=True)
    m=select(european(gamma,city),cfg,now=NOW)
    assert m['station']==EUROPE_CITIES[city][1]
    assert m['timezone']==EUROPE_CITIES[city][2]
    at=int(NOW.timestamp()*1000)
    with Store(cfg) as s:
        s.bind(m,{'synthetic':True});s.connection_started(at)
        from collections import Counter
        for n,(bid,ask) in enumerate([('0.47','0.49'),('0.60','0.62')]):
            app.process(json.dumps(make_raw(ask,at+n+1,bid=bid)),at+n+1,m,s,Counter())
        rows=s.c.execute("select value from meta where key like 'alert_delivery_v1:%'").fetchall()
        assert len(rows)==1
        v=json.loads(rows[0][0]);assert '13°C' in v['html'] and '50%' in v['html'] and v['track']=='PRICE_ALERT'
    with Store(cfg) as s:
        assert s.c.execute("select count(*) from meta where key like 'alert_delivery_v1:%'").fetchone()[0]==1
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert s.c.execute('pragma user_version').fetchone()[0]==1


@pytest.mark.parametrize('city',EUROPE_CITIES)
def test_europe_rejects_wrong_units_station_lamp(city,gamma,cfg):
    cfg=replace(cfg,city=city,bucket='13°C')
    with pytest.raises(ValueError,match='LAMP_UNAVAILABLE'):replace(cfg,lamp_context=True,weather_context=True)
    with pytest.raises(ValueError,match='LAMP_STATION'):lamp.source_for(EUROPE_CITIES[city][1])
    doc=european(gamma,city);doc[0]['markets'][0]['description']=doc[0]['markets'][0]['description'].replace('Celsius','Fahrenheit')
    with pytest.raises(ValueError):select(doc,cfg,now=NOW)
    doc=european(gamma,city);doc[0]['markets'][0]['resolutionSource']='https://aviationweather.gov/api/data/metar?ids=EGLL'
    with pytest.raises(ValueError,match='STATION'):select(doc,cfg,now=NOW)
    assert EUROPE_CITIES[city][1] in weather.source_for(EUROPE_CITIES[city][1])


@pytest.mark.parametrize('city',EUROPE_CITIES)
def test_local_day_is_station_day(city,gamma,cfg):
    cfg=replace(cfg,city=city,bucket='13°C',date='2026-11-14')
    with pytest.raises(ValueError,match='LOCAL_DATE'):
        select(european(gamma,city),cfg,now=datetime(2026,11,15,0,1,tzinfo=timezone.utc))


@pytest.mark.parametrize('city',EUROPE_CITIES)
def test_rejects_conflicting_airport_in_rules(city,gamma,cfg):
    cfg=replace(cfg,city=city,bucket='13°C')
    doc=european(gamma,city)
    doc[0]['markets'][0]['description']+=' Airport (EGLL)'
    with pytest.raises(ValueError,match='RULE_STATION_CONFLICT'):
        select(doc,cfg,now=NOW)

@pytest.mark.parametrize('before,after',[
    ('SPECI is a special observation and does not count','SPECI counts'),
    ('half rounded up','half rounded down'),
    ('Weather Underground Daily Observations table','another source'),
    ('whether or not it is marked as a correction (COR)','only if marked COR'),
    ('11:59 PM ET','10:59 PM ET'),
    ('the lowest bracket','the highest bracket'),
    ('station\'s local time','UTC time'),
])
def test_europe_rejects_changed_settlement_contract(gamma,cfg,before,after):
    doc=european(gamma,'london');text=doc[0]['markets'][0]['description']
    assert before in text
    doc[0]['markets'][0]['description']=text.replace(before,after)
    with pytest.raises(ValueError):select(doc,replace(cfg,city='london',bucket='13°C'),now=NOW)


def test_europe_rejects_appended_contradiction(gamma,cfg):
    doc=european(gamma,'london')
    doc[0]['markets'][0]['description']+=' SPECI observations count for settlement.'
    with pytest.raises(ValueError,match='RULE_TEMPLATE'):
        select(doc,replace(cfg,city='london',bucket='13°C'),now=NOW)
