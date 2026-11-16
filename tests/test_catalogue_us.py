from dataclasses import replace
from datetime import datetime,timezone
import json
import pytest
from holy_weather.catalogue import CatalogueConfig,CatalogueStore,Router,local_days
from holy_weather.core import Config,US_CITIES,sha
from holy_weather.discovery import select
from holy_weather.storage import StorageStop
from test_catalogue import feed
from conftest import NOW

@pytest.mark.parametrize('city',US_CITIES)
def test_us_independent_baseline_restart(gamma,tmp_path,city):
    slug,station,zone=US_CITIES[city]
    doc=json.loads(json.dumps(gamma).replace('nyc',slug).replace('KLGA',station))
    m=select(doc,Config(city=city),now=NOW)
    cfg=CatalogueConfig(region='us',city=city,data_dir=str(tmp_path/'us'))
    receipt={'received_at':NOW.isoformat(),'raw_body':json.dumps(doc)}
    at=int(NOW.timestamp()*1000)
    with CatalogueStore(cfg) as s:
        s.bind(m,receipt);s.connection_started(at);r=Router(s)
        feed(r,m,'0.49',at+1);feed(r,m,'0.71',at+2)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
    with CatalogueStore(cfg) as s:
        s.connection_started(at+3);r=Router(s)
        feed(r,m,'0.81',at+4);feed(r,m,'0.49',at+5);feed(r,m,'0.71',at+6)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()
    assert city in local_days(at,US_CITIES)
    with pytest.raises(StorageStop,match='CONFIG_RULE_DRIFT'):
        CatalogueStore(replace(cfg,region='europe'))

def test_europe_hash_unchanged():
    assert CatalogueConfig().rule_hash()==sha(['EUROPE_PRICE_CATALOGUE_V1',Config(city='london',bucket='catalogue',source_clock_tolerance_ms=100,price_alert_enabled=True,alert_queue_enabled=True).rule_hash()])

@pytest.mark.parametrize('region,city,count',[('europe','london',11),('us','new-york',10)])
def test_regional_cli_selects_matching_identity(monkeypatch,tmp_path,region,city,count):
    from holy_weather import catalogue
    import sys
    captured=[]
    def observe(cfg,**kwargs):
        captured.append(cfg)
        assert kwargs['token'] is None
        return {'state':'COMPLETED_WINDOW'}
    monkeypatch.setattr(catalogue,'observe_catalogue',observe)
    monkeypatch.setattr(sys,'argv',['catalogue','--region',region,'--data-dir',str(tmp_path)])
    assert catalogue.main()==0
    assert captured[0].region==region and captured[0].city==city
    assert len(captured[0].cities)==count
    assert not captured[0].telegram_enabled

@pytest.mark.parametrize('region,city',[('europe','london'),('us','new-york')])
def test_day_selection_uses_region_at_utc_midnight(region,city):
    cfg=CatalogueConfig(region=region,city=city)
    at=int(datetime(2026,11,16,0,30,tzinfo=timezone.utc).timestamp()*1000)
    days=local_days(at,cfg.cities)
    assert days[city]==('2026-11-16' if region=='europe' else '2026-11-15')
