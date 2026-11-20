"""Regional AWC integration and pre-peak delivery use offline transports."""
import json
import sqlite3
from dataclasses import replace
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

import pytest

from holy_weather import alert_queue, catalogue, catalogue_metar, city_peak, lamp, weather
from holy_weather.core import EUROPE_CITIES, US_CITIES, canonical, sha
from test_us_lamp import AT, ISSUE, events, receipt, setup


def observations(market, at, current=5):
    zone = ZoneInfo(market['timezone'])
    day = datetime.fromtimestamp(at/1000, zone).date()
    start = int(datetime.combine(day, time.min, zone).timestamp()) * 1000
    rows = []
    for ms in range(start, at+1, city_peak.HOUR):
        utc = datetime.fromtimestamp(ms/1000, timezone.utc)
        temp = current if ms + city_peak.HOUR > at else 7
        raw = f"METAR {market['station']} {utc:%d%H%M}Z 00000KT CLR {temp:02d}/01 A3000"
        rows.append(dict(icaoId=market['station'], obsTime=ms//1000, rawOb=raw, temp=temp))
    body = canonical(rows)
    return dict(source=weather.source_for(market['station']), raw_body=body, sha256=sha(body.encode()),
                requested_ms=at-1, received_ms=at, status='OK')


@pytest.mark.parametrize('region,cities', [('us',US_CITIES), ('europe',EUROPE_CITIES)])
def test_all_stations_restart_schedule_and_europe_observation_only(tmp_path, region, cities):
    cfg = catalogue.CatalogueConfig(region=region, city=next(iter(cities)), catalogue_metar_enabled=True,
                                   data_dir=str(tmp_path))
    now = AT
    calls = []
    def fetch(station):
        calls.append(station)
        _, _, zone = next(v for v in cities.values() if v[1] == station)
        return observations(dict(station=station, timezone=zone), now)
    with catalogue.CatalogueStore(cfg, session=False) as store:
        for _, station, _ in cities.values():
            collector = catalogue_metar.MetarCollection(cities, fetch)
            collector.tick(store, now)
            collector.worker.join(2)
            assert collector.tick(store, now+10) == station
            state = catalogue_metar.load(store, catalogue_metar.PREFIX+station)
            assert state['status'] == 'OK' and state['records'][-1]['station'] == station
            now += weather.POLL_SECONDS * 1000 // len(cities)
        assert calls == [s for _, s, _ in cities.values()]
        assert not events(store)
        assert not store.c.execute("select 1 from meta where key like 'alert_delivery_v1:%'").fetchone()


def test_access_denied_survives_restart(tmp_path):
    cfg = catalogue.CatalogueConfig(data_dir=str(tmp_path), catalogue_metar_enabled=True)
    def denied(station):
        return dict(source=weather.source_for(station), status='ACCESS_DENIED', http=429,
                    requested_ms=AT, received_ms=AT)
    with catalogue.CatalogueStore(cfg,session=False) as store:
        c = catalogue_metar.MetarCollection(cfg.cities, denied)
        c.tick(store, AT)
        c.worker.join(2)
        c.tick(store, AT+1)
        again = catalogue_metar.MetarCollection(cfg.cities, denied)
        again.tick(store, AT+86400000)
        assert again.worker is None and again.schedule['blocked'] == 'ACCESS_DENIED'


def test_chicago_one_card_for_day_and_atomic_restart(gamma, tmp_path):
    cfg, bindings = setup(gamma, tmp_path, buckets=3)
    cfg = replace(cfg, catalogue_metar_enabled=True)
    chicago = [(m,r) for m,r in bindings if m['station']=='KORD']
    markets = []
    at = AT+3*city_peak.HOUR  # 09:00 Chicago; inside the new local send window.
    with catalogue.CatalogueStore(cfg, session=False) as store:
        for (m,r),bucket in zip(chicago, ('44-45°F','46-47°F','48°F or higher')):
            m = dict(m,bucket=bucket,url=m['url'].rsplit('/',1)[0]+'/'+bucket[:2]+'f')
            store.bind(m,r)
            markets.append(m)
        m = markets[0]
        store.record_lamp(receipt('KORD',46),m,AT,markets=markets)
        r = observations(m,at)
        w,_ = weather.update({},r,m,at)
        store.set_meta(catalogue_metar.PREFIX+'KORD',dict(w,day=m['date']))
        # Past high 44.6 F, current 41 F, future 47 F.
        second = receipt('KORD',47,at,ISSUE+3*city_peak.HOUR)
        store.c.execute("create temp trigger fault before insert on meta when new.key like 'alert_delivery_v1:%' begin select raise(abort,'queue fault'); end")
        with pytest.raises(sqlite3.IntegrityError,match='queue fault'):
            store.record_lamp(second,m,at,markets=markets)
        assert not events(store)
        store.c.execute('drop trigger fault')
        store.record_lamp(second,m,at,markets=markets)
        event, = events(store)
        assert event['city_outlook']['warming_f']=='6.0'
        assert event['market_url']==markets[1]['url']
        key,value = store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'").fetchone()
        sent = json.loads(value)
        assert '+6°F' in sent['html'] and '46–47°F' in sent['html'] and '10:00 CST' in sent['html']
        assert sent['expires_ms']==event['city_outlook']['peak_ms']-1
        p = alert_queue.preview(store,key)
        assert alert_queue.claim(store,p,at+1)
        alert_queue.finish(store,p,dict(status='SENT',message_id=1),at+2)
    with catalogue.CatalogueStore(cfg,session=False) as store:
        store.record_lamp(second,m,at+5,markets=markets)
        store.record_lamp(receipt('KORD',50,at+city_peak.HOUR,ISSUE+4*city_peak.HOUR),m,
                          at+city_peak.HOUR,markets=markets)
        assert len(events(store))==1
        assert not alert_queue.claim(store,p,at+6)
        assert store.c.execute('pragma integrity_check').fetchone()[0]=='ok'


def test_coverage_observed_higher_than_future_and_speci(gamma,tmp_path):
    _, bindings = setup(gamma,tmp_path)
    m = next(m for m,_ in bindings if m['station']=='KORD')
    r = observations(m,AT)
    rows = json.loads(r['raw_body'])
    rows[1].update(rawOb=rows[1]['rawOb'].replace('07/01','15/01'),temp=15)
    extra = dict(rows[1],rawOb=rows[1]['rawOb'].replace('METAR','SPECI').replace('15/01','30/01'))
    extra['obsTime']+=60
    utc=datetime.fromtimestamp(extra['obsTime'],timezone.utc)
    extra.update(rawOb=f"SPECI KORD {utc:%d%H%M}Z 00000KT CLR 30/01 A3000",temp=30)
    body=canonical(rows+[extra])
    w,_=weather.update({},dict(r,raw_body=body,sha256=sha(body.encode())),m,AT)
    w['day']=m['date']
    f=lamp.decode(receipt('KORD',47),m,AT)
    view,status=city_peak.outlook(w,f,m,AT)
    assert status=='OK' and view['daily_peak_f']=='59.0' and view['future_peak_f']=='47'
    assert city_peak.outlook(dict(w,records=w['records'][1:]),f,m,AT)[1]=='INCOMPLETE_METAR_DAY'
    assert city_peak.outlook(dict(w,day='2026-11-13'),f,m,AT)[1]=='METAR_UNAVAILABLE'
    assert city_peak.outlook(dict(w,poll_available_ms=AT+1),f,m,AT)[1]=='METAR_UNAVAILABLE'
    assert city_peak.outlook(w,lamp.decode(receipt('KORD',40),m,AT),m,AT)[1]=='NO_FUTURE_WARMING'


@pytest.mark.parametrize('instant', ['2026-03-08T19:00:00+00:00','2026-11-01T19:00:00+00:00'])
def test_chicago_elapsed_hours_on_dst_days(instant):
    at=int(datetime.fromisoformat(instant).timestamp()*1000)
    zone=ZoneInfo('America/Chicago')
    m=dict(station='KORD',timezone=str(zone),date=datetime.fromtimestamp(at/1000,zone).date().isoformat())
    r=observations(m,at)
    w,_=weather.update({},r,m,at)
    w['day']=m['date']
    f=lamp.decode(receipt('KORD',47,at,at-1800000),m,at)
    assert city_peak.outlook(w,f,m,at)[1]=='OK'
    assert city_peak.outlook(dict(w,records=w['records'][:2]+w['records'][3:]),f,m,at)[1]=='INCOMPLETE_METAR_DAY'
    assert city_peak.outlook(w,dict(f,hours=f['hours'][2:]),m,at)[1]=='INCOMPLETE_FORECAST_DAY'


def test_europe_multiplex_runs_metar_without_touching_price(gamma,tmp_path):
    from test_catalogue import setup as europe_setup
    cfg,bindings=europe_setup(gamma,tmp_path,count=1)
    cfg=replace(cfg,catalogue_metar_enabled=True,duration_seconds=1)
    market, metadata=bindings[0]
    now=[AT];ticks=[0.]
    messages=iter(('0.49','0.71'))
    class Socket:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def send(self,raw):pass
        def recv(self,timeout):
            import time
            time.sleep(.001);ticks[0]+=.05;now[0]+=50
            price=next(messages,None)
            if price is None:return 'PONG'
            return canonical(dict(event_type='best_bid_ask',market=market['condition_id'],asset_id=market['tokens']['YES'],
                                  timestamp=str(now[0]),best_bid=price,best_ask=price))
    result=catalogue.observe_catalogue(cfg, connect=lambda *a,**k:Socket(),
        discover=lambda city:([market],metadata,[]) if city=='london' else ([],{},[]),
        clock=lambda:now[0],monotonic=lambda:ticks[0],metar_fetch=lambda station:observations(market,AT))
    assert result['state']=='COMPLETED_WINDOW' and result['price_events']==1
    with catalogue.CatalogueStore(cfg,session=False) as s:
        assert catalogue_metar.load(s,catalogue_metar.PREFIX+'EGLC')['status']=='OK'
        assert not events(s)


def test_next_market_date_gets_its_own_baseline(gamma,tmp_path):
    cfg, bindings = setup(gamma, tmp_path)
    cfg = replace(cfg,catalogue_metar_enabled=True)
    m,r = next((m,r) for m,r in bindings if m['station']=='KORD')
    with catalogue.CatalogueStore(cfg,session=False) as s:
        s.bind(m,r)
        s.set_meta('us_lamp_city_v1:KORD',dict(day='2026-11-13',event_id='yesterday'))
        s.record_lamp(receipt('KORD'),m,AT)
        saved=catalogue_metar.load(s,'us_lamp_city_v1:KORD')
        assert saved==dict(day=m['date'],baseline_ms=ISSUE,status='BASELINE')
        assert not events(s)


def test_block_missing_ambiguous_and_closed_daily_bucket(gamma,tmp_path):
    cfg, bindings = setup(gamma,tmp_path,buckets=2)
    cfg=replace(cfg,catalogue_metar_enabled=True)
    group=[(m,r) for m,r in bindings if m['station']=='KORD']
    markets=[m for m,_ in group]
    m=markets[0]
    at=AT+3*city_peak.HOUR  # Keep this coverage/bucket test inside daytime.
    with catalogue.CatalogueStore(cfg,session=False) as s:
        for market,r in group:s.bind(market,r)
        s.record_lamp(receipt('KORD'),m,AT,markets=markets)
        f=receipt('KORD',47,at,ISSUE+3*city_peak.HOUR)
        s.record_lamp(f,m,at,markets=markets)
        get_status=lambda:catalogue_metar.load(s,'us_lamp_city_v1:KORD')['status']
        assert get_status()=='METAR_UNAVAILABLE'
        r=observations(m,at)
        w,_=weather.update({},r,m,at)
        s.set_meta(catalogue_metar.PREFIX+'KORD',dict(w,day=m['date']))
        s.record_lamp(f,m,at+1,markets=markets)
        assert get_status()=='NO_UNIQUE_VERIFIED_BUCKET'
        s.set_meta('market_closed:'+m['condition_id'],{'closed':True})
        s.record_lamp(f,m,at+2,markets=[m])
        assert get_status()=='MARKET_NOT_AVAILABLE'
        assert not events(s)


def test_station_day_history_survives_rollover_and_restart(tmp_path):
    import zlib
    cfg=catalogue.CatalogueConfig(data_dir=str(tmp_path),catalogue_metar_enabled=True)
    station,zone='EGLC','Europe/London'
    cities={'london':EUROPE_CITIES['london']}
    with catalogue.CatalogueStore(cfg,session=False) as store:
        for at in (AT,AT+86400000,AT+86400000+300000):
            reader=catalogue_metar.MetarCollection(cities,lambda station:observations(dict(station=station,timezone=zone),at))
            reader.tick(store,at)
            reader.worker.join(2)
            assert reader.tick(store,at)==station
        rows=store.c.execute('select raw,raw_sha from inputs').fetchall()
        assert len(rows)==1
        raw=zlib.decompress(rows[0][0])
        assert sha(raw)==rows[0][1]
        history=json.loads(raw)['projection']
        assert history['day']=='2026-11-14' and history['records'][-1]['raw_metar'].startswith('METAR EGLC')
        assert catalogue_metar.load(store,catalogue_metar.PREFIX+station)['day']=='2026-11-15'
