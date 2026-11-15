"""Every supported station uses its own rules, receipts and local day."""
from dataclasses import replace
from datetime import datetime,timezone
import json
import pytest
from holy_weather.core import US_CITIES as CITIES,Config,sha
from holy_weather.discovery import select
from holy_weather import weather,lamp
from holy_weather.storage import Store
from test_weather import report,receipt as awc_receipt,AT
from test_lamp import bulletin,receipt as lamp_receipt,observed
from conftest import NOW


def city_market(city,gamma,cfg):
    slug,station,zone=CITIES[city]
    doc=json.loads(json.dumps(gamma).replace('nyc',slug).replace('KLGA',station))
    return select(doc,replace(cfg,city=city,weather_context=True,lamp_context=True),now=NOW)


@pytest.mark.parametrize('city',CITIES)
def test_station_sources_day_and_restart(city,gamma,cfg,make_raw):
    m=city_market(city,gamma,cfg);station=m['station']
    cfg=replace(cfg,city=city,weather_context=True,lamp_context=True)
    a=awc_receipt([report(5,station=station)]);a['source']=weather.source_for(station)
    l=lamp_receipt(bulletin(station=station));l['source']=lamp.source_for(station)
    with Store(cfg) as s:
        s.bind(m,{'synthetic':True});s.record_weather(a,m,AT+1);s.record_lamp(l,m,AT+2)
        for p,t in [('0.49',AT+10),('0.60',AT+20)]:
            from holy_weather.core import normalize
            raw=make_raw(p,t);raw['market']=m['condition_id']
            result=s.apply(normalize(raw,t,m))
        e=result['events'][0]
        assert e['weather']['station']==station and e['lamp']['station']==station
        assert e['lamp']['future_peak_f']=='46'
        assert all(h['local_time'][:10]==m['date'] for h in e['lamp']['hours'])
        assert station in lamp.format_card(s.forecast_card(AT+30))
        saved=s.c.execute('select payload from events').fetchall()
    with Store(cfg) as s:
        assert s.c.execute('select payload from events').fetchall()==saved
        assert s.stats()['sqlite_integrity']=='ok'


@pytest.mark.parametrize('city',CITIES)
def test_cross_station_and_zone_fail_closed(city,gamma,cfg):
    m=city_market(city,gamma,cfg)
    other='KSEA' if m['station']!='KSEA' else 'KLGA'
    a=awc_receipt([report(5,station=other)]);a['source']=weather.source_for(m['station'])
    assert weather.update({},a,m,AT+1)[0]['status']=='INVALID_DATA'
    l=lamp_receipt(bulletin(station=other));l['source']=lamp.source_for(m['station'])
    assert lamp.update({},l,m,AT+1)['status']=='INVALID_DATA'
    with pytest.raises(ValueError):weather.station_check(dict(m,timezone='UTC'))
    wrong=awc_receipt([report(5,station=m['station'])]);wrong['source']=weather.source_for(other)
    assert weather.update({},wrong,m,AT+1)[0]['status']=='INVALID_DATA'


@pytest.mark.parametrize('city',CITIES)
def test_local_midnight_and_forecast_utc_rollover(city,gamma,cfg):
    m=city_market(city,gamma,cfg)
    from zoneinfo import ZoneInfo
    midnight=datetime(2026,11,15,tzinfo=ZoneInfo(m['timezone']))
    midnight_ms=int(midnight.timestamp()*1000)
    issue=midnight_ms-90*60000
    r=lamp_receipt(bulletin(issue=issue,station=m['station']),issue+1000)
    r['source']=lamp.source_for(m['station'])
    s=lamp.decode(r,m,issue+1001)
    f=lamp.context(s,observed(),m,midnight_ms-3600000-1)
    assert len(f['hours'])==1 and f['hours'][0]['local_time'][11:16]=='23:00'
    assert lamp.context(s,observed(),m,midnight_ms)['status']=='NO_REMAINING_HOURS'


def test_all_ten_are_explicit():
    assert len(CITIES)==10
    assert len({v[1] for v in CITIES.values()})==10
    with pytest.raises(ValueError):Config(city='unknown',weather_context=True)


@pytest.mark.parametrize('city',CITIES)
def test_http_requests_target_exact_station(city):
    station=CITIES[city][1];urls=[]
    class Response:
        status=204
        def read(self,n):return b''
        def __enter__(self):return self
        def __exit__(self,*args):pass
    def fetch(request,**kwargs):urls.append(request.full_url);return Response()
    a=weather.fetch_weather(station=station,fetch=fetch,clock=lambda:AT)
    l=lamp.fetch_lamp(station=station,fetch=fetch,clock=lambda:AT)
    assert urls==[weather.source_for(station),lamp.source_for(station)]
    assert a['source']==urls[0] and l['source']==urls[1]
    assert a['status']==l['status']=='NO_DATA'
