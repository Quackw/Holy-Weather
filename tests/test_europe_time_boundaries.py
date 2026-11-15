"""UTC identity survives local midnight, 23/25-hour days and repeated hours."""
from datetime import datetime,timedelta,timezone
from zoneinfo import ZoneInfo
import pytest
from holy_weather.core import EUROPE_CITIES,Config
from holy_weather.mosmix import STATIONS,decode
from holy_weather.alert_queue import day_expiry
from holy_weather import weather
from test_mosmix import body
from test_weather import report,receipt

@pytest.mark.parametrize('city',EUROPE_CITIES)
@pytest.mark.parametrize('day',['2026-03-29','2026-10-25','2026-11-15'])
def test_europe_local_day_and_remaining_utc_hours(city,day):
    _,station,zone=EUROPE_CITIES[city];tz=ZoneInfo(zone)
    start=datetime.fromisoformat(day).replace(tzinfo=tz)
    end=(datetime.fromisoformat(day)+timedelta(days=1)).replace(tzinfo=tz)
    lo=int(start.timestamp()*1000);hi=int(end.timestamp()*1000)
    expected=24 if zone in ('Europe/Moscow','Europe/Istanbul') or day=='2026-11-15' else 23 if day=='2026-03-29' else 25
    assert hi-lo==expected*3600000
    assert day_expiry(hi+90000,day,zone)==hi-1
    cfg=Config(city=city)
    assert cfg.day(datetime.fromtimestamp((hi-1)/1000,timezone.utc))==day
    assert cfg.day(datetime.fromtimestamp(hi/1000,timezone.utc))==end.date().isoformat()
    steps=[datetime.fromtimestamp((lo+i*3600000)/1000,timezone.utc).isoformat() for i in range(30)]
    issue=datetime.fromtimestamp((lo-3600000)/1000,timezone.utc).isoformat()
    f=decode(body(point=STATIONS[station][0],issue=issue,times=steps,values=' '.join(['283.15']*30)),
             station=station,requested_ms=lo-1,received_ms=lo,at_ms=lo)
    hours=f['remaining_hours'];assert len(hours)==expected-1
    assert len({h['valid_ms'] for h in hours})==len(hours)
    assert all(lo<h['valid_ms']<hi for h in hours)
    if expected==25:
        local=[datetime.fromtimestamp(h['valid_ms']/1000,tz) for h in hours]
        assert any(h.fold==1 for h in local)
        assert len({h.strftime('%Y-%m-%d %H') for h in local})<len(local)

@pytest.mark.parametrize('city',[c for c,v in EUROPE_CITIES.items() if v[2] not in ('Europe/Moscow','Europe/Istanbul')])
def test_repeated_hour_observations_keep_distinct_utc_identity(city):
    _,station,zone=EUROPE_CITIES[city];tz=ZoneInfo(zone)
    hour=1 if city=='london' else 3 if city=='helsinki' else 2
    first=datetime(2026,10,25,hour,30,tzinfo=tz,fold=0)
    second=first.replace(fold=1)
    a,b=[int(t.timestamp()*1000) for t in (first,second)]
    assert b-a==3600000
    r=receipt([report(10,a,station=station),report(11,b,station=station)],b+60000)
    r['source']=weather.source_for(station)
    market=dict(station=station,timezone=zone,date='2026-10-25')
    state,flags=weather.update({},r,market,b+60001)
    assert state['status']=='OK' and len(state['records'])==2
    assert [x['observed_ms'] for x in state['records']]==[a,b]
