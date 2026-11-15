"""UTC identity survives local midnight, 23/25-hour days and repeated hours."""
from datetime import datetime,timedelta,timezone
from zoneinfo import ZoneInfo
import pytest
from holy_weather.core import EUROPE_CITIES,Config
from holy_weather.alert_queue import day_expiry

@pytest.mark.parametrize('city',EUROPE_CITIES)
@pytest.mark.parametrize('day',['2026-03-29','2026-10-25','2026-11-15'])
def test_europe_local_day_and_queue_expiry(city,day):
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
