"""One pre-peak LAMP notice per US station and local market day."""
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import re
from zoneinfo import ZoneInfo

from . import catalogue_metar, lamp_alert
from .alert_queue import enqueue
from .core import canonical, sha
from .lamp import bucket_context, c_to_f
from .lamp_card import position
from .weather import WEATHER_AGE_MS

HOUR = 3_600_000


def outlook(weather, forecast, market, at):
    hours, status = lamp_alert.remaining(forecast, market, at)
    if status != 'OK':
        return None, status
    day = date.fromisoformat(market['date'])
    zone = ZoneInfo(market['timezone'])
    start = int(datetime.combine(day, time.min, zone).timestamp()) * 1000
    end = int(datetime.combine(day + timedelta(days=1), time.min, zone).timestamp()) * 1000
    if not start <= at < end or set(hours) != set(range((at // HOUR + 1) * HOUR, end, HOUR)):
        return None, 'INCOMPLETE_FORECAST_DAY'
    if (weather.get('status') != 'OK' or weather.get('station') != market['station']
            or weather.get('day') != market['date'] or weather['poll_available_ms'] > at):
        return None, 'METAR_UNAVAILABLE'
    reports = weather['records']
    if not reports or any(r['station'] != market['station'] or r['local_date'] != market['date']
                          or r.get('conflict') or not start <= r['observed_ms'] <= r['received_ms'] <= at
                          or r['available_ms'] > at for r in reports):
        return None, 'METAR_UNVERIFIED'
    latest = reports[-1]
    if at - latest['observed_ms'] >= WEATHER_AGE_MS:
        return None, 'STALE_METAR'
    # Require an observation in every elapsed local hour, including both DST folds.
    routine = [r for r in reports if r['raw_metar'].split()[0] != 'SPECI']
    if not routine:
        return None, 'NO_ROUTINE_METAR'
    covered = {r['observed_ms'] // HOUR for r in routine}
    if not all(hour in covered for hour in range(start // HOUR, at // HOUR)):
        return None, 'INCOMPLETE_METAR_DAY'
    now = Decimal(c_to_f(latest['temperature_c']))
    # For settlement the routine report's T remark takes precedence over the
    # body group. AWC's decoded field is independently checked by weather.decode.
    qualifying = []
    for report in routine:
        raw = report['raw_metar']
        precise = re.search(r'(?:^|\s)T([01])(\d{3})[01]\d{3}(?=\s|$)', raw)
        if precise:
            c = Decimal(precise[2]) / 10 * (-1 if precise[1] == '1' else 1)
        else:
            group = re.search(r'(?:^|\s)(M?\d{2})/(?:M?\d{2}|//)(?=\s|$)', raw)
            if not group:
                return None, 'METAR_TEMPERATURE_UNVERIFIED'
            c = Decimal(group[1].replace('M', '-'))
        qualifying.append(Decimal(c_to_f(c)))
    observed = max(qualifying)
    peak = max(map(Decimal, hours.values()))
    if peak <= now:
        return None, 'NO_FUTURE_WARMING'
    when = min(h for h, temp in hours.items() if Decimal(temp) == peak)
    return dict(current_f=str(now), observed_max_f=str(observed), future_peak_f=str(peak),
                warming_f=str(peak-now), daily_peak_f=str(max(observed, peak)), peak_ms=when,
                observed_ms=latest['observed_ms']), 'OK'


def record(store, markets, forecast, at):
    if not markets or forecast.get('status') != 'OK':
        return
    market = markets[0]
    station, day = market['station'], market['date']
    key = 'us_lamp_city_v1:' + station
    saved = catalogue_metar.load(store, key)
    def save(value):
        store.c.execute('insert or replace into meta values(?,?)', (key, canonical(value)))

    if saved.get('day', '') > day:
        return
    if saved.get('day') != day:
        save(dict(day=day, baseline_ms=forecast['issue_ms'], status='BASELINE'))
        return
    if saved.get('event_id') or forecast['issue_ms'] <= saved['baseline_ms']:
        return
    weather = catalogue_metar.load(store, catalogue_metar.PREFIX + station)
    result, status = outlook(weather, forecast, market, at)
    if result:
        matches = [m for m in markets if m['station'] == station and m['date'] == day
                   and m['event_id'] == market['event_id']
                   and m['metadata_state'] == 'OPEN_AT_DISCOVERY_RECEIPT'
                   and position(Decimal(result['daily_peak_f']), bucket_context(m, None, None)) == 'внутри']
        if len(matches) != 1:
            status = 'NO_UNIQUE_VERIFIED_BUCKET'
        else:
            match = matches[0]
            proof = store.discovery_reference(match['tokens']['YES'])
            if (not proof or not store.active(match['condition_id'].lower(), at) or
                    store.c.execute('select received_ms from inputs where id=?', (proof[0],)).fetchone()[0] > at):
                status = 'MARKET_NOT_AVAILABLE'
    if status != 'OK':
        if saved.get('status') != status:
            save(dict(saved, status=status))
        return
    market = matches[0]
    token = market['tokens']['YES']
    event_id = sha(['US_LAMP_PRE_PEAK_V1', station, day])
    event = dict(type='LAMP_ALERT', track='LAMP', status='DETECTED', reason='PRE_PEAK_WARMING',
                 station=station, at_ms=at, market_url=market['url'], bucket=market['bucket'],
                 city_outlook=result, current_forecast=forecast, metar=weather)
    store.c.execute('insert into events values(?,?,?,?,?,?,?,?)',
                    (event_id, forecast['input_id'], token, at, 'LAMP_ALERT', 'DETECTED',
                     event['reason'], canonical(event)))
    refs = {forecast['input_id'], proof[0]}
    store.c.executemany('insert into event_inputs values(?,?)', [(event_id, ref) for ref in refs])
    enqueue(store, event_id, event, market, at)
    save(dict(saved, status='RECORDED', event_id=event_id))
