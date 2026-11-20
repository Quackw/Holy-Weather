"""Russian rendering of a new US LAMP notification, using event-time evidence."""
from datetime import date, datetime
from zoneinfo import ZoneInfo
from decimal import Decimal
from html import escape
import re

from .lamp import bucket_context


CITY_NAMES = {
    'KLGA': 'Нью-Йорк', 'KATL': 'Атланта', 'KSEA': 'Сиэтл', 'KMIA': 'Майами',
    'KORD': 'Чикаго', 'KDAL': 'Даллас', 'KAUS': 'Остин', 'KHOU': 'Хьюстон',
    'KLAX': 'Лос-Анджелес', 'KSFO': 'Сан-Франциско',
}
MONTHS = ('января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
          'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря')


def bucket_label(bucket):
    label = re.sub(r'(?<=\d)-(?=-?\d)', '–', bucket)
    return escape(label.replace(' or below', ' и ниже').replace(' or higher', ' и выше'))


def position(peak, bounds):
    if bounds.get('status') != 'VERIFIED':
        return None
    if bounds['lower_f'] is not None and peak < Decimal(bounds['lower_f']):
        return 'ниже'
    if bounds['upper_f'] is not None and peak >= Decimal(bounds['upper_f']):
        return 'выше'
    return 'внутри'


def render(event, market, verified_markets):
    station = market['station']
    day = date.fromisoformat(market['date'])
    if 'city_outlook' in event:
        view = event['city_outlook']
        zone = ZoneInfo(market['timezone'])
        local = datetime.fromtimestamp(view['peak_ms'] / 1000, zone)
        observed = datetime.fromtimestamp(view['observed_ms'] / 1000, zone)
        def degrees(key):
            return format(Decimal(view[key]).quantize(Decimal('0.1')).normalize(), 'f')
        label = bucket_label(market['bucket'])
        if event.get('notification_policy')=='US_WARMING_HOURLY_V2':
            title='Изменение температуры · ежечасная проверка.'
            if event['reason']=='WARMING_START':
                start=datetime.fromtimestamp(view['warming_start_ms']/1000,zone)
                title=f'Прогнозный прогрев с {start:%H:%M %Z}.'
            revisions=[]
            previous=event.get('previous_review') or {}
            current={h['valid_ms']:h['temperature_f'] for h in event['current_forecast']['hours']}
            for hour in event.get('changed_forecast_hours',[])[:3]:
                when=datetime.fromtimestamp(hour/1000,zone)
                revisions.append(f'Прогноз на {when:%H:%M %Z}: {previous["hours"][str(hour)]} → {current[hour]}°F.')
            if len(event.get('changed_forecast_hours',[]))>3:revisions.append('Изменились и другие часы прогноза.')
            if event.get('metar_changes'):
                values=[Decimal(previous['current_f'])]+[Decimal(r['temperature_f']) for r in event['metar_changes']]
                revisions.append(f'Изменения METAR за час: {min(values):f}–{max(values):f}°F.')
            change=Decimal(view['warming_f'])
            future=(f'Ожидается ещё +{degrees("warming_f")}°F, до {degrees("future_peak_f")}°F к {local:%H:%M %Z}.'
                    if change>0 else f'Дополнительного прогрева не ожидается. Прогноз оставшихся часов — до {degrees("future_peak_f")}°F.')
            return [f'☀️ <b>LAMP · {CITY_NAMES[station]}</b>',
                    f'{day.day} {MONTHS[day.month-1]} · {station} · METAR / NOAA LAMP','',title,
                    *revisions,f'Сейчас {degrees("current_f")}°F · METAR {observed:%H:%M %Z}.',future,
                    f'Максимум по наблюдениям {degrees("observed_max_f")}°F.',
                    f'<a href="{escape(market["url"], quote=True)}">Ожидаемый диапазон дня {label} ↗</a>']
        return [f'☀️ <b>LAMP · {CITY_NAMES[station]}</b>',
                f'{day.day} {MONTHS[day.month-1]} · {station} · METAR / NOAA LAMP', '',
                f'По METAR в {observed:%H:%M} — {degrees("current_f")}°F. К {local:%H:%M %Z} ожидается {degrees("future_peak_f")}°F '
                f'(+{degrees("warming_f")}°F).',
                f'Максимум по наблюдениям {degrees("observed_max_f")}°F.',
                f'<a href="{escape(market["url"], quote=True)}">Ожидаемый диапазон дня {label} ↗</a>']
    before = Decimal(event['previous_peak_f'])
    after = Decimal(event['future_peak_f'])
    change = 'снизился' if after < before else 'повысился'
    label = bucket_label(market['bucket'])
    lines = [f'☀️ <b>LAMP · {CITY_NAMES[station]}</b>',
             f'{day.day} {MONTHS[day.month-1]} · аэропорт {station} · прогноз NOAA', '',
             f'Прогноз пика до конца дня {change} с {before:f}°F до {after:f}°F.']

    where = position(after, event['bucket_boundaries'])
    if where == 'внутри':
        lines.append(f'Новый прогноз в диапазоне {label}.')
    elif where:
        sentence = f'Новый прогноз {where} диапазона {label}'
        # Only contracts selected for this poll can supply a destination.
        matches = [other for other in verified_markets
                   if other['condition_id'] != market['condition_id']
                   and other['station'] == station and other['date'] == market['date']
                   and other['event_id'] == market['event_id']
                   and other['metadata_state'] == 'OPEN_AT_DISCOVERY_RECEIPT'
                   and position(after, bucket_context(other, None, None)) == 'внутри']
        if len(matches) == 1:
            sentence += ' и попадает в ' + bucket_label(matches[0]['bucket'])
        lines.append(sentence + '.')

    weather = event['weather']
    latest = weather.get('latest') or {}
    if (event['observed_max_f'] is not None and weather.get('station') == station
            and latest.get('station') == station and latest.get('local_date') == market['date']):
        maximum = format(Decimal(event['observed_max_f']).normalize(), 'f')
        lines.append(f'Максимум среди полученных наблюдений: {maximum}°F.')
    lines.extend(['', f'<a href="{escape(market["url"], quote=True)}">Открыть рынок {label} ↗</a>'])
    return lines
