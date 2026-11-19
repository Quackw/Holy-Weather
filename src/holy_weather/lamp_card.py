"""Russian rendering of a new US LAMP notification, using event-time evidence."""
from datetime import date
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
