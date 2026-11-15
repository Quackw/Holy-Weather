"""Pure causal quote/event rules; strings preserve exact decimal prices."""
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import os
import re
import tomllib
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

US_CITIES = {'new-york': ('nyc', 'KLGA', 'America/New_York'),
          'atlanta': ('atlanta', 'KATL', 'America/New_York'),
          'seattle': ('seattle', 'KSEA', 'America/Los_Angeles'),
          'miami': ('miami', 'KMIA', 'America/New_York'),
          'chicago': ('chicago', 'KORD', 'America/Chicago'),
          'dallas': ('dallas', 'KDAL', 'America/Chicago'),
          'austin': ('austin', 'KAUS', 'America/Chicago'),
          'houston': ('houston', 'KHOU', 'America/Chicago'),
          'los-angeles': ('los-angeles', 'KLAX', 'America/Los_Angeles'),
          'san-francisco': ('san-francisco', 'KSFO', 'America/Los_Angeles')}


EUROPE_CITIES = {'london': ('london', 'EGLC', 'Europe/London'),
                 'paris': ('paris', 'LFPB', 'Europe/Paris'),
                 'amsterdam': ('amsterdam', 'EHAM', 'Europe/Amsterdam'),
                 'munich': ('munich', 'EDDM', 'Europe/Berlin'),
                 'milan': ('milan', 'LIMC', 'Europe/Rome'),
                 'madrid': ('madrid', 'LEMD', 'Europe/Madrid'),
                 'warsaw': ('warsaw', 'EPWA', 'Europe/Warsaw'),
                 'helsinki': ('helsinki', 'EFHK', 'Europe/Helsinki'),
                 'moscow': ('moscow', 'UUWW', 'Europe/Moscow'),
                 'istanbul': ('istanbul', 'LTFM', 'Europe/Istanbul'),
                 'ankara': ('ankara', 'LTAC', 'Europe/Istanbul')}
CITIES = {**US_CITIES, **EUROPE_CITIES}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def sha(value):
    return hashlib.sha256(value if isinstance(value, bytes) else canonical(value).encode()).hexdigest()


def decimal(value, *, price=True):
    if not isinstance(value, str) or len(value) > 30 or not re.fullmatch(r'\d+(?:\.\d+)?', value):
        raise ValueError('INVALID_DECIMAL')
    result = Decimal(value)
    if result < 0 or (price and result > 1):
        raise ValueError('PRICE_OR_SIZE_RANGE')
    return result


@dataclass(frozen=True)
class Config:
    city: str = 'new-york'
    bucket: str = '46-47°F'
    date: str | None = None
    duration_seconds: int = 120
    data_dir: str = 'data'
    storage_bytes: int = 256 * 1024 * 1024
    quote_age_seconds: int = 90
    source_clock_tolerance_ms: int = 0
    move: str = '0.10'
    window_seconds: int = 900
    levels: tuple = ('0.50', '0.70', '0.90')
    return_fraction: str = '0.50'
    hysteresis: str = '0.02'
    cooldown_seconds: int = 300
    max_reconnects: int = 3
    weather_context: bool = False
    lamp_context: bool = False
    price_alert_enabled: bool = False
    lamp_alert_enabled: bool = False
    lamp_peak_change_f: str = "2"
    alert_queue_enabled: bool = False
    telegram_enabled: bool = False
    price_notification_filter: bool = False

    def __post_init__(self):
        if type(self.price_notification_filter)is not bool:raise ValueError('PRICE_NOTIFICATION_FILTER_BOOLEAN')
        if self.city in EUROPE_CITIES and self.lamp_context:
            raise ValueError("LAMP_UNAVAILABLE_FOR_EUROPE")
        if type(self.alert_queue_enabled)is not bool or type(self.telegram_enabled)is not bool or (self.telegram_enabled and not self.alert_queue_enabled):
            raise ValueError("TELEGRAM_REQUIRES_ALERT_QUEUE")
        if type(self.lamp_alert_enabled)is not bool or (self.lamp_alert_enabled and (not self.lamp_context or self.city!="new-york")):
            raise ValueError("LAMP_ALERT_REQUIRES_KLGA_CONTEXT")
        if not isinstance(self.lamp_peak_change_f,str) or not 0<decimal(self.lamp_peak_change_f,price=False)<=20:
            raise ValueError("LAMP_PEAK_CHANGE_BOUND")
        if type(self.price_alert_enabled)is not bool:raise ValueError("PRICE_ALERT_BOOLEAN")
        if type(self.lamp_context)is not bool or (self.lamp_context and not self.weather_context):
            raise ValueError("LAMP_REQUIRES_WEATHER_CONTEXT")
        if type(self.weather_context) is not bool:
            raise ValueError("WEATHER_CONTEXT_BOOLEAN")
        if self.city not in CITIES or not isinstance(self.bucket, str) or not 1 <= len(self.bucket) <= 80:
            raise ValueError('CITY_OR_BUCKET')
        limits = {'duration_seconds': (1, 3600), 'storage_bytes': (8*1024*1024, 256*1024*1024),
                  'quote_age_seconds': (1, 90), 'source_clock_tolerance_ms': (0, 100), 'window_seconds': (1, 3600), 'cooldown_seconds': (0, 3600), 'max_reconnects': (0, 5)}
        for key, (low, high) in limits.items():
            value = getattr(self, key)
            if type(value) is not int or not low <= value <= high:
                raise ValueError('CONFIG_BOUND:' + key)
        for key in ('move', 'return_fraction', 'hysteresis'):
            if decimal(getattr(self, key)) <= 0:
                raise ValueError('CONFIG_POSITIVE:' + key)
        if not 1 <= len(self.levels) <= 10 or len(set(self.levels)) != len(self.levels):
            raise ValueError('CONFIG_LEVELS')
        for level in self.levels:
            if not 0 < decimal(level) < 1:
                raise ValueError('CONFIG_LEVEL_RANGE')
        if self.date:
            datetime.strptime(self.date, '%Y-%m-%d')

    @classmethod
    def load(cls, path):
        data = tomllib.loads(Path(path).read_text())
        if set(data) - {f.name for f in fields(cls)}:
            raise ValueError('UNKNOWN_CONFIG_KEY')
        if 'HOLY_WEATHER_DATA_DIR' in os.environ:
            data['data_dir'] = os.environ['HOLY_WEATHER_DATA_DIR']
        if 'HOLY_WEATHER_DURATION_SECONDS' in os.environ:
            data['duration_seconds'] = int(os.environ['HOLY_WEATHER_DURATION_SECONDS'])
        if 'levels' in data:
            data['levels'] = tuple(data['levels'])
        return cls(**data)

    def day(self, now):
        return self.date or now.astimezone(ZoneInfo(CITIES[self.city][2])).date().isoformat()

    def rule_hash(self):
        rules={k: v for k, v in asdict(self).items() if k not in ('data_dir', 'duration_seconds', 'max_reconnects', 'weather_context', 'lamp_context', 'price_alert_enabled', 'lamp_alert_enabled', 'lamp_peak_change_f', 'alert_queue_enabled', 'telegram_enabled', 'price_notification_filter')}
        # Preserve accepted configurations/database hashes when tolerance is off.
        if not self.source_clock_tolerance_ms:rules.pop('source_clock_tolerance_ms')
        return sha(rules)


def quote_clock_status(cfg, source_ms, received_ms, at_ms):
    """Original clocks remain immutable; receipt age never borrows clock tolerance."""
    lead=source_ms-received_ms
    if received_ms>at_ms:return 'CLOCK_UNVERIFIED'
    if lead>cfg.source_clock_tolerance_ms or source_ms-at_ms>cfg.source_clock_tolerance_ms:
        return 'FUTURE'
    if max(at_ms-source_ms,at_ms-received_ms)>cfg.quote_age_seconds*1000:
        return 'STALE'
    return 'FRESH'


def normalize(message, received_ms, market, *, change=None):
    """Relevant native public messages only. Never infer a quote from a trade."""
    if not isinstance(message, dict) or message.get('error') or message.get('type') == 'error':
        raise ValueError('PROVIDER_ERROR_MESSAGE')
    kind = message.get('event_type')
    if kind not in ('book', 'best_bid_ask', 'last_trade_price', 'market_resolved','price_change'):
        return None
    condition = str(message.get('market', message.get('condition_id', ''))).lower()
    if condition != market['condition_id'].lower():
        raise ValueError('CONDITION_MISMATCH')
    if kind == 'market_resolved':
        return {'kind': kind, 'received_ms': received_ms, 'raw': message, 'id': sha(message)}
    if kind=='price_change' and not isinstance(change,dict):
        raise ValueError('EXPLICIT_PRICE_CHANGE_REQUIRED')
    quote=change if kind=='price_change'else message
    token = str(quote.get('asset_id', ''))
    if token not in market['tokens'].values():
        raise ValueError('TOKEN_MISMATCH')
    source = str(message.get('timestamp', ''))
    if not re.fullmatch(r'\d{13}', source):
        raise ValueError('SOURCE_CLOCK_MISSING')
    sample = {'kind': kind, 'token': token, 'source_ms': int(source), 'received_ms': received_ms,
              'bid': None, 'ask': None, 'bid_size': None, 'ask_size': None, 'trade': None, 'trade_size': None,
              'snapshot_trade': None, 'raw': message, 'id': sha([message,change])if kind=='price_change'else sha(message)}
    if kind == 'book':
        for side, field, select in (('bids', 'bid', max), ('asks', 'ask', min)):
            levels = message.get(side)
            if not isinstance(levels, list) or len(levels) > 5000:
                raise ValueError('INVALID_BOOK_LEVELS')
            valid = []
            for level in levels:
                price, size = decimal(level['price']), decimal(level['size'], price=False)
                if size > 0:
                    valid.append((price, level['price'], level['size']))
            if valid:
                _, sample[field], sample[field+'_size'] = select(valid)
        value = message.get('last_trade_price')
        if value not in (None, ''):
            decimal(value); sample['snapshot_trade'] = value  # timestamp/size unknown
    elif kind in ('best_bid_ask','price_change'):
        for field in ('bid', 'ask'):
            value = quote.get('best_' + field)
            if value not in (None, ''):
                price = decimal(value)
                if price != (0 if field == 'bid' else 1):
                    sample[field] = value  # size remains unknown
        if kind=='price_change'and quote.get('side')in ('BUY','SELL'):
            field='bid'if quote['side']=='BUY'else'ask'
            if quote.get('price')==sample[field]and quote.get('size')is not None:
                if decimal(quote['size'],price=False)>0:sample[field+'_size']=quote['size']
    else:
        decimal(message['price']); decimal(message['size'], price=False)
        if message.get('side') not in ('BUY', 'SELL'):
            raise ValueError('TRADE_SIDE')
        sample['trade'], sample['trade_size'] = message['price'], message['size']
    if sample['bid'] is not None and sample['ask'] is not None and decimal(sample['bid']) > decimal(sample['ask']):
        raise ValueError('CROSSED_BOOK')
    return sample


def reset(state):
    state = deepcopy(state)
    state['high_source']=max(x for x in (state.get('high_source'),state.get('last_source'),-1) if x is not None)
    state.update(points=[], excursion=None, last_source=None)
    return state


def evolve(previous, sample, cfg):
    state = deepcopy(previous or {'points': [], 'excursion': None, 'last_source': None, 'gates': {}})
    if sample['kind'] == 'last_trade_price':
        return state, [], 'TRADE_RECORDED'
    clock_status=quote_clock_status(cfg,sample['source_ms'],sample['received_ms'],sample['received_ms'])
    status = (clock_status if clock_status!='FRESH' else
              'MISSING_ASK' if sample['ask'] is None else
              'OUT_OF_ORDER' if sample['source_ms'] < max(x for x in (state.get('high_source'),state.get('last_source'),-1) if x is not None) else 'FRESH')
    if status != 'FRESH':
        return reset(state), [], status
    point = {k: sample[k] for k in ('source_ms', 'received_ms', 'ask', 'input_id')}
    value = decimal(point['ask']); events = []
    points = [p for p in state['points'] if 0 <= point['received_ms']-p['received_ms'] <= cfg.window_seconds*1000
              and 0 <= point['source_ms']-p['source_ms'] <= cfg.window_seconds*1000]
    last = points[-1] if points else None
    for level in cfg.levels:
        gate = state['gates'].setdefault(level, {'UP': True, 'DOWN': True, 'last_ms': None})
        threshold, distance = decimal(level), decimal(cfg.hysteresis)
        if value <= threshold-distance:
            gate['UP'] = True
        if value >= threshold+distance:
            gate['DOWN'] = True
        if not last:
            continue
        before = decimal(last['ask'])
        direction = 'UP' if before < threshold <= value else 'DOWN' if value < threshold <= before else None
        if direction:
            reason = 'HYSTERESIS' if not gate[direction] else 'COOLDOWN' if gate['last_ms'] is not None and point['received_ms']-gate['last_ms'] < cfg.cooldown_seconds*1000 else None
            events.append({'type': 'CROSS', 'level': level, 'direction': direction,
                           'status': 'SUPPRESSED' if reason else 'DETECTED', 'reason': reason,
                           'evidence': [last, point]})
            if not reason:
                gate.update(UP=False, DOWN=False, last_ms=point['received_ms'])
    excursion = state['excursion']
    if excursion:
        origin, peak = excursion['origin'], excursion['peak']
        sign = 1 if decimal(peak['ask']) > decimal(origin['ask']) else -1
        if (value-decimal(peak['ask']))*sign > 0:
            excursion['peak'] = peak = point
        if point['received_ms']-peak['received_ms'] > cfg.window_seconds*1000 or point['source_ms']-peak['source_ms'] > cfg.window_seconds*1000:
            excursion = None
        elif point['received_ms'] > peak['received_ms'] and (decimal(peak['ask'])-value)*sign >= abs(decimal(peak['ask'])-decimal(origin['ask']))*decimal(cfg.return_fraction):
            events.append({'type':'RETURN','status':'DETECTED','evidence':[origin,peak,point]});excursion=None
    if not excursion:
        origin = next((p for p in points if abs(value-decimal(p['ask'])) >= decimal(cfg.move)), None)
        if origin:
            events.append({'type':'MOVE','status':'DETECTED','evidence':[origin,point]});excursion={'origin':origin,'peak':point}
    if last and decimal(last['ask']) == value:
        points[-1] = point
    else:
        points.append(point)
    if len(points) > 4096:
        return reset(state), [], 'ENGINE_POINT_BOUND'
    state.update(points=points, excursion=excursion, last_source=sample['source_ms'],high_source=sample['source_ms'])
    return state, events, 'FRESH'
