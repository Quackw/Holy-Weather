"""Bounded station observations, never forecasts or official daily maxima."""
from datetime import datetime, timezone
from decimal import Decimal
import json
import math
import queue
import re
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
from .core import sha, CITIES

SOURCE = 'https://aviationweather.gov/api/data/metar?ids=KLGA&format=json&hours=24'
MAX_BODY = 131072
MAX_REPORTS = 128
POLL_SECONDS = 300
WEATHER_AGE_MS = 90 * 60 * 1000  # observation context, NOT the market's 90-second limit


def millis():
    return time.time_ns() // 1_000_000


def station_check(market):
    if (market['station'],market['timezone']) not in {(v[1],v[2]) for v in CITIES.values()}:
        raise ValueError('WEATHER_STATION_TIMEZONE')


def source_for(station):
    if station not in {v[1] for v in CITIES.values()}:raise ValueError('WEATHER_STATION')
    return f'https://aviationweather.gov/api/data/metar?ids={station}&format=json&hours=24'


def fetch_weather(*, station="KLGA", fetch=urlopen, clock=millis):
    source=source_for(station)
    requested = clock()
    try:
        with fetch(Request(source, headers={'User-Agent': 'Holy-Weather/0.2 bounded-KLGA-research'}), timeout=5) as response:
            status = response.status
            body = response.read(MAX_BODY + 1)
        received = clock()
        if len(body) > MAX_BODY:
            return dict(source=source, requested_ms=requested, received_ms=received, status='BODY_BOUND', terminal=True)
        return dict(source=source, requested_ms=requested, received_ms=received, http=status,
                    status='OK' if status == 200 else 'NO_DATA' if status == 204 else 'HTTP_ERROR',
                    terminal=status not in (200, 204), raw_body=body.decode('utf-8'), sha256=sha(body))
    except HTTPError as exc:
        return dict(source=source, requested_ms=requested, received_ms=clock(), http=exc.code,
                    status='ACCESS_DENIED' if exc.code in (401,403,429) else 'HTTP_ERROR', terminal=True)
    except (OSError, URLError, UnicodeError, ValueError) as exc:
        return dict(source=source, requested_ms=requested, received_ms=clock(),
                    status='NETWORK_OR_DECODE_ERROR', error=type(exc).__name__, terminal=True)


def decode(receipt, market, available_ms):
    """All-or-nothing validation; preserve the exact raw response in the journal."""
    station_check(market)
    if receipt['source'] != source_for(market['station']):
        raise ValueError('WEATHER_SOURCE')
    requested, received = receipt['requested_ms'], receipt['received_ms']
    if any(type(x) is not int for x in (requested, received, available_ms)) or not 0 <= requested <= received <= available_ms:
        raise ValueError('WEATHER_RECEIPT_CLOCK')
    if receipt['status'] != 'OK':
        return []
    body = receipt['raw_body'].encode('utf-8')
    if len(body) > MAX_BODY or sha(body) != receipt['sha256']:
        raise ValueError('WEATHER_BODY_SHA_OR_BOUND')
    rows = json.loads(body)
    if not isinstance(rows, list) or len(rows) > MAX_REPORTS:
        raise ValueError('WEATHER_REPORT_BOUND')
    reports = []
    for row in rows:
        raw = row.get('rawOb', '')
        if row.get('icaoId') != market['station'] or not isinstance(raw,str) or len(raw)>2048:
            raise ValueError('WEATHER_STATION_OR_RAW')
        match = re.match(r'^(?:(METAR|SPECI)\s+)?('+re.escape(market['station'])+r')\s+(\d{2})(\d{2})(\d{2})Z\b', raw)
        if not match:
            raise ValueError('WEATHER_RAW_STATION_OR_CLOCK')
        observed = row.get('obsTime')
        if type(observed) is not int or not 0 < observed * 1000 <= received:
            raise ValueError('WEATHER_OBSERVATION_CLOCK')
        observed_ms = observed * 1000
        utc = datetime.fromtimestamp(observed, timezone.utc)
        if (utc.day,utc.hour,utc.minute) != tuple(int(match.group(i)) for i in (3,4,5)):
            raise ValueError('WEATHER_RAW_CLOCK_MISMATCH')
        temp = row.get('temp')
        if isinstance(temp,bool) or not isinstance(temp,(int,float)) or not math.isfinite(temp) or not -90 <= temp <= 60:
            raise ValueError('WEATHER_TEMPERATURE')
        # AWC provides decoded Celsius; the raw whole-degree group must agree.
        group = re.search(r'(?:^|\s)(M?\d{2})/(?:M?\d{2}|//)(?:\s|$)',raw)
        if not group:
            raise ValueError('WEATHER_RAW_TEMPERATURE_MISSING')
        whole = int(group[1].replace('M','-'))
        if abs(Decimal(str(temp))-whole) > Decimal('0.6'):
            raise ValueError('WEATHER_TEMPERATURE_MISMATCH')
        local_date=utc.astimezone(ZoneInfo(market['timezone'])).date().isoformat()
        reports.append(dict(station=market['station'], raw_metar=raw, temperature_c=str(temp), units='C',
                            observed_ms=observed_ms, received_ms=received, available_ms=available_ms,
                            local_date=local_date, source=receipt['source'], correction='COR' in raw.split(),
                            report_type=match[1] or row.get('metarType','METAR'),
                            identity=sha([market['station'],observed_ms,raw,str(temp)])))
    return reports


def update(previous, receipt, market, available_ms):
    """Small current-day projection; the immutable input retains every receipt."""
    previous = previous or {}
    records = {r['observed_ms']: dict(r) for r in previous.get('records', [])
               if r['local_date']==market['date']}
    status=receipt['status']; flags=[]
    try:
        reports=decode(receipt,market,available_ms)
    except (ValueError,KeyError,TypeError,OverflowError) as exc:
        reports=[]; status='INVALID_DATA'; flags=[{'reason':str(exc)}]
    for report in sorted(reports,key=lambda r:(r['observed_ms'],r['correction'])):
        old=records.get(report['observed_ms'])
        flag='NEW'
        if report['local_date'] != market['date']:
            flag='OUTSIDE_MARKET_DAY'
        elif old and old['identity']==report['identity']:
            flag='DUPLICATE'
        elif old and old['correction'] and not report['correction']:
            flag='SUPERSEDED_ORIGINAL'
        elif old and not report['correction']:
            flag='CONFLICT_UNVERIFIED'
            records[report['observed_ms']]=dict(old,conflict=True,conflict_available_ms=available_ms,conflict_input_id=None)
        else:
            flag='CORRECTION' if report['correction'] else 'NEW'
            records[report['observed_ms']]=report
        flags.append(dict(identity=report['identity'], observed_ms=report['observed_ms'],
                          duplicate=flag=='DUPLICATE', correction=report['correction'], status=flag))
    if len(records)>MAX_REPORTS:
        return dict(previous, status='WEATHER_STATE_BOUND', poll_available_ms=available_ms), flags
    if status=='OK' and not reports:status='NO_DATA'
    return dict(station=market['station'], records=sorted(records.values(),key=lambda r:r['observed_ms']), status=status,
                poll_received_ms=receipt['received_ms'],poll_available_ms=available_ms),flags


def context(state, event_ms, start_ms):
    """Freeze only already-available observations. Never backfill an event."""
    state=state or {}
    rows=[r for r in state.get('records',[]) if r['received_ms']<event_ms and r['available_ms']<event_ms
          and r['observed_ms']<=r['received_ms']]
    valid=[r for r in rows if not r.get('conflict')]
    last=valid[-1] if valid else None; previous=valid[-2] if len(valid)>1 else None
    age=event_ms-last['observed_ms'] if last else None
    quality='MISSING' if not last else 'STALE' if age>WEATHER_AGE_MS else 'FRESH'
    return dict(station=state.get('station',last['station'] if last else None), units='C', latest=last, previous=previous,
                temperature_change_c=str(Decimal(last['temperature_c'])-Decimal(previous['temperature_c'])) if previous else None,
                observed_max_c=str(max(Decimal(r['temperature_c']) for r in valid)) if valid else None,
                official_daily_tmax=None, max_scope='RECEIVED_MARKET_DAY_OBSERVATIONS_ONLY',
                age_ms=age, freshness=quality, completeness='PARTIAL_RECEIVED_OBSERVATIONS',
                unverified_conflicts=any(r.get('conflict') for r in rows),
                provider_status=state.get('status','NOT_RECEIVED') if max(state.get('poll_available_ms',event_ms),state.get('poll_received_ms',event_ms))<event_ms else 'NOT_YET_AVAILABLE',
                new_observation_in_interval=any(start_ms<r['available_ms']<event_ms and not r['correction'] for r in rows),
                correction_in_interval=any(start_ms<r['available_ms']<event_ms and r['correction'] for r in rows),
                cause_of_price_move='NOT_ESTABLISHED',
                input_ids=sorted({r['input_id'] for r in rows} | {r['conflict_input_id'] for r in rows if r.get('conflict_input_id')} | ({state['poll_input_id']} if max(state.get('poll_available_ms',event_ms),state.get('poll_received_ms',event_ms))<event_ms and 'poll_input_id' in state else set())))


class WeatherPoller:
    """One bounded HTTP worker, no SQLite access, no service or retry loop."""
    source=SOURCE
    interval=POLL_SECONDS
    record_method='record_weather'
    status_method='weather_status'
    failed_states=('INVALID_DATA','WEATHER_STATE_BOUND')

    def __init__(self, fetcher=fetch_weather, monotonic=time.monotonic):
        self.fetcher=fetcher; self.clock=monotonic; self.next_at=0; self.thread=None
        self.queue=queue.Queue(maxsize=1); self.stopped=False

    def tick(self, store, market, at):
        try:
            receipt=self.queue.get_nowait()
        except queue.Empty:
            receipt=None
        if receipt is not None:
            getattr(store,self.record_method)(receipt, market, millis())
            self.stopped=bool(receipt.get('terminal')) or getattr(store,self.status_method)() in self.failed_states
        if not self.stopped and self.clock()>=self.next_at and (self.thread is None or not self.thread.is_alive()) and self.queue.empty():
            self.next_at=self.clock()+self.interval
            def request():
                try:result=self.fetcher()
                except Exception as exc:
                    result=dict(source=self.source,requested_ms=at,received_ms=millis(),status='WORKER_ERROR',error=type(exc).__name__,terminal=True)
                self.queue.put_nowait(result)
            self.thread=threading.Thread(target=request,daemon=True,name=self.status_method+'-one-request');self.thread.start()

    def close(self):
        self.stopped=True  # worker has a bounded socket timeout and never writes
