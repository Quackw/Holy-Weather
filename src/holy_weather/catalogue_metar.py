"""Staggered per-station AWC reads. Only the catalogue owner updates SQLite."""
from datetime import datetime
import json
import queue
import threading
import zlib
from zoneinfo import ZoneInfo

from . import weather
from .core import canonical, sha

POLL_KEY = 'catalogue_metar_poll_v1'
PREFIX = 'catalogue_metar_station_v1:'


def load(store, key):
    row = store.c.execute('select value from meta where key=?', (key,)).fetchone()
    return json.loads(row[0]) if row else {}


class MetarCollection:
    def __init__(self, cities, fetcher=weather.fetch_weather):
        self.stations = [(station, zone) for _, station, zone in cities.values()]
        self.fetcher = fetcher
        self.pending = queue.Queue(maxsize=1)
        self.worker = None
        self.schedule = None

    def tick(self, store, at):
        if self.schedule is None:
            self.schedule = load(store, POLL_KEY)
        if self.worker is not None:
            try:
                station, state, denied, closed_day = self.pending.get_nowait()
            except queue.Empty:
                return
            if state['poll_received_ms'] > at:
                self.pending.put_nowait((station, state, denied, closed_day))
                return
            state['poll_available_ms'] = at
            with store.transaction():
                if closed_day:
                    digest, raw = closed_day
                    store.c.execute('insert or ignore into inputs(identity,received_ms,raw,raw_sha) values(?,?,?,?)',
                                    (digest, at, raw, digest))
                store.c.execute('insert or replace into meta values(?,?)', (PREFIX + station, canonical(state)))
                if denied:
                    self.schedule['blocked'] = state['status']
                    store.c.execute('insert or replace into meta values(?,?)', (POLL_KEY, canonical(self.schedule)))
            self.worker = None
            return station
        if self.schedule.get('blocked') or at < self.schedule.get('next_ms', 0):
            return
        index = self.schedule.get('cursor', 0)
        station, zone = self.stations[index]
        previous = load(store, PREFIX + station)
        # Reserve the slot before HTTP so a worker restart cannot storm AWC.
        self.schedule.update(cursor=(index + 1) % len(self.stations),
                             next_ms=at + weather.POLL_SECONDS * 1000 // len(self.stations))
        store.set_meta(POLL_KEY, self.schedule)

        def request():
            closed_day = None
            try:
                receipt = self.fetcher(station=station)
                received = receipt['received_ms']
                day = datetime.fromtimestamp(received / 1000, ZoneInfo(zone)).date().isoformat()
                market = dict(station=station, timezone=zone, date=day)
                state, _ = weather.update(previous, receipt, market, received)
                state.update(day=day, source=receipt['source'], requested_ms=receipt['requested_ms'],
                             body_sha256=receipt.get('sha256'))
                if previous.get('records') and previous['day'] < day:
                    raw = canonical(dict(type='METAR_STATION_DAY', projection=previous)).encode()
                    closed_day = sha(raw), zlib.compress(raw, 3)
                denied = receipt.get('http') in (401, 403, 429)
            except Exception:
                state = dict(previous, status='METAR_WORKER_ERROR', poll_received_ms=at)
                denied = False
            self.pending.put_nowait((station, state, denied, closed_day))

        self.worker = threading.Thread(target=request, daemon=True, name='catalogue-metar')
        self.worker.start()
