"""Staggered per-station AWC reads. Only the catalogue owner updates SQLite."""
from datetime import datetime
import json
import queue
import threading
from zoneinfo import ZoneInfo

from . import weather

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
                station, state, denied = self.pending.get_nowait()
            except queue.Empty:
                return
            if state['poll_received_ms'] > at:
                self.pending.put_nowait((station, state, denied))
                return
            state['poll_available_ms'] = at
            store.set_meta(PREFIX + station, state)
            self.worker = None
            if denied:
                self.schedule['blocked'] = state['status']
                store.set_meta(POLL_KEY, self.schedule)
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
            try:
                receipt = self.fetcher(station=station)
                received = receipt['received_ms']
                day = datetime.fromtimestamp(received / 1000, ZoneInfo(zone)).date().isoformat()
                market = dict(station=station, timezone=zone, date=day)
                state, _ = weather.update(previous, receipt, market, received)
                state.update(day=day, body_sha256=receipt.get('sha256'))
                denied = receipt.get('http') in (401, 403, 429)
            except Exception:
                state = dict(status='METAR_WORKER_ERROR', poll_received_ms=at)
                denied = False
            self.pending.put_nowait((station, state, denied))

        self.worker = threading.Thread(target=request, daemon=True, name='catalogue-metar')
        self.worker.start()
