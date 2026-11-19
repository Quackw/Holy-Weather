"""Local clocks and original-format synthetic sources; no real messages."""
from datetime import datetime, timedelta, timezone
import ast
import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import types
from zoneinfo import ZoneInfo

import pytest

from holy_weather import alert_queue, catalogue, catalogue_metar, city_peak, lamp, recovery, telegram, weather
from holy_weather.core import Config, US_CITIES, canonical, sha
from holy_weather.discovery import select
from test_catalogue_metar import observations
from test_us_lamp import events, native


def instant(city, day, hour, minute=0, *, fold=0):
    zone = ZoneInfo(US_CITIES[city][2])
    return int(datetime.fromisoformat(day).replace(hour=hour, minute=minute,
                                                  tzinfo=zone, fold=fold).timestamp()*1000)


def binding(gamma, city, day):
    slug, station, zone = US_CITIES[city]
    d = datetime.fromisoformat(day)
    month = d.strftime('%B').lower()
    doc = json.loads(json.dumps(gamma).replace('nyc', slug).replace('KLGA', station)
                     .replace('november-14-2026', f'{month}-{d.day}-{d.year}')
                     .replace("14 Nov '26", f"{d.day} {d:%b} '{d.year%100:02d}"))
    ident = d.toordinal()*100+list(US_CITIES).index(city)+1
    doc[0]['markets'][0].update(conditionId='0x'+f'{ident:064x}',
                              clobTokenIds=json.dumps([str(2*ident), str(2*ident+1)]))
    at = instant(city, day, 0)
    received = datetime.fromtimestamp(at/1000, timezone.utc)
    market = select(doc, Config(city=city), now=received)
    return market, dict(received_at=received.isoformat(), raw_body=canonical(doc))


def forecast(market, issue, received, peak_at, peak=47):
    body = native(market['station'], peak-1, issue).decode()
    first = datetime.fromtimestamp(issue/1000, timezone.utc).replace(minute=0)+timedelta(hours=1)
    values = [peak if int((first+timedelta(hours=n)).timestamp()*1000)==peak_at else peak-1
              for n in range(25)]
    lines = body.splitlines()
    lines[2] = ' TMP '+''.join(f'{v:3d}' for v in values)+' '
    raw = ('\n'.join(lines)+'\n').encode()
    return dict(source=lamp.nomads_source(issue), requested_ms=received-1, received_ms=received,
                status='OK', raw_body=raw.decode(), sha256=sha(raw), file_sha256=sha(raw))


def metar(store, market, at):
    state, _ = weather.update({}, observations(market, at), market, at)
    store.set_meta(catalogue_metar.PREFIX+market['station'], dict(state, day=market['date']))


def state(store, market):
    return catalogue_metar.load(store, 'us_lamp_city_v1:'+market['station'])


def config(city, root):
    return catalogue.CatalogueConfig(region='us', city=city, us_lamp_enabled=True,
                                    catalogue_metar_enabled=True, data_dir=str(root))


def seed(store, market, peak, *, city, day):
    at = instant(city, day, 0, 40)
    store.record_lamp(forecast(market, instant(city, day, 0, 30), at, peak), market, at)


def admit(store, market, at, issue, peak):
    metar(store, market, at)
    store.record_lamp(forecast(market, issue, at, peak), market, at)


@pytest.mark.parametrize('city', US_CITIES)
@pytest.mark.parametrize('day', ['2026-11-14', '2026-03-08', '2026-11-01'])
def test_waits_for_daytime_and_three_hour_peak_window(gamma, tmp_path, city, day):
    market, receipt = binding(gamma, city, day)
    cfg = catalogue.CatalogueConfig(region='us', city=city, us_lamp_enabled=True,
                                   catalogue_metar_enabled=True, data_dir=str(tmp_path))
    peak = instant(city, day, 14)
    with catalogue.CatalogueStore(cfg, session=False) as store:
        store.bind(market, receipt)
        # Retain the normal first-issue baseline, then a fresh overnight issue.
        baseline_at = instant(city, day, 0, 40)
        store.record_lamp(forecast(market, instant(city, day, 0, 30), baseline_at, peak),
                          market, baseline_at)
        night = instant(city, day, 1, 40)
        metar(store, market, night)
        store.record_lamp(forecast(market, instant(city, day, 1, 30), night, peak), market, night)
        assert not events(store), 'night must not consume the city/day card'
        assert not store.c.execute("select 1 from meta where key like 'alert_delivery_v1:%'").fetchone()
        assert 'event_id' not in state(store, market)

        early = instant(city, day, 10, 40)
        metar(store, market, early)
        store.record_lamp(forecast(market, instant(city, day, 10, 30), early, peak), market, early)
        assert not events(store), '3h20 before the peak is outside the send window'

    # The existing METAR completion path can admit the retained fresh forecast;
    # no new NOAA issue, worker, service or polling frequency is needed.
    opened = instant(city, day, 11)
    with catalogue.CatalogueStore(cfg, session=False) as store:
        metar(store, market, opened)
        saved_forecast = store.lamp_state(market)
        with store.transaction():
            city_peak.record(store, [market], saved_forecast, opened)
        event, = events(store)
        assert event['at_ms'] == opened and event['city_outlook']['peak_ms'] == peak
        assert event['delivery_window']['opens_ms'] == opened
        assert event['delivery_window']['closes_ms'] == peak
        assert state(store, market)['event_id']
        key = store.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        preview = alert_queue.preview(store, key)
        assert alert_queue.claim(store, preview, opened+1)
        alert_queue.finish(store, preview, dict(status='SENT', message_id=123), opened+2)
        frozen = store.c.execute('select payload from events').fetchone()[0]
    with catalogue.CatalogueStore(cfg, session=False) as store:
        later = opened+city_peak.HOUR
        metar(store, market, later)
        store.record_lamp(forecast(market, instant(city, day, 11, 30), later, peak, 48),
                          market, later)
        assert len(events(store)) == 1
        assert store.c.execute('select payload from events').fetchone()[0] == frozen
        assert not alert_queue.claim(store, preview, later)
        assert store.c.execute('pragma integrity_check').fetchone()[0] == 'ok'
        assert not store.c.execute('pragma foreign_key_check').fetchall()


@pytest.mark.parametrize('city', US_CITIES)
@pytest.mark.parametrize('boundary', ['claim', 'http'])
def test_daytime_deadline_checked_before_claim_and_http(gamma, tmp_path, city, boundary):
    day = '2026-11-14'
    market, metadata = binding(gamma, city, day)
    end = instant(city, day, 20)
    peak = instant(city, day, 22)
    with catalogue.CatalogueStore(config(city, tmp_path), session=False) as store:
        store.bind(market, metadata)
        seed(store, market, peak, city=city, day=day)
        at = end-2000
        admit(store, market, at, instant(city, day, 19, 30), peak)
        event, = events(store)
        assert event['delivery_window']['closes_ms'] == end
        key = store.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        preview = alert_queue.preview(store, key)
        assert preview['expires_ms'] == end-1
        if boundary == 'claim':
            assert not alert_queue.claim(store, preview, end)
            assert json.loads(store.c.execute('select value from meta where key=?', (key,)).fetchone()[0])['status'] == 'EXPIRED'
        else:
            assert alert_queue.claim(store, preview, end-1000)
            reply = telegram.post('offline', preview, clock=lambda: end,
                                  fetch=lambda *a, **kw: pytest.fail('no HTTP may start at night'))
            assert reply == dict(status='REJECTED', reason='QUOTE_EXPIRED_BEFORE_HTTP')
            alert_queue.finish(store, preview, reply, end)
        assert len(events(store)) == 1 and not alert_queue.claim(store, preview, end+1)


@pytest.mark.parametrize('city', US_CITIES)
def test_next_local_date_gets_new_baseline_without_reusing_old_card(gamma, tmp_path, city):
    cfg = config(city, tmp_path)
    m1, r1 = binding(gamma, city, '2026-11-14')
    m2, r2 = binding(gamma, city, '2026-11-15')
    with catalogue.CatalogueStore(cfg, session=False) as store:
        store.bind(m1, r1)
        peak = instant(city, m1['date'], 14)
        seed(store, m1, peak, city=city, day=m1['date'])
        at = instant(city, m1['date'], 11, 40)
        admit(store, m1, at, instant(city, m1['date'], 11, 30), peak)
        old = store.c.execute('select id,payload from events').fetchone()
        old_id, old_raw = tuple(old)
        key = store.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p = alert_queue.preview(store, key)
        assert alert_queue.claim(store, p, at+1)
        alert_queue.finish(store, p, dict(status='SENT', message_id=1), at+2)
    with catalogue.CatalogueStore(cfg, session=False) as store:
        store.bind(m2, r2)
        peak = instant(city, m2['date'], 14)
        seed(store, m2, peak, city=city, day=m2['date'])
        assert state(store, m2)['status'] == 'BASELINE'
        assert 'event_id' not in state(store, m2)
        night = instant(city, m2['date'], 1, 40)
        admit(store, m2, night, instant(city, m2['date'], 1, 30), peak)
        assert len(events(store)) == 1
        at = instant(city, m2['date'], 11, 40)
        admit(store, m2, at, instant(city, m2['date'], 11, 30), peak)
        assert len(events(store)) == 2
        assert store.c.execute('select payload from events where id=?', (old_id,)).fetchone()[0] == old_raw
        assert not alert_queue.claim(store, p, at)


@pytest.mark.parametrize('status', ['SENT', 'UNKNOWN', 'PENDING', 'EXPIRED', 'REJECTED'])
def test_old_city_card_is_not_rewritten_or_rearmed(gamma, tmp_path, status):
    accepted = 'bb1cee26152baf8ff6a254859f66fb9824757dd5'
    root = Path(__file__).resolve().parents[1]
    code = subprocess.check_output(['git', 'show', accepted+':src/holy_weather/city_peak.py'], cwd=root)
    old = types.ModuleType('holy_weather._legacy_city_fixture')
    old.__package__ = 'holy_weather'
    exec(compile(code, accepted+'/city_peak.py', 'exec'), old.__dict__)
    city, day = 'chicago', '2026-11-14'
    cfg = config(city, tmp_path)
    market, metadata = binding(gamma, city, day)
    peak = instant(city, day, 14)
    night = instant(city, day, 1, 40)
    with catalogue.CatalogueStore(cfg, session=False) as store:
        store.bind(market, metadata)
        seed(store, market, peak, city=city, day=day)
        admit(store, market, night, instant(city, day, 1, 30), peak)
        # Produce the real old shape using accepted code on this isolated DB.
        with store.transaction():
            old.record(store, [market], store.lamp_state(market), night)
        event, = events(store)
        assert 'delivery_window' not in event
        key = store.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p = alert_queue.preview(store, key)
        if status in ('SENT', 'UNKNOWN', 'REJECTED'):
            assert alert_queue.claim(store, p, night+1)
            if status != 'UNKNOWN':
                alert_queue.finish(store, p, dict(status=status, message_id=1), night+2)
        elif status == 'EXPIRED':
            assert not alert_queue.claim(store, p, p['expires_ms']+1)
        frozen = store.c.execute('select payload from events').fetchone()[0]
    with catalogue.CatalogueStore(cfg, session=False) as store:
        at = instant(city, day, 11, 40)
        recovery.prepare_resume(store, {'isolated_timing_test': True}, at)
        admit(store, market, at, instant(city, day, 11, 30), peak)
        assert len(events(store)) == 1
        assert store.c.execute('select payload from events').fetchone()[0] == frozen
        assert alert_queue.preview(store, key) == p  # Stored text/deadline unchanged.
        telegram.Sender('offline', transport=lambda *a: pytest.fail('old city card repeated')).tick(store, at)
        current = json.loads(store.c.execute('select value from meta where key=?', (key,)).fetchone()[0])
        assert current['status'] == ('EXPIRED' if status == 'PENDING' else status)


def test_closed_local_window_does_not_take_the_daily_slot(gamma, tmp_path):
    city, day = 'chicago', '2026-11-14'
    market, metadata = binding(gamma, city, day)
    with catalogue.CatalogueStore(config(city, tmp_path), session=False) as store:
        store.bind(market, metadata)
        peak = instant(city, day, 22)
        seed(store, market, peak, city=city, day=day)
        at = instant(city, day, 20, 40)
        admit(store, market, at, instant(city, day, 20, 30), peak)
        assert state(store, market)['status'] == 'WAITING_LOCAL_SEND_WINDOW'
        assert not events(store) and 'event_id' not in state(store, market)


def test_morning_peak_can_use_the_eight_oclock_opening(gamma, tmp_path):
    city, day = 'chicago', '2026-11-14'
    market, metadata = binding(gamma, city, day)
    peak, opened = instant(city, day, 9), instant(city, day, 8)
    with catalogue.CatalogueStore(config(city, tmp_path), session=False) as store:
        store.bind(market, metadata)
        seed(store, market, peak, city=city, day=day)
        admit(store, market, opened-1, instant(city, day, 7, 30), peak)
        assert not events(store)
        metar(store, market, opened)
        with store.transaction():
            city_peak.record(store, [market], store.lamp_state(market), opened)
        event, = events(store)
        assert event['at_ms'] == opened and event['delivery_window']['opens_ms'] == opened


@pytest.mark.parametrize('bad', ['forecast_age', 'metar_age', 'past_higher_peak'])
def test_waiting_never_makes_bad_source_data_eligible(gamma, tmp_path, bad):
    city, day = 'chicago', '2026-11-14'
    market, metadata = binding(gamma, city, day)
    peak = instant(city, day, 14)
    at = instant(city, day, 13, 40)
    with catalogue.CatalogueStore(config(city, tmp_path), session=False) as store:
        store.bind(market, metadata)
        seed(store, market, peak, city=city, day=day)
        observed_at = instant(city, day, 10) if bad == 'metar_age' else at
        r = observations(market, observed_at)
        if bad == 'past_higher_peak':
            rows = json.loads(r['raw_body'])
            rows[0].update(temp=15, rawOb=rows[0]['rawOb'].replace('07/01', '15/01'))
            body = canonical(rows)
            r.update(raw_body=body, sha256=sha(body.encode()))
        w, _ = weather.update({}, r, market, observed_at)
        store.set_meta(catalogue_metar.PREFIX+market['station'], dict(w, day=day))
        issue = instant(city, day, 9, 30) if bad == 'forecast_age' else instant(city, day, 13, 30)
        store.record_lamp(forecast(market, issue, at, peak), market, at)
        reasons = dict(forecast_age='STALE_OR_FUTURE', metar_age='STALE_METAR',
                       past_higher_peak='PEAK_ALREADY_OBSERVED')
        assert state(store, market)['status'] == reasons[bad]
        assert not events(store) and 'event_id' not in state(store, market)


def test_night_price_keeps_its_own_event_thresholds_and_queue(gamma, tmp_path):
    city, day = 'chicago', '2026-11-14'
    market, metadata = binding(gamma, city, day)
    peak = instant(city, day, 14)
    at = instant(city, day, 1, 40)
    with catalogue.CatalogueStore(config(city, tmp_path), session=False) as store:
        store.bind(market, metadata)
        seed(store, market, peak, city=city, day=day)
        store.connection_started(at-2)
        router = catalogue.Router(store)
        for n, value in enumerate(['0.49', '0.71']):
            router.feed(canonical(dict(event_type='best_bid_ask', market=market['condition_id'],
                                       asset_id=market['tokens']['YES'], timestamp=str(at-1+n),
                                       best_bid=value, best_ask=value)), at-1+n)
        price = tuple(store.c.execute("select id,payload from events where type='PRICE_ALERT'").fetchone())
        fired = store.c.execute("select value from meta where key=?",
                                ('price_alert_v1:'+market['tokens']['YES'],)).fetchone()[0]
        delivery = tuple(store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'").fetchone())
        admit(store, market, at, instant(city, day, 1, 30), peak)
        assert not events(store) and 'event_id' not in state(store, market)
        later = instant(city, day, 11, 40)
        admit(store, market, later, instant(city, day, 11, 30), peak)
        assert len(events(store)) == 1
        assert tuple(store.c.execute("select id,payload from events where type='PRICE_ALERT'").fetchone()) == price
        assert store.c.execute("select value from meta where key=?",
                               ('price_alert_v1:'+market['tokens']['YES'],)).fetchone()[0] == fired
        assert tuple(store.c.execute('select key,value from meta where key=?', (delivery[0],)).fetchone()) == delivery


def test_accepted_reader_preserves_new_deadline_on_current_data(gamma, tmp_path):
    root = Path(__file__).resolve().parents[1]
    accepted = 'bb1cee26152baf8ff6a254859f66fb9824757dd5'
    code = subprocess.check_output(['git', 'show', accepted+':src/holy_weather/alert_queue.py'], cwd=root)
    old = types.ModuleType('holy_weather._legacy_queue_fixture')
    old.__package__ = 'holy_weather'
    exec(compile(code, accepted+'/alert_queue.py', 'exec'), old.__dict__)
    city, day = 'chicago', '2026-11-14'
    market, metadata = binding(gamma, city, day)
    end, peak = instant(city, day, 20), instant(city, day, 22)
    with catalogue.CatalogueStore(config(city, tmp_path), session=False) as store:
        store.bind(market, metadata)
        seed(store, market, peak, city=city, day=day)
        admit(store, market, end-2000, instant(city, day, 19, 30), peak)
        event = store.c.execute('select id,payload from events').fetchone()
        key = store.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p = alert_queue.preview(store, key)
        assert old.preview(store, key) == p and p['expires_ms'] == end-1
        assert not old.claim(store, p, end)
        assert not alert_queue.claim(store, p, end+1)
        assert tuple(store.c.execute('select id,payload from events').fetchone()) == tuple(event)
        assert store.c.execute('pragma user_version').fetchone()[0] == 1
        assert store.c.execute('pragma integrity_check').fetchone()[0] == 'ok'
        assert not store.c.execute('pragma foreign_key_check').fetchall()


def test_installed_price_stack_accepts_scoped_lamp_changes(tmp_path, record_property):
    """Keep installed US #40/#41 code while exercising every new timing case."""
    root = Path(__file__).resolve().parents[1]
    installed = '67e2f634bdc15a7035d989b4d7d67716af16aa99'
    parent = 'bb1cee26152baf8ff6a254859f66fb9824757dd5'
    target = tmp_path/'installed'

    def saved(ref, path):
        return subprocess.check_output(['git', 'show', ref+':'+path], cwd=root).decode()

    names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', installed,
                                     '--', 'src'], cwd=root, text=True).splitlines()
    originals = {}
    for name in names:
        p = target/name
        p.parent.mkdir(parents=True, exist_ok=True)
        originals[name] = saved(installed, name)
        p.write_text(originals[name])

    def function(source, name):
        node = next(n for n in ast.walk(ast.parse(source)) if isinstance(n, ast.FunctionDef) and n.name == name)
        lines = source.splitlines(keepends=True)
        return node, ''.join(lines[node.lineno-1:node.end_lineno])

    peak = 'src/holy_weather/city_peak.py'
    queue = 'src/holy_weather/alert_queue.py'
    sender = 'src/holy_weather/telegram.py'
    assert originals[peak] == saved(parent, peak)
    (target/peak).write_text((root/peak).read_text())
    for path, functions in [(queue, ['card', 'enqueue']), (sender, ['_collect'])]:
        current = originals[path]
        for name in functions:
            node, old_body = function(current, name)
            _, parent_body = function(saved(parent, path), name)
            assert ast.dump(ast.parse(textwrap.dedent(old_body))) == ast.dump(ast.parse(textwrap.dedent(parent_body)))
            _, new_body = function((root/path).read_text(), name)
            lines = current.splitlines(keepends=True)
            current = ''.join(lines[:node.lineno-1])+new_body+''.join(lines[node.end_lineno:])
        (target/path).write_text(current)
    for name, value in originals.items():
        if name not in (peak, queue, sender):
            assert (target/name).read_text() == value
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PYTHONPATH=str(target/'src'))
    import_check = subprocess.run([sys.executable, '-B', '-c',
        'import holy_weather.catalogue; print(holy_weather.catalogue.__file__)'],
        env=env, capture_output=True, text=True, timeout=10)
    assert import_check.returncode == 0 and str(target/'src') in import_check.stdout
    result = subprocess.run([sys.executable, '-B', '-m', 'pytest', '-q',
        str(Path(__file__)), '-k', 'not installed_price_stack',
        '-o', 'faulthandler_timeout=0', '--ci-stack-seconds=0'],
        cwd=root, env=env, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout+result.stderr
    record_property('installed_us_price_base', installed)
    record_property('installed_us_timing_cases', result.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize('outcome', ['local_expiry', 'unknown', 'api_reject'])
def test_lamp_window_expiry_does_not_disable_price_sender(gamma, tmp_path, monkeypatch, outcome):
    city, day = 'chicago', '2026-11-14'
    market, metadata = binding(gamma, city, day)
    end, peak = instant(city, day, 20), instant(city, day, 22)
    clock = [end]
    monkeypatch.setattr(telegram, 'millis', lambda: clock[0])
    with catalogue.CatalogueStore(config(city, tmp_path), session=False) as store:
        store.bind(market, metadata)
        seed(store, market, peak, city=city, day=day)
        admit(store, market, end-2000, instant(city, day, 19, 30), peak)
        lamp_id = state(store, market)['event_id']
        sent_price = []

        def transport(token, p):
            if p['event_id'] != lamp_id:
                sent_price.append(p['event_id'])
                return dict(status='SENT', message_id=42)
            if outcome == 'unknown':
                return dict(status='UNKNOWN', reason='TimeoutError')
            if outcome == 'api_reject':
                return dict(status='REJECTED', reason='API_REJECTED')
            return telegram.post(token, p, clock=lambda: clock[0],
                                 fetch=lambda *a, **kw: pytest.fail('expired LAMP must make no HTTP call'))

        sender = telegram.Sender('offline', transport=transport, minimum_interval_ms=3100)
        sender.tick(store, end-1000)  # Claim is valid; the HTTP thread sees the cutoff.
        sender.thread.join(1)
        sender.tick(store, end)
        if outcome != 'local_expiry':
            assert not sender.status()['telegram_enabled']
            assert sender.disabled_reason in ('SEND_RESULT_UNKNOWN', 'SEND_RESULT_NOT_SENT')
            return
        assert sender.status() == dict(telegram_enabled=True, telegram_disabled_reason=None)
        # PRICE keeps normal night delivery and pacing after the known no-HTTP refusal.
        at = end+4000
        clock[0] = at
        store.connection_started(at)
        router = catalogue.Router(store)
        for n, price in enumerate(['0.49', '0.71']):
            router.feed(canonical(dict(event_type='best_bid_ask', market=market['condition_id'],
                                       asset_id=market['tokens']['YES'], timestamp=str(at+n),
                                       best_bid=price, best_ask=price)), at+n)
        sender.tick(store, at+2)
        sender.thread.join(1)
        sender.tick(store, at+3)
        assert len(sent_price) == 1 and sender.status()['telegram_enabled']
        sender.tick(store, at+4000)
        assert len(sent_price) == 1


def test_all_ten_cities_share_one_store_and_keep_sent_unknown_after_restart(gamma, tmp_path):
    day = '2026-11-14'
    bindings = [binding(gamma, city, day) for city in US_CITIES]
    cfg = config('new-york', tmp_path)
    def utc(hour, minute):
        return int(datetime(2026, 11, 14, hour, minute, tzinfo=timezone.utc).timestamp()*1000)
    peak, ready = utc(22, 0), utc(19, 40)
    with catalogue.CatalogueStore(cfg, session=False) as store:
        for market, metadata in bindings:
            store.bind(market, metadata)
            store.record_lamp(forecast(market, utc(10, 30), utc(10, 40), peak), market, utc(10, 40))
            admit(store, market, utc(11, 40), utc(11, 30), peak)
        assert not events(store)
        assert not store.c.execute("select 1 from meta where key like 'alert_delivery_v1:%'").fetchone()
        for market, _ in bindings:
            admit(store, market, ready, utc(19, 30), peak)
        assert len(events(store)) == 10
        assert {event['station'] for event in events(store)} == {v[1] for v in US_CITIES.values()}
        rows = store.c.execute("select key from meta where key like 'alert_delivery_v1:%' order by key").fetchall()
        for n, (key,) in enumerate(rows):
            p = alert_queue.preview(store, key)
            assert alert_queue.claim(store, p, ready+n+1)
            if n % 2 == 0:
                alert_queue.finish(store, p, dict(status='SENT', message_id=n+1), ready+n+2)
        frozen_events = [tuple(row) for row in store.c.execute('select id,payload from events order by id')]
        frozen_queue = [tuple(row) for row in store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' order by key")]
    with catalogue.CatalogueStore(cfg, session=False) as store:
        recovery.prepare_resume(store, {'isolated_regional_test': True}, utc(20, 40))
        for market, _ in bindings:
            admit(store, market, utc(20, 40), utc(20, 30), peak)
        assert [tuple(row) for row in store.c.execute('select id,payload from events order by id')] == frozen_events
        assert [tuple(row) for row in store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' order by key")] == frozen_queue
        telegram.Sender('offline', transport=lambda *a: pytest.fail('regional duplicate')).tick(store, utc(20, 40))
        assert store.c.execute('pragma integrity_check').fetchone()[0] == 'ok'
        assert not store.c.execute('pragma foreign_key_check').fetchall()
