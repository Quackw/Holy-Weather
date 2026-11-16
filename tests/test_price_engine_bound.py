"""A research-window bound is not a failure of an otherwise valid PRICE quote."""
from collections import Counter
from dataclasses import replace
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tarfile

import pytest

from holy_weather.app import process
from holy_weather.core import canonical
from holy_weather.storage import Store
from test_price_alert import AT, alerts, send


def enabled(cfg):
    return replace(cfg, price_alert_enabled=True, alert_queue_enabled=True,
                   price_notification_filter=True)


def research_rows(store):
    return [tuple(r) for r in store.c.execute(
        "select * from events where type!='PRICE_ALERT' order by id")]


def prime_boundary(store, token):
    # Small fault fixture for boundary/failure cases. The load test below instead
    # reaches the bound through 4097 actual process()/SQLite input transactions.
    state = json.loads(store.c.execute('select payload from state where token=?', (token,)).fetchone()[0])
    last = state['points'][-1]
    state['points'] = [dict(last, ask='0.4700' if i % 2 else '0.4701') for i in range(4096)]
    with store.transaction():
        store.c.execute('update state set payload=? where token=?', (canonical(state), token))


def start(store, market, make_raw):
    store.bind(market, {})
    store.connection_started(AT)
    send(store, market, make_raw('0.49', AT, bid='0.47'), AT)


def test_4097_changed_asks_keep_price_cross_and_research_history(cfg, market, make_raw):
    counts = Counter()
    with Store(enabled(cfg)) as store:
        store.bind(market, {})
        store.connection_started(AT)
        for i in range(4096):
            ask = '0.51' if i == 1 else ('0.4700' if i % 2 else '0.4701')
            raw = make_raw(ask, AT+i, bid='0.10')
            process(json.dumps(raw), AT+i, market, store, counts)
        before = research_rows(store)
        assert before  # Existing research CROSS evidence must survive.
        assert store.c.execute('select count(*) from samples').fetchone()[0] == 4096
        assert len(json.loads(store.c.execute('select payload from state').fetchone()[0])['points']) == 4096
        raw = make_raw('0.62', AT+4096, bid='0.60')
        process(json.dumps(raw), AT+4096, market, store, counts)
        assert [e['thresholds'] for e in alerts(store)] == [['0.50', '0.55', '0.60']]
        assert research_rows(store) == before
        assert counts['FRESH'] == 4097
        assert store.c.execute("select count(*) from samples where status='ENGINE_POINT_BOUND'").fetchone()[0] == 1
        token = market['tokens']['YES']
        state = json.loads(store.c.execute('select payload from state where token=?', (token,)).fetchone()[0])
        assert state['points'] == [] and state['high_source'] == AT+4096
        assert token in store.ready_tokens and token in store.price_alert_ready
        snapshot = store.observation_snapshot(now_ms=AT+4096)
        assert snapshot['observation']['sides']['YES'] == 'READY'
        assert snapshot['latest'][0]['status'] == 'ENGINE_POINT_BOUND'
        assert snapshot['latest'][0]['quote_usable_now']
        row = store.c.execute("select value from meta where key like 'alert_delivery_v1:%'").fetchone()
        assert json.loads(row[0])['status'] == 'PENDING'
        process(json.dumps(raw), AT+4097, market, store, counts)
        assert len(alerts(store)) == 1 and counts['DUPLICATE'] == 1
        assert store.c.execute('pragma integrity_check').fetchone()[0] == 'ok'
        assert not store.c.execute('pragma foreign_key_check').fetchall()


@pytest.mark.parametrize('invalid', ['STALE', 'FUTURE', 'OUT_OF_ORDER', 'MISSING_ASK', 'MISSING_BID'])
def test_bound_does_not_whitelist_invalid_quotes(cfg, market, make_raw, invalid):
    with Store(enabled(cfg)) as store:
        start(store, market, make_raw)
        prime_boundary(store, market['tokens']['YES'])
        source = AT-90001 if invalid == 'STALE' else AT+100 if invalid == 'FUTURE' else AT-1 if invalid == 'OUT_OF_ORDER' else AT+1
        raw = make_raw(None if invalid == 'MISSING_ASK' else '0.82', source,
                       bid=None if invalid == 'MISSING_BID' else '0.80')
        send(store, market, raw, AT+1)
        assert not alerts(store) and market['tokens']['YES'] not in store.price_alert_ready
        send(store, market, make_raw('0.82', AT+101, bid='0.80'), AT+101)
        assert not alerts(store)


def test_research_only_mode_keeps_original_bound(cfg, market, make_raw):
    with Store(cfg) as store:
        start(store, market, make_raw)
        prime_boundary(store, market['tokens']['YES'])
        send(store, market, make_raw('0.62', AT+1, bid='0.60'), AT+1)
        assert not alerts(store)
        assert market['tokens']['YES'] not in store.ready_tokens
        assert store.c.execute('select status from samples order by input_id desc').fetchone()[0] == 'ENGINE_POINT_BOUND'


@pytest.mark.parametrize('kind', ['best_bid_ask', 'book', 'price_change'])
def test_boundary_quote_or_depth_keeps_price_and_waiting_delivery(cfg, market, make_raw, kind):
    with Store(enabled(cfg)) as store:
        start(store, market, make_raw)
        # Ask-only 70 crossing waits for a later full bid confirmation.
        send(store, market, make_raw('0.94', AT+1, bid='0.47'), AT+1)
        key, raw = store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'").fetchone()
        assert json.loads(raw)['status'] == 'WAITING_BID'
        prime_boundary(store, market['tokens']['YES'])
        raw = make_raw('0.93', AT+2, bid='0.49', kind=kind)
        if kind == 'price_change':
            raw = dict(event_type=kind, market=market['condition_id'], timestamp=str(AT+2),
                       price_changes=[dict(asset_id=market['tokens']['YES'], best_bid='0.49', best_ask='0.93')])
        send(store, market, raw, AT+2)
        if kind == 'price_change':
            assert json.loads(store.c.execute('select value from meta where key=?', (key,)).fetchone()[0])['status'] == 'WAITING_BID'
            send(store, market, make_raw('0.93', AT+3, bid='0.49'), AT+3)
        value = json.loads(store.c.execute('select value from meta where key=?', (key,)).fetchone()[0])
        assert value['status'] == 'PENDING' and value['confirmation'] == 'CONFIRMED'
        assert len(alerts(store)) == 1


@pytest.mark.parametrize('boundary', ['GAP', 'restart'])
def test_bound_then_real_discontinuity_still_requires_baseline(cfg, market, make_raw, boundary):
    cfg = enabled(cfg)
    with Store(cfg) as store:
        start(store, market, make_raw)
        prime_boundary(store, market['tokens']['YES'])
        send(store, market, make_raw('0.62', AT+1, bid='0.60'), AT+1)
        assert len(alerts(store)) == 1
        if boundary == 'GAP':
            store.gap('test', AT+2)
            store.connection_started(AT+3)
            send(store, market, make_raw('0.82', AT+4, bid='0.80'), AT+4)
            assert len(alerts(store)) == 1
    with Store(cfg) as store:
        store.connection_started(AT+5)
        # Older source cannot become the new baseline after the research reset.
        send(store, market, make_raw('0.82', AT, bid='0.80'), AT+6)
        send(store, market, make_raw('0.82', AT+7, bid='0.80'), AT+7)
        assert len(alerts(store)) == 1
        send(store, market, make_raw('0.49', AT+8, bid='0.47'), AT+8)
        send(store, market, make_raw('0.82', AT+9, bid='0.80'), AT+9)
        assert [e['thresholds'] for e in alerts(store)] == [['0.50', '0.55', '0.60'], ['0.70', '0.80']]


def test_boundary_sqlite_error_rolls_back_input_event_queue_and_diagnostic(cfg, market, make_raw):
    with Store(enabled(cfg)) as store:
        start(store, market, make_raw)
        prime_boundary(store, market['tokens']['YES'])
        tables = ('inputs', 'samples', 'events', 'event_inputs', 'meta', 'state', 'diagnostics')
        before = {t: [tuple(r) for r in store.c.execute('select * from '+t)] for t in tables}
        store.c.execute("create temp trigger fail_bound before insert on events when NEW.type='PRICE_ALERT' begin select raise(abort,'BOUND_SQLITE_FAILURE'); end")
        raw = make_raw('0.62', AT+1, bid='0.60')
        with pytest.raises(sqlite3.IntegrityError, match='BOUND_SQLITE_FAILURE'):
            send(store, market, raw, AT+1)
        assert {t: [tuple(r) for r in store.c.execute('select * from '+t)] for t in tables} == before
        store.c.execute('drop trigger fail_bound')
        send(store, market, raw, AT+1)
        assert len(alerts(store)) == 1
        assert not store.c.execute('pragma foreign_key_check').fetchall()


@pytest.mark.parametrize('region', ['europe', 'us'])
def test_exact_accepted_code_rollback_after_new_boundary_records(gamma, tmp_path, region):
    from holy_weather.core import Config
    from holy_weather.discovery import select
    from conftest import NOW
    from test_europe import european
    import websockets
    root = Path(__file__).resolve().parents[1]
    accepted = '116597bb0ab94578069c5b205eb08b713a41eb9a'
    # Repository history is obtained by checkout, never by test-time networking.
    archive = subprocess.run(['git', 'archive', accepted, 'src'], cwd=root,
                             capture_output=True, check=True, timeout=10).stdout
    old = tmp_path/'accepted'
    with tarfile.open(fileobj=io.BytesIO(archive)) as files:
        for member in files:
            assert member.isdir() or member.isfile()
            target = (old/member.name).resolve()
            assert target.is_relative_to(old.resolve())
            if member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(files.extractfile(member).read())
    city = 'london' if region == 'europe' else 'new-york'
    doc = european(gamma, city) if region == 'europe' else gamma
    market = select(doc, Config(city=city, bucket='13°C' if region == 'europe' else '46-47°F'), now=NOW)
    (tmp_path/'market.json').write_text(json.dumps(market))
    probe = str(Path(__file__).with_name('regional_rollback_probe.py'))
    dependency = str(Path(websockets.__file__).resolve().parents[1])
    for phase, source in [('old', old/'src'), ('new', root/'src'), ('rollback', old/'src')]:
        env = dict(os.environ, PYTHONPATH=str(source)+os.pathsep+dependency,
                   PYTHONDONTWRITEBYTECODE='1', HOLY_WEATHER_RESEARCH_BOUND_PROBE='1')
        code = ('from pathlib import Path; import holy_weather.storage as s, runpy; '
                'assert Path(s.__file__).resolve().is_relative_to(Path('+repr(str(source))+')); '
                'runpy.run_path('+repr(probe)+',run_name="__main__")')
        result = subprocess.run([sys.executable, '-B', '-c', code, region, phase, str(tmp_path)],
                                env=env, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stdout+result.stderr
        data = json.loads(result.stdout)
        assert data['price_events'] == {'old': 1, 'new': 2, 'rollback': 3}[phase]
        if phase != 'old':
            assert {'SENT', 'UNKNOWN'} <= set(data['queue'])
