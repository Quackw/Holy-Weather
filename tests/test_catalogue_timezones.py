"""Europe's full date scan must not repeatedly load timezone files."""
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import types
from zoneinfo import ZoneInfo, _common

import websockets
from holy_weather import catalogue, history
from holy_weather.catalogue import CatalogueStore, DayDiscovery
from test_catalogue import AT, setup

ACCEPTED = 'bb1cee26152baf8ff6a254859f66fb9824757dd5'
ROOT = Path(__file__).resolve().parents[1]


def accepted_catalogue(monkeypatch):
    source = subprocess.check_output(['git', 'show', ACCEPTED+':src/holy_weather/catalogue.py'], cwd=ROOT)
    module = types.ModuleType('holy_weather._accepted_timezone_probe')
    module.__package__ = 'holy_weather'
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(compile(source, ACCEPTED+'/catalogue.py', 'exec'), module.__dict__)
    return module


def test_europe_date_checks_do_not_reload_tzif_per_market_frame(gamma, tmp_path, monkeypatch):
    cfg, markets = setup(gamma, tmp_path)
    by_city = dict(zip(cfg.cities, markets))

    def discover(city):
        market, receipt = by_city[city]
        return [market], receipt, []

    loads = []
    original = _common.load_data

    def load(file):
        loads.append(file.name)
        return original(file)

    ZoneInfo.clear_cache()
    monkeypatch.setattr(_common, 'load_data', load)
    try:
        with CatalogueStore(cfg) as store:
            days = DayDiscovery(store, discover, lambda: AT)
            days.refresh()
            assert len(days.selected) == 11
            loads.clear()
            for _ in range(100):
                assert not days.due()
                assert all(store.active(condition, AT) for condition in days.selected)
            assert not loads, f'{len(loads)} repeated TZif reads during unchanged local dates'
    finally:
        ZoneInfo.clear_cache()


def test_native_20s_burst_before_after_timezone_retention(gamma, tmp_path, monkeypatch, record_property):
    from europe_timezone_burst import burst
    old = accepted_catalogue(monkeypatch)
    cfg, markets = setup(gamma, tmp_path)
    before = burst(old, replace(cfg, data_dir=str(tmp_path/'old')), markets)
    after = burst(catalogue, replace(cfg, data_dir=str(tmp_path/'new')), markets)
    record_property('native_20s_tzif_burst_before', json.dumps(before))
    record_property('native_20s_tzif_burst_after', json.dumps(after))
    # A deliberately bounded synthetic publication and explicit 1-ms TZif I/O
    # cost. This demonstrates the mechanism, not the historical WAN workload.
    assert before['close_sent'] == 1011 and before['frames'] < before['count'], before
    assert before['disconnects'][0]['close_code'] == 1011
    assert 'keepalive ping timeout' in before['disconnects'][0]['detail']
    assert after['frames'] == after['count'] and after['published'], after
    assert after['close_sent'] == 1000 and after['native_rtt'] > 0 and not after['disconnects']
    assert after['reads'] <= 10 and before['reads'] > 1000
    for result in (before, after):
        assert result['price_events'] == 11
        assert result['integrity'] == 'ok' and result['foreign_keys'] == 0
        assert result['data_bytes'] < cfg.storage_bytes


def test_same_inputs_preserve_all_price_tables_with_timezone_retention(gamma, tmp_path, monkeypatch):
    old = accepted_catalogue(monkeypatch)
    cfg, markets = setup(gamma, tmp_path)
    results = []
    for name, module in [('old', old), ('new', catalogue)]:
        case = module.CatalogueConfig(**dict(asdict(cfg), data_dir=str(tmp_path/name)))
        by_city = {city: ([market], receipt, [])
                   for city, (market, receipt) in zip(case.cities, markets)}
        with module.CatalogueStore(case, session=False) as store:
            days = module.DayDiscovery(store, lambda city: by_city[city], lambda: AT)
            days.refresh()
            store.connection_started(AT)
            router = module.Router(store, days.selected)
            for n in range(2200):
                assert not days.due()
                market = markets[n % 11][0]
                side = 'YES' if (n // 11) % 2 == 0 else 'NO'
                price = ['0.49', '0.56', '0.71', '0.81'][(n // 22) % 4]
                raw = json.dumps(dict(event_type='best_bid_ask', market=market['condition_id'],
                                      asset_id=market['tokens'][side], timestamp=str(AT+n+1),
                                      best_bid=price, best_ask=price))
                router.feed(raw, AT+n+1)
                if n % 100 == 0:
                    router.feed(raw, AT+n+1)  # Exact duplicate, not a new confirmation.
                if n == 1100:
                    store.gap('OFFLINE_CONTROL', AT+n+1)
                    store.connection_started(AT+n+1)
            history.check(store.c)
            results.append(history.fingerprint(store.c))
            assert store.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0] == 66
    assert results[0] == results[1]


def test_accepted_code_rollback_after_new_price_and_queue_records(gamma, tmp_path):
    _, markets = setup(gamma, tmp_path, 1)
    (tmp_path/'market.json').write_text(json.dumps(markets[0][0]))
    accepted = tmp_path/'accepted'
    shutil.copytree(ROOT/'src', accepted)
    source = subprocess.check_output(['git', 'show', ACCEPTED+':src/holy_weather/catalogue.py'], cwd=ROOT)
    (accepted/'holy_weather/catalogue.py').write_bytes(source)
    for phase, path, count in [('old', accepted, 1), ('new', ROOT/'src', 2), ('rollback', accepted, 3)]:
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1',
                   PYTHONPATH=str(path)+os.pathsep+str(Path(websockets.__file__).resolve().parents[1]))
        result = subprocess.run([sys.executable, '-B', str(ROOT/'tests/regional_rollback_probe.py'),
                                 'europe', phase, str(tmp_path)], env=env, text=True,
                                capture_output=True, timeout=30)
        assert result.returncode == 0, result.stdout+result.stderr
        proof = json.loads(result.stdout)
        assert proof['price_events'] == count and proof['integrity'] == 'ok'
        if phase == 'rollback':
            assert sorted(proof['queue']) == ['PENDING', 'SENT', 'UNKNOWN']
