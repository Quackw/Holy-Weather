"""Legacy day refusals; synthetic offline discovery, real SQLite and PRICE state."""
from datetime import timedelta
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from holy_weather.catalogue import CatalogueConfig, CatalogueStore, DayDiscovery, DAY_PREFIX, Router
from holy_weather.core import Config, EUROPE_CITIES, US_CITIES, canonical
from holy_weather.discovery import select
from holy_weather.history import fingerprint
from holy_weather.alert_queue import preview, claim, finish
from conftest import NOW
from test_catalogue import feed
from test_europe import european

AT = int(NOW.timestamp() * 1000)
REFUSAL_PREFIX = 'price_catalogue_day_refusal_v1:'
CASES = [('europe', c) for c in EUROPE_CITIES] + [('us', c) for c in US_CITIES]


def fixture(gamma, tmp_path, region, city):
    cfg = CatalogueConfig(region=region, city=city, data_dir=str(tmp_path/'data'))
    if region == 'europe':
        doc = european(gamma, city)
        bucket = '13°C'
    else:
        slug, station, _ = US_CITIES[city]
        doc = json.loads(json.dumps(gamma).replace('nyc', slug).replace('KLGA', station))
        bucket = '46-47°F'
    m = select(doc, Config(city=city, bucket=bucket), now=NOW)
    receipt = dict(received_at=NOW.isoformat(), raw_body=json.dumps(doc))
    old = dict(day=m['date'], at_ms=AT-1000, attempts=1, status='BLOCKED',
               skipped=[{'reason': 'SESSION_LOCAL_DATE_ENDED'}])
    return cfg, m, receipt, old


def discoverer(city, m, receipt, calls):
    def discover(c):
        calls.append(c)
        return ([m], receipt, []) if c == city else ([], {}, [])
    return discover


def saved(s, key):
    row = s.c.execute('select value from meta where key=?', (key,)).fetchone()
    return row[0] if row else None


@pytest.mark.parametrize('region,city', CASES)
def test_legacy_refusal_revalidates_each_city_and_retains_original(gamma, tmp_path, region, city):
    cfg, m, receipt, old = fixture(gamma, tmp_path, region, city)
    key = DAY_PREFIX+city
    audit = REFUSAL_PREFIX+city+':'+m['date']
    calls = []
    with CatalogueStore(cfg, session=False) as s:
        s.set_meta(key, old)
        raw = saved(s, key)
        d = DayDiscovery(s, discoverer(city, m, receipt, calls), lambda: AT,
                         session_day=m['date'])
        d.refresh()
        assert calls.count(city) == 1
        assert d.selected == {m['condition_id']}
        assert saved(s, audit) == raw
        new = json.loads(saved(s, key))
        assert new['status'] == 'VERIFIED' and new['attempts'] == 2
        assert new['receipt_input_id'] and s.discovery_reference(m['tokens']['YES'])
        s.connection_started(AT)
        feed(Router(s, d.selected), m, '0.81', AT+1)
        assert not s.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()
    with CatalogueStore(cfg, session=False) as s:
        d = DayDiscovery(s, discoverer(city, m, receipt, calls), lambda: AT+2,
                         session_day=m['date'])
        d.refresh()
        assert saved(s, audit) == raw
        assert s.c.execute('select count(*) from meta where key like ?',
                           (REFUSAL_PREFIX+'%',)).fetchone()[0] == 1
        assert s.c.execute('pragma user_version').fetchone()[0] == 1
        assert s.c.execute('pragma integrity_check').fetchone()[0] == 'ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()


@pytest.mark.parametrize('change', [
    {'status': 'IN_PROGRESS'}, {'reason': 'HTTPError:403'},
    {'receipt_input_id': 1}, {'attempts': 3}, {'attempts': True},
    {'at_ms': AT+1}, {'at_ms': AT-86400000},
    {'skipped': [{'reason': 'RULE_MISMATCH'}]},
    {'skipped': [{'reason': 'SESSION_LOCAL_DATE_ENDED'}, {'reason': 'HTTP_429'}]},
])
def test_other_blocked_or_unknown_states_are_never_reopened(gamma, tmp_path, change):
    cfg, m, receipt, old = fixture(gamma, tmp_path, 'europe', 'paris')
    old.update(change);calls = []
    with CatalogueStore(cfg, session=False) as s:
        s.set_meta(DAY_PREFIX+'paris', old)
        d = DayDiscovery(s, discoverer('paris', m, receipt, calls), lambda: AT,
                         session_day=m['date'])
        d.refresh()
        assert 'paris' not in calls
        assert json.loads(saved(s, DAY_PREFIX+'paris')) == old
        assert not s.c.execute('select 1 from meta where key like ?',
                               (REFUSAL_PREFIX+'%',)).fetchone()


@pytest.mark.parametrize('region,city', [('europe', 'paris'), ('us', 'new-york')])
def test_no_retry_reset_and_original_survives_network_failure(gamma, tmp_path, region, city):
    cfg, m, receipt, old = fixture(gamma, tmp_path, region, city)
    key = DAY_PREFIX+city;at = [AT];calls = []
    def failing(c):
        if c == city:
            calls.append(c)
            raise TimeoutError('synthetic timeout')
        return [], {}, []
    for _ in range(4):
        with CatalogueStore(cfg, session=False) as s:
            if not saved(s, key):s.set_meta(key, old)
            d = DayDiscovery(s, failing, lambda: at[0], session_day=m['date'])
            d.refresh()
        at[0] += 300000
    assert calls == [city, city]  # Historical attempt 1 is not reset.
    with CatalogueStore(cfg, session=False) as s:
        assert json.loads(saved(s, key))['attempts'] == 3
        assert json.loads(saved(s, REFUSAL_PREFIX+city+':'+m['date'])) == old


@pytest.mark.parametrize('failed_key', ['audit', 'day'])
def test_audit_and_in_progress_commit_atomically(gamma, tmp_path, failed_key):
    cfg, m, receipt, old = fixture(gamma, tmp_path, 'europe', 'paris')
    key = DAY_PREFIX+'paris';audit = REFUSAL_PREFIX+'paris:'+m['date'];calls = []
    with CatalogueStore(cfg, session=False) as s:
        s.set_meta(key, old);raw = saved(s, key)
        target = audit if failed_key == 'audit' else key
        s.c.execute("CREATE TEMP TRIGGER fault BEFORE INSERT ON meta WHEN NEW.key='"+
                    target+"' BEGIN SELECT RAISE(ABORT,'synthetic SQLite fault'); END")
        with pytest.raises(sqlite3.IntegrityError):
            DayDiscovery(s, discoverer('paris', m, receipt, calls), lambda: AT,
                         session_day=m['date']).refresh()
        assert 'paris' not in calls and saved(s, key) == raw and saved(s, audit) is None
        s.c.execute('DROP TRIGGER fault')
        DayDiscovery(s, discoverer('paris', m, receipt, calls), lambda: AT,
                     session_day=m['date']).refresh()
        assert calls.count('paris') == 1 and saved(s, audit) == raw


def test_prior_audit_or_unknown_attempt_never_repeats(gamma, tmp_path):
    cfg, m, receipt, old = fixture(gamma, tmp_path, 'europe', 'paris')
    key = DAY_PREFIX+'paris';audit = REFUSAL_PREFIX+'paris:'+m['date']
    calls = []
    def interrupted(c):
        if c == 'paris':calls.append(c);raise RuntimeError('synthetic interrupted request')
        return [], {}, []
    with CatalogueStore(cfg, session=False) as s:
        s.set_meta(key, old)
        with pytest.raises(RuntimeError):
            DayDiscovery(s, interrupted, lambda: AT, session_day=m['date']).refresh()
        assert json.loads(saved(s, key))['status'] == 'IN_PROGRESS'
        raw = saved(s, audit)
    with CatalogueStore(cfg, session=False) as s:
        DayDiscovery(s, interrupted, lambda: AT+1, session_day=m['date']).refresh()
        assert calls == ['paris']
        # Even if an old writer put the exact legacy refusal back, do not repeat it.
        s.set_meta(key, old)
        DayDiscovery(s, interrupted, lambda: AT+2, session_day=m['date']).refresh()
        assert calls == ['paris'] and saved(s, audit) == raw


@pytest.mark.parametrize('failure', ['denied', 'wrong_station', 'wrong_date'])
def test_revalidation_does_not_bypass_current_source_or_contract_refusal(gamma, tmp_path, failure):
    from urllib.error import HTTPError
    cfg, m, receipt, old = fixture(gamma, tmp_path, 'europe', 'paris')
    calls = []
    def rejected(c):
        if c != 'paris':return [], {}, []
        calls.append(c)
        if failure == 'denied':raise HTTPError('https://example.invalid', 403, 'denied', {}, None)
        market = dict(m, station='EHAM') if failure == 'wrong_station' else dict(m, date='2026-11-13')
        return [market], receipt, []
    for offset in (0, 1000):
        with CatalogueStore(cfg, session=False) as s:
            if not saved(s, DAY_PREFIX+'paris'):s.set_meta(DAY_PREFIX+'paris', old)
            d = DayDiscovery(s, rejected, lambda: AT+offset, session_day=m['date'])
            d.refresh()
            assert json.loads(saved(s, DAY_PREFIX+'paris'))['status'] == 'BLOCKED'
            assert not d.selected and not s.markets
            assert json.loads(saved(s, REFUSAL_PREFIX+'paris:'+m['date'])) == old
    assert calls == ['paris']


@pytest.mark.parametrize('region,city', [('europe', 'paris'), ('us', 'new-york')])
def test_ended_finite_session_does_not_admit_legacy_refusal(gamma, tmp_path, region, city):
    cfg, m, receipt, old = fixture(gamma, tmp_path, region, city)
    calls = []
    with CatalogueStore(cfg, session=False) as s:
        s.set_meta(DAY_PREFIX+city, old)
        DayDiscovery(s, discoverer(city, m, receipt, calls), lambda: AT,
                     session_day=(NOW-timedelta(days=1)).date().isoformat()).refresh()
        assert not calls and json.loads(saved(s, DAY_PREFIX+city)) == old
        assert not s.c.execute('select 1 from meta where key like ?',
                               (REFUSAL_PREFIX+'%',)).fetchone()


@pytest.mark.parametrize('region,city', [('europe', 'paris'), ('us', 'new-york')])
def test_exact_main_rollback_preserves_new_history_fired_and_queue(gamma, tmp_path, region, city):
    cfg, m, receipt, old = fixture(gamma, tmp_path, region, city)
    with CatalogueStore(cfg, session=False) as s:
        s.bind(m, receipt);s.connection_started(AT);router = Router(s)
        for n, price in enumerate(('0.49', '0.51', '0.56', '0.61')):
            feed(router, m, price, AT+n+1)
        rows = s.c.execute("select key from meta where key like 'alert_delivery_v1:%' order by json_extract(value,'$.event_ms')").fetchall()
        first = preview(s, rows[0][0]);assert claim(s, first, AT+5)
        finish(s, first, {'status': 'SENT', 'message_id': 123}, AT+6)
        assert claim(s, preview(s, rows[1][0]), AT+7)  # UNKNOWN, no guessed ACK.
        s.set_meta(DAY_PREFIX+city, old)
        original_receipts = {r[0]:r[1] for r in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")}
        calls = []
        DayDiscovery(s, discoverer(city, m, receipt, calls), lambda: AT+10,
                     session_day=m['date']).refresh()
        assert calls.count(city) == 1
        assert original_receipts == {r[0]:r[1] for r in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")}
        s.connection_started(AT+10);router = Router(s)
        feed(router, m, '0.81', AT+11)  # Restart baseline, never a false crossing.
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0] == 3
        feed(router, m, '0.49', AT+12);feed(router, m, '0.71', AT+13)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0] == 4
        expected = fingerprint(s.c)
        audit_raw = saved(s, REFUSAL_PREFIX+city+':'+m['date'])
    base = tmp_path/'accepted-main';base.mkdir()
    ref = 'e41bf60a51de46810d8076a6287c9e2711dcbd6b'
    for name in subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', ref, 'src']).decode().splitlines():
        if name.endswith('.py'):
            dest = base/Path(name).relative_to('src');dest.parent.mkdir(exist_ok=True, parents=True)
            dest.write_bytes(subprocess.check_output(['git', 'show', ref+':'+name]))
    script = '''
import json,sys
sys.path.insert(0,sys.argv[1])
from holy_weather.catalogue import CatalogueConfig,CatalogueStore,Router
from holy_weather.history import fingerprint
from holy_weather.alert_queue import preview,claim
cfg=CatalogueConfig(**json.loads(sys.argv[2]));expected=json.loads(sys.argv[3]);at=int(sys.argv[4])
with CatalogueStore(cfg,session=False) as s:
 assert fingerprint(s.c)==expected,'OLD_CODE_CHANGED_NEW_DATA'
 before={r[0]:r[1] for r in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")}
 for key,raw in before.items():
  if json.loads(raw)['status'] in ('SENT','UNKNOWN'):assert not claim(s,preview(s,key),at+20)
 s.connection_started(at+20);m=next(iter(s.markets.values()));router=Router(s)
 for n,price in enumerate(('0.81','0.49','0.71','0.79','0.81')):
  router.feed(json.dumps(dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens']['YES'],timestamp=str(at+21+n),best_bid=price,best_ask=price)),at+21+n)
 assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==5
 assert all(s.c.execute('select value from meta where key=?',(k,)).fetchone()[0]==v for k,v in before.items())
 assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
 assert not s.c.execute('pragma foreign_key_check').fetchall()
 print(json.dumps(fingerprint(s.c)))
'''
    from dataclasses import asdict
    result = subprocess.run([sys.executable, '-B', '-c', script, str(base), json.dumps(asdict(cfg)),
                             json.dumps(expected), str(AT)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout+result.stderr
    with CatalogueStore(cfg, session=False) as s:
        assert fingerprint(s.c) == json.loads(result.stdout)
        assert saved(s, REFUSAL_PREFIX+city+':'+m['date']) == audit_raw
        assert json.loads(saved(s, 'price_alert_v1:'+m['tokens']['YES']))['fired'] == ['0.50','0.55','0.60','0.70','0.80']
