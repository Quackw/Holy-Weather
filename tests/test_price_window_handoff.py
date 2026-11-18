"""A PRICE card at a worker boundary, with real SQLite and queue transitions."""
from collections import deque
from contextlib import closing
from datetime import datetime, timezone
import io
import json
import os
import signal
import sqlite3
import threading
from types import SimpleNamespace

import pytest

from holy_weather import catalogue, recovery, storage, telegram
from holy_weather.core import Config
from holy_weather.discovery import select
from test_catalogue import AT
from test_price_recovery import private_tree


CROSS_MS = 11_700
WINDOW_MS = 12_000


class WorkerKilled(BaseException):
    pass


def replay_miami(gamma, tmp_path, monkeypatch, case='clean'):
    paced = case in ('paced_live', 'paced_expiry')
    start = int(datetime(2026, 11, 15, 4, 59, 46, tzinfo=timezone.utc).timestamp() * 1000) if case == 'midnight' else AT
    now = start
    gamma = json.loads(json.dumps(gamma).replace('nyc', 'miami').replace('LaGuardia', 'Miami International').replace('KLGA', 'KMIA'))
    when = datetime.fromtimestamp(now / 1000, timezone.utc)
    market = select(gamma, Config(city='miami', bucket='46-47°F'), now=when)
    yes = market['tokens']['YES']
    markets = {'miami': (market, gamma)}
    if paced:
        atlanta_doc = json.loads(json.dumps(gamma).replace('miami', 'atlanta').replace('Miami International', 'Atlanta International').replace('KMIA', 'KATL'))
        atlanta_doc[0]['id'] = '11'
        atlanta_doc[0]['markets'][0].update(id='124', conditionId='0x' + 'b' * 64, clobTokenIds='["12346","67891"]')
        atlanta = select(atlanta_doc, Config(city='atlanta', bucket='46-47°F'), now=when)
        markets['atlanta'] = (atlanta, atlanta_doc)
    config = catalogue.CatalogueConfig(
        city='miami', region='us', data_dir=str(tmp_path / 'data'), telegram_enabled=True,
        price_notification_filter=True, price_batch_seconds=10, max_reconnects=0,
    )
    plan = recovery.Plan(config, None, None, phase_seconds=WINDOW_MS // 1000, continuous=True)
    with catalogue.CatalogueStore(config, session=False) as store:
        db = store.db_path
    private_tree(db.parent)

    stop = threading.Event()
    launches, ready, closed, calls, senders = [], [], [], [], []

    def clock():
        return now

    def monotonic():
        return (now - start) / 1000

    def wait(seconds):
        nonlocal now
        now += round(seconds * 1000)

    def snapshot():
        with closing(sqlite3.connect(db.as_uri() + '?mode=ro', uri=True)) as c:
            events = {k: json.loads(v) for k, v in c.execute(
                "select id,payload from events where type='PRICE_ALERT'"
            )}
            fired = json.loads(c.execute('select value from meta where key=?', ('price_alert_v1:' + yes,)).fetchone()[0])['fired']
            queue = {k: json.loads(v) for k, v in c.execute(
                "select key,value from meta where key like 'alert_delivery_v1:%'"
            )}
            assert c.execute('pragma user_version').fetchone()[0] == 1
            assert c.execute('pragma integrity_check').fetchone()[0] == 'ok'
            assert not c.execute('pragma foreign_key_check').fetchall()
        return dict(at=now, events=events, fired=fired, queue=queue)

    class TrackedSender(telegram.Sender):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            senders.append(self)

    def transport(token, preview):
        calls.append((now, preview))
        if case == 'unknown':
            return {'status': 'UNKNOWN', 'reason': 'ACK_UNVERIFIED'}
        return {'status': 'SENT', 'message_id': 36 + len(calls)}

    def discover(city):
        if city not in markets:
            return [], {}, []
        if len(launches) > 1 and case in ('closed', 'midnight'):
            reason = 'CONTRACT_NOT_OPEN' if case == 'closed' else 'EXACT_EVENT_NOT_PUBLISHED'
            return [], {}, [{'reason': reason}]
        if len(launches) == 2 and case == 'slow_discovery':
            wait(1)
        if len(launches) == 2 and city == 'atlanta':
            wait(.05)  # Revalidate the second market 50 ms into the new window.
        contract, raw = markets[city]
        receipt = dict(received_at=datetime.fromtimestamp(now / 1000, timezone.utc).isoformat(), raw_body=json.dumps(raw))
        return [contract], receipt, []

    def observation(cfg, **kwargs):
        index = len(launches) - 1
        if index:
            ready.append(snapshot())  # Worker has run real prepare_resume.
        low, high = ('0.48', '0.49'), ('0.50', '0.52')
        crossing = ('0.48', '0.54') if case == 'waiting_bid' else high
        yes_quotes = [(1, low), (1860 if paced else CROSS_MS, crossing)] if index == 0 else [
            (1, high), (2, low), (3, ('0.55', '0.57') if case == 'grouped' and index == 1 else high),
        ]
        def frames_for(contract, series, *, side='YES', source_lag=0):
            return [(offset, contract['condition_id'], contract['tokens'][side], bid, ask, source_lag)
                    for offset, (bid, ask) in series]
        quotes = frames_for(market, yes_quotes)
        if case == 'unknown' and index == 0:
            quotes += frames_for(market, [(2, ('0.68', '0.69')), (3, ('0.71', '0.72'))], side='NO')
        if paced:
            if index == 0:
                # Even with this source delay, the original event is fresh when
                # admitted and would still be fresh at the start of window two.
                lag = 78_500 if case == 'paced_expiry' else 0
                quotes += frames_for(atlanta, [(2, low), (1861, high)], source_lag=lag)
            else:
                quotes += frames_for(atlanta, [(1, high)])
        quotes.sort(key=lambda frame: frame[0])
        tokens = {token for contract, _ in markets.values() for token in contract['tokens'].values()}

        class Socket:
            def __enter__(self):
                self.frames = deque((now + offset, *frame) for offset, *frame in quotes)
                return self

            def __exit__(self, *args):
                pass

            def send(self, message):
                if message != 'PING':
                    assert set(json.loads(message)['assets_ids']) == tokens

            def recv(self, timeout):
                nonlocal now
                if senders and (thread := senders[-1].thread):
                    thread.join(1)
                    assert not thread.is_alive()
                if index == 0 and not self.frames:
                    if case in ('crash', 'unknown_exit'):
                        raise WorkerKilled
                    if case == 'network_gap':
                        raise OSError('offline test disconnect')
                until = now + max(1, round(timeout * 1000))
                if self.frames and self.frames[0][0] <= until:
                    now, condition, token, bid, ask, lag = self.frames.popleft()
                    return json.dumps(dict(event_type='best_bid_ask', market=condition,
                                           asset_id=token, timestamp=str(now - lag), best_bid=bid, best_ask=ask))
                now = until
                return 'PONG'

        return catalogue.observe_catalogue(
            cfg, **kwargs, connect=lambda *a, **k: Socket(), transport=transport,
            clock=clock, monotonic=monotonic, wait=wait,
        )

    def spawn(root, nonce, token, guard_fd):
        index = len(launches)
        launches.append(nonce)
        delays = {'expired': 90, 'midnight': 2, 'slow_discovery': (CROSS_MS + 90_000 - WINDOW_MS - 500) / 1000}
        if index == 1 and case in delays:
            wait(delays[case])

        class Child:
            pid = os.getpid()
            stdin = io.StringIO()
            returncode = None

            def poll(self):
                if self.returncode is None:
                    assert self.stdin.closed  # The controller published RUNNING.
                    try:
                        recovery.worker(root, nonce, token, guard_fd)
                    except WorkerKilled:
                        self.returncode = -signal.SIGKILL if case == 'crash' else 0
                    else:
                        self.returncode = 0
                    closed.append(snapshot())
                    if len(launches) == 3:
                        stop.set()
                return self.returncode

        return Child()

    monkeypatch.setattr(recovery, 'time', SimpleNamespace(time_ns=lambda: now * 1_000_000, monotonic=monotonic))
    monkeypatch.setattr(storage, 'time', SimpleNamespace(time=lambda: now / 1000, monotonic=monotonic))
    monkeypatch.setattr(recovery.signal, 'signal', lambda *args: None)
    monkeypatch.setattr(recovery, 'discover_city', discover)
    monkeypatch.setattr(recovery, 'observe_catalogue', observation)
    monkeypatch.setattr(telegram, 'Sender', TrackedSender)
    monkeypatch.setattr(telegram, 'millis', clock)
    if recovery.lifecycle.process_identity() is None:
        monkeypatch.setattr(recovery.lifecycle, 'own_identity', lambda: dict(
            boot='offline', pid=os.getpid(), start_ticks=1, uid=os.getuid(), exe='pytest',
        ))
    state = recovery.supervise(
        plan, tmp_path / 'control', token='1:' + 'x' * 20, stop=stop, clock=clock,
        monotonic=monotonic, wait=wait, _spawn=spawn,
    )
    assert all(not a.get('error') for a in state['attempts']), state
    return state, closed, ready, calls


def test_miami_ready_card_survives_clean_window(gamma, tmp_path, monkeypatch):
    state, closed, ready, calls = replay_miami(gamma, tmp_path, monkeypatch)
    assert [a['state'] for a in state['attempts']] == ['COMPLETED_WINDOW'] * 3
    first, second, third = closed
    assert len(first['events']) == 1 and first['fired'] == ['0.50']
    key, card = next(iter(first['queue'].items()))
    assert card['status'] == 'PENDING' and card.get('claimed_ms') is None
    assert card['priority'] == 0 and card.get('confirmation') is None
    assert card['notification_thresholds'] == ['0.50']
    assert (card['event_ms'], card['not_before_ms'], card['expires_ms']) == (
        AT + CROSS_MS, AT + CROSS_MS + 10_000, AT + CROSS_MS + 90_000,
    )
    assert ready[0]['at'] == AT + WINDOW_MS < card['not_before_ms']
    print(json.dumps(dict(at_handoff=ready[0]['at'], not_before_ms=card['not_before_ms'],
                          expires_ms=card['expires_ms'], remaining_ms=card['expires_ms'] - ready[0]['at'],
                          events=len(third['events']), fired=third['fired'], attempts=len(calls),
                          handoff_status=ready[0]['queue'][key]['status'], final=third['queue'][key]['status'],
                          reason=third['queue'][key].get('reason'), claimed_ms=third['queue'][key].get('claimed_ms'),
                          completed_ms=third['queue'][key].get('completed_ms'))))
    assert ready[0]['queue'][key] == card
    assert len(calls) == 1
    sent_at, preview = calls[0]
    assert card['not_before_ms'] <= sent_at <= card['expires_ms']
    assert preview['key'] == key and preview['event_id'] == card['event_id']
    assert preview['expires_ms'] == card['expires_ms']
    assert second['queue'][key]['status'] == 'SENT' and second['queue'][key]['message_id'] == 37
    assert second['queue'][key]['claimed_ms'] == sent_at
    assert third['queue'] == second['queue']
    assert first['events'] == second['events'] == third['events']
    assert first['fired'] == second['fired'] == third['fired']


@pytest.mark.parametrize('case', [
    'expired', 'slow_discovery', 'midnight', 'closed', 'crash', 'network_gap', 'waiting_bid', 'unknown_exit',
])
def test_unusable_or_uncertain_cards_do_not_send_after_rotation(gamma, tmp_path, monkeypatch, case):
    state, closed, ready, calls = replay_miami(gamma, tmp_path, monkeypatch, case)
    first, last = closed[0], closed[-1]
    assert first['events'] == last['events'] and len(last['events']) == 1
    assert first['fired'] == last['fired'] == ['0.50']
    assert not calls
    key, original = next(iter(first['queue'].items()))
    card = last['queue'][key]
    assert card.get('claimed_ms') is None
    assert (card['not_before_ms'], card['expires_ms']) == (original['not_before_ms'], original['expires_ms'])
    if case == 'unknown_exit':
        assert state['status'] == 'UNKNOWN_EXIT' and not ready
        assert original['status'] == 'PENDING' and card == original
    elif case == 'waiting_bid':
        assert card == original and card['status'] == 'REJECTED'
        assert card['reason'] == 'CONFIRMATION_GAP:CATALOGUE_SESSION_ENDED'
    else:
        assert original['status'] == 'PENDING' and card['status'] == 'EXPIRED'
        if case in ('crash', 'network_gap'):
            expected = ('CRASH', -signal.SIGKILL) if case == 'crash' else ('NETWORK_GAP_NO_RETRY', 0)
            assert (state['attempts'][0]['state'], state['attempts'][0]['returncode']) == expected
            assert ready[0]['at'] < card['expires_ms'] and card['reason'] == 'NO_BACKLOG_ON_RECOVERY'
        elif case == 'closed':
            assert ready[0]['queue'][key] == original
            assert card['completed_ms'] < card['expires_ms']
            assert card['reason'] == 'MARKET_CLOSED_DATE_ENDED_OR_NOT_REVALIDATED'
        else:
            if case == 'slow_discovery':
                assert ready[0]['queue'][key] == original and ready[0]['at'] < card['expires_ms']
            assert card['completed_ms'] > card['expires_ms']


def test_unknown_disables_sender_across_clean_window(gamma, tmp_path, monkeypatch):
    state, closed, ready, calls = replay_miami(gamma, tmp_path, monkeypatch, 'unknown')
    assert state['attempts'][0]['state'] == 'COMPLETED_WINDOW'
    first, last = closed[0], closed[-1]
    assert first['events'] == last['events'] and len(last['events']) == 2
    assert len(calls) == 1
    unknown_key = calls[0][1]['key']
    assert first['queue'][unknown_key]['side'] == 'NO'
    assert first['queue'][unknown_key]['status'] == 'UNKNOWN'
    assert last['queue'][unknown_key] == first['queue'][unknown_key]
    yes_key, yes = next((k, v) for k, v in first['queue'].items() if v['side'] == 'YES')
    assert yes['status'] == 'PENDING' and ready[0]['at'] < yes['expires_ms']
    assert last['queue'][yes_key]['status'] == 'EXPIRED'
    assert last['queue'][yes_key].get('claimed_ms') is None
    assert state['telegram_disabled_reason'] == 'PREVIOUS_SEND_UNCONFIRMED'


def test_same_side_crossing_still_groups_in_original_window(gamma, tmp_path, monkeypatch):
    _, closed, ready, calls = replay_miami(gamma, tmp_path, monkeypatch, 'grouped')
    key, original = next(iter(closed[0]['queue'].items()))
    assert ready[0]['queue'][key] == original
    final = closed[-1]
    assert len(final['events']) == 2 and final['fired'] == ['0.50', '0.55']
    assert len(calls) == 1 and set(final['queue']) == {key}
    sent = final['queue'][key]
    assert sent['status'] == 'SENT' and set(sent['event_ids']) == set(final['events'])
    assert sent['notification_thresholds'] == ['0.50', '0.55']
    assert sent['not_before_ms'] == original['not_before_ms'] <= calls[0][0]
    assert calls[0][0] <= sent['expires_ms'] == original['expires_ms']


@pytest.mark.parametrize('case', ['paced_live', 'paced_expiry'])
def test_preserve_regional_send_interval_across_clean_window(gamma, tmp_path, monkeypatch, case):
    state, closed, ready, calls = replay_miami(gamma, tmp_path, monkeypatch, case)
    assert [a['state'] for a in state['attempts']] == ['COMPLETED_WINDOW'] * 3
    first, last = closed[0], closed[-1]
    assert first['events'] == last['events'] and len(last['events']) == 2
    (first_key, sent), = [(k, v) for k, v in first['queue'].items() if v['status'] == 'SENT']
    (second_key, pending), = [(k, v) for k, v in first['queue'].items() if v['status'] == 'PENDING']
    assert sent['batch_market'] != pending['batch_market']
    t0, preview0 = calls[0]
    assert preview0['key'] == first_key and sent['claimed_ms'] == t0
    assert AT + WINDOW_MS - 200 <= t0 < first['at'] == AT + WINDOW_MS
    assert pending['not_before_ms'] <= t0 and pending.get('claimed_ms') is None
    assert ready[0]['at'] < pending['expires_ms'] and ready[0]['queue'][second_key] == pending
    earliest = t0 + 3100
    final = last['queue'][second_key]
    assert last['queue'][first_key] == sent and closed[1]['queue'] == last['queue']
    assert (final['not_before_ms'], final['expires_ms']) == (pending['not_before_ms'], pending['expires_ms'])
    print(json.dumps(dict(t0_ms=t0, earliest_ms=earliest, expires_ms=pending['expires_ms'],
                          attempts=[at - t0 for at, _ in calls], second_status=final['status'],
                          second_completed_ms=final.get('completed_ms'))))
    if case == 'paced_expiry':
        assert pending['expires_ms'] < earliest
        assert len(calls) == 1 and final.get('claimed_ms') is None
        assert final['status'] == 'EXPIRED' and final['reason'] == 'DELIVERY_EXPIRED'
        assert pending['expires_ms'] < final['completed_ms'] < earliest
    else:
        assert len(calls) == 2 and final['status'] == 'SENT' and final['message_id'] != sent['message_id']
        t1, preview1 = calls[1]
        assert preview1['key'] == second_key and final['claimed_ms'] == t1
        assert earliest <= t1 <= min(pending['expires_ms'], earliest + 60)
