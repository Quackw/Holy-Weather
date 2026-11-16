"""Finite local socket-pair qualification; no market or Telegram connection."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import tracemalloc
import pytest
import websockets
from websockets.exceptions import ConnectionClosedError
from websockets.protocol import CLIENT, SERVER, OPEN, Protocol
from websockets.sync.connection import Connection
from holy_weather.catalogue import CatalogueStore, Router
from holy_weather.core import Config
from holy_weather.discovery import select
from test_catalogue import setup, AT
from test_catalogue_recovery import message
from test_europe import european
from conftest import NOW


@contextmanager
def pair():
    left, right = socket.socketpair()
    client = Connection(left, Protocol(CLIENT, state=OPEN, max_size=262144),
                        ping_interval=None, close_timeout=.1, max_queue=16)
    peer = Connection(right, Protocol(SERVER, state=OPEN, max_size=262144),
                      ping_interval=None, close_timeout=.1, max_queue=16)
    try:
        yield client, peer
    finally:
        client.close_socket(); peer.close_socket()
        client.recv_events_thread.join(1); peer.recv_events_thread.join(1)
        assert not client.recv_events_thread.is_alive()
        assert not peer.recv_events_thread.is_alive()


@pytest.mark.parametrize('fragmented', [False, True])
def test_sync_message_at_existing_payload_limit(fragmented):
    body = 'x' * 262144
    with pair() as (client, peer):
        peer.send([body[n:n + 65536] for n in range(0, len(body), 65536)] if fragmented else body)
        assert client.recv(timeout=2) == body


def test_sync_oversize_message_closes_with_1009():
    with pair() as (client, peer):
        peer.send('x' * 262145)
        with pytest.raises(ConnectionClosedError) as error:
            client.recv(timeout=2)
        assert error.value.sent.code == 1009


@pytest.mark.parametrize('pieces', [1, 4095])
def test_fragment_limit_accepts_valid_complete_message(pieces):
    # send(iterable) appends a final empty frame: 4095 pieces = 4096 frames.
    with pair() as (client, peer):
        peer.send([''] * pieces)
        assert client.recv(timeout=2) == ''


def test_excess_empty_fragments_are_rejected_with_bounded_memory(record_property):
    # Finite reproduction of GHSA-6m5v-9gmr-5665, never an unbounded stream.
    with pair() as (client, peer):
        tracemalloc.start()
        try:
            peer.send([''] * 4097)
            with pytest.raises(ConnectionClosedError) as error:
                client.recv(timeout=2)
            assert error.value.sent.code == 1009
            assert error.value.sent.reason == 'too many fragments'
            _, peak = tracemalloc.get_traced_memory()
            record_property('fragment_test_python_peak_bytes', peak)
            assert peak < 8 * 1024 * 1024
        finally:
            tracemalloc.stop()


def test_fragmented_receive_can_resume_after_timeout():
    first = threading.Event(); release = threading.Event(); errors = []
    with pair() as (client, peer):
        def fragments():
            yield 'first'
            first.set()
            assert release.wait(2)
            yield 'second'
        def send():
            try: peer.send(fragments())
            except Exception as exc: errors.append(exc)
        worker = threading.Thread(target=send, daemon=True); worker.start()
        try:
            assert first.wait(1)
            with pytest.raises(TimeoutError): client.recv(timeout=.02)
            release.set()
            assert client.recv(timeout=2) == 'firstsecond'
        finally:
            release.set(); worker.join(2)
        assert not worker.is_alive() and not errors


def test_real_transport_disconnect_keeps_price_baseline_and_thresholds(gamma, tmp_path):
    cfg, markets = setup(gamma, tmp_path, 1); market, receipt = markets[0]
    token = market['tokens']['YES']
    with CatalogueStore(cfg) as store:
        store.bind(market, receipt); store.connection_started(AT); router = Router(store)
        with pair() as (client, peer):
            for offset, price in [(1, '0.49'), (2, '0.51')]:
                peer.send(message(market, token, price, AT + offset))
                router.feed(client.recv(timeout=2), AT + offset)
            peer.close_socket()
            with pytest.raises(ConnectionClosedError): client.recv(timeout=2)
        store.gap('QUALIFIED_SOCKET_DISCONNECT', AT + 3)
        store.connection_started(AT + 4)
        with pair() as (client, peer):
            for offset, price in [(5, '0.81'), (6, '0.49'), (7, '0.61')]:
                peer.send(message(market, token, price, AT + offset))
                router.feed(client.recv(timeout=2), AT + offset)
        events = [json.loads(row[0]) for row in store.c.execute(
            "select payload from events where type='PRICE_ALERT' order by received_ms")]
        assert [event['thresholds'] for event in events] == [['0.50'], ['0.55', '0.60']]
        assert store.c.execute('pragma integrity_check').fetchone()[0] == 'ok'
        assert not store.c.execute('pragma foreign_key_check').fetchall()


@pytest.mark.parametrize('region', ['europe', 'us'])
def test_dependency_rollback_keeps_new_records_and_queue(gamma, tmp_path, region):
    old_root = os.environ.get('HOLY_WEATHER_WEBSOCKETS_ROLLBACK_DIR')
    if not old_root:
        pytest.skip('Provide isolated websockets 17.1 directory; mandatory in Linux CI')
    city = 'london' if region == 'europe' else 'new-york'
    document = european(gamma, city) if region == 'europe' else gamma
    market = select(document, Config(city=city, bucket='13°C' if region == 'europe' else '46-47°F'), now=NOW)
    (tmp_path / 'market.json').write_text(json.dumps(market))
    source = str(Path(__file__).resolve().parents[1] / 'src')
    candidate_root = str(Path(websockets.__file__).resolve().parents[1])
    probe = str(Path(__file__).with_name('regional_rollback_probe.py'))
    for phase, dependency, expected in [('old', old_root, '17.1'), ('new', candidate_root, '17.4'), ('rollback', old_root, '17.1')]:
        env = dict(os.environ, PYTHONPATH=source + os.pathsep + dependency, PYTHONDONTWRITEBYTECODE='1')
        code = 'import websockets,runpy; print(websockets.__version__); runpy.run_path(' + repr(probe) + ',run_name="__main__")'
        result = subprocess.run([sys.executable, '-B', '-c', code, region, phase, str(tmp_path)],
                                env=env, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stdout + result.stderr
        lines = result.stdout.splitlines(); assert lines[0] == expected
        data = json.loads(lines[-1])
        assert data['price_events'] == {'old': 1, 'new': 2, 'rollback': 3}[phase]
        if phase != 'old': assert {'SENT', 'UNKNOWN'} <= set(data['queue'])
