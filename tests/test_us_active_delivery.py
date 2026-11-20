"""US queue maintenance during peak receive traffic, using only isolated SQLite."""
from dataclasses import replace
import json
import socket
import threading
import time

import pytest
from websockets.frames import Frame, PING
from websockets.protocol import CLIENT, OPEN, SERVER, Protocol
from websockets.sync.connection import Connection

from holy_weather.alert_queue import PREFIX
from holy_weather.catalogue import CatalogueStore, Router
from test_catalogue import AT
from test_us_lamp import setup


def keys(store):
    return {k for k, in store.c.execute("select key from temp.active_deliveries")}


def test_active_keys_follow_rollback_replace_recovery_and_restart(gamma, tmp_path):
    cfg, _ = setup(gamma, tmp_path)
    key = PREFIX + 'one'
    with CatalogueStore(cfg, session=False) as store:
        schema = list(store.c.execute("select * from main.sqlite_schema"))
        def put(status):
            store.c.execute('insert or replace into meta values(?,?)',
                            (key, json.dumps({'status': status})))
        put('WAITING_BID')
        with pytest.raises(RuntimeError), store.transaction():
            put('UNKNOWN')
            assert not keys(store)
            raise RuntimeError('rollback')
        assert keys(store) == {key}
        put('SENT')
        assert not keys(store)
        store.c.execute('update meta set value=? where key=?', ('{"status":"PENDING"}', key))
        assert keys(store) == {key}
        store.c.execute('delete from meta where key=?', (key,))
        assert not keys(store)
        put('PENDING')
        assert list(store.c.execute("select * from main.sqlite_schema")) == schema
    with CatalogueStore(cfg, session=False) as store:
        assert keys(store) == {key}
    with CatalogueStore(replace(cfg, us_lamp_enabled=False, data_dir=str(tmp_path/'off'))) as store:
        assert store.active_delivery_source == 'meta'
        assert not store.c.execute("select name from sqlite_temp_master").fetchall()


def test_native_pong_with_mature_queue_under_burst(gamma, tmp_path):
    """Identical pulse and simulated SQL work cost; only the work selection varies."""
    cfg, markets = setup(gamma, tmp_path)
    market, receipt = markets[0]
    count = 160
    frames = [json.dumps(dict(event_type='best_bid_ask', market=market['condition_id'],
                              asset_id=market['tokens']['YES'], timestamp=str(AT+i),
                              best_bid=['0.41','0.42'][i%2], best_ask='0.43')) for i in range(count)]
    for indexed in (False, True):
        with CatalogueStore(replace(cfg,data_dir=str(tmp_path/str(indexed))), session=False) as store:
            store.bind(market, receipt)
            store.connection_started(AT)
            with store.transaction():
                store.c.executemany('insert into meta values(?,?)',
                    ((PREFIX+str(i), json.dumps({'status':'SENT','html':'x'*1000})) for i in range(347)))
            if not indexed:
                store.active_delivery_source = 'meta'  # The full-scan query used by the previous release.
            # Fixed cost per SQLite VM instruction block, independent of query/version.
            # Scaled test clocks keep the native 1011 reproduction finite in CI.
            def sql_cost():
                time.sleep(.001)
                return 0
            store.c.set_progress_handler(sql_cost, 500)
            router = Router(store)
            left, right = socket.socketpair()
            # Fit this finite burst in the socket even on macOS's small AF_UNIX default.
            right.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 256*1024)
            left.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 256*1024)
            seen = threading.Event()
            class Peer(Connection):
                def process_event(self, event):
                    super().process_event(event)
                    if isinstance(event, Frame) and event.opcode is PING:
                        seen.set()
            client = Connection(left, Protocol(CLIENT,state=OPEN), max_queue=16,
                                ping_interval=.04, ping_timeout=.8, close_timeout=.1)
            peer = Peer(right, Protocol(SERVER,state=OPEN), ping_interval=None, close_timeout=.1)
            try:
                for frame in frames:
                    peer.send(frame)
                client.start_keepalive()
                assert seen.wait(2)
                for i, frame in enumerate(frames):
                    if client.protocol.close_sent:
                        break
                    assert client.recv(timeout=2) == frame
                    router.feed(frame, AT+i)
                if indexed:
                    assert router.counters['routed_messages'] == count
                    # An acknowledged native ping also covers the final receive batch.
                    assert client.ping().wait(2)
                    assert client.latency > 0 and client.protocol.close_sent is None
                else:
                    client.keepalive_thread.join(2)
                    close = client.protocol.close_sent
                    assert close and close.code == 1011 and close.reason == 'keepalive ping timeout'
            finally:
                store.c.set_progress_handler(None,0)
                client.close_socket()
                peer.close_socket()
                client.recv_events_thread.join(1)
                peer.recv_events_thread.join(1)


def test_receipt_history_does_not_increase_idle_delivery_scan(gamma, tmp_path):
    from holy_weather.telegram import Sender
    cfg, _ = setup(gamma, tmp_path)
    with CatalogueStore(cfg, session=False) as store:
        sender = Sender('offline', transport=lambda *args: pytest.fail('no pending card'))
        def scan_steps():
            steps = 0
            def count():
                nonlocal steps
                steps += 1
                return 0
            store.c.set_progress_handler(count, 1)
            try:
                store.expire_closed_deliveries(AT)
                sender.tick(store, AT)
            finally:
                store.c.set_progress_handler(None, 0)
            return steps
        scan_steps()  # Warm the statement cache before measuring.
        before = scan_steps()
        with store.transaction():
            store.c.executemany('insert into meta values(?,?)',
                ((PREFIX+str(i), '{"status":"SENT"}') for i in range(500)))
        assert scan_steps() <= before + 32
        assert store.c.execute("select count(*) from meta where key like ?", (PREFIX+'%',)).fetchone()[0] == 500
