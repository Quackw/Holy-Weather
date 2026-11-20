"""Bounded local peer with normal keepalive; no public source or Telegram I/O."""
from dataclasses import asdict
import json
import socket
import threading
import time
from unittest.mock import patch
from zoneinfo import ZoneInfo, _common

from websockets.exceptions import ConnectionClosed
from websockets.protocol import CLIENT, OPEN, SERVER, Protocol
from websockets.sync.connection import Connection

from test_catalogue import AT


def burst(module, cfg, markets, *, read_latency=.001, count=4000):
    """Identical 1-ms TZif read cost, not a sleep in the market consumer.

    The peer publishes a bounded burst under its protocol lock. Native Pongs
    wait behind the publication. Application PONGs inside the burst isolate
    the native watchdog from the unchanged application heartbeat watchdog.
    """
    ZoneInfo.clear_cache()
    cfg = module.CatalogueConfig(**dict(asdict(cfg), duration_seconds=65, max_reconnects=3))
    by_city = {city: ([market], receipt, [])
               for city, (market, receipt) in zip(cfg.cities, markets)}
    done, published = threading.Event(), threading.Event()
    connections, threads, errors, disconnects = [], [], [], []
    frames = reads = 0
    original_load, original_feed = _common.load_data, module.Router.feed
    original_record = module.CatalogueStore.transport_record
    started, cpu = time.monotonic(), time.process_time()

    def load(file):
        nonlocal reads
        reads += 1
        time.sleep(read_latency)
        return original_load(file)

    def connect(*args, **kw):
        assert (kw['max_queue'], kw['max_size'], kw['ping_interval'], kw['ping_timeout'],
                kw['open_timeout'], kw['close_timeout']) == (16, 262144, 10, 20, 10, 2)
        assert not connections, 'the bounded experiment stops after the first failure'
        left, right = socket.socketpair()
        client = Connection(left, Protocol(CLIENT, state=OPEN, max_size=262144),
                            max_queue=16, ping_interval=10, ping_timeout=20, close_timeout=2)
        peer = Connection(right, Protocol(SERVER, state=OPEN, max_size=262144),
                          max_queue=16, ping_interval=None, close_timeout=2)
        connections.extend((client, peer))

        def publish():
            try:
                with peer.send_context():
                    for n in range(count):
                        market = markets[n % len(markets)][0]
                        price = '0.49' if n < len(markets) else '0.71'
                        raw = json.dumps(dict(event_type='best_bid_ask', market=market['condition_id'],
                                              asset_id=market['tokens']['YES'], timestamp=str(AT),
                                              best_bid=price, best_ask=price, hash=str(n))).encode()
                        peer.protocol.send_text(raw)
                        if n % 500 == 0:
                            peer.protocol.send_text(b'PONG')
                published.set()
                while not done.is_set():
                    try:
                        raw = peer.recv(timeout=.1)
                    except TimeoutError:
                        continue
                    if raw == 'PING':
                        peer.send('PONG')
            except (ConnectionClosed, OSError):
                pass
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=publish, daemon=True)
        thread.start()
        threads.append(thread)
        client.start_keepalive()
        return client

    def feed(router, raw, at):
        nonlocal frames
        original_feed(router, raw, at)
        frames += 1

    def record(store, value=None, snapshot=None):
        out = original_record(store, value, snapshot)
        if value and value['kind'] == 'DISCONNECTED':
            disconnects.append(value)
            done.set()  # Stop this experiment; do not change its reconnect budget.
        return out

    def progress(stage):
        if frames >= count and connections and connections[0].latency > 0:
            done.set()

    try:
        with patch.object(_common, 'load_data', load), patch.object(module.Router, 'feed', feed), \
                patch.object(module.CatalogueStore, 'transport_record', record):
            result = module.observe_catalogue(
                cfg, discover=lambda city: by_city[city], connect=connect,
                clock=lambda: AT + round((time.monotonic() - started) * 1000),
                stop=done, progress=progress)
    finally:
        done.set()
        for connection in connections:
            connection.close_socket()
        for thread in threads:
            thread.join(3)
            assert not thread.is_alive()
        for connection in connections:
            connection.recv_events_thread.join(3)
            assert not connection.recv_events_thread.is_alive()
            if connection.keepalive_thread:
                connection.keepalive_thread.join(3)
                assert not connection.keepalive_thread.is_alive()
        ZoneInfo.clear_cache()
    assert not errors
    return dict(frames=frames, count=count, reads=reads, disconnects=disconnects,
                published=published.is_set(), native_rtt=connections[0].latency,
                close_sent=connections[0].protocol.close_sent.code,
                wall_seconds=time.monotonic()-started, cpu_seconds=time.process_time()-cpu,
                price_events=result['price_events'], integrity=result['sqlite_integrity'],
                foreign_keys=result['foreign_key_errors'], data_bytes=result['data_bytes'])
