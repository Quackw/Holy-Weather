"""European receive backpressure: unchanged queue semantics and bounded hot scans."""
from dataclasses import replace
import json
import socket
import threading
import time

import pytest
from websockets.frames import Frame, PING, TEXT
from websockets.protocol import CLIENT, OPEN, SERVER, Protocol
from websockets.sync.connection import Connection

from holy_weather.alert_queue import PREFIX
from holy_weather.catalogue import CatalogueStore, Router
from test_catalogue import AT
from test_catalogue import setup


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
    with CatalogueStore(replace(cfg, region='us', city='new-york', data_dir=str(tmp_path/'off'))) as store:
        assert store.active_delivery_source == 'meta'
        assert not store.c.execute("select name from sqlite_temp_master").fetchall()


@pytest.mark.parametrize('native_limits', [False, True], ids=['scaled', 'native-20s'])
def test_native_pong_with_mature_queue_under_burst(gamma, tmp_path, native_limits):
    """Identical pulse and simulated SQL work cost; only the work selection varies."""
    cfg, markets = setup(gamma, tmp_path)
    market, receipt = markets[0]
    count = 920 if native_limits else 160
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
                time.sleep(.004 if native_limits else .001)
                return 0
            store.c.set_progress_handler(sql_cost, 300 if native_limits else 500)
            router = Router(store)
            left, right = socket.socketpair()
            # Fit this finite burst in the socket even on macOS's small AF_UNIX default.
            right.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2**20)
            left.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2**20)
            seen = threading.Event()
            class Peer(Connection):
                def process_event(self, event):
                    super().process_event(event)
                    if isinstance(event, Frame) and event.opcode is PING:
                        seen.set()
            client = Connection(left, Protocol(CLIENT,state=OPEN), max_queue=16,
                                ping_interval=10 if native_limits else .04,
                                ping_timeout=20 if native_limits else .8, close_timeout=.1)
            peer = Peer(right, Protocol(SERVER,state=OPEN), ping_interval=None, close_timeout=.1)
            def produce():
                try:
                    # One bounded socket write, still distinct WebSocket frames.
                    # Per-send AF_UNIX packet overhead otherwise changes where
                    # PONG enters the pulse on Linux. Serialize with its writer.
                    packet = b''.join(Frame(TEXT,frame.encode()).serialize(mask=False) for frame in frames)
                    assert len(packet) < right.getsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF)-16384
                    with peer.protocol_mutex:
                        right.sendall(packet)
                except Exception:
                    pass  # Expected when the deliberately overloaded baseline closes.
            producer = threading.Thread(target=produce)
            try:
                # Prove both the full pulse and the responding peer before
                # consuming. This avoids a scheduler-dependent PONG position.
                producer.start()
                producer.join(2)
                assert not producer.is_alive()
                client.start_keepalive()
                assert seen.wait(12 if native_limits else 2)
                for i, frame in enumerate(frames):
                    if client.protocol.close_sent:
                        break
                    assert client.recv(timeout=2) == frame
                    router.feed(frame, AT+i)
                assert seen.wait(2)
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
                if producer.ident is not None:
                    producer.join(2)
                    assert not producer.is_alive()
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


def test_accepted_binary_rollback_keeps_new_events_thresholds_and_queue(gamma, tmp_path):
    """Run the actual previous binary on current data after new PRICE writes."""
    from pathlib import Path
    import subprocess
    import sys
    from holy_weather.alert_queue import preview, claim, finish
    from holy_weather.history import fingerprint
    from holy_weather.recovery import Plan
    from test_catalogue import feed
    cfg, markets = setup(gamma, tmp_path, 1)
    market, receipt = markets[0]
    with CatalogueStore(cfg, session=False) as store:
        store.bind(market, receipt)
        store.connection_started(AT)
        router = Router(store)
        for i, value in enumerate(('0.49', '0.51', '0.56', '0.61')):
            feed(router, market, value, AT+i+1)
        rows = store.c.execute("select key from meta where key like ? order by json_extract(value,'$.event_ms')",
                               (PREFIX+'%',)).fetchall()
        assert len(rows) == 3
        first = preview(store, rows[0][0])
        assert claim(store, first, AT+5)
        finish(store, first, {'status':'SENT', 'message_id':123}, AT+6)
        assert claim(store, preview(store, rows[1][0]), AT+7)  # Durable UNKNOWN.
        expected = fingerprint(store.c)
        frozen = {r[0]:r[1] for r in store.c.execute("select key,value from meta where key like ?", (PREFIX+'%',))}
    accepted = '04ab3cf99b7257195499a53051488ee4e375d2ab'
    old = tmp_path/'accepted'
    for name in subprocess.check_output(['git','ls-tree','-r','--name-only',accepted,'src'],text=True).splitlines():
        if name.endswith('.py'):
            path = old/Path(name).relative_to('src')
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(subprocess.check_output(['git','show',accepted+':'+name]))
    script = '''
import json,sys
sys.path.insert(0,sys.argv[1])
from holy_weather.catalogue import CatalogueConfig,CatalogueStore,Router
from holy_weather.history import fingerprint
from holy_weather.alert_queue import preview,claim
cfg=CatalogueConfig(**json.loads(sys.argv[2]));at=int(sys.argv[4])
with CatalogueStore(cfg,session=False) as s:
 assert fingerprint(s.c)==json.loads(sys.argv[3]),'ROLLBACK_CHANGED_NEW_RECORDS'
 assert not s.c.execute('select name from sqlite_temp_master').fetchall()
 original=dict(s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'"))
 for key,raw in original.items():
  if json.loads(raw)['status'] in ('SENT','UNKNOWN'):assert not claim(s,preview(s,key),at+10)
 s.connection_started(at+10);m=next(iter(s.markets.values()));r=Router(s)
 for offset,side,p in ((11,'YES','0.81'),(12,'YES','0.49'),(13,'YES','0.61'),(14,'NO','0.49'),(15,'NO','0.81')):
  r.feed(json.dumps(dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens'][side],
   timestamp=str(at+offset),best_bid=p,best_ask=p)),at+offset)
 assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==4
 assert all(s.c.execute('select value from meta where key=?',(key,)).fetchone()[0]==raw for key,raw in original.items())
 assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
 assert not s.c.execute('pragma foreign_key_check').fetchall()
 print(json.dumps(fingerprint(s.c)))
'''
    result = subprocess.run([sys.executable,'-B','-c',script,str(old),json.dumps(Plan(cfg,None,None,continuous=True).value()['config']),
                             json.dumps(expected),str(AT)],capture_output=True,text=True,timeout=30)
    assert result.returncode == 0, result.stdout+result.stderr
    with CatalogueStore(cfg, session=False) as store:
        assert fingerprint(store.c) == json.loads(result.stdout)
        assert frozen.items() <= dict(store.c.execute("select key,value from meta where key like ?", (PREFIX+'%',))).items()
        assert len(keys(store)) == 2  # Original PENDING + new NO, SENT/UNKNOWN excluded.
        assert store.c.execute('pragma user_version').fetchone()[0] == 1
        store.connection_started(AT+20)
        router = Router(store)
        feed(router, market, '0.49', AT+21)
        feed(router, market, '0.81', AT+22)
        assert store.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0] == 5
        fired = json.loads(store.c.execute('select value from meta where key=?',('price_alert_v1:'+market['tokens']['YES'],)).fetchone()[0])
        assert fired['fired'] == ['0.50','0.55','0.60','0.70','0.80']
        assert store.c.execute('pragma integrity_check').fetchone()[0] == 'ok'
        assert not store.c.execute('pragma foreign_key_check').fetchall()
