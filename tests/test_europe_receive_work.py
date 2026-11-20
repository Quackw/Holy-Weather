"""Same filesystem budget and quote semantics with less European receive work."""
from contextlib import contextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import socket
import threading
import time
from unittest.mock import patch

import pytest
from websockets.frames import Frame, PING, TEXT
from websockets.protocol import CLIENT, OPEN, SERVER, Protocol
from websockets.sync.connection import Connection
from websockets.exceptions import ConnectionClosed

from holy_weather import core
from holy_weather.catalogue import CatalogueStore, Router
from holy_weather.storage import Store, StorageStop
from test_catalogue import AT
from test_catalogue import setup


def europe_market(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path)
    return cfg,*markets[0]


@contextmanager
def metadata_cost(root, seconds):
    """Equal explicit I/O cost per actual metadata read, not per frame."""
    original_stat, original_lstat, original_scan = os.stat, os.lstat, os.scandir
    count=[0]
    def charge(path):
        try:matches=os.path.dirname(os.fspath(path))==str(root)
        except TypeError:matches=False
        if matches:
            count[0]+=1
            if seconds:time.sleep(seconds)
    def stat(path,*args,**kw):charge(path);return original_stat(path,*args,**kw)
    def lstat(path,*args,**kw):charge(path);return original_lstat(path,*args,**kw)
    class Entry:
        def __init__(self,entry):self.entry=entry
        def __getattr__(self,key):return getattr(self.entry,key)
        def stat(self,*args,**kw):charge(self.entry.path);return self.entry.stat(*args,**kw)
    class Scan:
        def __init__(self,path):self.scan=original_scan(path)
        def __enter__(self):return self
        def __exit__(self,*args):self.scan.close()
        def __iter__(self):return self
        def __next__(self):return Entry(next(self.scan))
        def close(self):self.scan.close()
    with patch('os.stat',stat),patch('os.lstat',lstat),patch('os.scandir',Scan):yield count


def test_europe_guard_measures_same_bytes_with_fewer_metadata_reads(gamma,tmp_path):
    cfg,_,_=europe_market(gamma,tmp_path)
    with CatalogueStore(cfg,session=False) as s:
        with metadata_cost(s.root,0) as a:old=Store.usage(s)
        with metadata_cost(s.root,0) as b:new=s.usage()
        assert old==new and b[0] < a[0]
        assert b[0]==len(list(s.root.iterdir()))


@pytest.mark.parametrize('bad',['directory','symlink','extra_count','budget'])
def test_europe_guard_preserves_directory_and_storage_refusals(gamma,tmp_path,bad):
    cfg,_,_=europe_market(gamma,tmp_path)
    with CatalogueStore(replace(cfg,storage_bytes=8*1024**2),session=False) as s:
        if bad=='directory':(s.root/'unexpected').mkdir()
        elif bad=='symlink':(s.root/'unexpected').symlink_to(s.root/'monitor.sqlite3')
        elif bad=='extra_count':
            for i in range(33):(s.root/('extra-'+str(i))).touch()
        else:
            size=s.cfg.storage_bytes-s.reserve-s.usage()
            with (s.root/'padding').open('wb') as f:f.truncate(size)
            assert s.usage()==Store.usage(s)==s.cfg.storage_bytes-s.reserve
            with pytest.raises(StorageStop,match='STORAGE_BUDGET'):s._guard()
            return
        with pytest.raises(StorageStop,match='UNEXPECTED_DATA_DIRECTORY_CONTENT'):s.usage()
        with pytest.raises(StorageStop,match='UNEXPECTED_DATA_DIRECTORY_CONTENT'):Store.usage(s)


@pytest.mark.parametrize('value',['0','0.000001','0.50','1','1.00001','-0.1','NaN','1e-3','',None,{},[],5])
def test_cached_price_keeps_exact_decimal_validation(value):
    try:expected=core.decimal(value)
    except ValueError:
        with pytest.raises(ValueError):core.cached_price(value)
    else:assert core.cached_price(value)==expected


def test_europe_decimal_cache_is_bounded_and_default_evolve_stays_pure(monkeypatch):
    from copy import deepcopy
    core._cached_price.cache_clear()
    for n in range(5000):core.cached_price(f'0.{n:05d}')
    assert core._cached_price.cache_info().currsize==4096
    previous=dict(points=[],excursion=None,last_source=None,gates={})
    frozen=deepcopy(previous)
    sample=dict(kind='best_bid_ask',ask='0.49',source_ms=AT,received_ms=AT,input_id=1)
    old=core.evolve(previous,sample,core.Config())
    new=core.evolve(previous,sample,core.Config(),decimal_parser=core.cached_price)
    assert old==new and previous==frozen


def test_metadata_pressure_reproduces_1011_and_candidate_drains_same_burst(gamma,tmp_path):
    """Constructed slow-metadata burst; no claim that live stat latency was measured."""
    cfg,m,receipt=europe_market(gamma,tmp_path)
    count=920
    timeout=20
    frames=[json.dumps(dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens']['YES'],
                 timestamp=str(AT+n),best_bid='0.40' if n%2 else '0.41',best_ask='0.49')) for n in range(count)]
    before=None
    def history(s):
        return {table:[tuple(r) for r in s.c.execute('select * from '+table+' order by rowid')]
                for table in ('events','event_inputs','meta','state')}
    for indexed in [False,True]:
        with CatalogueStore(replace(cfg,data_dir=str(tmp_path/str(indexed))),session=False) as s:
            s.bind(m,receipt);s.connection_started(AT)
            with metadata_cost(s.root,0) as a:Store.usage(s)
            with metadata_cost(s.root,0) as b:s.usage()
            assert a[0]>b[0]
            # Center the unchanged native deadline between the measured work
            # budgets. Apply precisely the same per-read cost to both paths.
            costs=2*timeout/(count*(a[0]+b[0]))
            if not indexed:s.usage=lambda:Store.usage(s)
            router=Router(s);left,right=socket.socketpair();seen=threading.Event()
            right.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,2**20)
            left.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,2**20)
            class Peer(Connection):
                def process_event(self,event):
                    super().process_event(event)
                    if isinstance(event,Frame) and event.opcode is PING:seen.set()
            client=Connection(left,Protocol(CLIENT,state=OPEN),max_queue=16,
                              ping_interval=10,ping_timeout=timeout,close_timeout=.1)
            peer=Peer(right,Protocol(SERVER,state=OPEN),ping_interval=None,close_timeout=.1)
            packet=b''.join(Frame(TEXT,raw.encode()).serialize(mask=False) for raw in frames)
            assert len(packet)<right.getsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF)-16384
            errors=[]
            def produce():
                try:
                    with peer.protocol_mutex:right.sendall(packet)
                except Exception as exc:errors.append(type(exc).__name__)
            writer=threading.Thread(target=produce)
            try:
                writer.start();writer.join(2);assert not writer.is_alive() and not errors
                client.start_keepalive();assert seen.wait(12)
                processed=0;started=time.monotonic()
                with metadata_cost(s.root,costs):
                    for n,raw in enumerate(frames):
                        if client.protocol.close_sent:break
                        try:received=client.recv(timeout=2)
                        except ConnectionClosed:break
                        assert received==raw
                        router.feed(raw,AT+n);processed+=1
                        if indexed and processed==before[0]:assert history(s)==before[1]
                if indexed:
                    assert processed==count and client.protocol.close_sent is None
                    assert client.ping().wait(2)
                else:
                    before=(processed,history(s))
                    client.keepalive_thread.join(2)
                    assert client.protocol.close_sent.code==1011
                    assert client.protocol.close_sent.reason=='keepalive ping timeout'
                print(json.dumps(dict(candidate=indexed,frames=processed,timeout_seconds=timeout,
                                      processing_seconds=round(time.monotonic()-started,3))))
            finally:
                client.close_socket();peer.close_socket()
                writer.join(2);client.recv_events_thread.join(1);peer.recv_events_thread.join(1)
