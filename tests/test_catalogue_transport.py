"""Bounded transport evidence and finite recovery; no remote services."""
from dataclasses import replace
import json
import pytest
from holy_weather.catalogue import CatalogueStore,observe_catalogue,Router
from test_catalogue import setup,AT
from test_catalogue_recovery import message


def test_maintenance_is_not_repeated_for_every_feed_message(gamma,tmp_path,monkeypatch):
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0];ticks=[0.0];calls=[0];frames=[0]
    active=CatalogueStore.active
    def count(self,*args):calls[0]+=1;return active(self,*args)
    monkeypatch.setattr(CatalogueStore,'active',count)
    class WS:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def send(self,raw):pass
        def recv(self,timeout):
            ticks[0]+=.01;frames[0]+=1
            return message(m,m['tokens']['YES'],'0.49',AT+int(ticks[0]*1000))
    r=observe_catalogue(replace(cfg,duration_seconds=2),discover=lambda city:([m],receipt,[]) if city=='london' else ([],{},[]),connect=lambda *a,**k:WS(),clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0])
    assert frames[0]>=190 and calls[0]<frames[0]*2
    assert r['price_events']==0 and r['sqlite_integrity']=='ok'


def test_disconnect_history_survives_diagnostic_rotation_and_restart(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0];ticks=[0.0];calls=[0]
    def connect(*a,**k):
        calls[0]+=1
        raise OSError('synthetic failure '+str(calls[0]))
    r=observe_catalogue(replace(cfg,duration_seconds=30,max_reconnects=3),discover=lambda city:([m],receipt,[]) if city=='london' else ([],{},[]),connect=connect,clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0],wait=lambda d:ticks.__setitem__(0,ticks[0]+d))
    assert r['state']=='RECONNECT_BUDGET_EXHAUSTED' and calls[0]==4
    history=r['transport'];errors=[x for x in history['recent'] if x['kind']=='DISCONNECTED']
    assert [x['detail'] for x in errors]==['synthetic failure '+str(i) for i in range(1,5)]
    assert history['recent'][-1]['state']==r['state']
    with CatalogueStore(cfg) as s:
        for n in range(600):s.diagnostic('PRICE_ALERT','rotation',AT+n)
    with CatalogueStore(cfg) as s:
        saved=json.loads(s.c.execute("select value from meta where key='catalogue_transport_v1'").fetchone()[0])
        assert saved==history
        assert not s.c.execute("select 1 from diagnostics where reason='CATALOGUE_NETWORK_GAP'").fetchone()
        for n in range(100):s.transport_record({'kind':'CONNECTING','at_ms':AT+n})
        saved=json.loads(s.c.execute("select value from meta where key='catalogue_transport_v1'").fetchone()[0])
        assert len(saved['recent'])==32 and len(json.dumps(saved))<32000
        assert saved['counts']['DISCONNECTED']==4 and saved['sequence']>100


def test_pong_and_processing_timing_are_distinct(gamma,tmp_path,monkeypatch):
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0];ticks=[0.0];feed=Router.feed;counter=[0]
    def slow(self,raw,at):
        try:return feed(self,raw,at)
        finally:ticks[0]+=.4
    monkeypatch.setattr(Router,'feed',slow)
    class WS:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,raw):pass
        def recv(self,timeout):
            ticks[0]+=.1;counter[0]+=1
            if counter[0]==1:return 'PONG'
            return message(m,m['tokens']['YES'],'0.49',AT+int(ticks[0]*1000))
    r=observe_catalogue(replace(cfg,duration_seconds=2),discover=lambda city:([m],receipt,[]) if city=='london' else ([],{},[]),connect=lambda *a,**k:WS(),clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0])
    metrics=r['transport']['snapshot']
    assert metrics['application_last_pong_ms']==AT+100
    assert metrics['processing_ms_max']==400 and metrics['frames']>=3
    assert 0<metrics['max_frame_bytes']<262144
    assert metrics['application_last_ping_ms'] is None # No invented PONG/PING clocks.


def test_native_pong_clock_does_not_use_application_messages(monkeypatch):
    from holy_weather.catalogue import TimedClientConnection
    from websockets.sync.client import ClientConnection
    from websockets.frames import Frame,PONG,TEXT
    monkeypatch.setattr(ClientConnection,'process_event',lambda *args:None)
    monkeypatch.setattr('holy_weather.catalogue.time.time_ns',lambda:123456789000000)
    ws=object.__new__(TimedClientConnection)
    ws.process_event(Frame(TEXT,b'PONG'))
    assert ws.native_last_pong_ms is None
    ws.process_event(Frame(PONG,b''))
    assert ws.native_last_pong_ms==123456789


def test_native_queue_backpressure_can_timeout_with_responding_peer():
    import socket,threading,time
    from websockets.sync.connection import Connection
    from websockets.protocol import Protocol,CLIENT,SERVER,OPEN
    from websockets.frames import Frame,PING
    seen=threading.Event();s1,s2=socket.socketpair()
    class Peer(Connection):
        def process_event(self,event):
            if isinstance(event,Frame) and event.opcode is PING:seen.set()
            super().process_event(event)
    a=Connection(s1,Protocol(CLIENT,state=OPEN),ping_interval=.02,ping_timeout=.2,close_timeout=.05,max_queue=16)
    b=Peer(s2,Protocol(SERVER,state=OPEN),ping_interval=None,close_timeout=.05)
    try:
        for _ in range(18):b.send('x')
        deadline=time.monotonic()+2
        while not a.recv_messages.paused and time.monotonic()<deadline:seen.wait(.001)
        assert a.recv_messages.paused
        a.start_keepalive();assert seen.wait(2)
        a.keepalive_thread.join(2)
        assert a.protocol.close_sent.code==1011
        assert a.protocol.close_sent.reason=='keepalive ping timeout'
    finally:
        a.close_socket();b.close_socket()
        a.recv_events_thread.join(.5);b.recv_events_thread.join(.5)
