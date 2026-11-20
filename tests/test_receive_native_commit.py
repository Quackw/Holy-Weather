"""Constructed commit-pressure burst; native10s/20s/max_queue16 remain real."""
from contextlib import contextmanager
from dataclasses import replace
import json,socket,threading,time
from types import SimpleNamespace
from unittest.mock import patch
import pytest
from websockets.frames import Frame,PING,TEXT
from websockets.protocol import CLIENT,SERVER,OPEN,Protocol
from websockets.sync.connection import Connection
from holy_weather import catalogue
from test_catalogue import AT,setup


def test_receive_fix_preserves_native_pong_under_equal_commit_pressure(gamma,tmp_path,record_property):
    cfg,markets=setup(gamma,tmp_path,8);by_city=dict(zip(cfg.cities,markets));count=4400
    frames=[]
    for n in range(count):
        m=markets[(n//2)%8][0];side='YES'if n%2==0 else'NO';value='0.49'if(n//16)%2 else'0.50'
        frames.append(json.dumps(dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens'][side],timestamp=str(AT),best_bid=value,best_ask=value,synthetic_sequence=n)))
    reports=[]
    original_store=catalogue.CatalogueStore
    for optimized in(False,True):
        stop=threading.Event();seen_ping=threading.Event();reads=[0];errors=[]
        class CommitDelay:
            def __init__(self,connection):self.connection=connection
            def __getattr__(self,key):return getattr(self.connection,key)
            def execute(self,sql,*args):
                if sql.lower()=='commit':time.sleep(.006)
                return self.connection.execute(sql,*args)
        class MeasuredStore(original_store):
            def __init__(self,*args,**kwargs):
                super().__init__(*args,**kwargs);self.c=CommitDelay(self.c)
                if not optimized:self._research_cache=None
            @contextmanager
            def receive_batch(self):
                if optimized:
                    with super().receive_batch():yield
                else:yield
        class Peer(Connection):
            def process_event(self,event):
                super().process_event(event)
                if isinstance(event,Frame)and event.opcode is PING:seen_ping.set()
        left,right=socket.socketpair();left.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,2**21);right.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,2**21)
        client=catalogue.TimedClientConnection(left,Protocol(CLIENT,state=OPEN,max_size=262144),max_queue=16,ping_interval=10,ping_timeout=20,close_timeout=.1)
        # The socketpair starts after the HTTP handshake; mark its response complete.
        client.response=SimpleNamespace(status_code=101)
        peer=Peer(right,Protocol(SERVER,state=OPEN,max_size=262144),ping_interval=None,close_timeout=.1)
        packet=b''.join(Frame(TEXT,raw.encode()).serialize(mask=False)for raw in frames)
        assert len(packet)<right.getsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF)-16384
        def produce():
            try:
                with peer.protocol_mutex:right.sendall(packet)
            except Exception as e:errors.append(type(e).__name__)
        writer=threading.Thread(target=produce);writer.start();writer.join(3);assert not writer.is_alive()and not errors
        class Socket:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def send(self,raw):client.send(raw)
            def recv(self,timeout):
                close=client.protocol.close_sent
                if close is not None and close.code==1011:stop.set();raise TimeoutError()
                raw=client.recv(timeout=timeout);reads[0]+=1
                if reads[0]>=count:stop.set()
                return raw
        try:
            client.start_keepalive();assert seen_ping.wait(12)
            began=time.monotonic()
            with patch.object(catalogue,'CatalogueStore',MeasuredStore):
                result=catalogue.observe_catalogue(replace(cfg,data_dir=str(tmp_path/str(optimized)),duration_seconds=90,max_reconnects=0),connect=lambda *a,**k:Socket(),discover=lambda city:([by_city[city][0]],by_city[city][1],[])if city in by_city else([],markets[0][1],[]),clock=lambda:AT+int((time.monotonic()-began)*1000),stop=stop)
            until=time.monotonic()+1
            while optimized and client.latency<=0 and time.monotonic()<until:time.sleep(.001)
            closed=client.protocol.close_sent
            report=dict(optimized=optimized,read=reads[0],elapsed=time.monotonic()-began,native_close=closed.code if closed else None,native_latency=client.latency,commit_delay_seconds=.006,source='CONSTRUCTED_FIXTURE_NOT_MEASURED_LIVE_LATENCY',sqlite=result['sqlite_integrity'])
            reports.append(report)
            if optimized:assert reads[0]==count and(not closed or closed.code!=1011)and client.latency>0
            else:assert closed is not None and closed.code==1011 and reads[0]<count
        finally:
            stop.set();client.close();peer.close();writer.join(1)
    record_property('native_commit_pressure',json.dumps(reports))
