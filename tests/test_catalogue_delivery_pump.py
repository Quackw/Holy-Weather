"""Keep delivery maintenance bounded without delaying it by ordinary grouping."""
from dataclasses import replace
import json
import socket
import threading
import time
import pytest
from holy_weather.catalogue import CatalogueStore,observe_catalogue
from test_catalogue import setup,AT
from test_catalogue_recovery import message


def test_no_sender_delivery_work_is_bounded_by_time_not_frame_rate(gamma,tmp_path,monkeypatch):
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0]
    ticks=[0.];frames=[0];calls=[]
    from holy_weather.alert_queue import advance_waits
    def count(s,at):calls.append(at);return advance_waits(s,at)
    monkeypatch.setattr('holy_weather.alert_queue.advance_waits',count)
    class WS:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,raw):pass
        def recv(self,timeout):
            ticks[0]+=.001;frames[0]+=1
            return message(m,m['tokens']['YES'],'0.49',AT+int(ticks[0]*1000))
    r=observe_catalogue(replace(cfg,duration_seconds=1),discover=lambda c:([m],receipt,[]) if c=='london' else ([],{},[]),
        connect=lambda *a,**k:WS(),clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0])
    assert frames[0]>=990
    assert 1<=len(calls)<=21
    assert r['sqlite_integrity']=='ok' and not r['foreign_key_errors']


def test_responsive_peer_survives_delivery_load_with_original_receive_bound(gamma,tmp_path,monkeypatch):
    """Scaled test heartbeat; application kwargs/timeouts remain unchanged."""
    from websockets.sync.connection import Connection
    from websockets.protocol import Protocol,CLIENT,SERVER,OPEN
    from websockets.exceptions import ConnectionClosed
    from holy_weather.telegram import Sender
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0]
    origin=time.monotonic();done=threading.Event();errors=[];sent=[];connections=[];workers=[];calls=[]
    class SlowSender(Sender):
        def tick(self,s,at):
            calls.append(at);time.sleep(.04)
            return super().tick(s,at)
    monkeypatch.setattr('holy_weather.telegram.Sender',SlowSender)
    def connect(*args,**kwargs):
        assert kwargs['max_queue']==16 and kwargs['max_size']==262144
        assert kwargs['ping_interval']==10 and kwargs['ping_timeout']==20
        assert kwargs['open_timeout']==10 and kwargs['close_timeout']==2
        a,b=socket.socketpair()
        client=Connection(a,Protocol(CLIENT,state=OPEN,max_size=262144),max_queue=16,
            ping_interval=.03,ping_timeout=.25,close_timeout=.1)
        peer=Connection(b,Protocol(SERVER,state=OPEN,max_size=262144),max_queue=16,
            ping_interval=None,close_timeout=.1)
        connections.extend((client,peer))
        def produce():
            try:
                while not done.is_set():
                    at=AT+int((time.monotonic()-origin)*1000)
                    peer.send(message(m,m['tokens']['YES'],'0.49',at));sent.append(at)
                    done.wait(1/75)
            except ConnectionClosed:pass
            except Exception as exc:errors.append(exc)
        worker=threading.Thread(target=produce,daemon=True);worker.start();workers.append(worker)
        client.start_keepalive()
        return client
    try:
        r=observe_catalogue(replace(cfg,duration_seconds=2,telegram_enabled=True,max_reconnects=0),
            discover=lambda c:([m],receipt,[]) if c=='london' else ([],{},[]),connect=connect,
            clock=lambda:AT+int((time.monotonic()-origin)*1000),token='offline',
            transport=lambda *a:pytest.fail('constant baseline must not send'))
        assert r['state']=='COMPLETED_WINDOW',r
        assert r['transport']['snapshot']['frames']>=60
        assert len(calls)<r['transport']['snapshot']['frames']/2
        assert r['price_events']==0 and r['sqlite_integrity']=='ok'
    finally:
        done.set()
        for c in connections:c.close_socket()
        for t in workers:t.join(1);assert not t.is_alive()
        for c in connections:c.recv_events_thread.join(1);assert not c.recv_events_thread.is_alive()
    assert not errors


def test_priority_crossing_does_not_wait_for_ordinary_batch(gamma,tmp_path,monkeypatch):
    from holy_weather.telegram import Sender
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0]
    ticks=[0.];frames=[0];senders=[];sent=[]
    class TrackedSender(Sender):
        def __init__(self,*a,**kw):super().__init__(*a,**kw);senders.append(self)
    monkeypatch.setattr('holy_weather.telegram.Sender',TrackedSender)
    monkeypatch.setattr('holy_weather.telegram.millis',lambda:AT+round(ticks[0]*1000))
    class WS:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,raw):pass
        def recv(self,timeout):
            if senders[0].thread:senders[0].thread.join(1)
            assert 0<=timeout<=.051
            ticks[0]+=.005;frames[0]+=1
            return message(m,m['tokens']['YES'],'0.49' if frames[0]==1 else '0.71',AT+round(ticks[0]*1000))
    r=observe_catalogue(replace(cfg,duration_seconds=1,telegram_enabled=True,price_notification_filter=True),
        discover=lambda c:([m],receipt,[]) if c=='london' else ([],{},[]),connect=lambda *a,**k:WS(),
        clock=lambda:AT+round(ticks[0]*1000),monotonic=lambda:ticks[0],token='offline',
        transport=lambda token,p:(sent.append((p,round(ticks[0]*1000))) or {'status':'SENT','message_id':1}))
    assert r['price_events']==1 and r['deliveries']=={'SENT':1}
    assert len(sent)==1 and 10<=sent[0][1]<=65 # No ordinary ten-second delay.


@pytest.mark.parametrize('region',['us','europe'])
def test_catalogue_code_rollback_preserves_new_records(gamma,tmp_path,region):
    """Pinned accepted catalogue -> candidate -> accepted, with new state each phase."""
    import os,shutil,subprocess,sys
    from pathlib import Path
    import websockets
    from holy_weather.core import Config
    from holy_weather.discovery import select
    from conftest import NOW
    from test_europe import european
    root=Path(__file__).resolve().parents[1];old=tmp_path/'accepted'
    shutil.copytree(root/'src',old)
    accepted=subprocess.check_output(['git','show','6f450159edd26f15fa9578c1b481dd3f70469cb3:src/holy_weather/catalogue.py'],cwd=root)
    (old/'holy_weather/catalogue.py').write_bytes(accepted)
    city='new-york' if region=='us' else 'london'
    market=select(gamma if region=='us' else european(gamma,city),
        Config(city=city,bucket='46-47°F' if region=='us' else '13°C'),now=NOW)
    (tmp_path/'market.json').write_text(json.dumps(market))
    for phase,source,count in [('old',old,1),('new',root/'src',2),('rollback',old,3)]:
        env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=str(source)+os.pathsep+str(Path(websockets.__file__).resolve().parents[1]))
        r=subprocess.run([sys.executable,'-B',str(root/'tests/regional_rollback_probe.py'),region,phase,str(tmp_path)],
            env=env,capture_output=True,text=True,timeout=30)
        assert r.returncode==0,r.stdout+r.stderr
        data=json.loads(r.stdout)
        assert data['price_events']==count and data['integrity']=='ok'
        if phase=='rollback':assert data['queue']==['SENT','UNKNOWN','PENDING']
