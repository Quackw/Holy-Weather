"""One-shot observability, not a claim about the historical WAN root cause."""
from contextlib import contextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import socket
import threading
import time
from types import SimpleNamespace

import pytest
from websockets.exceptions import ConnectionClosed
from websockets.frames import Frame, PONG
from websockets.server import ServerProtocol
from websockets.sync.client import connect
from websockets.sync.server import ServerConnection

from holy_weather.catalogue import CatalogueStore, TimedClientConnection, observe_catalogue
from holy_weather import ws_diagnostic as diagnostic
from test_catalogue import AT, setup
from test_catalogue_recovery import message


def arm(store):
    path=store.root/diagnostic.ARM
    path.write_bytes(diagnostic.ARM_CONTENT)
    path.chmod(0o600)
    capture=diagnostic.armed(store)
    assert capture is not None
    return capture


def records(path):
    lines=[json.loads(line) for line in path.read_text().splitlines()]
    assert lines[0]['format']=='HW_WS_DIAGNOSTIC_V1' and lines[-1]['end'] is True
    return lines[0],lines[1:-1],lines[-1]


def close1011():
    return SimpleNamespace(sent=SimpleNamespace(code=1011),rcvd=None)


def test_explicit_europe_only_one_shot_and_unchanged_rule_hash(gamma,tmp_path):
    import hashlib
    cfg,_=setup(gamma,tmp_path)
    with CatalogueStore(cfg,session=False) as store:
        rule=cfg.rule_hash()
        assert diagnostic.armed(store) is None
        capture=arm(store)
        capture.add('SAMPLE',1,0,0,0,None,None,None)
        result=capture.persist(store,close1011())
        assert result['status']=='SAVED' and cfg.rule_hash()==rule
        assert diagnostic.armed(store) is None
        original=(store.root/diagnostic.OUTPUT).read_bytes()
        assert result['sha256']==hashlib.sha256(original).hexdigest()
        assert capture.persist(store,close1011())==result
        assert (store.root/diagnostic.OUTPUT).read_bytes()==original
        assert (store.root/diagnostic.OUTPUT).stat().st_mode&0o777==0o600
        assert not (store.root/diagnostic.PENDING).exists()
        header,body,footer=records(store.root/diagnostic.OUTPUT)
        assert header['buffer_bytes']==512*1024 and body[0][-3:]==[-1,-1,-1]
        assert footer['records_written']==1 and store.c.execute('pragma user_version').fetchone()[0]==1
    us=replace(cfg,region='us',city='new-york',data_dir=str(tmp_path/'us'))
    with CatalogueStore(us,session=False) as store:
        path=store.root/diagnostic.ARM;path.write_bytes(diagnostic.ARM_CONTENT);path.chmod(0o600)
        assert diagnostic.armed(store) is None


@pytest.mark.parametrize('fault',['permissions','content','partial'])
def test_bad_request_or_existing_partial_does_not_arm(gamma,tmp_path,fault):
    cfg,_=setup(gamma,tmp_path)
    with CatalogueStore(cfg,session=False) as store:
        path=store.root/diagnostic.ARM;path.write_bytes(diagnostic.ARM_CONTENT);path.chmod(0o600)
        if fault=='permissions':path.chmod(0o644)
        if fault=='content':path.write_bytes(b'not an approval')
        if fault=='partial':(store.root/diagnostic.PENDING).write_bytes(b'original partial proof')
        assert diagnostic.armed(store) is None


def test_ring_is_bounded_nonblocking_and_retires_by_monotonic_time():
    clock=[1_000_000_000]
    capture=diagnostic.Capture(clock=lambda:clock[0])
    for n in range(diagnostic.CAPACITY+20):capture.add('PING_QUEUED',1,n)
    assert len(capture.buffer)==512*1024 and capture.count==diagnostic.CAPACITY
    assert capture.dropped_capacity==20
    with capture.lock:
        capture.add('PING_QUEUED',1,123)
        capture.phase('ROUTER')
    assert capture.dropped_busy==2
    clock[0]+=diagnostic.WINDOW_NS+1
    capture.add('PING_QUEUED',1,100)
    assert capture.count==1 and capture.retired_time==diagnostic.CAPACITY


def test_file_bound_budget_failure_and_no_retry_after_partial(gamma,tmp_path,monkeypatch):
    cfg,_=setup(gamma,tmp_path)
    with CatalogueStore(cfg,session=False) as store:
        capture=arm(store)
        for n in range(diagnostic.CAPACITY):capture.add('PENDING_PING',1,*([2**63-1]*6))
        result=capture.persist(store,close1011())
        assert result['status']=='SAVED' and (store.root/diagnostic.OUTPUT).stat().st_size<=1024*1024
        _,body,footer=records(store.root/diagnostic.OUTPUT)
        assert len(body)==footer['records_written']
        assert footer['omitted_for_file_bound_or_age']>0
    cfg=replace(cfg,data_dir=str(tmp_path/'budget'))
    with CatalogueStore(cfg,session=False) as store:
        capture=arm(store)
        monkeypatch.setattr(store,'usage',lambda:cfg.storage_bytes-store.reserve)
        assert capture.persist(store,close1011())['reason']=='DIAGNOSTIC_STORAGE_HEADROOM'
        assert not (store.root/diagnostic.OUTPUT).exists()
    cfg=replace(cfg,data_dir=str(tmp_path/'failed'))
    with CatalogueStore(cfg,session=False) as store:
        capture=arm(store)
        def broken(*args):raise OSError('private failure text must not be logged')
        with monkeypatch.context() as patch:
            patch.setattr(diagnostic.os,'write',broken)
            assert capture.persist(store,close1011())['status']=='NOT_SAVED'
        assert (store.root/diagnostic.PENDING).exists()
        assert diagnostic.armed(store) is None
        assert capture.persist(store,close1011())['status']=='NOT_SAVED'
        assert not (store.root/diagnostic.OUTPUT).exists()


@contextmanager
def pair(factory,*,native=False,protocol=None):
    left,right=socket.socketpair()
    peer=ServerConnection(right,protocol or ServerProtocol(),SimpleNamespace(socket_closed=threading.Event()),
                          ping_interval=None,close_timeout=2)
    errors=[]
    def handshake():
        try:peer.handshake()
        except Exception as exc:errors.append(type(exc).__name__)
    worker=threading.Thread(target=handshake,daemon=True);worker.start()
    try:
        with connect('ws://localhost/diagnostic-test',sock=left,proxy=None,compression=None,
                     create_connection=factory,ping_interval=10 if native else None,ping_timeout=20,
                     close_timeout=2,max_queue=16,max_size=262144) as client:
            worker.join(1);assert not errors
            yield client,peer
    finally:
        peer.close_socket();left.close();worker.join(1)


def test_native_ping_parse_dispatch_correlation_and_payload_privacy(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path)
    secret=b'PRIVATE-PING-PAYLOAD-DO-NOT-STORE'
    with CatalogueStore(cfg,session=False) as store:
        capture=arm(store)
        with pair(capture.factory(TimedClientConnection)) as (client,peer):
            assert client.ping(secret).wait(1)
            deadline=time.monotonic()+1
            while client.diagnostic.pending and time.monotonic()<deadline:time.sleep(.001)
            peer.pong(b'unmatched-private-pong')
            deadline=time.monotonic()+1
            while not any(diagnostic.RECORD.unpack_from(capture.buffer,n*64)[1]==diagnostic.KINDS['PONG_PROCESSED']
                          and diagnostic.RECORD.unpack_from(capture.buffer,n*64)[3]==-1 for n in range(capture.count)) and time.monotonic()<deadline:
                time.sleep(.001)
        assert capture.persist(store,close1011())['status']=='SAVED'
        raw=(store.root/diagnostic.OUTPUT).read_bytes()
        assert secret not in raw and b'unmatched-private-pong' not in raw
        header,body,_=records(store.root/diagnostic.OUTPUT)
        events={kind:[r for r in body if r[1]==code] for kind,code in diagnostic.KINDS.items()}
        times=[next(r[0] for r in events[k] if r[3]==1) for k in ('PING_QUEUED','PING_FLUSHED','PONG_PARSED','PONG_PROCESSED')]
        assert times==sorted(times) and events['PONG_PROCESSED'][0][3] in (-1,1)
        assert header['sessions'][0]['sampler_stopped'] is True


def test_flow_pause_resume_records_duration_and_faults_do_not_change_socket(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path)
    with CatalogueStore(cfg,session=False) as store:
        capture=arm(store)
        with pair(capture.factory(TimedClientConnection)) as (client,peer):
            for _ in range(18):peer.send('test quote')
            deadline=time.monotonic()+1
            while not client.recv_messages.paused and time.monotonic()<deadline:time.sleep(.001)
            assert client.recv_messages.paused
            time.sleep(.03)
            assert [client.recv(timeout=1) for _ in range(18)]==['test quote']*18
            # Optional measurement errors must not replace original flow control.
            original=capture.clock
            capture.clock=lambda:(_ for _ in ()).throw(RuntimeError('private measurement failure'))
            for _ in range(18):peer.send('second')
            time.sleep(.02)
            assert [client.recv(timeout=1) for _ in range(18)]==['second']*18
            capture.clock=original
            assert client.ping().wait(1)
        capture.persist(store,close1011())
        header,body,_=records(store.root/diagnostic.OUTPUT)
        assert any(r[1]==diagnostic.KINDS['READ_RESUMED'] and r[4]>=20_000_000 for r in body)
        assert header['faults']>0


@pytest.mark.parametrize('filtered',[False,True])
def test_full_catalogue_price_and_queue_equal_with_capture_off_on(gamma,tmp_path,filtered):
    cfg,markets=setup(gamma,tmp_path,1);market,receipt=markets[0]
    outputs=[]
    for enabled in (False,True):
        run=replace(cfg,data_dir=str(tmp_path/str(enabled)),duration_seconds=1,price_notification_filter=filtered)
        with CatalogueStore(run,session=False) as store:
            if enabled:arm(store)
        ticks=[0.0];index=[0]
        frames=[message(market,market['tokens'][side],price,AT+n+1) for n,(side,price) in enumerate(
            [('YES','0.49'),('NO','0.49'),('YES','0.81'),('NO','0.81')])]
        class WS:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def send(self,*args):pass
            def recv(self,timeout):
                ticks[0]+=.01
                value=frames[index[0]] if index[0]<len(frames) else 'PONG';index[0]+=1
                return value
        result=observe_catalogue(run,discover=lambda city:([market],receipt,[]) if city=='london' else ([],{},[]),
            connect=lambda *args,**kwargs:WS(),clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0])
        assert result['price_events']==2
        with CatalogueStore(run,session=False) as store:
            events=[tuple(r) for r in store.c.execute("select id,payload from events where type='PRICE_ALERT' order by id")]
            meta=[tuple(r) for r in store.c.execute("select key,value from meta where key like 'price_alert_v1:%' or key like 'alert_delivery_v1:%' order by key")]
            outputs.append((events,meta))
            assert store.c.execute('pragma integrity_check').fetchone()[0]=='ok'
            assert not store.c.execute('pragma foreign_key_check').fetchall()
    assert outputs[0]==outputs[1]


@pytest.mark.parametrize('received',[None,1000])
def test_full_observer_persists_once_and_keeps_reconnect_budget_and_fired(gamma,tmp_path,received):
    from websockets.exceptions import ConnectionClosedError
    from websockets.frames import Close
    cfg,markets=setup(gamma,tmp_path,1);market,receipt=markets[0]
    cfg=replace(cfg,duration_seconds=30,max_reconnects=3)
    with CatalogueStore(cfg,session=False) as store:arm(store)
    ticks=[0.0];calls=[]
    class WS:
        def __enter__(self):self.index=0;return self
        def __exit__(self,*args):pass
        def send(self,*args):pass
        def recv(self,timeout):
            ticks[0]+=.01;self.index+=1
            if self.index<=2:return message(market,market['tokens']['YES'],
                '0.49' if self.index==1 else '0.81',AT+int(ticks[0]*1000))
            raise ConnectionClosedError(Close(received,'') if received is not None else None,
                Close(1011,'keepalive ping timeout'),False if received is not None else None)
    def connect(*args,**kwargs):
        assert kwargs['ping_interval']==10 and kwargs['ping_timeout']==20 and kwargs['max_queue']==16
        calls.append(1);return WS()
    result=observe_catalogue(cfg,discover=lambda city:([market],receipt,[]) if city=='london' else ([],{},[]),
        connect=connect,clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0],
        wait=lambda delay:ticks.__setitem__(0,ticks[0]+delay))
    assert len(calls)==4 and result['reconnects']==3 and result['state']=='RECONNECT_BUDGET_EXHAUSTED'
    assert result['price_events']==1 and result['websocket_diagnostic']['status']=='SAVED'
    errors=[r for r in result['transport']['recent'] if r['kind']=='DISCONNECTED']
    assert len(errors)==4 and sum('diagnostic' in r for r in errors)==1
    path=Path(cfg.data_dir)/diagnostic.OUTPUT
    original=path.read_bytes()
    header,_,_=records(path)
    assert header['sent_close']==1011 and header['received_close']==received
    with CatalogueStore(cfg,session=False) as store:
        assert diagnostic.armed(store) is None
        assert json.loads(store.c.execute("select value from meta where key like 'price_alert_v1:%'").fetchone()[0])['fired']==[
            '0.50','0.55','0.60','0.70','0.80']
    assert path.read_bytes()==original


def test_actual_68e7d93_rollback_preserves_new_records_queue_and_diagnostic(gamma,tmp_path):
    from dataclasses import asdict
    import hashlib
    import subprocess
    import sys
    from holy_weather.alert_queue import PREFIX,preview,claim,finish
    from holy_weather.catalogue import Router
    from holy_weather.history import fingerprint
    from test_catalogue import feed
    cfg,markets=setup(gamma,tmp_path,1);market,receipt=markets[0]
    with CatalogueStore(cfg,session=False) as store:
        store.bind(market,receipt);store.connection_started(AT);router=Router(store)
        for n,value in enumerate(('0.49','0.51','0.56','0.61')):feed(router,market,value,AT+n+1)
        rows=store.c.execute("select key from meta where key like ? order by json_extract(value,'$.event_ms')",(PREFIX+'%',)).fetchall()
        first=preview(store,rows[0][0]);assert claim(store,first,AT+5)
        finish(store,first,{'status':'SENT','message_id':123},AT+6)
        assert claim(store,preview(store,rows[1][0]),AT+7)  # UNKNOWN cannot be retried.
        capture=arm(store);capture.add('PING_QUEUED',1,1)
        assert capture.persist(store,close1011())['status']=='SAVED'
        expected=fingerprint(store.c)
        frozen={r[0]:r[1] for r in store.c.execute("select key,value from meta where key like ?",(PREFIX+'%',))}
    old=tmp_path/'accepted';base='68e7d93493cc642984b256881e3400784ca864fe'
    for name in subprocess.check_output(['git','ls-tree','-r','--name-only',base,'src/holy_weather'],text=True).splitlines():
        if name.endswith('.py'):
            path=old/Path(name).relative_to('src');path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(subprocess.check_output(['git','show',base+':'+name]))
    settings=asdict(cfg)
    # The rollback plan explicitly disables options unknown to this older code.
    assert settings.pop('us_weather_continuity_enabled') is False
    assert settings.pop('us_lamp_notice_mode')=='pre_peak'
    configuration=tmp_path/'config.json';configuration.write_text(json.dumps(settings))
    snapshot=tmp_path/'expected.json';snapshot.write_text(json.dumps(expected))
    script='''
import json,sys
sys.path.insert(0,sys.argv[1])
from holy_weather.catalogue import CatalogueConfig,CatalogueStore,Router
from holy_weather.alert_queue import PREFIX,preview,claim
from holy_weather.history import fingerprint
v=json.load(open(sys.argv[2]));v['levels']=tuple(v['levels'])
with CatalogueStore(CatalogueConfig(**v),session=False) as s:
 assert fingerprint(s.c)==json.load(open(sys.argv[3]))
 keys=s.c.execute("select key from meta where key like ? order by json_extract(value,'$.event_ms')",(PREFIX+'%',)).fetchall()
 assert not claim(s,preview(s,keys[0][0]),int(sys.argv[4]))
 assert not claim(s,preview(s,keys[1][0]),int(sys.argv[4]))
 s.connection_started(int(sys.argv[4]));m=next(iter(s.markets.values()));r=Router(s)
 for n,p in enumerate(('0.49','0.81')):
  at=int(sys.argv[4])+n+1
  r.feed(json.dumps(dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens']['NO'],timestamp=str(at),best_bid=p,best_ask=p)),at)
 assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==4
 assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
 assert not s.c.execute('pragma foreign_key_check').fetchall()
'''
    subprocess.run([sys.executable,'-B','-c',script,str(old),str(configuration),str(snapshot),str(AT+20)],
                   check=True,cwd=tmp_path,timeout=15,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'))
    with CatalogueStore(cfg,session=False) as store:
        assert store.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==4
        assert all(store.c.execute('select value from meta where key=?',(key,)).fetchone()[0]==raw for key,raw in frozen.items())
        assert diagnostic.armed(store) is None
        assert len(hashlib.sha256((store.root/diagnostic.OUTPUT).read_bytes()).hexdigest())==64


@pytest.mark.parametrize('mode',['paused_consumer','missing_native_pong'])
def test_native_20s_trace_distinguishes_receive_pause_and_missing_pong(gamma,tmp_path,mode):
    """Two synthetic causes, unchanged native deadlines; no claim about live cause."""
    cfg,_=setup(gamma,tmp_path)
    private_local='PRIVATE-LOCAL-VALUE-NOT-FOR-TRACE'
    peer_pongs=[]
    class Protocol(ServerProtocol):
        def send_frame(self,frame):
            if frame.opcode is PONG:
                if mode=='missing_native_pong':return
                peer_pongs.append(time.monotonic())
            return super().send_frame(frame)
    with CatalogueStore(cfg,session=False) as store:
        capture=arm(store)
        with pair(capture.factory(TimedClientConnection),native=True,protocol=Protocol()) as (client,peer):
            assert client.ping_interval==10 and client.ping_timeout==20 and client.recv_messages.high==16
            if mode=='paused_consumer':
                capture.phase('ROUTER')
                for _ in range(18):peer.send('bounded synthetic quote')
                deadline=time.monotonic()+2
                while not client.recv_messages.paused and time.monotonic()<deadline:time.sleep(.001)
                assert client.recv_messages.paused
                client.keepalive_thread.join(35)
            else:
                capture.phase('RECV')
                def application_pong():
                    try:
                        while True:
                            if peer.recv(timeout=35)=='PING':peer.send('PONG')
                    except Exception:pass
                worker=threading.Thread(target=application_pong,daemon=True);worker.start()
                deadline=time.monotonic()+35;next_ping=0;app_pongs=0
                while time.monotonic()<deadline:
                    if time.monotonic()>=next_ping:
                        try:client.send('PING')
                        except ConnectionClosed:break
                        next_ping=time.monotonic()+10
                    try:
                        if client.recv(timeout=.1)=='PONG':app_pongs+=1
                    except TimeoutError:continue
                    except ConnectionClosed:break
                assert app_pongs>=2
            assert client.protocol.close_sent.code==1011
            client.keepalive_thread.join(1)
            assert not client.keepalive_thread.is_alive()
        capture.persist(store,close1011())
        header,body,_=records(store.root/diagnostic.OUTPUT)
        raw=(store.root/diagnostic.OUTPUT).read_text()
        assert private_local not in raw and 'bounded synthetic quote' not in raw
        assert any(r[1]==diagnostic.KINDS['CLIENT_FAIL'] and r[3:5]==[1011,1] for r in body)
        assert header['stacks'] and all(v['threads']['main'] for v in header['stacks'])
        assert not any(r[1]==diagnostic.KINDS['PONG_PARSED'] and r[3]==1 for r in body)
        if mode=='paused_consumer':
            assert peer_pongs and any(r[1]==diagnostic.KINDS['SAMPLE'] and r[4]==1 and r[8]>=20_000_000_000 for r in body)
        else:
            assert not peer_pongs and not any(r[1]==diagnostic.KINDS['READ_PAUSED'] for r in body)
