"""Finite offline producer/consumer probe, selectable old or prototype source.

Linux uses real /proc identities, inherited locks, subprocesses and Unix notify.
Mac fallback identities are labeled explicitly and never count as Linux proof.
"""
from dataclasses import replace
from datetime import datetime
import io
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys
import tempfile
import threading
import time
import types

from holy_weather import alert_queue,catalogue,lifecycle as l,recovery as r,service,us_lamp
from holy_weather.core import US_CITIES,canonical,sha
from holy_weather.storage import StorageStop
from test_catalogue import feed
from test_catalogue_metar import observations
from test_lamp_send_window import binding,config,seed,admit,instant
from test_us_lamp import native

os.umask(0o077)
REAL_MONOTONIC=time.monotonic
DUMMY_TOKEN='123456:offline_only_not_a_real_telegram_credential'
if sys.platform!='linux':
    l.own_identity=lambda:dict(boot='MAC_MODEL_NOT_LINUX',pid=os.getpid(),start_ticks=1,uid=os.getuid(),exe=sys.executable)
    l.alive=lambda identity:False

def load(path):return json.loads(path.read_text())
def ro(data):
    import sqlite3
    c=sqlite3.connect((data/'monitor.sqlite3').as_uri()+'?mode=ro',uri=True)
    c.execute('pragma query_only=on');return c

def writer(data,cfg):
    pids=[]
    if sys.platform=='linux':
        info=(data/'.writer.lock').stat();key=f'{os.major(info.st_dev):02x}:{os.minor(info.st_dev):02x}:{info.st_ino}'
        for line in Path('/proc/locks').read_text().splitlines():
            row=line.split()
            if len(row)>5 and row[1]=='FLOCK' and row[5]==key:pids.append(int(row[4]))
        assert pids==[os.getpid()]
    try:
        with catalogue.CatalogueStore(cfg,session=False):raise AssertionError('SECOND_WRITER_ADMITTED')
    except StorageStop as exc:assert str(exc)=='WRITER_ALREADY_RUNNING'
    return pids

def worker(root,nonce,fd):
    saved=load(root/'plan.json');cfg=r.Plan.load(saved['plan']).config
    base=root.parent;market=load(base/'fixture.json')['market'];data=Path(cfg.data_dir)
    now=[load(base/'clock.json')['at_ms']];start=now[0];proof=dict(kernel=platform.system(),http_sends=0)
    from holy_weather import telegram
    original_sender=telegram.Sender
    def test_sender(token,**kwargs):
        assert token==DUMMY_TOKEN
        def transport(*args):
            proof['http_sends']+=1
            raise AssertionError('NO_REAL_TELEGRAM_TRANSPORT_IN_PROBE')
        return original_sender(token,**dict(kwargs,transport=transport))
    telegram.Sender=test_sender
    r.time=types.SimpleNamespace(**{n:getattr(time,n) for n in dir(time) if not n.startswith('__')})
    r.time.time_ns=lambda:now[0]*1_000_000
    original=r.observe_catalogue
    class WS:
        n=0;want_pong=False
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,value):
            if value=='PING':self.want_pong=True
        def recv(self,timeout):
            now[0]+=1000;time.sleep(.002)
            if self.want_pong:self.want_pong=False;return 'PONG'
            if self.n==0:proof['writer_pids']=writer(data,cfg)
            if self.n==1:
                with ro(data) as c:
                    assert c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==2
                proof['first_quote_is_baseline']=True
            values=['0.81','0.49','0.81']
            if self.n<len(values):
                value=values[self.n];self.n+=1
                return canonical(dict(event_type='best_bid_ask',market=market['condition_id'],asset_id=market['tokens']['YES'],
                    timestamp=str(now[0]),best_bid=value,best_ask=value))
            return 'PONG'
    def lamp_fetch(issue):
        body=b''.join(native(station,49,issue) for _,station,_ in US_CITIES.values())
        class Response(io.BytesIO):
            status=200;headers={'Content-Length':str(len(body))}
            def geturl(self):
                from holy_weather.lamp import nomads_source
                return nomads_source(issue)
        result=us_lamp.fetch_cycle(issue,fetch=lambda *a,**k:Response(body),clock=lambda:now[0])
        assert result['status']=='OK';proof['new_noaa_issue_ms']=issue;proof['noaa_source_blocks']=len(result['receipts'])
        return result
    def observe(cfg,**kw):
        fixture=load(base/'fixture.json')
        kw.update(
            connect=lambda *a,**k:WS(),discover=lambda city:([market],fixture['receipt'],[]) if city=='new-york' else ([],{},[]),
            lamp_fetch=lamp_fetch,metar_fetch=lambda station:observations(dict(station=station,timezone=US_CITIES['new-york'][2]),now[0]),
            clock=lambda:now[0],monotonic=lambda:(now[0]-start)/1000)
        result=original(replace(cfg,duration_seconds=20),**kw)
        proof.update(observer_state=result['state'],at_ms=now[0])
        return result
    r.observe_catalogue=observe
    r.worker(root,nonce,DUMMY_TOKEN,fd)
    proof.update(at_ms=now[0],worker_result=load(root/(nonce+'.result.json')))
    (base/'child-proof.json').write_text(canonical(proof))

def fixture(base):
    market,receipt=binding(load(base/'gamma.json'),'new-york','2026-11-14')
    cfg=replace(config('new-york',base/'data'),telegram_enabled=True)
    t0=instant('new-york','2026-11-14',11,39)
    old_issue=instant('new-york','2026-11-14',10,30)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        s.bind(market,receipt);s.connection_started(t0-20000);router=catalogue.Router(s)
        for side in ['YES','NO']:
            feed(router,market,'0.49',t0-19000,side);feed(router,market,'0.71',t0-18000,side)
        seed(s,market,instant('new-york','2026-11-14',14),city='new-york',day='2026-11-14')
        admit(s,market,t0-15000,old_issue,instant('new-york','2026-11-14',14))
        rows=s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'").fetchall()
        assert len(rows)==3
        for n,(key,raw) in enumerate(rows):
            p=alert_queue.preview(s,key);assert alert_queue.claim(s,p,t0-10000+n)
            if n!=1:alert_queue.finish(s,p,dict(status='SENT',message_id=n+1),t0-9000+n)
        frozen={k:v for k,v in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")}
        old_events={i:p for i,p in s.c.execute("select id,payload from events where type in ('PRICE_ALERT','LAMP_ALERT')")}
        fired={k:json.loads(v)['fired'] for k,v in s.c.execute("select key,value from meta where key like 'price_alert_v1:%'")}
        assert sum(map(len,fired.values()))==8
    (base/'fixture.json').write_text(canonical(dict(market=market,receipt=receipt)))
    from test_price_recovery import private_tree
    private_tree(base/'data')
    plan=r.Plan(cfg,None,None,phase_seconds=20,continuous=True)
    root=base/'control'
    with r.owned_control(root,cfg.data_dir):pass
    identity=dict(control=str(root),plan=sha(plan.value()),code=r.code_identity())
    debts=[t0-86400000+120000,t0-20*3600000,t0-16*3600000,t0-12*3600000,t0-8*3600000,t0-4*3600000]
    state=dict(identity=identity,attempts=[dict(nonce='a'*32,at_ms=t0-1000,finished_ms=t0-1,
        state='RECONNECT_BUDGET_EXHAUSTED',returncode=0,error=None,reason=None,guard_protocol=1,pid=9999999)],
        restarts=debts,last_ms=t0,active=None,retry=True,status='RECONNECT_BUDGET_EXHAUSTED',controller_process=l.own_identity())
    r.write(root,'state.json',state);r.write(root,'plan.json',dict(plan=plan.value(),code=identity['code'],identity=identity))
    with catalogue.CatalogueStore(cfg,session=False) as s:r.prepare_resume(s,identity,t0)
    (base/'baseline.json').write_text(canonical(dict(t0=t0,debts=debts,frozen=frozen,old_events=old_events,old_issue=old_issue,fired=fired)))
    return plan,root,t0,debts,frozen,old_events,old_issue

def controller(base,interrupt=None):
    if (base/'baseline.json').exists():
        b=load(base/'baseline.json');root=base/'control';plan=r.Plan.load(load(root/'plan.json')['plan'])
        t0,debts,frozen,old_events,old_issue=(b[k] for k in ('t0','debts','frozen','old_events','old_issue'))
    else:plan,root,t0,debts,frozen,old_events,old_issue=fixture(base)
    wall=[load(base/'clock.json')['at_ms'] if (base/'clock.json').exists() else t0]
    stop=threading.Event();launches=[];notices=[];children=[]
    with tempfile.TemporaryDirectory(prefix='hw-quota-',dir='/tmp') as temp:
        path=str(Path(temp)/'notify')
        with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as listener:
            listener.bind(path);listener.setblocking(False)
            health=service.Health(environment=dict(WATCHDOG_USEC='360000000',NOTIFY_SOCKET=path),monotonic=lambda:(wall[0]-t0)/1000)
            def drain():
                while True:
                    try:notices.append(listener.recv(4096).decode())
                    except BlockingIOError:return
            def wait(seconds):
                drain()
                state=load(root/'state.json')
                if ((interrupt=='quota' and state['status']=='WAITING_NETWORK_QUOTA')
                        or (interrupt=='paid_backoff' and state['status']=='BACKOFF' and state.get('recovery_charged'))):
                    assert not launches
                    (base/'clock.json').write_text(canonical(dict(at_ms=wall[0])))
                    os._exit(91)  # Test crash: release actual OS locks; no child exists yet.
                if launches:time.sleep(min(seconds,.02))
                else:wall[0]+=round(seconds*1000)
            def spawn(root,nonce,token,fd):
                assert wall[0]>=t0+420000 and not launches
                with (base/'launch-once.json').open('x') as f:f.write(canonical(dict(at_ms=wall[0])))
                launches.append(wall[0]);(base/'clock.json').write_text(canonical(dict(at_ms=wall[0])))
                child=subprocess.Popen([sys.executable,'-B',str(Path(__file__)),'worker',str(root),nonce,str(fd)],
                    stdin=subprocess.PIPE,text=True,pass_fds=(fd,),stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
                children.append(child)
                class Child:
                    pid=child.pid;stdin=child.stdin
                    def poll(self):
                        rc=child.poll()
                        if rc is not None:
                            if rc:raise RuntimeError(child.stderr.read())
                            proof=load(base/'child-proof.json')
                            assert proof['worker_result']['error'] is None,proof
                            wall[0]=proof['at_ms'];stop.set()
                        return rc
                    def terminate(self):child.terminate()
                return Child()
            result=r.supervise(plan,root,token=DUMMY_TOKEN,clock=lambda:wall[0],monotonic=REAL_MONOTONIC,wait=wait,stop=stop,_spawn=spawn,on_health=health)
            if not launches:
                # The old latch remains closed even after quota AND backoff elapsed.
                wall[0]=t0+421000
                result=r.supervise(plan,root,token=DUMMY_TOKEN,clock=lambda:wall[0],monotonic=REAL_MONOTONIC,wait=wait,stop=stop,_spawn=spawn,on_health=health)
            drain()
    with catalogue.CatalogueStore(plan.config,session=False) as s:
        assert all(s.c.execute('select value from meta where key=?',(k,)).fetchone()[0]==raw for k,raw in frozen.items())
        assert all(s.c.execute('select payload from events where id=?',(i,)).fetchone()[0]==raw for i,raw in old_events.items())
        fired=load(base/'baseline.json')['fired']
        current_fired={k:json.loads(v)['fired'] for k,v in s.c.execute("select key,value from meta where key like 'price_alert_v1:%'")}
        assert all(set(levels)<=set(current_fired[key]) for key,levels in fired.items())
        assert sum(map(len,current_fired.values()))==8+bool(launches)
        for key,raw in frozen.items():
            assert not alert_queue.claim(s,alert_queue.preview(s,key),wall[0])
            assert s.c.execute('select value from meta where key=?',(key,)).fetchone()[0]==raw
        prices=s.c.execute("select payload from events where type='PRICE_ALERT' order by received_ms").fetchall()
        forecast=s.lamp_state(load(base/'fixture.json')['market'])
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok' and not s.c.execute('pragma foreign_key_check').fetchall()
        proof=dict(kernel=platform.system(),old_behavior=not launches,launches=len(launches),launch_ms=launches,
            permitted_ms=t0+420000,charges_before=debts,charges_after=result['restarts'],
            duplicate_sends=0,old_receipts_preserved=len(frozen),old_events_preserved=len(old_events),
            old_thresholds_preserved=sum(map(len,fired.values())),fired_after=sum(map(len,current_fired.values())),
            added_price_events=len(prices)-2,new_thresholds=json.loads(prices[-1][0])['thresholds'],
            forecast_before_ms=old_issue,forecast_after_ms=forecast['issue_ms'],
            watchdog_notifications=len(notices),waiting_visible=any('collection=PAUSED' in n for n in notices),
            terminal=result['status'],child=load(base/'child-proof.json') if launches else None,integrity='ok',schema=s.c.execute('pragma user_version').fetchone()[0])
        if launches:
            assert proof['child']['http_sends']==0
            assert result['restarts']==debts+[t0+120000]
            assert len(prices)==3 and proof['new_thresholds']==['0.80'] and forecast['issue_ms']>old_issue
            assert proof['waiting_visible'] and proof['child']['first_quote_is_baseline']
        else:assert result['status']=='RESTART_BUDGET_EXHAUSTED' and result['restarts']==debts and len(prices)==2
    (base/'RESULT.json').write_text(canonical(proof));print(canonical(proof))

if __name__=='__main__':
    if sys.argv[1]=='worker':worker(Path(sys.argv[2]),sys.argv[3],int(sys.argv[4]))
    else:controller(Path(sys.argv[2]),sys.argv[3] if len(sys.argv)>3 else None)
