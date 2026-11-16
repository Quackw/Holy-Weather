"""Three PR28 review reproductions; real Store/Sender, synthetic transport only."""
from collections import deque
from contextlib import contextmanager
from dataclasses import replace
import json,signal,threading,time
import pytest
from holy_weather import recovery as r,telegram,catalogue
from holy_weather.alert_queue import preview,claim
from holy_weather.catalogue import CatalogueStore
from holy_weather.core import canonical
from test_catalogue import setup,AT
from test_price_recovery import seed,Sim


@contextmanager
def delayed_phase(gamma,tmp_path,monkeypatch,*,ack_during_close):
    cfg,markets=setup(gamma,tmp_path,1);cfg=replace(cfg,telegram_enabled=True)
    market,receipt=markets[0];root=tmp_path/'control';root.mkdir(mode=0o700)
    plan=r.Plan(cfg,market['date'],AT+3600000,phase_seconds=2);nonce='a'*32
    r.write(root,'plan.json',dict(plan=plan.value(),code=r.code_identity(),identity={'control':str(root)}))
    started=threading.Event();release=threading.Event();senders=[];calls=[];timers=[];observation=[];ticks=[0.]
    monkeypatch.setattr(telegram,'CLOSE_WAIT_SECONDS',.1,raising=False)
    class Sender(telegram.Sender):
        def __init__(self,*a,**k):super().__init__(*a,**k);self.close_times=[];senders.append(self)
        def close(self,store):
            if not self.close_times and ack_during_close:
                timer=threading.Timer(.03,release.set);timers.append(timer);timer.start()
            at=time.monotonic();answer=super().close(store);self.close_times.append(time.monotonic()-at);return answer
    monkeypatch.setattr(telegram,'Sender',Sender)
    def transport(token,p):
        calls.append(p);started.set();assert release.wait(2)
        return {'status':'SENT','message_id':71}
    seq=deque([(side,price) for side in ('YES','NO') for price in ('0.49','0.71')])
    class WS:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,body):pass
        def recv(self,timeout):
            ticks[0]+=.1
            if seq:
                side,price=seq.popleft()
                return canonical(dict(event_type='best_bid_ask',market=market['condition_id'],asset_id=market['tokens'][side],timestamp=str(AT+int(ticks[0]*1000)),best_bid=price,best_ask=price))
            assert started.wait(1);ticks[0]=3.;return 'PONG'
    def observe(current,**kw):
        kw.update(connect=lambda *a,**k:WS(),discover=lambda city:([market],receipt,[]) if city=='london' else ([],{},[]),
                  transport=transport,clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0])
        result=catalogue.observe_catalogue(current,**kw);observation.append(result);return result
    monkeypatch.setattr(r,'observe_catalogue',observe)
    monkeypatch.setattr(r.time,'time_ns',lambda:AT*1000000)
    monkeypatch.setattr(r.signal,'signal',lambda *a:None)
    try:
        r.worker(root,nonce,'synthetic-token')
        yield dict(config=cfg,worker=json.loads((root/(nonce+'.result.json')).read_text()),observation=observation[0],
                   sender=senders[0],calls=calls,release=release,root=root)
    finally:
        release.set()
        for sender in senders:
            if sender.thread:sender.thread.join(1)
        for timer in timers:timer.cancel();timer.join(1)


def deliveries(store):return {k:json.loads(v) for k,v in store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")}


def test_phase_close_records_delayed_ack_before_store_close_and_resume(gamma,tmp_path,monkeypatch):
    with delayed_phase(gamma,tmp_path,monkeypatch,ack_during_close=True) as p:
        p['sender'].thread.join(1)  # A late ACK alone must not be mistaken for DB success.
        with CatalogueStore(p['config'],session=False) as s:
            values=deliveries(s);sent=[v for v in values.values() if v['status']=='SENT']
            assert len(sent)==1 and sent[0]['message_id']==71
            fired={k:v for k,v in s.c.execute("select key,value from meta where key like 'price_alert_v1:%'")}
            assert not r.prepare_resume(s,{'control':str(p['root'])},AT+4000)
            assert fired==dict(s.c.execute("select key,value from meta where key like 'price_alert_v1:%'"))
            assert not claim(s,preview(s,p['calls'][0]['key']),AT+4001)
        assert p['worker']['telegram_enabled'] is True
        assert p['worker']['telegram_disabled_reason'] is None
        assert len(p['calls'])==1 and len(p['sender'].close_times)==2


def test_phase_close_timeout_is_bounded_idempotent_and_visible(gamma,tmp_path,monkeypatch):
    with delayed_phase(gamma,tmp_path,monkeypatch,ack_during_close=False) as p:
        assert p['observation']['telegram_enabled'] is False
        assert p['worker']['telegram_enabled'] is False
        assert p['worker']['telegram_disabled_reason']=='SEND_RESULT_UNKNOWN_AT_CLOSE'
        assert .08<=p['sender'].close_times[0]<.8 and p['sender'].close_times[1]<.05
        p['release'].set();p['sender'].thread.join(1)
        with CatalogueStore(p['config'],session=False) as s:
            p['sender'].close(s)  # A late result must not reopen a finalized close.
            p['sender'].tick(s,AT+4000)
            before=deliveries(s);assert before[p['calls'][0]['key']]['status']=='UNKNOWN'
            assert r.prepare_resume(s,{'control':str(p['root'])},AT+4000)
            assert deliveries(s)[p['calls'][0]['key']]==before[p['calls'][0]['key']]
            assert not claim(s,preview(s,p['calls'][0]['key']),AT+4001)
        assert len(p['calls'])==1


def test_existing_unknown_is_explicit_in_worker_progress_and_result(gamma,tmp_path,monkeypatch):
    plan,m=seed(gamma,tmp_path);plan=replace(plan,config=replace(plan.config,telegram_enabled=True))
    with CatalogueStore(plan.config) as store:
        store.set_meta('alert_delivery_v1:uncertain',dict(status='UNKNOWN',track='PRICE_ALERT',claimed_ms=AT,reason='synthetic-private-http-detail'))
    root=tmp_path/'control';root.mkdir(mode=0o700);nonce='b'*32
    r.write(root,'plan.json',dict(plan=plan.value(),code=r.code_identity(),identity={'control':str(root)}))
    called=[]
    def observe(cfg,**kw):
        assert not cfg.telegram_enabled and kw['token'] is None;called.append(True)
        progress=json.loads((root/(nonce+'.progress.json')).read_text())
        assert progress['telegram_enabled'] is False and progress['telegram_disabled_reason']=='PREVIOUS_SEND_UNCONFIRMED'
        return {'state':'COMPLETED_WINDOW'}
    monkeypatch.setattr(r,'observe_catalogue',observe);monkeypatch.setattr(r.time,'time_ns',lambda:AT*1000000);monkeypatch.setattr(r.signal,'signal',lambda *a:None)
    r.worker(root,nonce,'synthetic-token')
    result=json.loads((root/(nonce+'.result.json')).read_text())
    assert called and result['telegram_enabled'] is False and result['telegram_disabled_reason']=='PREVIOUS_SEND_UNCONFIRMED'
    assert 'synthetic-private-http-detail' not in canonical(result)
    with CatalogueStore(plan.config,session=False) as store:
        assert deliveries(store)['alert_delivery_v1:uncertain']['status']=='UNKNOWN'


def test_controller_reports_disabled_sender_while_price_child_is_running(gamma,tmp_path,monkeypatch):
    plan,m=seed(gamma,tmp_path);ticks=[0.];root=tmp_path/'control'
    status=dict(telegram_enabled=False,telegram_disabled_reason='PREVIOUS_SEND_UNCONFIRMED')
    def spawn(root,nonce,token):
        import io
        class Child:
            pid=12345;stdin=io.StringIO();polls=0;done=False
            def poll(self):
                if self.done:return 0
                self.polls+=1
                if self.polls==1:return None
                state=json.loads((root/'state.json').read_text())
                assert state['status']=='RUNNING' and all(state[k]==v for k,v in status.items())
                r.write(root,nonce+'.result.json',dict(nonce=nonce,pid=self.pid,state='ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED',error=None,**status))
                self.done=True;return 0
        child=Child();r.write(root,nonce+'.progress.json',dict(nonce=nonce,pid=child.pid,stage='OBSERVING',monotonic=0.,**status));return child
    monkeypatch.setattr(r,'preflight',lambda p:None)
    result=r.supervise(plan,root,clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0],wait=lambda s:ticks.__setitem__(0,ticks[0]+s),_spawn=spawn)
    assert result['status']=='ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED' and all(result[k]==v for k,v in status.items())


@pytest.mark.parametrize('cancel',['stop','deadline'])
def test_preflight_cancellation_never_spawns_or_transfers_token(gamma,tmp_path,monkeypatch,cancel):
    plan,m=seed(gamma,tmp_path);plan=replace(plan,config=replace(plan.config,telegram_enabled=True))
    sim=Sim([]);stop=threading.Event()
    def preflight(plan):
        if cancel=='stop':stop.set()
        else:sim.t=3601
    monkeypatch.setattr(r,'preflight',preflight)
    value=r.supervise(plan,tmp_path/'control',token='12345:'+'a'*24,stop=stop,clock=sim.clock,monotonic=sim.mono,
                      wait=sim.wait,_spawn=lambda *a:pytest.fail('spawn after preflight cancellation'))
    assert value['status']==('STOPPED' if cancel=='stop' else 'DEADLINE')
    assert value['active'] is None and value['attempts']==[] and value['restarts']==[]


def test_cancellation_after_intent_clears_unlaunched_attempt(gamma,tmp_path,monkeypatch):
    plan,m=seed(gamma,tmp_path);sim=Sim([]);stop=threading.Event();write=r.write
    def save(root,name,value):
        write(root,name,value)
        if name=='state.json' and value['status']=='STARTING':stop.set()
    monkeypatch.setattr(r,'write',save);monkeypatch.setattr(r,'preflight',lambda p:None)
    value=r.supervise(plan,tmp_path/'control',stop=stop,clock=sim.clock,monotonic=sim.mono,wait=sim.wait,
                      _spawn=lambda *a:pytest.fail('spawn after cancelled intent'))
    assert value['status']=='STOPPED' and value['active'] is None and value['restarts']==[]
    assert value['attempts'][0]['state']=='CANCELLED_BEFORE_SPAWN' and 'pid' not in value['attempts'][0]


def test_preflight_stop_on_recovery_does_not_rearm_a_stopped_controller(gamma,tmp_path,monkeypatch):
    plan,m=seed(gamma,tmp_path);sim=Sim(['NETWORK_GAP_NO_RETRY'],final_deadline=False);stop=threading.Event();checks=[]
    def preflight(plan):
        checks.append(True)
        if len(checks)==2:stop.set()
    monkeypatch.setattr(r,'preflight',preflight)
    value=r.supervise(plan,tmp_path/'control',stop=stop,clock=sim.clock,monotonic=sim.mono,wait=sim.wait,_spawn=sim.spawn)
    assert value['status']=='STOPPED' and not value['retry'] and value['active'] is None
    assert len(sim.calls)==1 and len(value['attempts'])==1 and value['restarts']==[]
    assert Sim([]).run(plan,tmp_path/'control')['status']=='STOPPED'


@pytest.mark.parametrize('reported,stalled',[('COMPLETED_WINDOW',False),('SIGNAL_STOP',True)])
@pytest.mark.parametrize('returncode',[-signal.SIGSEGV,7])
def test_result_file_cannot_hide_abnormal_exit_during_stopping(gamma,tmp_path,monkeypatch,reported,stalled,returncode):
    plan,m=seed(gamma,tmp_path);sim=Sim([reported],hang=stalled,final_deadline=False);spawn=sim.spawn
    def launch(root,nonce,token):
        assert not sim.calls,'unexpected next phase after abnormal exit'
        child=spawn(root,nonce,token);poll=child.poll
        def exited():
            code=poll()
            if code is None:return None
            r.write(root,nonce+'.progress.json',dict(nonce=nonce,pid=child.pid,stage='STOPPING',monotonic=sim.t))
            return returncode
        child.poll=exited;return child
    sim.spawn=launch;value=sim.run(plan,tmp_path/'control')
    assert value['status']==('CRASH' if returncode in r.CRASH_SIGNALS else 'UNKNOWN_EXIT')
    assert len(sim.calls)==1 and value['restarts']==[] and not value['retry']


@pytest.mark.parametrize('returncode',[-signal.SIGSEGV,0])
def test_result_and_exit_agreement_controls_recovery_budget(gamma,tmp_path,returncode):
    plan,m=seed(gamma,tmp_path);sim=Sim(['COMPLETED_WINDOW','ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED'],final_deadline=False);spawn=sim.spawn
    def launch(root,nonce,token):
        child=spawn(root,nonce,token)
        if len(sim.calls)==1:
            poll=child.poll
            child.poll=lambda:None if poll() is None else returncode
        return child
    sim.spawn=launch;value=sim.run(plan,tmp_path/'control')
    assert len(sim.calls)==2 and len(value['restarts'])==int(returncode!=0)
    assert value['attempts'][0]['state']==('CRASH' if returncode!=0 else 'COMPLETED_WINDOW')
    assert value['attempts'][1]['recovery'] is (returncode!=0)
