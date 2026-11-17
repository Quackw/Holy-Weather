"""Durable STARTING/spawn faults, no real children, clocks or network calls."""
from dataclasses import replace
import json
from unittest.mock import patch
import pytest
from holy_weather import recovery as r,lifecycle as l
from test_price_lifecycle import OWNER,identity,prepared,rolling
from test_price_recovery import seed
from test_catalogue import AT


class LaunchInterrupted(BaseException):pass


def charged_backoff(gamma,tmp_path):
    finite,_=seed(gamma,tmp_path);plan=replace(rolling(finite),deadline_ms=None)
    root,state=prepared(tmp_path,plan,active=False,status='BACKOFF')
    at=AT+10*3600000  # The rolling-24h case crosses UTC midnight.
    state.update(retry=True,recovery_charged=True,restarts=[at],last_ms=at,next_ms=at+30000)
    r.write(root,'state.json',state)
    r.write(root,'plan.json',dict(plan=plan.value(),identity=state['identity'],code=state['identity']['code']))
    return plan,root,at


@pytest.mark.parametrize('spacing,limit',[(1000,3),(3600001,6)],ids=['3-per-hour','6-per-rolling-24h'])
@pytest.mark.parametrize('boundary',['inside-spawn','before-running-write'])
@pytest.mark.parametrize('evidence',['missing','owner','result'])
def test_repeated_interrupted_launches_consume_recovery_budget(gamma,tmp_path,identity,monkeypatch,spacing,limit,boundary,evidence):
    plan,root,at=charged_backoff(gamma,tmp_path);now=[at];controller=[100];launches=[]
    monkeypatch.setattr(l,'own_identity',lambda:dict(OWNER,pid=controller[0]))
    original_write=r.write
    def write(path,name,value):
        if boundary=='before-running-write' and name=='state.json' and value.get('status')=='RUNNING':
            raise LaunchInterrupted()
        original_write(path,name,value)
    class ExitedChild:
        pid=12345
        def poll(self):return 0
    def spawn(path,nonce,token,fd):
        l.validate_guard(path,fd)
        saved=json.loads((path/'state.json').read_text());launches.append(saved)
        assert saved['status']=='STARTING' and saved['active']==nonce
        if evidence!='missing':
            original_write(path,nonce+'.owner.json',dict(protocol=1,nonce=nonce,binding=saved['identity'],
                controller=saved['controller_process'],process=dict(OWNER,pid=ExitedChild.pid)))
        if evidence=='result':
            original_write(path,nonce+'.result.json',dict(nonce=nonce,pid=ExitedChild.pid,state='SIGNAL_STOP',error=None))
        if boundary=='inside-spawn':raise LaunchInterrupted()
        return ExitedChild()
    def wait(seconds):now[0]+=round(seconds*1000)
    def run():
        return r.supervise(plan,root,clock=lambda:now[0],wait=wait,_spawn=spawn)
    with patch.object(r,'write',side_effect=write):
        for number in range(limit):
            with pytest.raises(LaunchInterrupted):run()
            durable=json.loads((root/'state.json').read_text())
            assert durable['status']=='STARTING' and len(durable['restarts'])==number+1
            assert len(durable['attempts'])==number+1
            controller[0]+=1;now[0]+=spacing
        exhausted=run()
        assert exhausted['status']=='RESTART_BUDGET_EXHAUSTED'
        assert len(exhausted['restarts'])==len(launches)==limit
        assert all('recovery_charged' not in saved for saved in launches)
        if limit==6:
            assert sum(now[0]-t<3600000 for t in exhausted['restarts'])<3
            assert now[0]//86400000>at//86400000
        # A later service entry must not clear the exhausted stop or launch again.
        controller[0]+=1
        assert run()['status']=='RESTART_BUDGET_EXHAUSTED' and len(launches)==limit


def test_repeated_backoff_entries_keep_one_charge_and_original_deadline(gamma,tmp_path,identity,monkeypatch):
    plan,root,at=charged_backoff(gamma,tmp_path);now=[at];controller=[100]
    monkeypatch.setattr(l,'own_identity',lambda:dict(OWNER,pid=controller[0]))
    def interrupted_wait(seconds):raise LaunchInterrupted()
    for _ in range(3):
        with pytest.raises(LaunchInterrupted):
            r.supervise(plan,root,clock=lambda:now[0],wait=interrupted_wait,
                        _spawn=lambda *args:pytest.fail('spawn during interrupted BACKOFF'))
        saved=json.loads((root/'state.json').read_text())
        assert saved['status']=='BACKOFF' and saved['recovery_charged'] is True
        assert saved['restarts']==[at] and saved['next_ms']==at+30000
        assert saved['active'] is None and saved['attempts']==[]
        controller[0]+=1;now[0]+=1000
