"""Isolated service health and recovery; no live databases, systemd or Telegram."""
from dataclasses import replace
import json,os,socket,tempfile,threading
from pathlib import Path
from unittest.mock import patch
import pytest
from holy_weather import recovery as r,service
from test_price_recovery import seed,Sim
from test_price_lifecycle import rolling


def test_unit_supervises_controller_not_its_worker():
    unit=(Path(__file__).resolve().parents[1]/'deploy/holy-weather-price@.service.in').read_text()
    assert 'WatchdogSec=360s' in unit and 'NotifyAccess=main' in unit
    assert 'SendSIGKILL=no' in unit and 'RestartPreventExitStatus=78' in unit


def test_service_result_does_not_hide_disabled_delivery(gamma,tmp_path,monkeypatch,capsys):
    plan,_=seed(gamma,tmp_path);path=tmp_path/'plan.json'
    path.write_text(json.dumps(replace(rolling(plan),deadline_ms=None).value()))
    monkeypatch.setattr(service.signal,'signal',lambda *a:None)
    monkeypatch.setattr(service,'supervise',lambda *a,**kw:dict(status='FATAL',attempts=[],
        telegram_enabled=False,telegram_disabled_reason='SEND_RESULT_UNKNOWN'))
    assert service.run(path,tmp_path/'control')==78
    assert 'SEND_RESULT_UNKNOWN' in capsys.readouterr().out


def test_disabled_sender_is_visible_without_retry_or_per_tick_logs(capsys):
    sent=[];ticks=[0.]
    health=service.Health(environment={},monotonic=lambda:ticks[0],notify=sent.append)
    value=dict(status='RUNNING',telegram_enabled=False,telegram_disabled_reason='SEND_RESULT_UNKNOWN')
    for _ in range(100):health(value)
    output=capsys.readouterr().out.splitlines()
    assert len(output)==1
    record=json.loads(output[0]);assert record['telegram']=='DISABLED' and record['reason']=='SEND_RESULT_UNKNOWN'
    assert record['watchdog']=='OFF' and not sent
    assert value==dict(status='RUNNING',telegram_enabled=False,telegram_disabled_reason='SEND_RESULT_UNKNOWN')


def test_controller_reports_progress_through_poll_and_backoff(gamma,tmp_path):
    plan,_=seed(gamma,tmp_path);sim=Sim(['NETWORK_GAP_NO_RETRY','COMPLETED_WINDOW']);seen=[]
    with patch.object(r,'preflight',return_value=None):
        state=r.supervise(plan,tmp_path/'control',clock=sim.clock,monotonic=sim.mono,
                          wait=sim.wait,_spawn=sim.spawn,on_health=lambda v:seen.append((sim.t,dict(v))))
    assert state['status']=='DEADLINE' and len(state['restarts'])==1
    assert any(v['status']=='RUNNING' for _,v in seen)
    backoff=[at for at,v in seen if v['status']=='BACKOFF']
    assert len(backoff)>20 and max(b-a for a,b in zip(backoff,backoff[1:]))<=1
    assert all(set(v)<={'status','telegram_enabled','telegram_disabled_reason'} for _,v in seen)


def test_watchdog_uses_elapsed_main_loop_progress_and_reports_disable_immediately(capsys):
    sent=[];ticks=[0.]
    env=dict(WATCHDOG_USEC='3000000',WATCHDOG_PID=str(os.getpid()),NOTIFY_SOCKET='/synthetic/notify')
    health=service.Health(environment=env,monotonic=lambda:ticks[0],notify=sent.append)
    value=dict(status='RUNNING',telegram_enabled=True,telegram_disabled_reason=None)
    health(value)
    for t in (.1,.25,.999):ticks[0]=t;health(value)
    assert len(sent)==1 and sent[0].startswith('WATCHDOG=1\nSTATUS=')
    ticks[0]=1.;health(value);assert len(sent)==2
    # No timer/thread exists to keep feeding the watchdog during a blocked call.
    ticks[0]=100.;assert len(sent)==2
    health(dict(value,telegram_enabled=False,telegram_disabled_reason='SEND_RESULT_UNKNOWN'))
    assert len(sent)==3 and 'Telegram=DISABLED; reason=SEND_RESULT_UNKNOWN' in sent[-1]
    records=[json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(records)==2 and records[-1]['telegram']=='DISABLED'


def test_foreign_pid_cannot_feed_watchdog_and_child_cannot_reuse_notifier(capsys):
    sent=[]
    env=dict(WATCHDOG_USEC='3000000',WATCHDOG_PID=str(os.getpid()+1),NOTIFY_SOCKET='/synthetic/notify')
    health=service.Health(environment=env,notify=sent.append)
    health(dict(status='RUNNING'));assert not health.armed and not sent
    with patch.object(service.os,'getpid',return_value=health.pid+1):
        with pytest.raises(r.RecoveryStop,match='HEALTH_OWNER'):health(dict(status='RUNNING'))


def test_helper_thread_cannot_hide_blocked_controller():
    sent=[];errors=[]
    health=service.Health(environment=dict(WATCHDOG_USEC='3000000',NOTIFY_SOCKET='/x'),notify=sent.append)
    def helper():
        try:health(dict(status='RUNNING'))
        except r.RecoveryStop as exc:errors.append(str(exc))
    thread=threading.Thread(target=helper);thread.start();thread.join(1)
    assert not thread.is_alive() and errors==['SERVICE_HEALTH_OWNER'] and not sent


@pytest.mark.parametrize('env',[
    dict(WATCHDOG_USEC='0',NOTIFY_SOCKET='/x'),
    dict(WATCHDOG_USEC='-1',NOTIFY_SOCKET='/x'),
    dict(WATCHDOG_USEC='not-a-time',NOTIFY_SOCKET='/x'),
    dict(WATCHDOG_USEC='360000001',NOTIFY_SOCKET='/x'),
    dict(WATCHDOG_USEC='3000000'),
    dict(WATCHDOG_USEC='3000000',NOTIFY_SOCKET='relative'),
    dict(WATCHDOG_USEC='3000000',NOTIFY_SOCKET='/bad\x00socket')])
def test_bad_or_unbounded_watchdog_environment_refused(env):
    with pytest.raises(r.RecoveryStop,match='WATCHDOG_BOUND|NOTIFY_SOCKET_REQUIRED'):service.Health(environment=env)


def test_real_unix_datagram_and_private_values_never_in_status(capsys):
    with tempfile.TemporaryDirectory(prefix='hw-notify-',dir='/tmp') as root:
        path=str(Path(root)/'notify')
        with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as listener:
            listener.bind(path);listener.settimeout(1)
            health=service.Health(environment=dict(WATCHDOG_USEC='3000000',NOTIFY_SOCKET=path))
            health(dict(status='RUNNING\nPRIVATE_TOKEN',telegram_enabled=False,
                        telegram_disabled_reason='https://private.invalid/key'))
            raw=listener.recv(2048).decode()
        assert raw.startswith('WATCHDOG=1\nSTATUS=PRICE')
        assert 'PRIVATE_TOKEN' not in raw and 'private.invalid' not in raw
        assert 'PRIVATE_TOKEN' not in capsys.readouterr().out


def test_notify_failure_is_bounded_visible_and_does_not_stop_collection(capsys):
    ticks=[0.];calls=[]
    def unavailable(message):calls.append(message);raise OSError('private endpoint must not be logged')
    health=service.Health(environment=dict(WATCHDOG_USEC='3000000',NOTIFY_SOCKET='/x'),
                          monotonic=lambda:ticks[0],notify=unavailable)
    for _ in range(100):health(dict(status='RUNNING'))
    output=capsys.readouterr().out
    assert len(calls)==1 and output.count('PRICE_SERVICE_NOTIFY_UNAVAILABLE')==1
    assert 'private endpoint' not in output
    health.notify=calls.append;ticks[0]=1.;health(dict(status='RUNNING'))
    assert 'PRICE_SERVICE_NOTIFY_RECOVERED' in capsys.readouterr().out


def test_disabled_sender_remains_durable_while_controller_collects(gamma,tmp_path,capsys):
    plan,_=seed(gamma,tmp_path);sim=Sim(['COMPLETED_WINDOW']);spawn=sim.spawn
    def disabled(root,nonce,token):
        child=spawn(root,nonce,token)
        value=json.loads((root/(nonce+'.progress.json')).read_text())
        value.update(telegram_enabled=False,telegram_disabled_reason='SEND_RESULT_UNKNOWN')
        r.write(root,nonce+'.progress.json',value)
        return child
    health=service.Health(environment={})
    with patch.object(r,'preflight',return_value=None):
        state=r.supervise(plan,tmp_path/'control',clock=sim.clock,monotonic=sim.mono,
                          wait=sim.wait,_spawn=disabled,on_health=health)
    assert len(sim.calls)==1 and state['status']=='DEADLINE' and not state['restarts']
    saved=json.loads((tmp_path/'control/state.json').read_text())
    assert saved['telegram_enabled'] is False and saved['telegram_disabled_reason']=='SEND_RESULT_UNKNOWN'
    assert '"telegram":"DISABLED"' in capsys.readouterr().out
