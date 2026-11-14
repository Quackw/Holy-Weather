from dataclasses import replace
from collections import Counter
import json
from urllib.error import HTTPError,URLError
import pytest
from holy_weather import app,weather
from holy_weather.storage import Store
from test_weather import AT,receipt,report


@pytest.mark.parametrize('error',[TimeoutError('synthetic read timeout'),URLError(TimeoutError('synthetic TLS timeout'))])
def test_timeout_retains_failure_and_waits_for_scheduled_poll(error):
    calls=[];clocks=iter((AT,AT+5000))
    def transport(request,timeout):
        calls.append((request.full_url,timeout));raise error
    result=weather.fetch_weather(station='KSEA',fetch=transport,clock=lambda:next(clocks))
    assert calls==[(weather.source_for('KSEA'),5)]
    assert result['status']=='NETWORK_OR_DECODE_ERROR' and result['error']==type(error).__name__
    assert result['requested_ms']==AT and result['received_ms']==AT+5000
    assert not result['terminal'] and result['retry_policy']=='NEXT_SCHEDULED_POLL'
    assert 'raw_body' not in result and 'sha256' not in result


@pytest.mark.parametrize('error',[URLError('synthetic DNS error'),UnicodeError('synthetic decoding error'),ValueError('synthetic invalid response')])
def test_unclassified_errors_do_not_silently_retry(error):
    calls=[]
    def transport(*args,**kwargs):calls.append(1);raise error
    result=weather.fetch_weather(fetch=transport,clock=lambda:AT)
    assert len(calls)==1 and result['terminal'] and result['retry_policy']=='NO_RETRY'


@pytest.mark.parametrize('status',[401,403,429,500])
def test_http_refusal_keeps_weather_stopped(status):
    calls=[]
    def transport(*args,**kwargs):calls.append(1);raise HTTPError(weather.SOURCE,status,'synthetic',None,None)
    result=weather.fetch_weather(fetch=transport,clock=lambda:AT)
    assert len(calls)==1 and result['http']==status and result['terminal']


def test_price_continues_during_timeout_and_weather_resumes_at_normal_interval(cfg,market,make_raw,monkeypatch):
    wall=[AT];monotonic=[1000];calls=[]
    monkeypatch.setattr(weather,'millis',lambda:wall[0])
    class Response:
        status=200
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def read(self,n):return json.dumps([report(11,AT+240000)]).encode()
    def transport(*args,**kwargs):
        calls.append(wall[0])
        if len(calls)==1:raise TimeoutError('synthetic')
        return Response()
    poller=weather.WeatherPoller(lambda:weather.fetch_weather(fetch=transport,clock=lambda:wall[0]),monotonic=lambda:monotonic[0])
    cfg=replace(cfg,weather_context=True,price_alert_enabled=True,alert_queue_enabled=True)
    counts=Counter()
    try:
        with Store(cfg) as s:
            s.bind(market,{'synthetic':True});s.connection_started(AT)
            s.record_weather(receipt([report(10)],AT-100),market,AT-99)
            old_reports=s.weather_state()['records']
            poller.tick(s,market,wall[0]);poller.thread.join(1);assert not poller.thread.is_alive()
            wall[0]+=1;poller.tick(s,market,wall[0])
            assert s.weather_status()=='NETWORK_OR_DECODE_ERROR' and not poller.stopped
            assert s.weather_state()['records']==old_reports
            # AWC failure neither freezes prices nor invents a weather update.
            for token in market['tokens'].values():
                app.process(json.dumps(make_raw('0.49',AT+10,token=token,bid='0.47')),AT+10,market,s,counts)
            app.process(json.dumps(make_raw('0.61',AT+20,bid='0.59')),AT+21,market,s,counts)
            assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
            events=[tuple(x) for x in s.c.execute('select * from events order by id')]
            frozen_weather=[json.loads(x['payload'])['weather'] for x in s.c.execute("select payload from events where type='CROSS'")]
            assert frozen_weather and all(x['provider_status']=='NETWORK_OR_DECODE_ERROR' and not x['new_observation_in_interval'] for x in frozen_weather)
            old_inputs=[tuple(x) for x in s.c.execute('select * from inputs')]
            monotonic[0]=1299.999;wall[0]=AT+299999
            for _ in range(10):poller.tick(s,market,wall[0])
            assert len(calls)==1
            monotonic[0]=1300;wall[0]=AT+300000
            poller.tick(s,market,wall[0]);poller.thread.join(1);assert not poller.thread.is_alive()
            wall[0]+=1;poller.tick(s,market,wall[0])
            assert calls==[AT,AT+300000] and s.weather_status()=='OK'
            assert s.weather_state()['records'][-1]['temperature_c']=='11'
            assert events==[tuple(x) for x in s.c.execute('select * from events order by id')]
            assert all(x in [tuple(r) for r in s.c.execute('select * from inputs')] for x in old_inputs)
            assert s.c.execute('pragma user_version').fetchone()[0]==1
            assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
            assert not s.c.execute('pragma foreign_key_check').fetchall()
            for _ in range(10):poller.tick(s,market,wall[0])
            assert len(calls)==2
        with Store(cfg) as s:
            assert s.weather_state()['records'][-1]['temperature_c']=='11'
            assert events==[tuple(x) for x in s.c.execute('select * from events order by id')]
    finally:
        poller.close()
        if poller.thread:poller.thread.join(1)
