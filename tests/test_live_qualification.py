from dataclasses import replace
from datetime import datetime,timezone
from zoneinfo import ZoneInfo
import gzip
import importlib.util
import json
from pathlib import Path
import time
import pytest
from holy_weather import app,weather
from holy_weather.storage import Store
from test_weather import receipt,report

spec=importlib.util.spec_from_file_location('live_qualification',Path(__file__).resolve().parents[1]/'tools/qualify_live.py')
live=importlib.util.module_from_spec(spec);spec.loader.exec_module(live)


@pytest.mark.parametrize('value',[0,-1,True,2])
def test_baseline_wait_cannot_exceed_finite_window(cfg,value):
    with pytest.raises(ValueError,match='BASELINE_WAIT_MUST_FIT_OBSERVATION_WINDOW'):
        app.observe(replace(cfg,duration_seconds=1),baseline_wait_seconds=value)
    assert not Path(cfg.data_dir).exists()


@pytest.mark.parametrize('invalidate_at_end',[False,True])
def test_old_rest_does_not_prevent_websocket_or_late_fresh_baseline(cfg,market,make_raw,monkeypatch,tmp_path,invalidate_at_end):
    market=dict(market,date=datetime.now(timezone.utc).astimezone(ZoneInfo(market['timezone'])).date().isoformat())
    monkeypatch.setattr(app,'discover',lambda _: (market,{'synthetic':True,'rest_diagnostic_source_age_ms':133000}))
    monkeypatch.setattr(app,'BASELINE_RECOVERY_SECONDS',.03)
    calls=[];started=time.monotonic();at=app.millis()
    class Socket:
        stage=0
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def send(self,v):pass
        def recv(self,timeout):
            time.sleep(.005)
            if self.stage==0:
                self.stage+=1
                return json.dumps([make_raw('0.49',at-91000,token=t,kind='book') for t in market['tokens'].values()])
            if time.monotonic()-started<.08:raise TimeoutError()
            if self.stage==1:
                self.stage+=1
                return json.dumps([make_raw('0.79',app.millis(),token=t,bid='0.77') for t in market['tokens'].values()])
            if self.stage==2:
                self.stage+=1;return json.dumps(make_raw('0.81',app.millis(),bid='0.79'))
            if self.stage==3 and invalidate_at_end:
                self.stage+=1;return 'synthetic malformed JSON {'
            raise TimeoutError()
    def connect(*args,**kwargs):calls.append(1);return Socket()
    cfg=replace(cfg,duration_seconds=1,weather_context=True,price_alert_enabled=True,alert_queue_enabled=True)
    def current_weather():
        now=app.millis();return receipt([report(10,(now//60000)*60000-60000)],now)
    result=live.qualify(cfg,tmp_path/'qualification',connect=connect,weather_fetch=current_weather)
    assert calls==[1] and result['result']['state']=='COMPLETED_WINDOW'
    assert result['rest_book_reads']==0 and result['quote_age_seconds']==90
    assert result['both_sides_verified'] and result['verdict']==('PARTIAL' if invalidate_at_end else 'PASS')
    assert result['result']['counters']['STALE']==2 and 'reconnects' not in result['result']['counters']
    assert result['result']['baseline_wait_seconds']==1
    assert result['events_by_type']=={'PRICE_ALERT':1}
    assert len(result['queue_previews'])==1
    assert result['sqlite_integrity']=='ok' and result['foreign_key_errors']==0
    capture=tmp_path/'qualification/wire.jsonl.gz'
    assert len(gzip.decompress(capture.read_bytes()).splitlines())==(4 if invalidate_at_end else 3)
    # The first fresh high quote is a baseline, not retroactive 50/55/60/70 events.
    with Store(replace(cfg,data_dir=str(tmp_path/'qualification/data')),session=False) as s:
        event=json.loads(s.c.execute("select payload from events where type='PRICE_ALERT'").fetchone()[0])
        assert event['thresholds']==['0.80']


@pytest.mark.parametrize('confirm_current_top',[False,True])
def test_controlled_timeout_is_labelled_and_followup_is_distinct(cfg,market,make_raw,monkeypatch,tmp_path,confirm_current_top):
    wall=[app.millis()];real_calls=[]
    market=dict(market,date=datetime.fromtimestamp(wall[0]/1000,timezone.utc).astimezone(ZoneInfo(market['timezone'])).date().isoformat())
    monkeypatch.setattr(app,'millis',lambda:wall[0])
    cfg=replace(cfg,duration_seconds=330,weather_context=True,price_alert_enabled=True)
    def real():
        real_calls.append(wall[0])
        return receipt([report(10,(wall[0]//60000)*60000-60000)],wall[0])
    def observe(config,**kwargs):
        assert kwargs['baseline_wait_seconds']==330
        assert config.max_reconnects==0 and config.quote_age_seconds==90
        with Store(config) as s:
            s.bind(market,{'synthetic':True});s.connection_started(wall[0])
            first=kwargs['weather_fetch']()
            assert first['experiment_fault']=='CONTROLLED_TIMEOUT_NO_HTTP_REQUEST'
            assert first['status']=='NETWORK_OR_DECODE_ERROR' and not first['terminal']
            s.record_weather(first,market,wall[0])
            from collections import Counter
            app.process(json.dumps([make_raw('0.49',wall[0],token=t) for t in market['tokens'].values()]),wall[0],market,s,Counter())
            wall[0]+=300000
            second=kwargs['weather_fetch']()
            assert 'experiment_fault' not in second
            s.record_weather(second,market,wall[0])
            wall[0]+=1
            if confirm_current_top:
                # Real price_change messages can confirm an already established
                # top; they do not create a full baseline or PRICE crossing.
                changes=[dict(asset_id=t,price='0.48',size='25',side='BUY',best_bid='0.48',best_ask='0.49') for t in market['tokens'].values()]
                app.process(json.dumps(dict(event_type='price_change',market=market['condition_id'],timestamp=str(wall[0]),price_changes=changes)),wall[0],market,s,Counter())
            return dict(state='COMPLETED_WINDOW',storage=s.stats(now_ms=wall[0]))
    monkeypatch.setattr(app,'observe',observe)
    result=live.qualify(cfg,tmp_path/'timeout',controlled_timeout=True,weather_fetch=real)
    assert len(real_calls)==1 and len(result['awc_calls'])==2
    assert result['awc_calls'][0]['injected'] and not result['awc_calls'][1]['injected']
    assert result['awc_recovered_after_controlled_timeout']
    # Recovery does not refresh the native prices seen five minutes earlier.
    assert result['both_sides_verified']
    assert result['verdict']==('PASS' if confirm_current_top else 'PARTIAL')
    assert all(e['within_90s_at_end']==confirm_current_top for e in result['verified_sides'].values())
    assert result['events_by_type']=={} and result['queue_previews']==[]


def test_qualification_cannot_send_or_overwrite_history(cfg,tmp_path):
    with pytest.raises(ValueError,match='TELEGRAM_MUST_BE_DISABLED'):
        live.qualify(replace(cfg,telegram_enabled=True,alert_queue_enabled=True),tmp_path/'telegram')
    path=tmp_path/'existing';path.mkdir();(path/'history').write_text('preserve')
    with pytest.raises(ValueError,match='NEW_QUALIFICATION_DIRECTORY_REQUIRED'):
        live.qualify(replace(cfg,weather_context=True,price_alert_enabled=True),path)
    assert (path/'history').read_text()=='preserve'


def test_controlled_timeout_requires_real_poll_interval(cfg,tmp_path):
    with pytest.raises(ValueError,match='MUST_INCLUDE_NEXT_WEATHER_POLL'):
        live.qualify(replace(cfg,duration_seconds=120,weather_context=True,price_alert_enabled=True),tmp_path/'short',controlled_timeout=True)
