from contextlib import closing
from dataclasses import replace
import json,os,shutil,subprocess,sys,time,types
from pathlib import Path
import pytest
import websockets
from holy_weather import catalogue,core,history,storage
from holy_weather.alert_queue import preview,claim,finish
from test_catalogue import AT,setup

ROOT=Path(__file__).resolve().parents[1]
ACCEPTED='bbbbe70ee0ddcfbf824589d0a435d0eec7ebc6b9'


def test_current_main_and_europe_fast_path_have_equal_all_table_fingerprints(gamma,tmp_path,monkeypatch,record_property):
    source=subprocess.check_output(['git','show',ACCEPTED+':src/holy_weather/core.py'],cwd=ROOT)
    old=types.ModuleType('holy_weather._accepted_receive_core');old.__package__='holy_weather'
    monkeypatch.setitem(sys.modules,old.__name__,old);exec(compile(source,'accepted/core.py','exec'),old.__dict__)
    cfg,markets=setup(gamma,tmp_path);results=[];timings={}
    for fast in (False,True):
        began=time.process_time()
        with monkeypatch.context()as patch:
            if not fast:patch.setattr(storage,'evolve',lambda previous,sample,cfg,**kw:old.evolve(previous,sample,cfg))
            with catalogue.CatalogueStore(replace(cfg,data_dir=str(tmp_path/str(fast))),session=False)as s:
                if not fast:s.usage=lambda:storage.Store.usage(s)
                for market,receipt in markets:s.bind(market,receipt)
                s.connection_started(AT);router=catalogue.Router(s)
                for n in range(5000):
                    m=markets[n%11][0];side='YES'if(n//11)%2==0 else'NO';price=('0.49','0.56','0.71','0.81')[(n//22)%4]
                    raw=json.dumps({'event_type':'best_bid_ask','market':m['condition_id'],'asset_id':m['tokens'][side],
                                    'timestamp':str(AT+n+1),'best_bid':price,'best_ask':price})
                    router.feed(raw,AT+n+1)
                    if n%100==0:router.feed(raw,AT+n+1)
                    if n==2500:s.gap('OFFLINE_CONTROL',AT+n+1);s.connection_started(AT+n+1)
                pending=[key for key,raw in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")if json.loads(raw)['status']=='PENDING']
                assert len(pending)>=3
                for i,key in enumerate(pending[:2]):
                    card=preview(s,key);assert claim(s,card,AT+20000+i*10)
                    if not i:finish(s,card,{'status':'SENT','message_id':17},AT+20001)
                history.check(s.c);results.append(history.fingerprint(s.c))
                assert s.c.execute("select count(*)from events where type='PRICE_ALERT'").fetchone()[0]>0
                assert {'SENT','UNKNOWN','PENDING'} <= {json.loads(raw)['status']for _,raw in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")}
        timings[str(fast)]=time.process_time()-began
    assert results[0]==results[1]
    record_property('europe_current_main_vs_fast_cpu_seconds',json.dumps(timings))


def test_exact_parent_code_rollback_reads_new_europe_records(gamma,tmp_path):
    _,markets=setup(gamma,tmp_path,1);(tmp_path/'market.json').write_text(json.dumps(markets[0][0]))
    old=tmp_path/'accepted';shutil.copytree(ROOT/'src',old)
    for name in ('core.py','storage.py','catalogue.py'):
        (old/'holy_weather'/name).write_bytes(subprocess.check_output(['git','show',ACCEPTED+':src/holy_weather/'+name],cwd=ROOT))
    for phase,path,count in [('old',old,1),('new',ROOT/'src',2),('rollback',old,3)]:
        env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=str(path)+os.pathsep+str(Path(websockets.__file__).resolve().parents[1]))
        p=subprocess.run([sys.executable,'-B',str(ROOT/'tests/regional_rollback_probe.py'),'europe',phase,str(tmp_path)],env=env,text=True,capture_output=True,timeout=30)
        assert p.returncode==0,p.stdout+p.stderr
        result=json.loads(p.stdout);assert result['price_events']==count and result['integrity']=='ok'
        if phase=='rollback':assert sorted(result['queue'])==['PENDING','SENT','UNKNOWN']
