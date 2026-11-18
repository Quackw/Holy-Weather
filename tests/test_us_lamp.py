"""US LAMP integration uses original source bytes and the existing queue owner."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime,timedelta,timezone
import io,json,sqlite3,threading,time
import os,subprocess,sys
from pathlib import Path
from dataclasses import asdict
from urllib.error import HTTPError

import pytest

from holy_weather import catalogue,lamp,lamp_alert,recovery,us_lamp
from holy_weather.alert_queue import claim,finish,preview
from holy_weather.core import Config,US_CITIES,canonical,sha
from holy_weather.discovery import select
from holy_weather.history import prepare,restore_history,digest
from test_catalogue import AT,feed

ISSUE=AT-1800000


def native(station,temperature=46,issue=ISSUE):
    issued=datetime.fromtimestamp(issue/1000,timezone.utc)
    first=issued.replace(minute=0)+timedelta(hours=1)
    hours=[(first+timedelta(hours=i)).hour for i in range(25)]
    return (' '+station+'   GFS LAMP GUIDANCE  '+issued.strftime('%m/%d/%Y  %H%M UTC')+'\n'
            +' UTC '+''.join(f' {h:02d}' for h in hours)+' \n'
            +' TMP '+''.join(f'{temperature:3d}' for _ in hours)+' \n\n').encode()


def receipt(station,temperature=46,at=AT,issue=ISSUE):
    body=native(station,temperature,issue)
    return dict(source=lamp.nomads_source(issue),requested_ms=at-1,received_ms=at,status='OK',
                raw_body=body.decode(),sha256=sha(body),file_sha256=sha(body))


def setup(gamma,tmp_path,*,at=AT,buckets=1):
    now=datetime.fromtimestamp(at/1000,timezone.utc)
    cfg=catalogue.CatalogueConfig(region='us',city='new-york',us_lamp_enabled=True,data_dir=str(tmp_path/'data'))
    markets=[]
    for n,(city,(slug,station,zone)) in enumerate(US_CITIES.items()):
        doc=json.loads(json.dumps(gamma).replace('nyc',slug).replace('KLGA',station))
        for b in range(buckets):
            ident=n*16+b+1
            doc[0]['markets'][0].update(conditionId='0x'+f'{ident:064x}',
                clobTokenIds=json.dumps([str(ident*2),str(ident*2+1)]))
            m=select(doc,Config(city=city),now=now)
            markets.append((m,dict(received_at=now.isoformat(),raw_body=json.dumps(doc))))
    return cfg,markets


def events(store):
    return [json.loads(r[0]) for r in store.c.execute("select payload from events where type='LAMP_ALERT' order by rowid")]


@pytest.mark.parametrize('city',US_CITIES)
def test_source_native_station_clock_and_header(city,gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path)
    market=next(m for m,_ in markets if m['station']==US_CITIES[city][1])
    r=receipt(market['station'])
    state=lamp.decode(r,market,AT+1)
    assert state['source']==lamp.nomads_source(ISSUE) and state['raw_bulletin']==r['raw_body']
    assert len(state['hours'])==25 and state['issue_ms']==ISSUE
    assert all(h['temperature_f']=='46' for h in state['hours'])
    with pytest.raises(ValueError,match='STATION_OR_HEADER'):
        lamp.decode(receipt('KJFK'),market,AT+1)
    with pytest.raises(ValueError,match='SOURCE_ISSUE_MISMATCH'):
        lamp.decode(dict(r,source=lamp.nomads_source(ISSUE+3600000)),market,AT+1)


def test_ten_stations_all_buckets_independent_restart_atomic_queue(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,buckets=2)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        for m,r in markets:s.bind(m,r)
        for station in {m['station'] for m,_ in markets}:
            group=[m for m,_ in markets if m['station']==station]
            s.record_lamp(receipt(station),group[0],AT,markets=group)
            s.record_lamp(receipt(station,48,AT+1000),group[0],AT+1000,markets=group)
        assert len(events(s))==20 and not s.c.execute('select 1 from samples').fetchone()
        assert s.c.execute("select count(*) from inputs where identity like 'LAMP:%'").fetchone()[0]==20
        assert s.c.execute("select count(*) from meta where key like 'lamp_alert_v1:%'").fetchone()[0]==20
        queue={k:json.loads(v) for k,v in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")}
        assert len(queue)==20
        key=next(iter(queue));p=preview(s,key);assert claim(s,p,AT+1001)
        finish(s,p,{'status':'SENT','message_id':1},AT+1002)
        key2=next(k for k in queue if k!=key);p2=preview(s,key2);assert claim(s,p2,AT+1003)
        frozen=events(s)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        for station in {m['station'] for m,_ in markets}:
            group=[m for m,_ in markets if m['station']==station]
            s.record_lamp(receipt(station,48,AT+2000),group[0],AT+2000,markets=group)
        assert events(s)==frozen
        assert not claim(s,p,AT+2001) and not claim(s,p2,AT+2001)
        assert recovery.prepare_resume(s,{'isolated':'test'},AT+2002)
        assert events(s)==frozen
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()
        assert s.c.execute('pragma user_version').fetchone()[0]==1


def test_event_queue_failure_rolls_back_station_and_all_buckets(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,buckets=2);group=[m for m,_ in markets if m['station']=='KLGA']
    with catalogue.CatalogueStore(cfg,session=False) as s:
        for m,r in markets:s.bind(m,r)
        s.record_lamp(receipt('KLGA'),group[0],AT,markets=group)
        before=[tuple(r) for r in s.c.execute("select key,value from meta order by key")]
        s.c.execute("create temp trigger fault before insert on meta when new.key like 'alert_delivery_v1:%' begin select raise(abort,'isolated queue failure'); end")
        with pytest.raises(sqlite3.IntegrityError):
            s.record_lamp(receipt('KLGA',48,AT+1),group[0],AT+1,markets=group)
        assert not events(s)
        assert before==[tuple(r) for r in s.c.execute("select key,value from meta order by key")]
        s.c.execute('drop trigger fault')
        s.record_lamp(receipt('KLGA',48,AT+1),group[0],AT+1,markets=group)
        assert len(events(s))==2


@pytest.mark.parametrize('city',US_CITIES)
def test_local_day_new_market_baseline_and_dst(city,gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path);old=next(m for m,_ in markets if m['station']==US_CITIES[city][1])
    from zoneinfo import ZoneInfo
    zone=ZoneInfo(old['timezone'])
    # UTC hours stay unique across both DST folds and the local date boundary.
    for stamp in ('2026-03-08T06:30:00+00:00','2026-11-01T05:30:00+00:00'):
        issue=int(datetime.fromisoformat(stamp).timestamp()*1000);at=issue+600000
        m=dict(old,date=datetime.fromtimestamp(at/1000,zone).date().isoformat())
        state=lamp.decode(receipt(m['station'],at=at,issue=issue),m,at)
        hours,why=lamp_alert.remaining(state,m,at)
        assert why=='OK' and len(hours)==len(set(hours))
        assert all(datetime.fromtimestamp(h/1000,zone).date().isoformat()==m['date'] for h in hours)
    tomorrow=dict(old,date='2026-11-15',condition_id='0x'+'f'*64,tokens={'YES':'9999','NO':'9998'})
    at=int(datetime(2026,11,15,15,tzinfo=timezone.utc).timestamp()*1000);issue=at-1800000
    with catalogue.CatalogueStore(cfg,session=False) as s:
        original=next(r for m,r in markets if m==old);s.bind(old,original)
        s.record_lamp(receipt(old['station']),old,AT)
        s.bind(tomorrow,dict(received_at=datetime.fromtimestamp(at/1000,timezone.utc).isoformat()))
        s.record_lamp(receipt(old['station'],60,at,issue),tomorrow,at)
        assert not events(s)
        assert s.lamp_state(tomorrow)['day']=='2026-11-15'
        s.record_lamp(receipt(old['station'],62,at+1,issue),tomorrow,at+1)
        assert len(events(s))==1
        s.expire_closed_deliveries(at+86400000)
        assert s.c.execute("select json_extract(value,'$.status') from meta where key like 'alert_delivery_v1:%'").fetchone()[0]=='EXPIRED'


def test_shared_download_sha_size_and_missing_station():
    body=b''.join(native(station) for _,station,_ in US_CITIES.values())
    class Response(io.BytesIO):
        status=200
        headers={'Content-Length':str(len(body)),'Last-Modified':'synthetic'}
    calls=[]
    def fetch(request,timeout):
        calls.append((request.full_url,timeout));return Response(body)
    result=us_lamp.fetch_cycle(ISSUE,fetch=fetch,clock=lambda:AT)
    assert len(calls)==1 and result['status']=='OK'
    assert result['file_sha256']==sha(body) and result['bytes']==len(body)
    assert set(result['receipts'])=={s for _,s,_ in US_CITIES.values()}
    assert all(r['raw_body'].encode()==native(s) for s,r in result['receipts'].items())
    class Truncated(Response):headers={'Content-Length':str(len(body)+1)}
    assert us_lamp.fetch_cycle(ISSUE,fetch=lambda *a,**k:Truncated(body),clock=lambda:AT)['status']=='INVALID_DATA'


@pytest.mark.parametrize('code,blocked',[(403,True),(429,True),(404,False),(503,False)])
def test_http_policy(code,blocked):
    def denied(request,timeout):raise HTTPError(request.full_url,code,'offline',None,None)
    result=us_lamp.fetch_cycle(ISSUE,fetch=denied,clock=lambda:AT)
    assert result['terminal'] is blocked and result['status']!='OK'


def test_schedule_restart_backoff_and_skipped_issue(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path);m,r=markets[0];calls=[]
    def fetch(issue):
        calls.append(issue)
        return dict(status='NETWORK_ERROR',terminal=False,receipts={},bytes=0)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        s.bind(m,r);selected=set(s.markets)
        first=us_lamp.Collection(fetch);first.tick(s,selected,AT);first.thread.join(1)
        first.tick(s,selected,AT+1);first.close()
        second=us_lamp.Collection(fetch);second.tick(s,selected,AT+2)
        assert second.thread is None and len(calls)==1
        second.tick(s,selected,AT+us_lamp.RETRY_MS);second.thread.join(1)
        assert len(calls)==2
        second.tick(s,selected,AT+us_lamp.RETRY_MS+1)
        s.set_meta(us_lamp.KEY,dict(cycle_ms=ISSUE,attempts=1,next_ms=0,blocked=True,status='ACCESS_DENIED'))
        third=us_lamp.Collection(fetch);third.tick(s,selected,AT+7200000)
        assert third.thread is None


def test_price_projection_unchanged_when_forecasts_added(gamma,tmp_path):
    projections=[]
    for enabled in (False,True):
        cfg,markets=setup(gamma,tmp_path/str(enabled));cfg=replace(cfg,us_lamp_enabled=enabled)
        with catalogue.CatalogueStore(cfg,session=False) as s:
            for m,r in markets:s.bind(m,r)
            s.connection_started(AT);router=catalogue.Router(s)
            for m,_ in markets:
                feed(router,m,'0.49',AT+1);feed(router,m,'0.81',AT+2)
            if enabled:
                for m,_ in markets:
                    s.record_lamp(receipt(m['station']),m,AT+3)
                    s.record_lamp(receipt(m['station'],48,AT+4),m,AT+4)
            projections.append((
                [tuple(r) for r in s.c.execute("select id,payload from events where type='PRICE_ALERT' order by id")],
                [tuple(r) for r in s.c.execute("select key,value from meta where key like 'price_alert_v1:%' order by key")]))
    assert projections[0]==projections[1]


def test_500_duplicates_one_station_bounded(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path);m,r=markets[0]
    with catalogue.CatalogueStore(cfg,session=False) as s:
        s.bind(m,r);s.record_lamp(receipt(m['station']),m,AT)
        before=s.c.total_changes
        for i in range(500):s.record_lamp(receipt(m['station'],at=AT+i+1),m,AT+i+1)
        assert not events(s)
        assert s.c.total_changes-before<=3000
        assert s.usage()<2*1024**2


@pytest.mark.parametrize('bucket,old,new,boundary',[
    ('45°F or below',45,46,'45.5'),('48°F or higher',47,48,'47.5'),
    ('46-47°F',48,47,'47.5'),
])
def test_all_bucket_shapes_cross_one_degree(gamma,tmp_path,bucket,old,new,boundary):
    cfg,markets=setup(gamma,tmp_path);m,r=markets[0];m=dict(m,bucket=bucket)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        s.bind(m,r)
        s.record_lamp(receipt(m['station'],old),m,AT)
        s.record_lamp(receipt(m['station'],new,AT+1),m,AT+1)
        e=events(s)[0]
        assert e['reason']=='BOUNDARY_CROSS' and e['crossed_boundaries_f']==[boundary]
        assert e['threshold_f']=='2'


def test_forecast_collection_and_delivery_during_market_outage(gamma,tmp_path,monkeypatch):
    cfg,markets=setup(gamma,tmp_path);m,r=markets[0];cfg=replace(cfg,telegram_enabled=True,duration_seconds=2,max_reconnects=0)
    with catalogue.CatalogueStore(cfg,session=False) as s:
        s.bind(m,r);s.record_lamp(receipt(m['station']),m,AT)
    now=[AT+3600000];mono=[0.];sent=[];calls=[]
    def fetch(issue):
        calls.append(issue)
        return dict(receipts={m['station']:receipt(m['station'],48,now[0],issue)},status='OK',terminal=False,bytes=100)
    def wait(seconds):
        time.sleep(.001);now[0]+=round(seconds*1000);mono[0]+=seconds
    def connect(*a,**k):raise OSError('offline market outage')
    monkeypatch.setattr('holy_weather.telegram.millis',lambda:now[0])
    result=catalogue.observe_catalogue(cfg,connect=connect,discover=lambda city:([m],r,[]) if city=='new-york' else ([],{},[]),
        token='offline',transport=lambda t,p:(sent.append(p) or {'status':'SENT','message_id':1}),
        lamp_fetch=fetch,clock=lambda:now[0],monotonic=lambda:mono[0],wait=wait)
    assert len(calls)==1 and result['state']=='NETWORK_GAP_NO_RETRY'
    assert len(sent)==1 and result['deliveries']=={'SENT':1} and result['price_events']==0
    with catalogue.CatalogueStore(cfg,session=False) as s:
        assert len(events(s))==1 and not s.c.execute('select 1 from samples').fetchone()


def test_forecast_failure_does_not_block_price(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path);m,r=markets[0];cfg=replace(cfg,duration_seconds=1)
    now=[AT];ticks=[0.];seq=iter(('0.49','0.71'))
    class Socket:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def send(self,raw):pass
        def recv(self,timeout):
            time.sleep(.001);ticks[0]+=.05;now[0]+=50
            price=next(seq,None)
            if price is None:return 'PONG'
            return canonical(dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens']['YES'],
                                  timestamp=str(now[0]),best_bid=price,best_ask=price))
    def fetch(issue):return dict(receipts={},status='ACCESS_DENIED',terminal=True,bytes=0)
    result=catalogue.observe_catalogue(cfg,connect=lambda *a,**k:Socket(),
        discover=lambda city:([m],r,[]) if city=='new-york' else ([],{},[]),lamp_fetch=fetch,
        clock=lambda:now[0],monotonic=lambda:ticks[0])
    assert result['price_events']==1 and result['state']=='COMPLETED_WINDOW'
    assert result['lamp'][us_lamp.KEY]['blocked']


def test_archive_retains_forecast_evidence_and_restores_history(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path);m,r=markets[0]
    with catalogue.CatalogueStore(cfg,session=False) as s:
        s.bind(m,r)
        for i in range(12):s.record_lamp(receipt(m['station'],46 if i==0 else 48,AT+i),m,AT+i)
        key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p=preview(s,key);assert claim(s,p,AT+20);finish(s,p,{'status':'SENT','message_id':3},AT+21)
        original=[tuple(r) for r in s.c.execute('select id,payload from events')]
        s.reject({'synthetic_day':5},'TEST',AT+5*86400000,invalidate_state=False)
    db=tmp_path/'data/monitor.sqlite3';part=tmp_path/'part'
    prepare(tmp_path/'data',part,expected_sha=digest(db),compact_price_inputs=True,disk_index=True,retire_at_ms=AT+5*86400000)
    with closing(sqlite3.connect(part/'candidate.sqlite3')) as c:
        assert not c.execute("select 1 from meta where key like 'lamp_alert_v1:%' or key like 'lamp_context_v1:%'").fetchone()
        assert not c.execute("select 1 from latest where kind in ('lamp','lamp_alert')").fetchone()
    restore_history(part/'candidate.sqlite3',part,tmp_path/'restored')
    with closing(sqlite3.connect(tmp_path/'restored/restored.sqlite3')) as c:
        assert original==[tuple(r) for r in c.execute('select id,payload from events')]
        assert c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not c.execute('pragma foreign_key_check').fetchall()


def test_accepted_code_can_read_write_after_disabling_lamp(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path);m,r=markets[0]
    with catalogue.CatalogueStore(cfg,session=False) as s:
        s.bind(m,r);s.record_lamp(receipt(m['station']),m,AT)
        s.record_lamp(receipt(m['station'],48,AT+1),m,AT+1)
        key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p=preview(s,key);assert claim(s,p,AT+2)
        frozen=[tuple(r) for r in s.c.execute('select id,payload from events')]
        queue=[tuple(r) for r in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")]
    old=tmp_path/'accepted'
    ref='c58cc8cd9a3a83d79075175be015f213e533e2f2'
    for name in subprocess.check_output(['git','ls-tree','-r','--name-only',ref,'src'],text=True).splitlines():
        if name.endswith('.py'):
            dest=old/Path(name).relative_to('src');dest.parent.mkdir(parents=True,exist_ok=True)
            dest.write_bytes(subprocess.check_output(['git','show',ref+':'+name]))
    plan=recovery.Plan(replace(cfg,us_lamp_enabled=False),None,None,continuous=True)
    script="""import json,sys
from holy_weather.catalogue import CatalogueConfig,CatalogueStore,Router
from holy_weather.alert_queue import preview,claim
cfg=CatalogueConfig(**json.loads(sys.argv[1]));at=int(sys.argv[2])
with CatalogueStore(cfg,session=False) as s:
 s.reject({'offline':'old writer after new LAMP'},'TEST',at+3,invalidate_state=False)
 s.connection_started(at+4);m=next(iter(s.markets.values()));router=Router(s)
 for offset,p in ((5,'0.49'),(6,'0.71')):
  router.feed(json.dumps(dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens']['YES'],
   timestamp=str(at+offset),best_bid=p,best_ask=p)),at+offset)
 assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
 assert not s.c.execute('pragma foreign_key_check').fetchall()
"""
    result=subprocess.run([sys.executable,'-B','-c',script,json.dumps(plan.value()['config']),str(AT)],
        env=dict(PATH=os.defpath,PYTHONPATH=str(old),PYTHONDONTWRITEBYTECODE='1'),capture_output=True,text=True,timeout=15)
    assert result.returncode==0,result.stderr
    with catalogue.CatalogueStore(cfg,session=False) as s:
        assert frozen==[tuple(r) for r in s.c.execute("select id,payload from events where type='LAMP_ALERT'")]
        assert queue==[tuple(r) for r in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' and json_extract(value,'$.track')='LAMP_ALERT'")]
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1


def test_explicit_flag_preserves_accepted_plan_and_europe_boundary(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path);off=replace(cfg,us_lamp_enabled=False)
    assert off.rule_hash()==cfg.rule_hash()
    plan=recovery.Plan(off,None,None,continuous=True)
    assert 'us_lamp_enabled' not in plan.value()['config']
    assert recovery.Plan.load(plan.value()).value()==plan.value()
    assert recovery.Plan.load(recovery.Plan(cfg,None,None,continuous=True).value()).config.us_lamp_enabled
    with pytest.raises(ValueError,match='REQUIRES_US'):replace(cfg,region='europe')
