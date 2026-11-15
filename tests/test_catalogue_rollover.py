"""Synthetic dates/transport, real verifier, SQLite and queue: no live sends."""
from collections import deque
from dataclasses import replace
from datetime import datetime,timedelta,timezone
from zoneinfo import ZoneInfo
import json
import sqlite3
import pytest
from holy_weather.catalogue import (CatalogueStore,DayDiscovery,Router,observe_catalogue,
    discover_city,local_days,PREFIX,DAY_PREFIX)
from holy_weather.core import Config,EUROPE_CITIES,canonical
from holy_weather.discovery import select
from holy_weather.alert_queue import preview,claim,finish
from holy_weather.telegram import Sender
from holy_weather.storage import StorageStop
from test_catalogue import setup,feed
from test_catalogue_recovery import message
from test_europe import european


def ms(s):return int(datetime.fromisoformat(s).timestamp()*1000)


def doc_for(gamma,city,at):
    dt=datetime.fromtimestamp(at/1000,timezone.utc)
    day=Config(city=city).day(dt);d=datetime.fromisoformat(day)
    month=('january','february','march','april','may','june','july','august','september','october','november','december')[d.month-1]
    doc=json.loads(json.dumps(european(gamma,city)).replace('november-14-2026',f'{month}-{d.day}-{d.year}').replace("14 Nov '26",f"{d.day} {month[:3].title()} '{d.year%100:02d}"))
    n=d.toordinal()*100+list(EUROPE_CITIES).index(city)
    doc[0]['markets'][0].update(conditionId='0x'+f'{n:064x}',clobTokenIds=json.dumps([str(2*n),str(2*n+1)]))
    return doc


def discovered(gamma,city,at):
    doc=doc_for(gamma,city,at)
    market=select(doc,Config(city=city,bucket='13°C'),now=datetime.fromtimestamp(at/1000,timezone.utc))
    return [market],dict(received_at=datetime.fromtimestamp(at/1000,timezone.utc).isoformat(),raw_body=json.dumps(doc)),[]


@pytest.mark.parametrize('city',EUROPE_CITIES)
@pytest.mark.parametrize('day',['2026-03-29','2026-10-25','2026-11-15'])
def test_each_city_midnight_dst_revalidates_and_expires(gamma,tmp_path,city,day):
    cfg,_=setup(gamma,tmp_path,1)
    boundary=int(datetime.fromisoformat(day).replace(tzinfo=ZoneInfo(EUROPE_CITIES[city][2])).timestamp()*1000)
    at=[boundary-1000];calls=[]
    def discover(c):
        calls.append((c,local_days(at[0])[c]))
        return discovered(gamma,c,at[0]) if c==city else ([],{},[])
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh()
        old=next(iter(s.markets.values()));s.connection_started(at[0]);r=Router(s,d.selected)
        feed(r,old,'0.49',at[0]+1);feed(r,old,'0.51',at[0]+2)
        key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p=preview(s,key);before=s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]
        at[0]=boundary;assert d.due();d.refresh()
        assert not claim(s,p,at[0])
        assert json.loads(s.c.execute('select value from meta where key=?',(key,)).fetchone()[0])['status']=='EXPIRED'
        new=next(iter(s.markets.values()));assert new['date']==day and new['tokens']!=old['tokens']
        s.connection_started(at[0]);r=Router(s,d.selected)
        feed(r,new,'0.81',at[0]+1) # high first sample is not a crossing
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==before
        feed(r,new,'0.49',at[0]+2);feed(r,new,'0.61',at[0]+3)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==before+1
        assert s.c.execute('select count(*) from meta where key like ?',(PREFIX+'%',)).fetchone()[0]==2
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()


def test_live_loop_rolls_paris_while_london_keeps_date_and_no_old_send(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path,1);start=ms('2026-11-14T22:59:59+00:00');ticks=[0.0];sent=[];connections=[];calls=[]
    def now():return start+int(ticks[0]*1000)
    def discover(city):
        calls.append((city,local_days(now())[city]))
        return discovered(gamma,city,now()) if city in ('london','paris') else ([],{},[])
    class WS:
        def __enter__(self):connections.append(1);return self
        def __exit__(self,*a):pass
        def send(self,raw):
            if raw!='PING':sent.append((now(),json.loads(raw)))
        def recv(self,timeout):ticks[0]+=.25;return 'PONG'
    r=observe_catalogue(replace(cfg,duration_seconds=3),discover=discover,connect=lambda *a,**k:WS(),clock=now,monotonic=lambda:ticks[0])
    assert len(connections)==2 and r['state']=='COMPLETED_WINDOW'
    assert [day for c,day in calls if c=='paris']==['2026-11-14','2026-11-15']
    assert [day for c,day in calls if c=='london']==['2026-11-14']
    old=discovered(gamma,'paris',start)[0][0]['tokens'].values()
    assert not set(old)&set(sent[-1][1]['assets_ids'])
    assert r['price_events']==0 and r['deliveries']=={}


def test_restart_preserves_receipts_fired_levels_and_rechecks_rules(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path,1);at=[ms('2026-11-15T00:00:01+00:00')]
    def discover(c):return discovered(gamma,c,at[0]) if c=='london' else ([],{},[])
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh();m=next(iter(s.markets.values()))
        s.connection_started(at[0]);r=Router(s,d.selected)
        feed(r,m,'0.49',at[0]+1);feed(r,m,'0.61',at[0]+2)
        key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p=preview(s,key);assert claim(s,p,at[0]+3);finish(s,p,{'status':'SENT','message_id':123},at[0]+4)
    at[0]+=10
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh();s.connection_started(at[0]);r=Router(s,d.selected)
        feed(r,m,'0.81',at[0]+1);feed(r,m,'0.49',at[0]+2);feed(r,m,'0.61',at[0]+3)
        Sender('fake',lambda *a:pytest.fail('duplicate send')).tick(s,at[0]+4)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
        assert not claim(s,p,at[0]+4)
        assert s.c.execute('pragma user_version').fetchone()[0]==1


def test_registry_reuses_capacity_without_deleting_history(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path);start=ms('2026-11-14T12:00:00+00:00')
    for day in range(2):
        at=start+day*86400000
        with CatalogueStore(cfg) as s:
            s.retire(at)
            for city in EUROPE_CITIES:
                markets,receipt,_=discovered(gamma,city,at);base=markets[0]
                for bucket in range(16):
                    n=int(base['condition_id'],16)*16+bucket
                    s.bind(dict(base,condition_id='0x'+f'{n:064x}',tokens={'YES':str(2*n),'NO':str(2*n+1)},bucket=f'{bucket}°C'),receipt)
            assert len(s.markets)==176
    with CatalogueStore(cfg) as s:
        assert len(s.markets)==176 and len(s.tokens)==352
        assert s.c.execute('select count(*) from meta where key like ?',(PREFIX+'%',)).fetchone()[0]==352
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert s.usage()<8*1024*1024


def test_retry_budget_survives_restart_and_unknown_is_not_repeated(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path,1);at=[ms('2026-11-15T12:00:00+00:00')];calls=[]
    def discover(c):calls.append(c);return [],{},[]
    for n in range(4):
        with CatalogueStore(cfg) as s:
            d=DayDiscovery(s,discover,lambda:at[0]);d.refresh()
        at[0]+=300000
    assert calls.count('london')==3
    with CatalogueStore(cfg) as s:
        s.set_meta(DAY_PREFIX+'london',dict(day='2026-11-15',status='IN_PROGRESS',attempts=1))
        d=DayDiscovery(s,lambda c:pytest.fail('unknown repeated'),lambda:at[0]);d.refresh()
        assert d.results['london']['reason']=='DISCOVERY_IN_PROGRESS'


def test_denial_and_clock_regression_do_not_retry(gamma,tmp_path):
    from urllib.error import HTTPError
    cfg,_=setup(gamma,tmp_path,1);at=[ms('2026-11-15T12:00:00+00:00')];calls=[]
    def discover(c):calls.append(c);raise HTTPError('https://example.invalid',403,'denied',{},None)
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh()
        assert not d.retry
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh();assert len(calls)==11
        at[0]-=86400000;assert d.due();d.refresh()
        assert len(calls)==11 and d.results['london']['reason']=='LOCAL_DATE_REGRESSION'


@pytest.mark.parametrize('bad',['station','rules','tokens','date'])
def test_new_day_real_gamma_verifier_rejects_invalid_contract(gamma,tmp_path,bad):
    at=ms('2026-11-15T00:00:01+00:00');doc=doc_for(gamma,'london',at);m=doc[0]['markets'][0]
    if bad=='station':m['resolutionSource']=m['resolutionSource'].replace('EGLC','EGLL')
    if bad=='rules':m['description']+=' Changed settlement rule.'
    if bad=='tokens':m['clobTokenIds']='["1","1"]'
    if bad=='date':m['description']=m['description'].replace("15 Nov '26","14 Nov '26")
    class Response:
        status=200
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def read(self,n):return json.dumps(doc).encode()
    markets,receipt,skipped=discover_city('london',clock=lambda:datetime.fromtimestamp(at/1000,timezone.utc),fetch=lambda *a,**k:Response())
    assert not markets and len(skipped)==1 and receipt['raw_body']==json.dumps(doc)
    cfg,_=setup(gamma,tmp_path,1)
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,lambda c:(markets,receipt,skipped) if c=='london' else ([],{},[]),lambda:at)
        d.refresh()
        assert 'london' not in d.retry and not d.selected
        assert json.loads(s.c.execute('select value from meta where key=?',(DAY_PREFIX+'london',)).fetchone()[0])['status']=='BLOCKED'


def test_publication_delayed_then_recovers_without_resetting_budget(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path,1);at=[ms('2026-11-15T00:00:01+00:00')];calls=[]
    def discover(c):
        calls.append(c)
        if c=='london' and calls.count(c)>1:return discovered(gamma,c,at[0])
        return [],{},[]
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh();assert not d.selected
    at[0]+=299999
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh();assert calls.count('london')==1
        at[0]+=1;assert d.due();d.refresh()
        assert calls.count('london')==2 and len(d.selected)==1
        s.connection_started(at[0]);r=Router(s,d.selected);m=next(iter(s.markets.values()))
        feed(r,m,'0.81',at[0]+1)
        assert not s.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()


def test_historical_identity_and_token_collision_not_erased_by_retire(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path,1);at=ms('2026-11-14T12:00:00+00:00')
    old,r,_=discovered(gamma,'london',at);new,nr,_=discovered(gamma,'london',at+86400000)
    with CatalogueStore(cfg) as s:
        s.bind(old[0],r);s.retire(at+86400000)
        with pytest.raises(StorageStop,match='COLLISION'):s.bind(dict(new[0],tokens=old[0]['tokens']),nr)
        with pytest.raises(StorageStop,match='DRIFT'):s.bind(dict(new[0],condition_id=old[0]['condition_id']),nr)
        assert not s.markets
        assert s.c.execute('select count(*) from meta where key like ?',(PREFIX+'%',)).fetchone()[0]==1


def test_sqlite_fault_during_new_day_keeps_old_history_and_unknown_attempt(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path,1);at=[ms('2026-11-14T23:59:59+00:00')]
    def discover(c):return discovered(gamma,c,at[0]) if c=='london' else ([],{},[])
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh()
        old=[tuple(r) for r in s.c.execute('select * from inputs')]
        s.c.execute("CREATE TEMP TRIGGER failure BEFORE INSERT ON inputs BEGIN SELECT RAISE(ABORT,'synthetic SQLite fault'); END")
        at[0]+=1000
        with pytest.raises(sqlite3.IntegrityError):d.refresh()
        assert [tuple(r) for r in s.c.execute('select * from inputs')]==old
        assert json.loads(s.c.execute('select value from meta where key=?',(DAY_PREFIX+'london',)).fetchone()[0])['status']=='IN_PROGRESS'
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh()
        assert d.results['london']['reason']=='DISCOVERY_IN_PROGRESS'
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
        assert not s.c.execute('pragma foreign_key_check').fetchall()


def test_market_ends_before_midnight_runner_waits_for_next_day(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path,1);start=ms('2026-11-14T23:59:59+00:00');ticks=[0.0];subscriptions=[]
    def now():return start+int(ticks[0]*1000)
    old=discovered(gamma,'london',start)[0][0]
    class WS:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,raw):
            if raw!='PING':subscriptions.append(json.loads(raw))
        def recv(self,timeout):
            ticks[0]+=.1
            if ticks[0]<1:return json.dumps({'event_type':'market_resolved','market':old['condition_id']})
            return 'PONG'
    def wait(d):ticks[0]+=d;return False
    r=observe_catalogue(replace(cfg,duration_seconds=3),discover=lambda c:discovered(gamma,c,now()) if c=='london' else ([],{},[]),
                        connect=lambda *a,**k:WS(),clock=now,monotonic=lambda:ticks[0],wait=wait)
    fresh=discovered(gamma,'london',start+2000)[0][0]
    assert set(subscriptions[-1]['assets_ids'])==set(fresh['tokens'].values())
    assert r['state']=='COMPLETED_WINDOW' and r['price_events']==0


def test_gamma_not_published_is_retryable_and_retains_receipt(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path,1);at=ms('2026-11-15T00:00:01+00:00')
    class Response:
        status=200
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def read(self,n):return b'[]'
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,lambda c:discover_city(c,clock=lambda:datetime.fromtimestamp(at/1000,timezone.utc),fetch=lambda *a,**k:Response()),lambda:at)
        d.refresh();assert len(d.retry)==11 and not d.selected
        state=json.loads(s.c.execute('select value from meta where key=?',(DAY_PREFIX+'london',)).fetchone()[0])
        assert state['skipped']==[{'reason':'EXACT_EVENT_NOT_PUBLISHED'}]
        assert s.c.execute('select 1 from inputs where id=?',(state['receipt_input_id'],)).fetchone()


@pytest.mark.parametrize('start,end',[('2026-03-29T00:59:59+00:00','2026-03-29T01:00:01+00:00'),('2026-10-25T00:59:59+00:00','2026-10-25T01:00:01+00:00')])
def test_dst_clock_jump_same_date_does_not_rediscover(gamma,tmp_path,start,end):
    cfg,_=setup(gamma,tmp_path,1);at=[ms(start)];calls=[]
    def discover(c):calls.append(c);return discovered(gamma,c,at[0])
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh()
        at[0]=ms(end);assert not d.due();d.refresh()
        assert len(calls)==11


def test_delayed_old_frame_cannot_create_next_day_event(gamma,tmp_path):
    cfg,_=setup(gamma,tmp_path,1);at=[ms('2026-11-14T23:59:59+00:00')]
    def discover(c):return discovered(gamma,c,at[0]) if c=='london' else ([],{},[])
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh();old=next(iter(s.markets.values()))
        s.connection_started(at[0]);r=Router(s,d.selected);feed(r,old,'0.49',at[0]+1)
        at[0]+=1000;d.refresh();s.connection_started(at[0]);r=Router(s,d.selected)
        feed(r,old,'0.81',at[0]+1)
        assert r.counters['unsubscribed_messages']==1
        assert not s.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()


@pytest.mark.parametrize('status',['PENDING','SENT','UNKNOWN'])
def test_old_day_receipt_survives_restart_without_send(gamma,tmp_path,status):
    cfg,_=setup(gamma,tmp_path,1);at=[ms('2026-11-14T23:59:59+00:00')]
    def discover(c):return discovered(gamma,c,at[0]) if c=='london' else ([],{},[])
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh();m=next(iter(s.markets.values()))
        s.connection_started(at[0]);r=Router(s,d.selected)
        feed(r,m,'0.49',at[0]+1);feed(r,m,'0.51',at[0]+2)
        key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p=preview(s,key)
        if status!='PENDING':
            assert claim(s,p,at[0]+3)
            if status=='SENT':finish(s,p,{'status':'SENT','message_id':456},at[0]+4)
        original_event=tuple(s.c.execute('select * from events where id=?',(p['event_id'],)).fetchone())
    at[0]+=2000
    with CatalogueStore(cfg) as s:
        d=DayDiscovery(s,discover,lambda:at[0]);d.refresh()
        Sender('fake',lambda *a:pytest.fail('old day send')).tick(s,at[0])
        assert not claim(s,p,at[0])
        receipt=json.loads(s.c.execute('select value from meta where key=?',(key,)).fetchone()[0])
        assert receipt['status']==('EXPIRED' if status=='PENDING' else status)
        assert tuple(s.c.execute('select * from events where id=?',(p['event_id'],)).fetchone())==original_event
        assert not s.c.execute('pragma foreign_key_check').fetchall()
