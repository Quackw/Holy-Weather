"""Hourly US temperature policy: original-format fixtures, no real HTTP/send."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import pytest

from holy_weather import alert_queue, catalogue, catalogue_metar, city_peak, lamp, lamp_hourly, recovery, weather
from holy_weather.core import US_CITIES, canonical, sha
from test_lamp_send_window import binding, config, instant, metar, seed
from test_us_lamp import events, native
from test_catalogue_metar import observations

DAY='2026-11-14'
HOUR=city_peak.HOUR


def guidance(market, at, *, peak=47, changed=None, warming_at=None):
    issue=(at-1800000)//HOUR*HOUR+1800000
    first=datetime.fromtimestamp(issue/1000,timezone.utc).replace(minute=0)+timedelta(hours=1)
    peak_at=instant(next(k for k,v in US_CITIES.items() if v[1]==market['station']),market['date'],14)
    values=[]
    for n in range(25):
        valid=int((first+timedelta(hours=n)).timestamp()*1000)
        temp=peak if valid==peak_at else 46
        if warming_at and valid<warming_at:temp=41
        if changed and valid in changed:temp=changed[valid]
        values.append(temp)
    lines=native(market['station'],issue=issue).decode().splitlines()
    lines[2]=' TMP '+''.join(f'{t:3d}' for t in values)+' '
    body=('\n'.join(lines)+'\n').encode()
    return dict(source=lamp.nomads_source(issue),requested_ms=at-1,received_ms=at,
                status='OK',raw_body=body.decode(),sha256=sha(body),file_sha256=sha(body))


def hourly(city,root):return replace(config(city,root),us_lamp_notice_mode='warming_hourly')


def update(store, market, at, *, current=5, **kwargs):
    w,_=weather.update({},observations(market,at,current=current),market,at)
    store.set_meta(catalogue_metar.PREFIX+market['station'],dict(w,day=market['date']))
    store.record_lamp(guidance(market,at,**kwargs),market,at)


def initial(gamma,root,city='chicago',day=DAY):
    market,receipt=binding(gamma,city,day);cfg=hourly(city,root)
    store=catalogue.CatalogueStore(cfg,session=False);store.bind(market,receipt)
    seed(store,market,instant(city,day,14),city=city,day=day)
    at=instant(city,day,8,40);update(store,market,at)
    assert len(events(store))==1
    return store,market,cfg,at


@pytest.mark.parametrize('city',US_CITIES)
@pytest.mark.parametrize('day',[DAY,'2026-03-08','2026-11-01'])
def test_all_ten_daytime_warming_start_without_three_hour_peak_delay(gamma,tmp_path,city,day):
    store,m,_,at=initial(gamma,tmp_path,city,day)
    with store:
        event,=events(store)
        assert event['reason']=='WARMING_START'
        assert event['city_outlook']['peak_ms']-at>3*HOUR
        assert 0<event['city_outlook']['warming_start_ms']-at<=HOUR
        assert event['market_url']==m['url'] and event['notification_policy']==lamp_hourly.POLICY
        assert store.cfg.lamp_peak_change_f=='2'  # Legacy detector stays intact.


def test_waits_for_start_of_warming_and_never_sends_at_night(gamma,tmp_path):
    city='chicago';m,r=binding(gamma,city,DAY)
    with catalogue.CatalogueStore(hourly(city,tmp_path),session=False) as s:
        s.bind(m,r);seed(s,m,instant(city,DAY,14),city=city,day=DAY)
        for hour in (1,7,8,9):
            update(s,m,instant(city,DAY,hour,40),warming_at=instant(city,DAY,11))
            assert not events(s)
        at=instant(city,DAY,10)
        metar(s,m,at)
        with s.transaction():city_peak.record(s,[m],s.lamp_state(m),at)
        assert len(events(s))==1 and events(s)[0]['city_outlook']['warming_start_ms']==instant(city,DAY,11)


def test_any_future_temperature_change_and_metar_change_one_hourly_city_card(gamma,tmp_path):
    s,m,cfg,at=initial(gamma,tmp_path)
    changed={instant('chicago',DAY,17):45}
    with s:
        # New issue with identical temperatures and lost past hours isn't a change.
        update(s,m,at+HOUR);assert len(events(s))==1
        update(s,m,at+2*HOUR,changed=changed)
        assert events(s)[-1]['reason']=='FORECAST_TEMPERATURE_CHANGE'
        assert events(s)[-1]['changed_forecast_hours']==list(changed)
        assert events(s)[-1]['city_outlook']['future_peak_f']=='47'
        update(s,m,at+2*HOUR+60000,current=6,changed=changed)
        assert len(events(s))==2  # Combine within this hour; no per-bucket sends.
        update(s,m,at+3*HOUR,current=6,changed=changed)
        assert len(events(s))==3 and events(s)[-1]['reason']=='METAR_TEMPERATURE_CHANGE'
        frozen=[tuple(r) for r in s.c.execute('select id,payload from events order by id')]
    with catalogue.CatalogueStore(cfg,session=False) as again:
        update(again,m,at+3*HOUR+1,current=6,changed=changed)
        assert frozen==[tuple(r) for r in again.c.execute('select id,payload from events order by id')]
        update(again,m,at+4*HOUR,current=6,changed=changed)
        assert len(events(again))==3


def test_cooling_updates_use_original_source_expiry_and_twenty_oclock_cutoff(gamma,tmp_path):
    s,m,cfg,at=initial(gamma,tmp_path)
    with s:
        later=instant('chicago',DAY,15,40)
        # A long missing review is a baseline, not a backlog notification.
        update(s,m,later,current=8,peak=44)
        assert len(events(s))==1
        changed={instant('chicago',DAY,h):45 for h in range(17,24)}
        update(s,m,later+HOUR,current=8,peak=44,changed=changed)
        assert len(events(s))==2
        event=events(s)[-1]
        value=json.loads(s.c.execute("select value from meta where key like 'alert_delivery_v1:%' order by rowid desc limit 1").fetchone()[0])
        assert event['city_outlook']['warming_f'].startswith('-')
        assert value['expires_ms']>later+HOUR and 'Дополнительного прогрева не ожидается' in value['html']
        update(s,m,instant('chicago',DAY,20,40),current=6)
        assert len(events(s))==2


def test_old_initial_card_and_sent_unknown_are_not_rearmed(gamma,tmp_path):
    s,m,cfg,at=initial(gamma,tmp_path)
    with s:
        key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p=alert_queue.preview(s,key);assert alert_queue.claim(s,p,at+1)
        alert_queue.finish(s,p,dict(status='SENT',message_id=11),at+2)
        update(s,m,at+HOUR,peak=46)
        key2=s.c.execute("select key from meta where key like 'alert_delivery_v1:%' and key!=?",(key,)).fetchone()[0]
        p2=alert_queue.preview(s,key2);assert alert_queue.claim(s,p2,at+HOUR+1)
        frozen=[tuple(r) for r in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' order by key")]
    with catalogue.CatalogueStore(cfg,session=False) as again:
        assert recovery.prepare_resume(again,{'isolated':'hourly'},at+HOUR+2,carry_pending_lamp=True)
        assert not alert_queue.claim(again,p,at+HOUR+3) and not alert_queue.claim(again,p2,at+HOUR+3)
        assert frozen==[tuple(r) for r in again.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' order by key")]
        assert again.c.execute('pragma integrity_check').fetchone()[0]=='ok'


def test_event_queue_and_hourly_projection_commit_atomically(gamma,tmp_path):
    s,m,_,at=initial(gamma,tmp_path)
    with s:
        before=catalogue_metar.load(s,lamp_hourly.PREFIX+m['station'])
        s.c.execute("create temp trigger fault before insert on meta when new.key like 'alert_delivery_v1:%' begin select raise(abort,'hourly admission fault'); end")
        with pytest.raises(sqlite3.IntegrityError,match='hourly admission fault'):
            update(s,m,at+HOUR,peak=46)
        assert len(events(s))==1 and catalogue_metar.load(s,lamp_hourly.PREFIX+m['station'])==before


def test_fresh_unclaimed_lamp_crosses_owner_boundary_but_unknown_never_retries(gamma,tmp_path):
    s,m,cfg,at=initial(gamma,tmp_path)
    with s:
        key,raw=s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'").fetchone()
        assert not recovery.prepare_resume(s,{'isolated':'owner-handoff'},at+1,carry_pending_lamp=True)
        assert s.c.execute('select value from meta where key=?',(key,)).fetchone()[0]==raw
    with catalogue.CatalogueStore(cfg,session=False) as s:
        assert not recovery.prepare_resume(s,{'isolated':'owner-handoff'},at+2,carry_pending_lamp=True)
        p=alert_queue.preview(s,key);assert alert_queue.claim(s,p,at+3)
        # claim is durable UNKNOWN before transport. A crash cannot resend it.
    with catalogue.CatalogueStore(cfg,session=False) as s:
        assert recovery.prepare_resume(s,{'isolated':'owner-handoff'},at+4,carry_pending_lamp=True)
        assert not alert_queue.claim(s,p,at+5)


def test_new_local_day_establishes_new_baseline_and_preserves_previous_review(gamma,tmp_path):
    import zlib
    s,m,cfg,at=initial(gamma,tmp_path)
    with s:
        before=catalogue_metar.load(s,lamp_hourly.PREFIX+m['station'])
        tomorrow,r=binding(gamma,'chicago','2026-11-15');s.bind(tomorrow,r)
        now=instant('chicago','2026-11-15',8,40)
        update(s,tomorrow,now)
        assert len(events(s))==1
        current=catalogue_metar.load(s,lamp_hourly.PREFIX+m['station'])
        assert current['status']=='BASELINE' and not current.get('initial_event_id')
        proofs=[json.loads(zlib.decompress(row[0])) for row in s.c.execute("select raw from inputs where length(identity)=64")]
        assert any(p.get('type')=='US_LAMP_HOURLY_DAY' and p['state']==before for p in proofs)
        update(s,tomorrow,now+HOUR)
        assert len(events(s))==2 and events(s)[-1]['at_ms']==now+HOUR


def test_previous_main_reads_current_data_after_new_hourly_records(gamma,tmp_path):
    """Code rollback opens today's data; never restore an older SQLite snapshot."""
    import os
    from pathlib import Path
    import subprocess
    import sys
    s,m,cfg,at=initial(gamma,tmp_path/'data')
    with s:
        update(s,m,at+HOUR,peak=46)
        rows=s.c.execute("select key from meta where key like 'alert_delivery_v1:%' order by rowid").fetchall()
        for n,(key,) in enumerate(rows):
            p=alert_queue.preview(s,key)
            if alert_queue.claim(s,p,at+1 if not n else at+HOUR+n+1) and n:
                alert_queue.finish(s,p,dict(status='SENT',message_id=99),at+HOUR+n+2)
        frozen=[tuple(r) for r in s.c.execute('select id,payload from events order by id')]
        receipts=[tuple(r) for r in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' order by key")]
    root=Path(__file__).resolve().parents[1];target=tmp_path/'previous';base='c32e3ba8d58b8b1b95bd350512a679fbe65a5846'
    for name in subprocess.check_output(['git','ls-tree','-r','--name-only',base,'src'],cwd=root,text=True).splitlines():
        if name.endswith('.py'):
            dest=target/Path(name).relative_to('src');dest.parent.mkdir(parents=True,exist_ok=True)
            dest.write_bytes(subprocess.check_output(['git','show',base+':'+name],cwd=root))
    plan=recovery.Plan(replace(cfg,us_weather_continuity_enabled=False,us_lamp_notice_mode='pre_peak'),None,None,continuous=True)
    script="""import json,sys
from holy_weather.catalogue import CatalogueConfig,CatalogueStore
from holy_weather.alert_queue import preview,claim
cfg=CatalogueConfig(**json.loads(sys.argv[1]));at=int(sys.argv[2])
with CatalogueStore(cfg,session=False) as s:
 s.reject({'isolated':'new input after rollback'},'TEST',at,invalidate_state=False)
 for key,raw in s.c.execute(\"select key,value from meta where key like 'alert_delivery_v1:%'\"):
  if json.loads(raw)['status'] in ('SENT','UNKNOWN'):assert not claim(s,preview(s,key),at)
 assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
 assert not s.c.execute('pragma foreign_key_check').fetchall()
"""
    done=subprocess.run([sys.executable,'-B','-c',script,json.dumps(plan.value()['config']),str(at+HOUR+100)],
                        env=dict(PATH=os.defpath,PYTHONPATH=str(target),PYTHONDONTWRITEBYTECODE='1'),
                        capture_output=True,text=True,timeout=20)
    assert done.returncode==0,done.stderr
    with catalogue.CatalogueStore(cfg,session=False) as s:
        assert frozen==[tuple(r) for r in s.c.execute('select id,payload from events order by id')]
        assert receipts==[tuple(r) for r in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' order by key")]
