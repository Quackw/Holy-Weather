"""New storage policy on private fixtures. No server, service or Telegram HTTP."""
from dataclasses import replace
from datetime import datetime,timezone,timedelta
import json
from pathlib import Path
import importlib.util
import os
import subprocess
import sys
import sqlite3
from types import SimpleNamespace
import pytest

from holy_weather import history as h,history_files as f,recovery as r
from holy_weather.alert_queue import preview,claim,finish
from holy_weather.catalogue import CatalogueStore,Router,observe_catalogue
from holy_weather.core import canonical,sha
from holy_weather.storage import Store,StorageStop
from test_history_checkpoints import seed
from test_price_alert import AT
from test_price_recovery import seed as seed_plan
from test_catalogue import setup,feed


def ready(cfg,market,make_raw,tmp_path):
    cfg=seed(cfg,market,make_raw);db=Path(cfg.data_dir)/'monitor.sqlite3';part=tmp_path/'part-001'
    m=h.prepare(cfg.data_dir,part,expected_sha=h.digest(db),disk_index=True,compact_price_inputs=True)
    h.adopt(cfg.data_dir,part,cleanup=True)
    return cfg,db,part,m


def next_part(cfg,db,tmp_path):
    with Store(cfg) as store:
        for n in range(2):store.reject({'fixture':'next','n':n},'TEST',AT+3600000+n,invalidate_state=False)
    part=tmp_path/'part-002'
    h.prepare(cfg.data_dir,part,expected_sha=h.digest(db),disk_index=True,compact_price_inputs=True)
    h.adopt(cfg.data_dir,part,cleanup=True)
    return part


def test_file_index_exceeds_legacy_memory_bound_and_preserves_duplicate_semantics(cfg,market,make_raw,tmp_path,monkeypatch):
    cfg=seed(cfg,market,make_raw);db=Path(cfg.data_dir)/'monitor.sqlite3';before=h.digest(db)
    monkeypatch.setattr(h,'MAX_INDEX_BYTES',32)
    part=tmp_path/'part-001'
    m=h.prepare(cfg.data_dir,part,expected_sha=before,disk_index=True,compact_price_inputs=True)
    assert m['file_index']['count']>90 and h.digest(db)==before
    h.adopt(cfg.data_dir,part)
    with Store(cfg) as store:
        assert store.history_seen.buckets=={} and store.history_seen.file is not None
        assert store.history_seen.contains(sha(make_raw('0.53',AT+50,bid='0.52')))
        assert not store.history_seen.contains(sha('never received'))
        assert not store.c.execute('select 1 from meta where key like ?',(h.INDEX+'%',)).fetchone()
        assert store.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
        assert json.loads(store.c.execute("select value from meta where key like 'alert_delivery_v1:%'").fetchone()[0])['status']=='SENT'


def test_migration_does_not_allocate_then_free_the_old_inline_index(cfg,market,make_raw,tmp_path):
    cfg=seed(cfg,market,make_raw)
    with Store(cfg,session=False) as store:
        with store.transaction():h.Seen(store.c).add(store.c,(sha(['fixture historical input',n]) for n in range(30000)))
    db=Path(cfg.data_dir)/'monitor.sqlite3';before=h.digest(db);part=tmp_path/'part-001'
    m=h.prepare(cfg.data_dir,part,expected_sha=before,disk_index=True,compact_price_inputs=True)
    assert m['candidate_bytes']<256*1024 and m['candidate_bytes']<db.stat().st_size//4
    with sqlite3.connect(part/'candidate.sqlite3') as c:
        assert c.execute('pragma freelist_count').fetchone()[0]<4
        assert h.Seen(c).contains(sha(['fixture historical input',20000]))
    assert h.digest(db)==before


@pytest.mark.parametrize('fault',['truncate','content','symlink','reference'])
def test_file_index_corruption_refuses_without_writes(cfg,market,make_raw,tmp_path,fault):
    cfg,db,part,m=ready(cfg,market,make_raw,tmp_path)
    path=part/'seen.sha256';ref=m['file_index']
    if fault=='truncate':path.write_bytes(path.read_bytes()[:-1])
    elif fault=='content':
        raw=bytearray(path.read_bytes());raw[-1]^=1;path.write_bytes(raw)
    elif fault=='symlink':
        renamed=part/'actual';path.rename(renamed);path.symlink_to(renamed)
    else:ref=dict(ref,count=ref['count']+1)
    before=h.digest(db)
    with pytest.raises(ValueError,match='HISTORY_'):f.FileIndex(ref)
    assert h.digest(db)==before


def test_immutable_history_moves_to_second_disk_and_inverse_follows_locator(cfg,market,make_raw,tmp_path):
    cfg,db,part,m=ready(cfg,market,make_raw,tmp_path)
    cold=tmp_path/'history';cold.mkdir(mode=0o700);policy=f.HistoryStorage(str(cold))
    active=next_part(cfg,db,tmp_path)
    raw=sha(make_raw('0.53',AT+50,bid='0.52'))
    report=f.offload(tmp_path,policy,keep=(active.name,))
    assert report['parts']==['part-001'] and report['status']=='READY'
    assert not (part/'history.sqlite3.gz').exists() and not (part/'seen.sha256').exists()
    assert h.digest(f.payload(part,'history.sqlite3.gz'))==m['archive_sha256']
    with Store(cfg) as store:assert store.history_seen.contains(raw)
    restored=f.materialize_index(part,tmp_path/'restored-index')
    index=f.FileIndex(m['file_index'],index_path=restored['file'])
    assert index.contains(bytes.fromhex(raw));index.close()
    h.restore_history(db,active,tmp_path/'newest')
    result=h.restore_history(tmp_path/'newest/restored.sqlite3',part,tmp_path/'restored')
    assert result['inputs']['rows']==m['source_fingerprint']['inputs']['rows']+2
    again=f.offload(tmp_path,policy,keep=(active.name,))
    assert again['parts']==[] and len(list(cold.iterdir()))==1


@pytest.mark.parametrize('fault',['absent','full'])
def test_unavailable_history_disk_keeps_local_collection_usable(cfg,market,make_raw,tmp_path,monkeypatch,fault):
    cfg,db,part,m=ready(cfg,market,make_raw,tmp_path)
    cold=tmp_path/'history'
    if fault=='full':
        cold.mkdir(mode=0o700)
        monkeypatch.setattr(f.shutil,'disk_usage',lambda _:SimpleNamespace(free=0))
    policy=f.HistoryStorage(str(cold));before=h.digest(db)
    report=f.offload(tmp_path,policy)
    assert report['status']=='SPOOLING' and (part/'history.sqlite3.gz').exists() and h.digest(db)==before
    with Store(cfg) as store:
        store.reject({'fixture':'new record during history outage'},'TEST',AT+2000000,invalidate_state=False)
        assert store.c.execute('pragma quick_check').fetchone()[0]=='ok'


def test_copy_interruption_keeps_source_and_resumes_scratch(cfg,market,make_raw,tmp_path,monkeypatch):
    cfg,db,part,m=ready(cfg,market,make_raw,tmp_path)
    cold=tmp_path/'history';cold.mkdir(mode=0o700);policy=f.HistoryStorage(str(cold))
    active=next_part(cfg,db,tmp_path)
    def fail_once(source,dest):raise OSError('synthetic copy interruption')
    with monkeypatch.context() as context:
        context.setattr(f.os,'replace',fail_once)
        assert f.offload(tmp_path,policy,keep=(active.name,))['status']=='SPOOLING'
    assert (part/'history.sqlite3.gz').exists()
    assert f.offload(tmp_path,policy,keep=(active.name,))['status']=='READY'
    assert h.digest(f.payload(part,'history.sqlite3.gz'))==m['archive_sha256']


def test_crash_after_mirror_commit_retires_only_verified_payloads(cfg,market,make_raw,tmp_path,monkeypatch):
    cfg,db,part,m=ready(cfg,market,make_raw,tmp_path)
    cold=tmp_path/'history';cold.mkdir(mode=0o700);policy=f.HistoryStorage(str(cold))
    active=next_part(cfg,db,tmp_path)
    with monkeypatch.context() as context:
        context.setattr(f,'retire_payloads',lambda *a:(_ for _ in ()).throw(OSError('synthetic crash')))
        assert f.offload(tmp_path,policy,keep=(active.name,))['status']=='SPOOLING'
    assert (part/'MIRRORED.json').is_file() and (part/'seen.sha256').is_file()
    assert f.offload(tmp_path,policy,keep=(active.name,))['status']=='READY'
    assert not (part/'seen.sha256').exists()
    with Store(cfg) as store:assert store.history_seen.file is not None


def test_new_policy_roundtrip_keeps_old_plan_serialization(gamma,tmp_path):
    old,_=seed_plan(gamma,tmp_path);frozen=old.value()
    assert 'history_storage' not in frozen and r.Plan.load(frozen).value()==frozen
    rolling=replace(old,continuous=True,session_day=None,deadline_ms=None)
    plan=replace(rolling,archive_dir=str(tmp_path/'archive'),history_storage=f.HistoryStorage(str(tmp_path/'cold')))
    assert r.Plan.load(plan.value())==plan and old.config.rule_hash()==plan.config.rule_hash()
    with pytest.raises(ValueError,match='CONTINUOUS'):replace(old,history_storage=plan.history_storage)


def test_settled_old_days_leave_hot_database_unknown_stays_and_full_history_restores(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,2)
    cfg=replace(cfg,price_notification_filter=True)
    with CatalogueStore(cfg) as store:
        for market,receipt in markets:store.bind(market,receipt)
        store.connection_started(AT);router=Router(store)
        for n,(market,receipt) in enumerate(markets):
            feed(router,market,'0.49',AT+10*n+1);feed(router,market,'0.71',AT+10*n+2)
            key=store.c.execute("select key from meta where key like 'alert_delivery_v1:%' order by rowid desc limit 1").fetchone()[0]
            card=preview(store,key);assert claim(store,card,AT+10*n+3)
            if n==0:finish(store,card,{'status':'SENT','message_id':101},AT+4)
        rows=[tuple(row) for row in store.c.execute('select * from events order by id')]
        receipt_rows=dict(store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'"))
        store.gap('CLOSED',AT+14)
    db=Path(cfg.data_dir)/'monitor.sqlite3';before=h.digest(db);part=tmp_path/'part-001'
    m=h.prepare(cfg.data_dir,part,expected_sha=before,disk_index=True,compact_price_inputs=True,retire_at_ms=AT+4*86400000)
    first,second=[market for market,receipt in markets]
    assert m['retirement']['tokens']==sorted(first['tokens'].values())
    h.adopt(cfg.data_dir,part)
    with CatalogueStore(cfg,session=False) as store:
        assert store.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
        assert not store.c.execute('select 1 from meta where key=?',('price_catalogue_v1:'+first['condition_id'],)).fetchone()
        assert store.c.execute('select 1 from meta where key=?',('price_catalogue_v1:'+second['condition_id'],)).fetchone()
        assert r.prepare_resume(store,{'fixture':'control'},AT+4*86400000)  # UNKNOWN disables sender.
        with pytest.raises(StorageStop,match='RETIRED_CATALOGUE_TOKEN_REUSE'):store.bind(*markets[0])
        assert json.loads(store.c.execute("select value from meta where key='history_retired_counts_v1'").fetchone()[0])=={
            'price_events':1,'fired_thresholds':4,'deliveries':{'SENT':1},'last_sent_ms':AT+4}
        store.reject({'fixture':'new after retirement'},'TEST',AT+4*86400000+1,invalidate_state=False)
    result=h.restore_history(db,part,tmp_path/'inverse')
    assert result['inputs']['rows']==m['source_fingerprint']['inputs']['rows']+1
    with sqlite3.connect(tmp_path/'inverse/restored.sqlite3') as c:
        assert c.execute('select * from events order by id').fetchall()==rows
        assert dict(c.execute("select key,value from meta where key like 'alert_delivery_v1:%'"))==receipt_rows
        assert c.execute('pragma user_version').fetchone()[0]==1


def test_chained_projection_restores_older_legacy_part_and_preserves_new_rows(cfg,market,make_raw,tmp_path):
    cfg=seed(cfg,market,make_raw);db=Path(cfg.data_dir)/'monitor.sqlite3'
    legacy=tmp_path/'part-001';first=h.prepare(cfg.data_dir,legacy,expected_sha=h.digest(db))
    h.adopt(cfg.data_dir,legacy,cleanup=True)
    manifests=[]
    for n in (2,3):
        with Store(cfg) as s:
            for i in range(10):s.reject({'fixture':n,'value':i},'TEST',AT+n*2000000+i,invalidate_state=False)
            now=s.c.execute('select max(received_ms) from inputs').fetchone()[0]+100
        part=tmp_path/f'part-{n:03d}'
        m=h.prepare(cfg.data_dir,part,expected_sha=h.digest(db),disk_index=True,compact_price_inputs=True,retire_at_ms=now)
        h.adopt(cfg.data_dir,part,cleanup=True);manifests.append(m)
        with sqlite3.connect(db) as c:
            assert not c.execute('select 1 from meta where key like ?',(h.PART+'%',)).fetchone()
            assert len(json.loads(c.execute('select value from meta where key=?',(h.TIP,)).fetchone()[0]))<=6
    h.restore_history(db,tmp_path/'part-003',tmp_path/'newest')
    active=tmp_path/'newest/restored.sqlite3'
    h.restore_history(active,tmp_path/'part-002',tmp_path/'middle')
    result=h.restore_history(tmp_path/'middle/restored.sqlite3',legacy,tmp_path/'oldest')
    assert result['inputs']['rows']==first['source_fingerprint']['inputs']['rows']+20
    path=tmp_path/'part-002/MANIFEST.json';value=json.loads(path.read_text());value['untrusted_change']=True;path.write_text(canonical(value))
    with pytest.raises(ValueError,match='LINEAGE_SHA'):h.restore_history(db,legacy,tmp_path/'bad-chain')


def test_disappeared_second_mount_uses_primary_spool(cfg,market,make_raw,tmp_path,monkeypatch):
    cfg,db,part,m=ready(cfg,market,make_raw,tmp_path)
    mount=tmp_path/'mount';cold=mount/'history';cold.mkdir(parents=True,mode=0o700)
    policy=f.HistoryStorage(str(cold),mountpoint=str(mount))
    monkeypatch.setattr(f.os.path,'ismount',lambda _:False)
    report=f.offload(tmp_path,policy)
    assert report['status']=='SPOOLING' and report['reason']=='HISTORY_DISK_NOT_MOUNTED'
    assert not list(cold.iterdir()) and (part/'history.sqlite3.gz').exists()
    with Store(cfg):pass


def test_render_storage_policy_optional_mount_and_existing_quote_limits(tmp_path):
    path=Path(__file__).resolve().parents[1]/'deploy/render_price.py'
    spec=importlib.util.spec_from_file_location('storage_render',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    out=module.render('/opt/holy-weather/releases/'+'a'*40,tmp_path/'rendered',
                      history_root='/srv/history/holy-weather',history_mountpoint='/srv/history')
    unit=(out/'holy-weather-price@.service').read_text()
    assert 'ReadWritePaths=-/srv/history/holy-weather/%i' in unit and '@HISTORY_' not in unit
    assert 'RequiresMountsFor=/srv/history' not in unit and 'ReadWritePaths=/var/lib/holy-weather/%i' in unit
    for region in ('us','europe'):
        plan=r.Plan.load(json.loads((out/(region+'.json')).read_text()))
        assert plan.history_storage.directory=='/srv/history/holy-weather/'+region
        assert plan.config.storage_bytes==256*f.MiB and plan.config.quote_age_seconds==90
        assert plan.history_storage.spool_bytes==64*f.GiB and plan.history_storage.history_bytes==192*f.GiB


def test_working_storage_pressure_finishes_window_before_hard_limit(gamma,tmp_path,monkeypatch):
    cfg,markets=setup(gamma,tmp_path,1);market,receipt=markets[0]
    ticks=[0.];calls=[]
    original=CatalogueStore.usage
    def usage(store):return 160*f.MiB if calls else original(store)
    class WS:
        def __enter__(self):calls.append(1);return self
        def __exit__(self,*a):pass
        def send(self,raw):pass
        def recv(self,timeout):raise AssertionError('Hard storage pressure should close the window first')
    monkeypatch.setattr(CatalogueStore,'usage',usage)
    result=observe_catalogue(replace(cfg,duration_seconds=20),connect=lambda *a,**kw:WS(),
        discover=lambda city:([market],receipt,[]) if city=='london' else ([],{},[]),
        clock=lambda:AT,monotonic=lambda:ticks[0],rotate_bytes=160*f.MiB)
    assert result['state']=='COMPLETED_WINDOW' and result['rotation_reason']=='STORAGE_CHECKPOINT_DUE'
    assert result['sqlite_integrity']=='ok' and result['price_events']==0 and len(calls)==1


def test_continuous_checkpoints_pass_forty_parts_without_losing_thresholds_or_unknown(gamma,tmp_path,monkeypatch):
    finite,market=seed_plan(gamma,tmp_path)
    archive=tmp_path/'archive'
    plan=replace(finite,continuous=True,session_day=None,deadline_ms=None,archive_dir=str(archive),
                 history_storage=f.HistoryStorage(str(tmp_path/'unavailable-disk')))
    with CatalogueStore(plan.config) as store:
        store.connection_started(AT);router=Router(store)
        feed(router,market,'0.49',AT+1);feed(router,market,'0.71',AT+2)
        key=store.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        assert claim(store,preview(store,key),AT+3)  # Explicit UNKNOWN stays live.
        fired=store.c.execute("select value from meta where key like 'price_alert_v1:%'").fetchone()[0]
    monkeypatch.setattr(r,'ARCHIVE_TRIGGER_BYTES',1)
    sizes=[]
    for n in range(42):
        with CatalogueStore(plan.config,session=False) as store:
            for i in range(2):store.reject({'phase':n,'row':i},'FIXTURE',AT+1800000+n*1000000+i,invalidate_state=False)
        r.checkpoint(plan,lambda _:None)
        sizes.append((Path(plan.config.data_dir)/'monitor.sqlite3').stat().st_size)
    assert len(list(archive.glob('part-*')))==42 and max(sizes)<1024**2
    assert r.preflight(plan) is None
    with CatalogueStore(plan.config,session=False) as store:
        assert store.c.execute('select value from meta where key=?',(key,)).fetchone()[0]
        assert json.loads(store.c.execute('select value from meta where key=?',(key,)).fetchone()[0])['status']=='UNKNOWN'
        assert store.c.execute("select value from meta where key like 'price_alert_v1:%'").fetchone()[0]==fired
        assert store.history_seen.contains(sha({'phase':0,'row':0}))
        assert store.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
    db=Path(plan.config.data_dir)/'monitor.sqlite3';before=h.digest(db)
    with pytest.raises(r.RecoveryStop,match='ARCHIVE_BUDGET'):
        r.checkpoint(replace(plan,history_storage=None),lambda _:None)
    assert h.digest(db)==before


def test_thirty_five_closed_days_keep_hot_receipts_and_state_bounded(gamma,tmp_path):
    cfg,initial=setup(gamma,tmp_path,2);cfg=replace(cfg,price_notification_filter=True)
    archive=tmp_path/'archive';archive.mkdir(mode=0o700)
    first_hash=None;first_market=None;sizes=[]
    for day in range(35):
        now=datetime(2026,11,14,12,tzinfo=timezone.utc)+timedelta(days=day)
        at=int(now.timestamp()*1000)
        with CatalogueStore(cfg,session=False) as store:
            store.retire(at)
            for n,(template,receipt) in enumerate(initial):
                identity=day*2+n+100
                market=dict(template,date=now.date().isoformat(),condition_id='0x'+f'{identity:064x}',
                            tokens={'YES':str(identity*2),'NO':str(identity*2+1)})
                for key in ('url','slug'):
                    if key in market:market[key]=market[key].replace('november-14-2026',now.strftime('%B').lower()+'-'+str(now.day)+'-'+str(now.year))
                receipt={'received_at':now.isoformat(),'raw_body':canonical({'fixture_day':day,'station':market['station']})}
                store.bind(market,receipt)
                store.connection_started(at);router=Router(store)
                feed(router,market,'0.49',at+1);feed(router,market,'0.81',at+2)
                key=store.c.execute("select key from meta where key like 'alert_delivery_v1:%' order by rowid desc limit 1").fetchone()[0]
                card=preview(store,key);assert claim(store,card,at+3)
                finish(store,card,{'status':'SENT','message_id':identity},at+4)
                if first_hash is None:
                    first_market=market
                    first_hash=store.c.execute('select identity from inputs where id=(select min(input_id) from samples)').fetchone()[0]
            for n in range(2):store.reject({'fixture_day':day,'unreferenced':n},'TEST',at+5+n,invalidate_state=False)
            store.gap('FIXTURE_DAY_END',at+7)
        db=Path(cfg.data_dir)/'monitor.sqlite3';part=archive/f'part-{day+1:03d}'
        h.prepare(cfg.data_dir,part,expected_sha=h.digest(db),disk_index=True,compact_price_inputs=True,retire_at_ms=at+10)
        h.adopt(cfg.data_dir,part,cleanup=True);sizes.append(db.stat().st_size)
    with CatalogueStore(cfg,session=False) as store:
        hot=store.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]
        cold=json.loads(store.c.execute("select value from meta where key='history_retired_counts_v1'").fetchone()[0])
        assert hot<=6 and hot+cold['price_events']==70 and cold['deliveries']['SENT']==64
        assert store.c.execute("select count(*) from meta where key like 'price_catalogue_v1:%'").fetchone()[0]<=6
        assert store.c.execute("select count(*) from meta where key like 'price_alert_v1:%'").fetchone()[0]<=12
        assert store.history_seen.contains(first_hash)
        with pytest.raises(StorageStop,match='RETIRED_CATALOGUE_TOKEN_REUSE'):
            store.bind(first_market,{'received_at':now.isoformat()})
        assert hot*5+cold['fired_thresholds']==350
        assert max(sizes)<1024**2


def test_previous_runtime_with_compatible_reader_keeps_new_rows_and_queue(gamma,tmp_path):
    plan,market=seed_plan(gamma,tmp_path)
    with CatalogueStore(plan.config) as store:
        store.connection_started(AT);router=Router(store)
        feed(router,market,'0.49',AT+1);feed(router,market,'0.71',AT+2)
        key=store.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p=preview(store,key);assert claim(store,p,AT+3);finish(store,p,{'status':'SENT','message_id':2},AT+4)
        for n in range(2):store.reject({'fixture':'old','n':n},'TEST',AT+5+n,invalidate_state=False)
        original=[tuple(row) for row in store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' or key like 'price_alert_v1:%' order by key")]
    db=Path(plan.config.data_dir)/'monitor.sqlite3';part=tmp_path/'part-001'
    h.prepare(plan.config.data_dir,part,expected_sha=h.digest(db),disk_index=True,compact_price_inputs=True)
    h.adopt(plan.config.data_dir,part,cleanup=True)
    with CatalogueStore(plan.config,session=False) as store:
        store.reject({'fixture':'new'},'TEST',AT+7,invalidate_state=False)
        count=store.c.execute('select count(*) from inputs').fetchone()[0]
    old=tmp_path/'previous';old.mkdir();base='7b8ef9d16e158692bc2f25d884c45596e7f35209'
    names=subprocess.check_output(['git','ls-tree','-r','--name-only',base,'src/holy_weather'],text=True).splitlines()
    for name in names:
        if name.endswith('.py'):
            dest=old/Path(name).relative_to('src');dest.parent.mkdir(parents=True,exist_ok=True)
            dest.write_bytes(subprocess.check_output(['git','show',base+':'+name]))
    # Keep the reader/retired-token gate required by data already archived.
    for name in ('history.py','history_files.py','history_retention.py'):
        (old/'holy_weather'/name).write_bytes((Path(__file__).resolve().parents[1]/'src/holy_weather'/name).read_bytes())
    # Pin the accepted retired-token gate, not a future catalogue paired with
    # this historical binary's queue module. Exact current-main rollback is
    # exercised separately in test_europe_active_delivery.py.
    (old/'holy_weather/catalogue.py').write_bytes(subprocess.check_output([
        'git','show','04ab3cf99b7257195499a53051488ee4e375d2ab:src/holy_weather/catalogue.py']))
    cfg=tmp_path/'config.json';cfg.write_text(canonical(plan.value()['config']))
    code="""import json,sys
from holy_weather.catalogue import CatalogueConfig,CatalogueStore
v=json.load(open(sys.argv[1]));v['levels']=tuple(v['levels'])
with CatalogueStore(CatalogueConfig(**v),session=False) as s:
 assert s.history_seen.contains(sys.argv[2])
 assert s.c.execute('select count(*) from inputs').fetchone()[0]==int(sys.argv[3])
 s.reject({'fixture':'after compatibility rollback'},'TEST',int(sys.argv[4]),invalidate_state=False)
 print(json.dumps({'rows':[tuple(r) for r in s.c.execute(\"select key,value from meta where key like 'alert_delivery_v1:%' or key like 'price_alert_v1:%' order by key\")],
                   'integrity':s.c.execute('pragma integrity_check').fetchone()[0]}))
"""
    env=dict(os.environ,PYTHONPATH=str(old),PYTHONDONTWRITEBYTECODE='1')
    out=json.loads(subprocess.check_output([sys.executable,'-B','-c',code,str(cfg),sha({'fixture':'old','n':0}),str(count),str(AT+8)],env=env,text=True))
    assert out['integrity']=='ok' and [tuple(row) for row in out['rows']]==original
    with CatalogueStore(plan.config,session=False) as store:
        assert store.c.execute('select count(*) from inputs').fetchone()[0]==count+1
