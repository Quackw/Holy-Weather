from dataclasses import replace
import hashlib,json,shutil,sqlite3,zlib
from pathlib import Path
import pytest
from holy_weather.storage import Store
from holy_weather.history import Seen,prepare,restore_history,digest,INDEX
from holy_weather.core import canonical,sha
from holy_weather.alert_queue import preview,claim,finish
from test_price_notifications import feed,value
from test_price_alert import AT
from test_alert_queue import keys


def seed(cfg,market,make):
    cfg=replace(cfg,price_alert_enabled=True,alert_queue_enabled=True,price_notification_filter=True)
    with Store(cfg) as s:
        s.bind(market,{});s.connection_started(AT)
        for n in range(100):feed(s,market,make,'0.51' if n%2 else '0.52','0.53',AT+n)
        # Age the earlier input/research window; retain a fresh PRICE80 proof.
        feed(s,market,make,'0.78','0.80',AT+1800000)
        feed(s,market,make,'0.81','0.83',AT+1800001)
        k=keys(s)[0];p=preview(s,k);assert claim(s,p,AT+1800002)
        finish(s,p,{'status':'SENT','message_id':1},AT+1800003)
    return cfg


def open_candidate(cfg,part,tmp_path):
    d=tmp_path/'active';d.mkdir();(d/'.holy-weather-owned').write_text('HOLY_WEATHER_STAGE1\n')
    shutil.copyfile(part/'candidate.sqlite3',d/'monitor.sqlite3')
    return replace(cfg,data_dir=str(d))


def test_full_archive_projection_restart_new_records_and_inverse(cfg,market,make_raw,tmp_path):
    cfg=seed(cfg,market,make_raw);source=Path(cfg.data_dir)/'monitor.sqlite3';before=digest(source);part=tmp_path/'part'
    m=prepare(cfg.data_dir,part,expected_sha=before)
    assert m['removed_inputs']>90 and m['source_unchanged'] and digest(source)==before
    c=sqlite3.connect(source);old={t:[tuple(x) for x in c.execute('select * from '+t+' order by rowid')] for t in ('events','event_inputs','meta')};c.close()
    cfg=open_candidate(cfg,part,tmp_path)
    with Store(cfg) as s:
        for t in old:assert all(row in [tuple(x) for x in s.c.execute('select * from '+t)] for row in old[t])
        # Archived exact duplicate does not add another input or break PRICE readiness.
        old_raw=make_raw('0.53',AT+50,bid='0.52')
        assert s.known_duplicate({'id':sha(old_raw),'token':market['tokens']['YES']})
        previous=s.c.execute('select count(*) from inputs').fetchone()[0]
        from holy_weather.core import normalize
        assert s.apply(normalize(old_raw,AT+1800005,market))['status']=='DUPLICATE'
        s.reject(old_raw,'TEST_ARCHIVED_REJECTION',AT+1800006,invalidate_state=False)
        feed(s,market,make_raw,'0.52','0.53',AT+50)
        assert s.c.execute('select count(*) from inputs').fetchone()[0]==previous
        s.connection_started(AT+1800010)
        feed(s,market,make_raw,'0.67','0.71',AT+1800011)
        feed(s,market,make_raw,'0.71','0.73',AT+1800012)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==2
        # New PRICE70 is explicitly covered by old SENT80, never sent again.
        e=json.loads(s.c.execute("select payload from events where type='PRICE_ALERT' order by received_ms desc limit 1").fetchone()[0])
        assert e['delivery_admission']['reason']=='DELIVERED_HIGHER_LEVEL'
        new_inputs=s.c.execute('select count(*) from inputs').fetchone()[0]
        state=s.c.execute('select value from meta where key=?',('price_alert_v1:'+market['tokens']['YES'],)).fetchone()[0]
    restored=restore_history(Path(cfg.data_dir)/'monitor.sqlite3',part,tmp_path/'restore')
    assert restored['inputs']['rows']==m['source_fingerprint']['inputs']['rows']+2
    c=sqlite3.connect(tmp_path/'restore/restored.sqlite3')
    assert c.execute('select value from meta where key=?',('price_alert_v1:'+market['tokens']['YES'],)).fetchone()[0]==state
    assert c.execute("select json_extract(value,'$.status') from meta where key like 'alert_delivery_v1:%'").fetchone()[0]=='SENT'
    c.close();assert digest(source)==before


@pytest.mark.parametrize('mutation',['hash','wal','owned','symlink'])
def test_preflight_refuses_without_source_mutation(cfg,market,make_raw,tmp_path,mutation):
    cfg=seed(cfg,market,make_raw);root=Path(cfg.data_dir);db=root/'monitor.sqlite3';h=digest(db)
    expected=h
    if mutation=='hash':expected='0'*64
    if mutation=='wal':(root/'monitor.sqlite3-wal').write_bytes(b'not empty')
    if mutation=='owned':(root/'.holy-weather-owned').write_text('other')
    if mutation=='symlink':
        target=tmp_path/'linked';target.symlink_to(root,target_is_directory=True);root=target
    with pytest.raises(ValueError):prepare(root,tmp_path/'part',expected_sha=expected)
    assert digest(db)==h and not (tmp_path/'part').exists()


def test_active_writer_lock_refused(cfg,market,make_raw,tmp_path):
    cfg=seed(cfg,market,make_raw)
    with Store(cfg) as s:
        h=digest(Path(cfg.data_dir)/'monitor.sqlite3')
        with pytest.raises(BlockingIOError):prepare(cfg.data_dir,tmp_path/'part',expected_sha=h)
        assert not (tmp_path/'part').exists()


@pytest.mark.parametrize('fault',['sha','count','corrupt'])
def test_archive_index_fails_closed(cfg,market,make_raw,tmp_path,fault):
    cfg=seed(cfg,market,make_raw);part=tmp_path/'part';prepare(cfg.data_dir,part,expected_sha=digest(Path(cfg.data_dir)/'monitor.sqlite3'))
    cfg=open_candidate(cfg,part,tmp_path);c=sqlite3.connect(Path(cfg.data_dir)/'monitor.sqlite3')
    key,raw=c.execute('select key,value from meta where key like ? limit 1',(INDEX+'%',)).fetchone();v=json.loads(raw)
    if fault=='sha':v['sha256']='0'*64
    elif fault=='count':v['count']+=1
    else:v['data']='AA=='
    c.execute('update meta set value=? where key=?',(canonical(v),key));c.commit();c.close()
    with pytest.raises((ValueError,zlib.error)):Store(cfg)


def test_corrupt_archive_refuses_inverse_without_active_changes(cfg,market,make_raw,tmp_path):
    cfg=seed(cfg,market,make_raw);part=tmp_path/'part';prepare(cfg.data_dir,part,expected_sha=digest(Path(cfg.data_dir)/'monitor.sqlite3'))
    before=digest(part/'candidate.sqlite3');(part/'history.sqlite3.gz').write_bytes(b'broken')
    with pytest.raises(ValueError,match='ARCHIVE_SHA'):restore_history(part/'candidate.sqlite3',part,tmp_path/'restore')
    assert digest(part/'candidate.sqlite3')==before


def test_index_budget_transaction_rolls_back(cfg,tmp_path,monkeypatch):
    import holy_weather.history as h
    with Store(cfg) as s:
        seen=Seen(s.c);monkeypatch.setattr(h,'MAX_INDEX_BYTES',32)
        with pytest.raises(ValueError,match='INDEX_BOUND'):
            with s.transaction():seen.add(s.c,[sha('one'),sha('two')])
        assert not s.c.execute('select 1 from meta where key like ?',(INDEX+'%',)).fetchone()


def test_two_checkpoints_keep_duplicate_index_and_inverse_chain(cfg,market,make_raw,tmp_path):
    cfg=seed(cfg,market,make_raw);one=tmp_path/'one'
    prepare(cfg.data_dir,one,expected_sha=digest(Path(cfg.data_dir)/'monitor.sqlite3'))
    cfg=open_candidate(cfg,one,tmp_path)
    with Store(cfg) as s:
        s.connection_started(AT+3600000)
        for n in range(80):feed(s,market,make_raw,'0.67' if n%2 else '0.68','0.69',AT+3600000+n)
        feed(s,market,make_raw,'0.67','0.69',AT+5400000)
    two=tmp_path/'two';m=prepare(cfg.data_dir,two,expected_sha=digest(Path(cfg.data_dir)/'monitor.sqlite3'))
    assert m['removed_inputs']>50
    c=sqlite3.connect(two/'candidate.sqlite3');seen=Seen(c)
    for raw in (make_raw('0.53',AT+50,bid='0.52'),make_raw('0.69',AT+3600020,bid='0.68')):assert seen.contains(sha(raw))
    c.close()
    restore_history(two/'candidate.sqlite3',two,tmp_path/'inverse-two')
    full=restore_history(tmp_path/'inverse-two/restored.sqlite3',one,tmp_path/'inverse-one')
    assert full['inputs']['rows']>180


def test_explicit_adoption_is_one_time_and_cleanup_only_own_verified_scratch(cfg,market,make_raw,tmp_path):
    from holy_weather.history import adopt
    cfg=seed(cfg,market,make_raw);db=Path(cfg.data_dir)/'monitor.sqlite3';part=tmp_path/'part'
    m=prepare(cfg.data_dir,part,expected_sha=digest(db));adopt(cfg.data_dir,part,cleanup=True)
    assert digest(db)==m['candidate_sha256']
    assert (part/'history.sqlite3.gz').exists() and (part/'MANIFEST.json').exists()
    assert not (part/'snapshot.sqlite3').exists() and not (part/'restore-proof.sqlite3').exists()
    with pytest.raises(ValueError,match='ALREADY_ATTEMPTED'):adopt(cfg.data_dir,part)
    with Store(cfg) as s:assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'


@pytest.mark.parametrize('drift',['source','candidate','archive'])
def test_adoption_refuses_drift_before_replace(cfg,market,make_raw,tmp_path,drift):
    from holy_weather.history import adopt
    cfg=seed(cfg,market,make_raw);db=Path(cfg.data_dir)/'monitor.sqlite3';part=tmp_path/'part'
    prepare(cfg.data_dir,part,expected_sha=digest(db))
    p={'source':db,'candidate':part/'candidate.sqlite3','archive':part/'history.sqlite3.gz'}[drift]
    with p.open('ab') as f:f.write(b'x')
    before=digest(db)
    with pytest.raises(ValueError):adopt(cfg.data_dir,part)
    assert digest(db)==before and not (part/'ADOPT_INTENT.json').exists()


def test_interrupted_adoption_keeps_archive_and_forbids_blind_repeat(cfg,market,make_raw,tmp_path,monkeypatch):
    import holy_weather.history as h
    cfg=seed(cfg,market,make_raw);db=Path(cfg.data_dir)/'monitor.sqlite3';old=digest(db);part=tmp_path/'part'
    h.prepare(cfg.data_dir,part,expected_sha=old)
    def fail(*args):raise OSError('synthetic pre-replace failure')
    monkeypatch.setattr(h.os,'replace',fail)
    with pytest.raises(OSError):h.adopt(cfg.data_dir,part)
    assert digest(db)==old and (part/'history.sqlite3.gz').is_file()
    with pytest.raises(ValueError,match='ALREADY_ATTEMPTED'):h.adopt(cfg.data_dir,part)
