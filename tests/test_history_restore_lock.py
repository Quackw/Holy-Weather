"""Restore must establish a closed source, including a currently empty WAL."""
from pathlib import Path
import fcntl,json,sqlite3
import pytest
from holy_weather.storage import Store,StorageStop
from holy_weather.history import prepare,restore_history,digest
from test_history_checkpoints import seed,open_candidate
from test_price_notifications import feed,value
from test_alert_queue import keys
from test_price_alert import AT


def test_restore_refuses_live_owned_writer_with_empty_wal(cfg,market,make_raw,tmp_path):
    cfg=seed(cfg,market,make_raw);part=tmp_path/'part'
    prepare(cfg.data_dir,part,expected_sha=digest(Path(cfg.data_dir)/'monitor.sqlite3'))
    cfg=open_candidate(cfg,part,tmp_path);db=Path(cfg.data_dir)/'monitor.sqlite3'
    with Store(cfg) as store:
        # Offline fixture only: a writer can be alive even with a zero-byte WAL.
        store.c.execute('pragma wal_checkpoint(TRUNCATE)')
        assert db.with_name(db.name+'-wal').stat().st_size==0
        before=digest(db)
        with pytest.raises(BlockingIOError):restore_history(db,part,tmp_path/'restore')
        assert digest(db)==before and not (tmp_path/'restore').exists()


@pytest.mark.parametrize('fault',['missing','symlink'])
def test_restore_refuses_invalid_owned_writer_lock_before_output(cfg,market,make_raw,tmp_path,fault):
    cfg=seed(cfg,market,make_raw);part=tmp_path/'part'
    prepare(cfg.data_dir,part,expected_sha=digest(Path(cfg.data_dir)/'monitor.sqlite3'))
    cfg=open_candidate(cfg,part,tmp_path);db=Path(cfg.data_dir)/'monitor.sqlite3'
    lock=db.parent/'.writer.lock';assert not lock.exists()
    if fault=='symlink':
        target=tmp_path/'elsewhere.lock';target.touch();lock.symlink_to(target)
    before=digest(db)
    with pytest.raises(ValueError,match='HISTORY_RESTORE_WRITER_LOCK'):
        restore_history(db,part,tmp_path/'restore')
    assert digest(db)==before and not (tmp_path/'restore').exists()
    assert lock.is_symlink() if fault=='symlink' else not lock.exists()


@pytest.mark.parametrize('fail',['success','restore-error'])
def test_restore_holds_lock_preserves_new_queue_and_releases_on_exit(cfg,market,make_raw,tmp_path,monkeypatch,fail):
    import holy_weather.history as h
    cfg=seed(cfg,market,make_raw);part=tmp_path/'part'
    manifest=prepare(cfg.data_dir,part,expected_sha=digest(Path(cfg.data_dir)/'monitor.sqlite3'))
    cfg=open_candidate(cfg,part,tmp_path);db=Path(cfg.data_dir)/'monitor.sqlite3'
    with Store(cfg) as s:
        s.connection_started(AT+3600000)
        feed(s,market,make_raw,'0.48','0.50',AT+3600000,'NO')
        feed(s,market,make_raw,'0.71','0.73',AT+3600001,'NO')
        key=next(k for k in keys(s) if value(s,k)['side']=='NO')
        from holy_weather.alert_queue import claim,preview
        assert claim(s,preview(s,key),AT+3600002)  # Real claim remains UNKNOWN.
        feed(s,market,make_raw,'0.81','0.83',AT+3600003,'NO')
        meta=dict(s.c.execute('select key,value from meta'))
        assert {json.loads(v)['status'] for k,v in meta.items() if k.startswith('alert_delivery_v1:')}=={'SENT','UNKNOWN','PENDING'}
    before=digest(db);check=h.check;held=[]
    def checked(c):
        with pytest.raises(StorageStop,match='WRITER_ALREADY_RUNNING'):Store(cfg)
        held.append(True)
        if fail=='restore-error':raise ValueError('SYNTHETIC_RESTORE_FAILURE')
        check(c)
    monkeypatch.setattr(h,'check',checked)
    if fail=='restore-error':
        with pytest.raises(ValueError,match='SYNTHETIC_RESTORE_FAILURE'):
            restore_history(db,part,tmp_path/'restore')
    else:
        result=restore_history(db,part,tmp_path/'restore')
        assert result['inputs']['rows']==manifest['source_fingerprint']['inputs']['rows']+3
        c=sqlite3.connect(tmp_path/'restore/restored.sqlite3')
        try:
            assert dict(c.execute('select key,value from meta'))==meta
            assert c.execute('pragma integrity_check').fetchone()[0]=='ok'
            assert not c.execute('pragma foreign_key_check').fetchall()
        finally:c.close()
    assert held and digest(db)==before
    with (db.parent/'.writer.lock').open('rb') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
