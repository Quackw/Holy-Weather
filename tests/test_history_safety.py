"""Archive safety regressions, isolated SQLite only; no runtime/transport calls."""
from dataclasses import replace
from pathlib import Path
import json,sqlite3,zlib
import pytest
from holy_weather import history as h
from holy_weather.core import canonical,sha
from holy_weather.storage import Store
from test_history_checkpoints import seed
from test_price_alert import AT


def prepared(cfg,market,make_raw,tmp_path):
    cfg=seed(cfg,market,make_raw);db=Path(cfg.data_dir)/'monitor.sqlite3';part=tmp_path/'part'
    h.prepare(cfg.data_dir,part,expected_sha=h.digest(db))
    return cfg,db,part


@pytest.mark.parametrize('change',['missing','extra'])
def test_adopt_proves_complete_exact_index_not_just_valid_shards(cfg,market,make_raw,tmp_path,change):
    cfg,db,part=prepared(cfg,market,make_raw,tmp_path);candidate=part/'candidate.sqlite3'
    c=sqlite3.connect(candidate)
    if change=='missing':c.execute('delete from meta where key=(select key from meta where key like ? limit 1)',(h.INDEX+'%',))
    else:h.Seen(c).add(c,[sha('never received future input')])
    c.commit();h.Seen(c)  # Each remaining shard is internally valid.
    manifest=json.loads((part/'MANIFEST.json').read_text())
    manifest.update(candidate_sha256=h.digest(candidate),candidate_fingerprint=h.fingerprint(c));c.close()
    (part/'MANIFEST.json').write_text(canonical(manifest))
    before=h.digest(db)
    with pytest.raises(ValueError,match='INDEX_COVERAGE'):h.adopt(cfg.data_dir,part)
    assert h.digest(db)==before and not (part/'ADOPT_INTENT.json').exists()


def test_inverse_refuses_foreign_history_even_with_disjoint_input_ids(cfg,market,make_raw,tmp_path):
    cfg,db,part=prepared(cfg,market,make_raw,tmp_path)
    foreign=replace(cfg,data_dir=str(tmp_path/'foreign'))
    with Store(foreign,session=False) as s:
        with s.transaction():
            for n in range(101):
                raw=('foreign '+str(n)).encode();at=AT+n if n<100 else AT+1800000
                s.c.execute('insert into inputs(id,identity,received_ms,raw,raw_sha) values(?,?,?,?,?)',
                            (10000+n,sha(raw),at,zlib.compress(raw),sha(raw)))
    other=tmp_path/'foreign-part';h.prepare(foreign.data_dir,other,expected_sha=h.digest(Path(foreign.data_dir)/'monitor.sqlite3'))
    active=part/'candidate.sqlite3';before=h.digest(active)
    with pytest.raises(ValueError,match='LINEAGE'):h.restore_history(active,other,tmp_path/'inverse')
    assert h.digest(active)==before


def test_archive_parent_is_durable_before_active_database_replace(cfg,market,make_raw,tmp_path,monkeypatch):
    cfg,db,part=prepared(cfg,market,make_raw,tmp_path);synced=[]
    sync=h.sync_dir;rename=h.os.replace
    def sync_record(path):synced.append(Path(path));return sync(path)
    def replace_checked(source,destination):
        assert part.parent in synced,'archive directory entry not durable before replacement'
        return rename(source,destination)
    monkeypatch.setattr(h,'sync_dir',sync_record);monkeypatch.setattr(h.os,'replace',replace_checked)
    h.adopt(cfg.data_dir,part)


def test_inverse_refuses_conflicting_immutable_event(cfg,market,make_raw,tmp_path):
    cfg,db,part=prepared(cfg,market,make_raw,tmp_path);active=part/'candidate.sqlite3'
    c=sqlite3.connect(active);ident,raw=c.execute('select id,payload from events limit 1').fetchone()
    value=json.loads(raw);value['changed_immutable_evidence']=True
    c.execute('update events set payload=? where id=?',(canonical(value),ident));c.commit();c.close()
    before=h.digest(active)
    with pytest.raises(ValueError,match='RESTORE_CONFLICT'):h.restore_history(active,part,tmp_path/'inverse')
    assert h.digest(active)==before
