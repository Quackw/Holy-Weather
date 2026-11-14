from dataclasses import replace
import json
from pathlib import Path
import pytest
from holy_weather.core import sha
from holy_weather.replay import replay


def corpus(tmp_path,gamma,make_raw):
    root=tmp_path/'research';root.mkdir();g=root/'gamma.json';g.write_text(json.dumps(gamma));s=root/'stream.jsonl'
    prices=['0.699','0.701','0.699','0.702']
    s.write_text(''.join(json.dumps({'kind':'frame','received_at':f'2026-11-14T11:13:2{i}+00:00',
        'data':make_raw(p,1794654800000+i*1000)})+'\n'for i,p in enumerate(prices)))
    # Receipt/source clocks must be consistent, independent of execution time.
    from datetime import datetime
    lines=[]
    for line in s.read_text().splitlines():
        e=json.loads(line);e['data']['timestamp']=str(int(datetime.fromisoformat(e['received_at']).timestamp()*1000));lines.append(json.dumps(e))
    s.write_text('\n'.join(lines)+'\n');m=root/'manifest.json';m.write_text(json.dumps({'market_date':'2026-11-14',
        'gamma':{'path':'gamma.json','sha256':sha(g.read_bytes()),'received_at':'2026-11-14T11:00:00+00:00'},
        'stream':{'path':'stream.jsonl','sha256':sha(s.read_bytes())}}));return m


def test_cold_replay_and_duplicate_results(cfg,tmp_path,gamma,make_raw):
    manifest=corpus(tmp_path,gamma,make_raw);before=sha(manifest.read_bytes())
    a=replay(cfg,manifest);b=replay(replace(cfg,data_dir=str(tmp_path/'second')),manifest)
    assert a['storage']['events']==b['storage']['events']=={'DETECTED':1,'SUPPRESSED':2}
    duplicate=replay(cfg,manifest)
    assert duplicate['storage']['samples']==4 and duplicate['counters']['DUPLICATE']==4
    assert sha(manifest.read_bytes())==before


def test_replay_refuses_changed_artifact(cfg,tmp_path,gamma,make_raw):
    manifest=corpus(tmp_path,gamma,make_raw);p=manifest.parent/'stream.jsonl';p.write_text(p.read_text()+'\n')
    with pytest.raises(ValueError,match='ARTIFACT_SHA'):replay(cfg,manifest)
