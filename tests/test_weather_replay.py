from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from holy_weather.core import sha
from holy_weather.replay import replay
from test_weather import AT,report,receipt


def iso(ms):return datetime.fromtimestamp(ms/1000,timezone.utc).isoformat()


def test_weather_replay_real_receipt_order_and_late_delivery(cfg,tmp_path,gamma,make_raw):
    root=tmp_path/'corpus';root.mkdir()
    (root/'gamma.json').write_text(json.dumps(gamma))
    stream=[{'kind':'weather','received_at':iso(AT+1),'receipt':receipt([report(10)])},
            {'kind':'frame','received_at':iso(AT+10),'data':make_raw('0.49',AT+10)},
            {'kind':'frame','received_at':iso(AT+20),'data':make_raw('0.60',AT+20)},
            {'kind':'weather','received_at':iso(AT+31),'receipt':receipt([report(15,AT-120000)],AT+30)}]
    (root/'stream.jsonl').write_text(''.join(json.dumps(x)+'\n'for x in stream))
    manifest={'market_date':'2026-11-14','gamma':{'path':'gamma.json','sha256':sha((root/'gamma.json').read_bytes()),'received_at':iso(AT-100)},
              'stream':{'path':'stream.jsonl','sha256':sha((root/'stream.jsonl').read_bytes())}}
    m=root/'manifest.json';m.write_text(json.dumps(manifest));original={p:sha(p.read_bytes())for p in root.iterdir()}
    cfg=replace(cfg,weather_context=True)
    a=replay(cfg,m);b=replay(replace(cfg,data_dir=str(tmp_path/'second')),m)
    assert a==b
    with sqlite3.connect(Path(cfg.data_dir)/'monitor.sqlite3')as c:
        events=[json.loads(r[0])for r in c.execute('select payload from events')]
    assert events and all(e['weather']['observed_max_c']=='10' for e in events)
    assert all(e['weather']['latest']['received_ms']==AT for e in events)
    assert original=={p:sha(p.read_bytes())for p in root.iterdir()}


def test_weather_enabled_does_not_change_market_rule_hash(cfg):
    assert cfg.rule_hash()==replace(cfg,weather_context=True).rule_hash()
