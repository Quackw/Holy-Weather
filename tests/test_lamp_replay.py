from dataclasses import replace
from datetime import datetime,timezone
import json
from pathlib import Path
import sqlite3
from holy_weather.core import sha
from holy_weather.replay import replay
from test_lamp import AT,bulletin,receipt,enabled
from test_weather import report,receipt as weather_receipt


def test_lamp_saved_replay_preserves_causality(cfg,tmp_path,gamma,make_raw):
    root=tmp_path/'lamp-corpus';root.mkdir()
    iso=lambda t:datetime.fromtimestamp(t/1000,timezone.utc).isoformat()
    (root/'gamma.json').write_text(json.dumps(gamma))
    rows=[{'kind':'weather','received_at':iso(AT+1),'receipt':weather_receipt([report(5)])},
          {'kind':'lamp','received_at':iso(AT+2),'receipt':receipt()},
          {'kind':'frame','received_at':iso(AT+10),'data':make_raw('0.49',AT+10)},
          {'kind':'frame','received_at':iso(AT+20),'data':make_raw('0.60',AT+20)},
          {'kind':'lamp','received_at':iso(AT+31),'receipt':receipt(bulletin([55]*25),AT+30)}]
    (root/'stream.jsonl').write_text(''.join(json.dumps(r)+'\n'for r in rows))
    manifest={'market_date':'2026-11-14','gamma':{'path':'gamma.json','received_at':iso(AT-100),'sha256':sha((root/'gamma.json').read_bytes())},
              'stream':{'path':'stream.jsonl','sha256':sha((root/'stream.jsonl').read_bytes())}}
    p=root/'manifest.json';p.write_text(json.dumps(manifest));hashes={x:sha(x.read_bytes())for x in root.iterdir()}
    cfg=enabled(cfg);a=replay(cfg,p);b=replay(replace(cfg,data_dir=str(tmp_path/'second')),p)
    assert a==b
    with sqlite3.connect(Path(cfg.data_dir)/'monitor.sqlite3')as c:
        events=[json.loads(r[0])for r in c.execute('select payload from events')]
    assert events and all(e['lamp']['future_peak_f']=='46'for e in events)
    assert all(e['lamp']['received_ms']==AT for e in events)
    assert hashes=={x:sha(x.read_bytes())for x in root.iterdir()}
