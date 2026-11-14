"""Explicit immutable research artifacts; no POLYMETEO path or service imports."""
from dataclasses import replace
from datetime import datetime
import json
from pathlib import Path
from collections import Counter
from .app import process
from .core import sha
from .discovery import select
from .storage import Store


def replay(cfg, manifest_path):
    manifest_path=Path(manifest_path);manifest=json.loads(manifest_path.read_text())
    bodies={}
    for key in ('gamma','stream'):
        item=manifest[key];path=Path(item['path'])
        if not path.is_absolute():path=manifest_path.parent/path
        if path.is_symlink() or not path.is_file() or path.stat().st_size>64*1024*1024:
            raise ValueError('RESEARCH_ARTIFACT_BOUND_OR_TYPE')
        body=path.read_bytes()
        if sha(body)!=item['sha256']:raise ValueError('RESEARCH_ARTIFACT_SHA')
        bodies[key]=body
    at=datetime.fromisoformat(manifest['gamma']['received_at'].replace('Z','+00:00'))
    cfg=replace(cfg,date=manifest['market_date'])
    market=select(json.loads(bodies['gamma']),cfg,now=at);counters=Counter()
    with Store(cfg)as store:
        store.bind(market,{'mode':'SAVED_REPLAY','manifest_sha256':sha(manifest_path.read_bytes()),'source':manifest,
                          'received_at':manifest['gamma']['received_at'],'raw_body':bodies['gamma'].decode()})
        for line in bodies['stream'].decode().splitlines():
            e=json.loads(line);received=int(datetime.fromisoformat(e['received_at'].replace('Z','+00:00')).timestamp()*1000)
            if e['kind']=='frame':process(json.dumps(e['data']),received,market,store,counters)
            elif e['kind']=='control'and e.get('event')in ('gap','error','disconnect'):store.gap('REPLAY_'+e['event'],received)
        result={'state':'REPLAY_COMPLETE','mode':'SAVED_REAL_OR_SYNTHETIC_AS_DECLARED_BY_MANIFEST',
                'manifest_sha256':sha(manifest_path.read_bytes()),'counters':dict(counters),'storage':store.stats()}
    # Reading artifacts never gives permission to update them.
    for key in ('gamma','stream'):
        path=Path(manifest[key]['path']);path=path if path.is_absolute()else manifest_path.parent/path
        if sha(path.read_bytes())!=manifest[key]['sha256']:raise ValueError('RESEARCH_SOURCE_CHANGED')
    return result
