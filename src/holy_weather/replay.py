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
    def receipt(value):
        stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
        if stamp.utcoffset() is None:
            raise ValueError('REPLAY_RECEIPT_TIMEZONE_REQUIRED')
        return stamp

    at=receipt(manifest['gamma']['received_at'])
    # Validate the entire receipt timeline before opening or writing any database.
    # Provider timestamps stay untouched and are checked by the normal engine.
    previous=at
    for line in bodies['stream'].decode().splitlines():
        envelope=json.loads(line)
        if not isinstance(envelope,dict) or envelope.get('kind') not in ('frame','weather','lamp','control'):
            raise ValueError('REPLAY_ENVELOPE_KIND')
        if envelope['kind']=='control' and envelope.get('event') not in ('gap','error','disconnect'):
            raise ValueError('REPLAY_ENVELOPE_KIND')
        current=receipt(envelope['received_at'])
        if current<at:
            raise ValueError('REPLAY_FRAME_PRECEDES_GAMMA_RECEIPT')
        if current<previous:
            raise ValueError('REPLAY_RECEIPT_ORDER_REVERSED')
        previous=current
    cfg=replace(cfg,date=manifest['market_date'])
    market=select(json.loads(bodies['gamma']),cfg,now=at);counters=Counter()
    with Store(cfg,session=False)as store:
        store.gap('REPLAY_START',int(at.timestamp()*1000))
        store.bind(market,{'mode':'SAVED_REPLAY','manifest_sha256':sha(manifest_path.read_bytes()),'source':manifest,
                          'received_at':manifest['gamma']['received_at'],'raw_body':bodies['gamma'].decode()})
        for line in bodies['stream'].decode().splitlines():
            e=json.loads(line);received=int(receipt(e['received_at']).timestamp()*1000)
            if e['kind']=='frame':process(json.dumps(e['data']),received,market,store,counters)
            elif e['kind']=='weather':
                store.record_weather(e['receipt'],market,received)
            elif e['kind']=='lamp':
                store.record_lamp(e['receipt'],market,received)
            elif e['kind']=='control'and e.get('event')in ('gap','error','disconnect'):store.gap('REPLAY_'+e['event'],received)
        result={'state':'REPLAY_COMPLETE','mode':'SAVED_REAL_OR_SYNTHETIC_AS_DECLARED_BY_MANIFEST',
                'manifest_sha256':sha(manifest_path.read_bytes()),'counters':dict(counters),'storage':store.stats(now_ms=int(previous.timestamp()*1000))}
    # Reading artifacts never gives permission to update them.
    for key in ('gamma','stream'):
        path=Path(manifest[key]['path']);path=path if path.is_absolute()else manifest_path.parent/path
        if sha(path.read_bytes())!=manifest[key]['sha256']:raise ValueError('RESEARCH_SOURCE_CHANGED')
    return result
