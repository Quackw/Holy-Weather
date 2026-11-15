"""Finite WebSocket-first qualification; never sends Telegram or reads REST books."""
import argparse
from collections import Counter
from dataclasses import replace
from decimal import Decimal
import gzip
import json
from pathlib import Path
import sqlite3
import threading
from holy_weather import app,weather
from holy_weather.core import Config,CITIES,sha

MAX_RAW_CAPTURE=8*1024*1024
MAX_COMPRESSED_CAPTURE=1024*1024


def qualify(cfg,output,*,controlled_timeout=False,connect=None,weather_fetch=None,lamp_fetch=None,stop=None):
    """Observe the actual feed. A controlled timeout is not a provider failure."""
    if cfg.telegram_enabled:raise ValueError('QUALIFICATION_TELEGRAM_MUST_BE_DISABLED')
    if cfg.quote_age_seconds!=90:raise ValueError('QUALIFICATION_REQUIRES_90_SECOND_LIMIT')
    if not cfg.weather_context or not cfg.price_alert_enabled:
        raise ValueError('QUALIFICATION_REQUIRES_WEATHER_AND_PRICE_CONTEXT')
    if controlled_timeout and cfg.duration_seconds<weather.POLL_SECONDS+15:
        raise ValueError('QUALIFICATION_MUST_INCLUDE_NEXT_WEATHER_POLL')
    root=Path(output).resolve()
    if root.exists():raise ValueError('NEW_QUALIFICATION_DIRECTORY_REQUIRED')
    root.mkdir(parents=True,mode=0o700)
    cfg=replace(cfg,data_dir=str(root/'data'),max_reconnects=0)
    state=dict(mode='CONTROLLED_INITIAL_AWC_TIMEOUT_THEN_REAL_FETCH' if controlled_timeout else 'NATIVE_SOURCES',
               quote_age_seconds=cfg.quote_age_seconds,baseline_wait_seconds=cfg.duration_seconds,
               weather_poll_seconds=weather.POLL_SECONDS,rest_book_reads=0,telegram_enabled=False,
               production_changed=False,started_ms=app.millis())
    (root/'START.json').write_text(json.dumps(state,indent=2))
    original_process=app.process;calls=[];verified={};types=Counter();raw_bytes=0;frames=0;fault_done=False
    stop=stop or threading.Event();fetch_real=weather_fetch or (lambda:weather.fetch_weather(station=CITIES[cfg.city][1]))
    def awc():
        nonlocal fault_done
        started=app.millis()
        injected=controlled_timeout and not fault_done
        if injected:
            fault_done=True
            def timeout(*args,**kwargs):raise TimeoutError('CONTROLLED_QUALIFICATION_TIMEOUT_NO_HTTP_REQUEST')
            receipt=weather.fetch_weather(station=CITIES[cfg.city][1],fetch=timeout,clock=app.millis)
            receipt=dict(receipt,experiment_fault='CONTROLLED_TIMEOUT_NO_HTTP_REQUEST')
        else:receipt=fetch_real()
        calls.append(dict(started_ms=started,injected=injected,receipt=receipt))
        if receipt.get('http') in (401,403,429):stop.set()
        return receipt
    wire=root/'wire.jsonl.gz'
    with gzip.open(wire,'xb') as capture:
        def tapped(raw,at,market,store,counters):
            nonlocal raw_bytes,frames
            line=(json.dumps(dict(received_ms=at,raw=raw),separators=(',',':'))+'\n').encode()
            raw_bytes+=len(line)
            if raw_bytes>MAX_RAW_CAPTURE:raise ValueError('QUALIFICATION_RAW_CAPTURE_BOUND')
            capture.write(line);capture.flush()
            frames+=1
            if wire.stat().st_size>MAX_COMPRESSED_CAPTURE:raise ValueError('QUALIFICATION_COMPRESSED_CAPTURE_BOUND')
            try:parsed=json.loads(raw)
            except ValueError:
                parsed=[];types['INVALID_JSON']+=1
            for message in parsed if isinstance(parsed,list) else [parsed]:
                kind=message.get('event_type') if isinstance(message,dict) else None
                types[kind if kind in ('book','best_bid_ask','price_change','last_trade_price','new_market','market_resolved','tick_size_change') else 'UNKNOWN']+=1
            original_process(raw,at,market,store,counters)
            # A confirmed full native baseline is required independently per side.
            # Later price_change confirmations can prove top freshness, but they
            # still cannot establish the baseline or fire a PRICE event.
            for side,token in market['tokens'].items():
                if side in verified:verified[side]['usable_at_last_message']=False
                if token not in store.price_alert_ready:continue
                row=store.c.execute('select value from meta where key=?',('price_alert_v1:'+token,)).fetchone()
                if not row:continue
                previous=json.loads(row[0])['previous']
                if previous is None:continue
                top=store.seen_tops.get(token)
                if not top:continue
                b,a=top['top']
                if b is None or a is None or not 0<Decimal(b)<=Decimal(a)<1:continue
                if not (0<=at-top['source_ms']<=cfg.quote_age_seconds*1000 and 0<=at-top['received_ms']<=cfg.quote_age_seconds*1000):continue
                if side in verified:verified[side]['usable_at_last_message']=True
                if side in verified and verified[side]['last']['sample_identity']==top['identity']:continue
                evidence=dict(at_ms=at,bid=b,ask=a,source_ms=top['source_ms'],received_ms=top['received_ms'],
                              sample_identity=top['identity'],frame_number=frames,raw_frame_sha256=sha(raw.encode()),baseline=previous)
                entry=verified.setdefault(side,dict(first=evidence,last=evidence,usable_at_last_message=True))
                entry['last']=evidence
        app.process=tapped
        try:
            result=app.observe(cfg,connect=connect,stop=stop,weather_fetch=awc,lamp_fetch=lamp_fetch,
                               baseline_wait_seconds=cfg.duration_seconds)
        finally:app.process=original_process
    state.update(result=result,finished_ms=app.millis(),native_types=dict(types),raw_capture_bytes=raw_bytes,
                 compressed_capture_bytes=wire.stat().st_size,capture_sha256=sha(wire.read_bytes()),verified_sides=verified,
                 both_sides_verified=set(verified)=={'YES','NO'},awc_calls=calls)
    for entry in verified.values():
        last=entry['last']
        entry['within_90s_at_end']=(entry['usable_at_last_message'] and 0<=state['finished_ms']-last['source_ms']<=90000 and
                                    0<=state['finished_ms']-last['received_ms']<=90000)
    actual=[c for c in calls if not c['injected']]
    state['awc_recovered_after_controlled_timeout']=bool(controlled_timeout and len(calls)>=2 and calls[0]['injected']
        and calls[1]['started_ms']-calls[0]['started_ms']>=weather.POLL_SECONDS*1000
        and actual and actual[0]['receipt']['status']=='OK' and result['storage'].get('weather_status')=='OK')
    state['verdict']=('PASS' if any(e['within_90s_at_end'] for e in verified.values()) and result['state']=='COMPLETED_WINDOW' and result['storage'].get('weather_status')=='OK'
                      and (not controlled_timeout or state['awc_recovered_after_controlled_timeout']) else 'PARTIAL')
    db=root/'data/monitor.sqlite3'
    c=sqlite3.connect('file:'+str(db)+'?mode=ro&immutable=1',uri=True);c.row_factory=sqlite3.Row
    state['events_by_type']=dict(Counter(r['type'] for r in c.execute('select type from events')))
    state['queue_previews']=[dict(key=r['key'],**json.loads(r['value'])) for r in c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")]
    row=c.execute("select value from meta where key='weather_context_v1'").fetchone()
    state['weather_context']=weather.context(json.loads(row['value']) if row else {},state['finished_ms'],state['started_ms'])
    if state['weather_context']['freshness']!='FRESH' or state['weather_context']['latest'] is None:state['verdict']='PARTIAL'
    state['sqlite_integrity']=c.execute('pragma integrity_check').fetchone()[0]
    state['foreign_key_errors']=len(c.execute('pragma foreign_key_check').fetchall());c.close()
    if state['sqlite_integrity']!='ok' or state['foreign_key_errors']:state['verdict']='FAIL'
    if wire.stat().st_size>MAX_COMPRESSED_CAPTURE:state.update(verdict='FAIL',reason='QUALIFICATION_COMPRESSED_CAPTURE_BOUND')
    state['closed_sqlite_bytes']=db.stat().st_size;state['sqlite_sha256']=sha(db.read_bytes())
    (root/'RESULT.json').write_text(json.dumps(state,indent=2))
    return state


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True);parser.add_argument('--output',required=True)
    parser.add_argument('--controlled-first-awc-timeout',action='store_true')
    args=parser.parse_args(argv)
    result=qualify(Config.load(args.config),args.output,controlled_timeout=args.controlled_first_awc_timeout)
    print(json.dumps({k:result[k] for k in ('verdict','native_types','verified_sides','awc_recovered_after_controlled_timeout','events_by_type','sqlite_integrity')}))
    return 0 if result['verdict']=='PASS' else 5


if __name__=='__main__':raise SystemExit(main())
