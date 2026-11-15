"""Finite observation CLI; one process owns all writes and exits safely."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import signal
import sqlite3
import threading
import time
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo
from .core import Config, normalize, sha
from .discovery import discover
from .storage import Store, StorageStop

BASELINE_RECOVERY_SECONDS=5


class BaselineRecoveryTimeout(TimeoutError):
    pass


WS = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'


def millis():
    return time.time_ns()//1_000_000


def disk_write_bytes():
    """Linux's process I/O counter; absent elsewhere means unknown, not zero."""
    try:
        return int(dict(line.split(':',1) for line in Path('/proc/self/io').read_text().splitlines())['write_bytes'])
    except (OSError,KeyError,ValueError):
        return None


def price_confirmation_unchanged(store,sample):
    """Depth updates cannot prove that PRICE already processed this full BBO."""
    if not store.cfg.price_alert_enabled or sample['kind']!='best_bid_ask':return True
    if sample['token'] not in store.price_alert_ready:return False
    row=store.c.execute('select value from meta where key=?',('price_alert_v1:'+sample['token'],)).fetchone()
    previous=json.loads(row[0]).get('previous') if row else None
    return bool(previous and (previous['bid'],previous['ask'])==(sample['bid'],sample['ask']))


def process(raw, received_ms, market, store, counters):
    size = len(raw.encode())
    counters['wire_bytes'] += size; counters['frames'] += 1
    if size > 262144:
        raise ValueError('FRAME_BOUND')
    try:
        parsed = json.loads(raw)
    except ValueError:
        store.invalidate('MALFORMED_JSON_FRAME',received_ms); counters['invalid_frames']+=1; return
    expanded=[]
    for message in parsed if isinstance(parsed,list)else[parsed]:
        if isinstance(message,dict)and message.get('event_type')=='price_change':
            changes=message.get('price_changes')
            if not isinstance(changes,list)or any(not isinstance(c,dict)for c in changes):expanded.append((message,None));continue
            expanded.extend((message,c)for c in changes if isinstance(c,dict)and str(c.get('asset_id'))in market['tokens'].values())
        else:expanded.append((message,None))
    for message,change in expanded:
        sample=None
        try:
            sample = normalize(message,received_ms,market,change=change)
            if sample is None:
                counters['ignored_depth_or_control']+=1
                # Lifecycle broadcasts are not price updates for our tokens.
                # Fixed counter names avoid cardinality or per-frame SQL growth.
                kind=message.get('event_type')
                counter=('ignored_new_market' if kind=='new_market' else
                         'ignored_tick_size_change' if kind=='tick_size_change' else 'ignored_unknown_type')
                counters[counter]+=1
                continue
            counters['received_'+sample['kind']]+=1
            if store.known_duplicate(sample):
                counters['DUPLICATE']+=1
                if counters['DUPLICATE']==1 or counters['DUPLICATE']%128==0:
                    store.diagnostic('DUPLICATE','EXACT_PREVIOUSLY_SEEN_SAMPLE:COUNT='+str(counters['DUPLICATE']),received_ms)
                continue
            quote=sample['kind']not in ('last_trade_price','market_resolved')
            previous=store.seen_tops.get(sample.get('token')) if quote else None
            if previous and sample['source_ms']<previous['source_ms']:
                store.reject(message,'OUT_OF_ORDER_CONFIRMATION',received_ms,sample['token']);counters['OUT_OF_ORDER']+=1;continue
            if previous and received_ms<previous.get('received_ms',previous['persisted_ms']):
                store.reject(message,'RECEIPT_CLOCK_REVERSED',received_ms,sample['token']);counters['RECEIPT_CLOCK_REVERSED']+=1;continue
            if quote and previous and (sample['kind']!='best_bid_ask' or sample['token'] in store.ready_tokens) and sample['id']!=previous['identity']and sample['kind']in ('price_change','best_bid_ask')and(sample['bid'],sample['ask'])==previous['top']and 0<=received_ms-sample['source_ms']<=store.cfg.quote_age_seconds*1000 and 0<=received_ms-previous['persisted_ms']<15000 and price_confirmation_unchanged(store,sample):
                store.seen_tops[sample['token']]=dict(previous,source_ms=sample['source_ms'],received_ms=received_ms,identity=sample['id'])
                counters['coalesced_unchanged_top_confirmations']+=1;continue
            result = store.apply(sample)
            counters[result['status']] += 1
            counters['stored_input_raw_bytes']+=result.get('raw_bytes',0)
            counters['stored_input_compressed_bytes']+=result.get('compressed_bytes',0)
            if quote and result['status']in ('FRESH','MISSING_ASK','AWAITING_BASELINE'):
                store.seen_tops[sample['token']]={'top':(sample['bid'],sample['ask']),'source_ms':sample['source_ms'],'received_ms':received_ms,'persisted_ms':received_ms,'identity':sample['id']}
            elif quote and result['status']!='DUPLICATE':
                store.seen_tops.pop(sample['token'],None)
            for event in result['events']:
                counters[event['status']] += 1
        except (ValueError,KeyError,TypeError) as exc:
            counters['invalid_messages']+=1
            store.reject(message,str(exc),received_ms,sample.get('token') if sample else None)
            if counters['invalid_messages']>=20:
                raise ValueError('INVALID_INPUT_BUDGET_EXHAUSTED')


def observe(cfg, *, connect=None, stop=None, weather_fetch=None, lamp_fetch=None, telegram_transport=None,
            baseline_wait_seconds=None):
    if baseline_wait_seconds is not None and (type(baseline_wait_seconds)is not int or not 1<=baseline_wait_seconds<=cfg.duration_seconds):
        raise ValueError('BASELINE_WAIT_MUST_FIT_OBSERVATION_WINDOW')
    baseline_wait=BASELINE_RECOVERY_SECONDS if baseline_wait_seconds is None else baseline_wait_seconds
    if connect is None:
        from websockets.sync.client import connect
    from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException
    stop = stop or threading.Event(); counters=Counter(); reason='COMPLETED_WINDOW'; exit_code=0
    deadline=time.monotonic()+cfg.duration_seconds
    weather=None;lamp=None;sender=None
    if cfg.telegram_enabled:
        from .telegram import Sender,post,token_from_env
        sender=Sender(token_from_env(),telegram_transport or post)
    with Store(cfg) as store:
        logger=logging.getLogger('holy_weather');logger.setLevel(logging.INFO);logger.propagate=False
        handler=RotatingFileHandler(store.root/'monitor.log',maxBytes=524288,backupCount=2)
        logger.addHandler(handler)
        try:
            market,receipt=discover(cfg);store.bind(market,receipt)
            if cfg.weather_context:
                from .weather import WeatherPoller, fetch_weather, station_check, source_for as weather_source
                station_check(market)
                weather=WeatherPoller(weather_fetch or (lambda: fetch_weather(station=market['station'])))
                weather.source=weather_source(market['station'])
            if cfg.lamp_context:
                from .lamp import LampPoller,fetch_lamp,source_for as lamp_source
                lamp=LampPoller(lamp_fetch or (lambda: fetch_lamp(station=market['station'])))
                lamp.source=lamp_source(market['station'])
            initial_bytes=store.usage();initial_writes=disk_write_bytes();started=time.monotonic();attempt=0
            while not stop.is_set() and time.monotonic()<deadline:
                try:
                    with connect(WS,proxy=None,open_timeout=10,close_timeout=2,ping_interval=10,ping_timeout=20,
                                 max_size=262144,max_queue=16,compression=None) as ws:
                        store.connection_started(millis())
                        ws.send(json.dumps({'type':'market','assets_ids':list(market['tokens'].values()),'custom_feature_enabled':True}))
                        last_ping=time.monotonic();last_pong=last_ping;next_stats=last_ping+10
                        while not stop.is_set() and time.monotonic()<deadline:
                            if datetime.now(timezone.utc).astimezone(ZoneInfo(market['timezone'])).date().isoformat()!=market['date']:
                                reason='MARKET_LOCAL_DATE_ENDED';stop.set();break
                            if sender:sender.tick(store,millis())
                            if weather:weather.tick(store,market,millis())
                            if lamp:lamp.tick(store,market,millis())
                            if store.recovery_due(baseline_wait):
                                raise BaselineRecoveryTimeout('BASELINE_NOT_CONFIRMED')
                            if time.monotonic()>=next_stats:
                                health=store.observation_snapshot()['observation']['state']
                                store.set_meta('counters',dict(counters));logger.info(json.dumps({'state':health,'counters':dict(counters)}));next_stats=time.monotonic()+10
                            if time.monotonic()-last_pong>25:
                                raise TimeoutError('APPLICATION_HEARTBEAT_TIMEOUT')
                            if time.monotonic()-last_ping>=10:
                                ws.send('PING');last_ping=time.monotonic()
                            try:
                                raw=ws.recv(timeout=min(1,max(.01,deadline-time.monotonic())))
                            except TimeoutError:
                                continue
                            if raw=='PONG':
                                last_pong=time.monotonic();continue
                            if not isinstance(raw,str):
                                store.invalidate('UNEXPECTED_BINARY_FRAME',millis());counters['invalid_frames']+=1;continue
                            process(raw,millis(),market,store,counters)
                            if store.c.execute("select 1 from meta where key='market_closed'").fetchone():
                                reason='MARKET_RESOLVED';stop.set()
                    store.gap('OBSERVATION_ENDED:'+reason,millis())
                    break
                except InvalidStatus as exc:
                    status=exc.response.status_code
                    store.gap('WEBSOCKET_HTTP_'+str(status),millis())
                    if status in (401,403,429):
                        reason='ACCESS_DENIED_NO_RETRY';exit_code=2;break
                    reason='WEBSOCKET_HTTP_ERROR';exit_code=5;break
                except (OSError,TimeoutError,ConnectionClosed) as exc:
                    recovery=isinstance(exc,BaselineRecoveryTimeout)
                    store.gap('BASELINE_RECOVERY_RECONNECT' if recovery else 'NETWORK:'+type(exc).__name__,millis())
                    counters['baseline_recovery_gaps' if recovery else 'network_gaps']+=1
                    if isinstance(exc,ConnectionClosed) and exc.rcvd and exc.rcvd.code in (1007,1009):
                        reason='INVALID_OR_OVERSIZE_STREAM';exit_code=3;break
                    if attempt>=cfg.max_reconnects:
                        reason='BASELINE_RECOVERY_BUDGET_EXHAUSTED' if recovery else 'NETWORK_RECONNECT_BUDGET_EXHAUSTED';exit_code=5;break
                    attempt+=1;counters['reconnects']+=1;stop.wait(min(2**(attempt-1),max(0,deadline-time.monotonic())))
                except WebSocketException as exc:
                    store.gap('PROTOCOL:'+type(exc).__name__,millis());reason='WEBSOCKET_PROTOCOL_ERROR';exit_code=3;break
            elapsed=time.monotonic()-started
            if stop.is_set() and reason=='COMPLETED_WINDOW':
                reason='SIGNAL_STOP'
            store.set_meta('counters',dict(counters));store.set_meta('last_stop',{'reason':reason,'at_ms':millis()})
            written=disk_write_bytes()
            result={'state':reason,'exit_code':exit_code,'elapsed_seconds':elapsed,'market':{k:v for k,v in market.items() if k!='rules'},
                    'counters':dict(counters),'storage':store.stats(),'growth_since_discovery_bytes':max(0,store.usage()-initial_bytes),
                    'process_disk_write_bytes':written-initial_writes if written is not None and initial_writes is not None else None,
                    'weather_context_enabled':cfg.weather_context, 'lamp_context_enabled':cfg.lamp_context,
                    'baseline_wait_seconds':baseline_wait,'own_model_or_trading_implemented':False}
            result['data_state']=result['storage']['observation']['state']
            if result['storage']['sqlite_integrity']!='ok' or result['storage']['foreign_key_errors']:
                result.update(state='STORAGE_INTEGRITY_FAILURE',exit_code=4)
            logger.info(json.dumps({'state':reason,'counters':dict(counters)}));return result
        except HTTPError as exc:
            store.diagnostic('DISCOVERY_HTTP',str(exc.code),millis())
            return {'state':'DISCOVERY_DENIED' if exc.code in (401,403,429) else 'DISCOVERY_HTTP_ERROR','http':exc.code,'exit_code':2,'storage':store.stats()}
        finally:
            if sender:sender.close(store)
            if weather:weather.close()
            if lamp:lamp.close()
            handler.close();logger.removeHandler(handler)


def main(argv=None):
    parser=argparse.ArgumentParser();parser.add_argument('command',choices=['observe','stats','prune','replay'])
    parser.add_argument('--config',default='config.example.toml');parser.add_argument('--apply',action='store_true')
    parser.add_argument('--manifest')
    args=parser.parse_args(argv);stop=threading.Event()
    for sig in (signal.SIGINT,signal.SIGTERM):
        signal.signal(sig,lambda *_:stop.set())
    try:
        cfg=Config.load(args.config)
        if args.command=='observe':
            result=observe(cfg,stop=stop)
        elif args.command=='replay':
            if not args.manifest:raise ValueError('EXPLICIT_REPLAY_MANIFEST_REQUIRED')
            from .replay import replay
            result=replay(cfg,args.manifest)
        else:
            with Store(cfg,session=False) as store:
                result=store.stats() if args.command=='stats' else store.prune(millis(),apply=args.apply)
        print(json.dumps(result,ensure_ascii=False,sort_keys=True));return result.get('exit_code',0)
    except (StorageStop,sqlite3.Error,PermissionError) as exc:
        print(json.dumps({'state':'STORAGE_STOPPED','reason':str(exc),'exit_code':4}));return 4
    except URLError as exc:
        print(json.dumps({'state':'DISCOVERY_NETWORK_ERROR','reason':str(exc),'exit_code':5}));return 5
    except ValueError as exc:
        print(json.dumps({'state':'INVALID_CONFIG_OR_PROVIDER_DATA','reason':str(exc),'exit_code':3}));return 3
    except OSError as exc:
        print(json.dumps({'state':'LOCAL_FILESYSTEM_ERROR','reason':str(exc),'exit_code':4}));return 4


if __name__=='__main__':
    raise SystemExit(main())
