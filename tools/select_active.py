"""Finite native-quote selection for qualification, without signals or SQLite."""
from collections import Counter,deque
from dataclasses import replace
from datetime import datetime,timezone
from decimal import Decimal
import gzip
import json
from pathlib import Path
import time
from zoneinfo import ZoneInfo
from holy_weather.app import WS,millis
from holy_weather.core import normalize,sha
from holy_weather.discovery import select

MAX_MARKETS=16
MAX_RAW_BYTES=8*1024*1024
MAX_GZIP_BYTES=1024*1024


def verified_markets(document,cfg,now):
    if not isinstance(document,list) or len(document)!=1:raise ValueError('ONE_EXACT_EVENT_REQUIRED')
    markets=[]
    for item in document[0].get('markets',[]):
        if item.get('version') not in (None,'v1'):continue
        try:market=select(document,replace(cfg,bucket=item.get('groupItemTitle','')),now=now)
        except (ValueError,TypeError):continue
        markets.append(market)
    if len(markets)>MAX_MARKETS:raise ValueError('SELECTION_MARKET_BOUND')
    return markets


class NativeSelection:
    def __init__(self,markets):
        if not 1<=len(markets)<=MAX_MARKETS:raise ValueError('SELECTION_MARKET_BOUND')
        self.markets={m['condition_id'].lower():m for m in markets}
        self.tokens={t:condition for condition,m in self.markets.items() for t in m['tokens'].values()}
        if len(self.markets)!=len(markets) or len(self.tokens)!=2*len(markets):raise ValueError('SELECTION_IDENTITY_AMBIGUOUS')
        self.latest={};self.baselines={};self.high={};self.last_receipt={};self.closed=set();self.seen={};self.seen_set={};self.counters=Counter()

    def gap(self):
        self.latest.clear();self.baselines.clear()

    def feed(self,raw,at,frame_number):
        if len(raw.encode())>262144:raise ValueError('SELECTION_FRAME_BOUND')
        try:parsed=json.loads(raw)
        except ValueError:
            self.gap();self.counters['invalid_json']+=1;return
        for msg in parsed if isinstance(parsed,list) else [parsed]:
            if not isinstance(msg,dict):self.gap();self.counters['invalid_object']+=1;continue
            if msg.get('error') or msg.get('type')=='error':
                self.gap();raise ValueError('SELECTION_PROVIDER_ERROR_NO_RETRY')
            kind=msg.get('event_type')
            if kind not in ('book','best_bid_ask','price_change','market_resolved'):
                self.counters['ignored_non_quote']+=1;continue
            condition=str(msg.get('market',msg.get('condition_id',''))).lower()
            if condition not in self.markets:
                self.counters['ignored_other_market']+=1
                candidates=msg.get('price_changes',[]) if kind=='price_change' else [msg]
                for entry in candidates if isinstance(candidates,list) else []:
                    token=str(entry.get('asset_id','')) if isinstance(entry,dict) else ''
                    if token in self.tokens:self.latest.pop(token,None);self.baselines.pop(token,None)
                continue
            market=self.markets[condition]
            if kind=='market_resolved':
                self.closed.add(condition)
                for token in market['tokens'].values():self.latest.pop(token,None);self.baselines.pop(token,None)
                self.counters['market_resolved']+=1;continue
            changes=msg.get('price_changes') if kind=='price_change' else [None]
            if not isinstance(changes,list) or (kind=='price_change' and any(not isinstance(x,dict) for x in changes)):
                self.gap();self.counters['invalid_changes']+=1;continue
            for change in changes:
                token=str((change if isinstance(change,dict) else msg).get('asset_id',''))
                if token not in market['tokens'].values():self.counters['ignored_other_token']+=1;continue
                try:sample=normalize(msg,at,market,change=change)
                except (ValueError,TypeError,KeyError):
                    self.latest.pop(token,None);self.baselines.pop(token,None);self.counters['invalid_quote']+=1;continue
                seen=self.seen_set.setdefault(token,set());order=self.seen.setdefault(token,deque())
                if sample['id'] in seen:self.counters['duplicate']+=1;continue
                if len(order)==256:seen.remove(order.popleft())
                order.append(sample['id']);seen.add(sample['id'])
                age=at-sample['source_ms']
                if not 0<=age<=90000 or sample['source_ms']<self.high.get(token,-1):
                    self.latest.pop(token,None);self.baselines.pop(token,None)
                    self.counters['invalid_clock_or_stale']+=1;continue
                if at<self.last_receipt.get(token,-1):
                    self.latest.pop(token,None);self.baselines.pop(token,None);self.counters['receipt_clock_reversed']+=1;continue
                self.high[token]=sample['source_ms'];self.last_receipt[token]=at
                b,a=sample['bid'],sample['ask']
                if b is None or a is None or not 0<Decimal(b)<=Decimal(a)<1:
                    self.latest.pop(token,None);self.baselines.pop(token,None);self.counters['incomplete_top']+=1;continue
                proof={k:sample[k] for k in ('id','kind','token','source_ms','received_ms','bid','ask')}
                proof.update(frame_number=frame_number,raw_frame_sha256=sha(raw.encode()))
                if kind in ('book','best_bid_ask') and token not in self.baselines:self.baselines[token]=proof
                if token not in self.baselines:self.counters['awaiting_full_baseline']+=1;continue
                self.latest[token]=proof;self.counters['fresh_complete_top']+=1

    def choose(self,at):
        eligible=[]
        for condition,market in self.markets.items():
            if condition in self.closed:continue
            if datetime.fromtimestamp(at/1000,timezone.utc).astimezone(ZoneInfo(market['timezone'])).date().isoformat()!=market['date']:continue
            quotes={side:dict(self.latest[token],baseline=self.baselines[token]) for side,token in market['tokens'].items()
                    if token in self.latest and token in self.baselines and
                    self.latest[token]['source_ms']>self.baselines[token]['source_ms'] and
                    0<=at-self.latest[token]['source_ms']<=90000 and 0<=at-self.latest[token]['received_ms']<=90000}
            if not quotes:continue
            distance=min(abs((Decimal(q['bid'])+Decimal(q['ask']))/2-Decimal('0.5')) for q in quotes.values())
            spread=min(Decimal(q['ask'])-Decimal(q['bid']) for q in quotes.values())
            eligible.append(((-len(quotes),distance,spread,condition),dict(market=market,quotes=quotes,selected_at_ms=at)))
        return min(eligible,key=lambda x:x[0])[1] if eligible else None


def scan(markets,output,*,seconds=60,settle_seconds=1,connect=None,clock=millis,monotonic=time.monotonic):
    if type(seconds)is not int or not 1<=seconds<=60 or not 0<=settle_seconds<=seconds:raise ValueError('SELECTION_WINDOW_BOUND')
    if connect is None:
        from websockets.sync.client import connect
    root=Path(output)
    if root.exists():raise ValueError('NEW_SELECTION_DIRECTORY_REQUIRED')
    root.mkdir(parents=True,mode=0o700);selector=NativeSelection(markets)
    result=dict(state='STARTED',started_ms=clock(),markets=markets,quote_age_seconds=90,rest_book_reads=0,
                reconnects=0,events_created=0,telegram_enabled=False)
    raw_bytes=0;frames=0;selected=None;first_valid=None
    wire=root/'wire.jsonl.gz'
    try:
        with gzip.open(wire,'xb') as capture:
            with connect(WS,proxy=None,open_timeout=10,close_timeout=2,ping_interval=10,ping_timeout=20,max_size=262144,max_queue=16,compression=None) as ws:
                ws.send(json.dumps(dict(type='market',assets_ids=list(selector.tokens),custom_feature_enabled=True)))
                deadline=monotonic()+seconds;last_ping=monotonic();last_pong=last_ping
                while monotonic()<deadline:
                    if monotonic()-last_pong>25:raise TimeoutError('SELECTION_HEARTBEAT_TIMEOUT')
                    if monotonic()-last_ping>=10:ws.send('PING');last_ping=monotonic()
                    try:raw=ws.recv(timeout=min(1,max(.01,deadline-monotonic())))
                    except TimeoutError:raw=None
                    if raw=='PONG':last_pong=monotonic();continue
                    if raw is not None:
                        if not isinstance(raw,str):raise ValueError('SELECTION_BINARY_FRAME')
                        at=clock();line=(json.dumps(dict(received_ms=at,raw=raw),separators=(',',':'))+'\n').encode()
                        raw_bytes+=len(line)
                        if raw_bytes>MAX_RAW_BYTES:raise ValueError('SELECTION_RAW_CAPTURE_BOUND')
                        capture.write(line);capture.flush();frames+=1
                        if wire.stat().st_size>MAX_GZIP_BYTES:raise ValueError('SELECTION_GZIP_CAPTURE_BOUND')
                        selector.feed(raw,at,frames)
                    candidate=selector.choose(clock())
                    if candidate:
                        if first_valid is None:first_valid=monotonic()
                        if monotonic()-first_valid>=settle_seconds:selected=candidate;break
                    else:first_valid=None
                if selected is None:selected=selector.choose(clock())
        result.update(state='SELECTED' if selected else 'NO_CONFIRMED_ACTIVE_NATIVE_QUOTE',selection=selected)
    except Exception as exc:
        selector.gap()
        result.update(state='STOPPED_NO_RETRY',error=type(exc).__name__,reason=str(exc)[:300],selection=None)
        response=getattr(exc,'response',None)
        if response is not None:result['http']=getattr(response,'status_code',None)
    result.update(finished_ms=clock(),frames=frames,raw_capture_bytes=raw_bytes,counters=dict(selector.counters))
    if wire.exists():
        result.update(compressed_capture_bytes=wire.stat().st_size,capture_sha256=sha(wire.read_bytes()))
        if wire.stat().st_size>MAX_GZIP_BYTES:result.update(state='STOPPED_NO_RETRY',reason='SELECTION_GZIP_CAPTURE_BOUND',selection=None)
    (root/'RESULT.json').write_text(json.dumps(result,indent=2))
    return result
