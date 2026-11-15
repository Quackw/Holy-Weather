"""Finite PRICE-only catalogue: existing SQLite schema, one writer and sender.

No weather, forecasts, services, or implicit Telegram activation. A distinct
rule hash prevents the single-market CLI from opening a catalogue as one market.
"""
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import time
import zlib
from zoneinfo import ZoneInfo
from .core import Config, EUROPE_CITIES, canonical, sha
from .storage import Store, StorageStop
from .app import process

MAX_MARKETS = 176  # eleven cities, at most sixteen verified buckets each
PREFIX = 'price_catalogue_v1:'
DAY_PREFIX = 'price_catalogue_day_v1:'


def local_days(at):
    now=datetime.fromtimestamp(at/1000,timezone.utc)
    return {city:now.astimezone(ZoneInfo(v[2])).date().isoformat() for city,v in EUROPE_CITIES.items()}


@dataclass(frozen=True)
class CatalogueConfig(Config):
    city: str = 'london'
    bucket: str = 'catalogue'
    source_clock_tolerance_ms: int = 100
    price_alert_enabled: bool = True
    alert_queue_enabled: bool = True

    def __post_init__(self):
        super().__post_init__()
        if (self.weather_context or self.lamp_context or self.lamp_alert_enabled
                or not self.price_alert_enabled or not self.alert_queue_enabled):
            raise ValueError('CATALOGUE_PRICE_ONLY')

    def rule_hash(self):
        return sha(['EUROPE_PRICE_CATALOGUE_V1', super().rule_hash()])


class CatalogueStore(Store):
    def __init__(self, cfg, **kwargs):
        if not isinstance(cfg, CatalogueConfig):
            raise ValueError('CATALOGUE_CONFIG_REQUIRED')
        self.markets = {}; self.tokens = {}
        super().__init__(cfg, **kwargs)
        try:
            # Load only the latest retained date per station; older identities/evidence
            # remain in SQLite, not in the bounded subscription registry.
            for row in self.c.execute("""select m.value from meta m where m.key like ?
                and json_extract(m.value,'$.date')=(select max(json_extract(h.value,'$.date'))
                  from meta h where h.key like ? and json_extract(h.value,'$.station')=json_extract(m.value,'$.station'))
                limit ?""", (PREFIX+'%',PREFIX+'%',MAX_MARKETS+1)):
                self._register(json.loads(row[0]))
        except Exception:
            self.close(); raise

    def _register(self, market):
        condition=market['condition_id'].lower()
        if condition not in self.markets and len(self.markets)>=MAX_MARKETS:
            raise StorageStop('CATALOGUE_MARKET_BOUND')
        for token in market['tokens'].values():
            if token in self.tokens and self.tokens[token]!=condition:
                raise StorageStop('CATALOGUE_TOKEN_COLLISION')
        self.markets[condition]=market
        self.tokens.update({token:condition for token in market['tokens'].values()})

    def bind(self, market, receipt):
        condition=market['condition_id'].lower()
        if market['station'] not in {v[1] for v in EUROPE_CITIES.values()}:
            raise StorageStop('CATALOGUE_EUROPE_ONLY')
        saved=self.c.execute('select value from meta where key=?',(PREFIX+condition,)).fetchone()
        prior=json.loads(saved[0]) if saved else None
        if prior and prior!=market:raise StorageStop('MARKET_IDENTITY_OR_RULE_DRIFT')
        if condition not in self.markets and len(self.markets)>=MAX_MARKETS:
            raise StorageStop('CATALOGUE_MARKET_BOUND')
        if any(self.c.execute("select 1 from meta where key like ? and key!=? and (json_extract(value,'$.tokens.YES')=? or json_extract(value,'$.tokens.NO')=?) limit 1",
                              (PREFIX+'%',PREFIX+condition,t,t)).fetchone() for t in market['tokens'].values()):
            raise StorageStop('CATALOGUE_TOKEN_COLLISION')
        raw=canonical(receipt).encode()
        if len(raw)>4_100_000:raise StorageStop('CATALOGUE_RECEIPT_BOUND')
        at=int(datetime.fromisoformat(receipt['received_at']).timestamp()*1000)
        identity='GAMMA:'+sha(raw)
        with self.transaction():
            self.c.execute('insert or ignore into meta values(?,?)',(PREFIX+condition,canonical(market)))
            self.c.execute('insert or ignore into inputs(identity,received_ms,raw,raw_sha) values(?,?,?,?)',
                           (identity,at,zlib.compress(raw,3),sha(raw)))
            ref=self.c.execute('select id from inputs where identity=?',(identity,)).fetchone()[0]
            self.c.execute('insert or replace into latest values(?,?,?)',('__MARKET__'+condition,'discovery',ref))
        self._register(market)

    def retain_discovery(self, receipt):
        raw=canonical(receipt).encode()
        if len(raw)>4_100_000:raise StorageStop('CATALOGUE_RECEIPT_BOUND')
        at=int(datetime.fromisoformat(receipt['received_at']).timestamp()*1000)
        identity='GAMMA:'+sha(raw)
        with self.transaction():
            self.c.execute('insert or ignore into inputs(identity,received_ms,raw,raw_sha) values(?,?,?,?)',
                           (identity,at,zlib.compress(raw,3),sha(raw)))
        return self.c.execute('select id from inputs where identity=?',(identity,)).fetchone()[0]

    def market_for_token(self, token):
        if token in self.tokens:return self.markets[self.tokens[token]]
        row=self.c.execute("select value from meta where key like ? and (json_extract(value,'$.tokens.YES')=? or json_extract(value,'$.tokens.NO')=?) limit 1",(PREFIX+'%',token,token)).fetchone()
        if not row:raise StorageStop('UNKNOWN_CATALOGUE_TOKEN')
        return json.loads(row[0])

    def retire(self, at):
        """Remove ended markets from memory only; never delete audit or dedupe."""
        for condition in list(self.markets):
            if self.active(condition,at):continue
            market=self.markets.pop(condition)
            for token in market['tokens'].values():
                self.tokens.pop(token,None);self.seen_tops.pop(token,None)
                self.ready_tokens.discard(token);self.price_alert_ready.discard(token)
                self.pending_baselines.pop(token,None)

    def discovery_reference(self, token):
        return self.c.execute("select input_id from latest where token=? and kind='discovery'",
                              ('__MARKET__'+self.tokens[token],)).fetchone()

    def closed_key(self, sample):
        condition=(str(sample['raw'].get('market',sample['raw'].get('condition_id',''))).lower()
                   if sample['kind']=='market_resolved' else self.tokens[sample['token']])
        if condition not in self.markets:raise StorageStop('UNKNOWN_CATALOGUE_MARKET')
        return 'market_closed:'+condition

    def connection_started(self, at):
        super().connection_started(at)
        self.pending_baselines={t:time.monotonic() for t in self.tokens}

    def invalidate(self, reason, at, token=None):
        if token is not None:return super().invalidate(reason,at,token)
        # Includes subscribed tokens without a stored sample; one global gap.
        was_connected=self.connected
        self.gap('DATA:'+reason,at)
        self.connected=was_connected
        if was_connected:self.pending_baselines={t:time.monotonic() for t in self.tokens}

    def active(self, condition, at):
        row=None if condition in self.markets else self.c.execute('select value from meta where key=?',(PREFIX+condition,)).fetchone()
        market=self.markets[condition] if row is None else json.loads(row[0])
        today=datetime.fromtimestamp(at/1000,timezone.utc).astimezone(ZoneInfo(market['timezone'])).date().isoformat()
        return today==market['date'] and not self.c.execute('select 1 from meta where key=?',('market_closed:'+condition,)).fetchone()

    def expire_closed_deliveries(self, at, selected=None):
        rows=self.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' and json_extract(value,'$.status') in ('PENDING','WAITING_BID')").fetchall()
        expired=[]
        for row in rows:
            value=json.loads(row['value'])
            event=self.c.execute('select token from events where id=?',(value['event_id'],)).fetchone()
            condition=self.market_for_token(event[0])['condition_id'].lower() if event else None
            if event and (not self.active(condition,at) or (selected is not None and condition not in selected)):
                value.update(status='EXPIRED',completed_ms=at,reason='MARKET_CLOSED_DATE_ENDED_OR_NOT_REVALIDATED')
                expired.append((canonical(value),row['key']))
        if expired:
            with self.transaction():self.c.executemany('update meta set value=? where key=?',expired)


class Router:
    """Dispatch only to the actual condition; never fan a frame into every market."""
    def __init__(self, store, selected=None):
        self.store=store; self.counters=Counter()
        self.selected=set(store.markets) if selected is None else set(selected)
        self.per_market={key:Counter() for key in store.markets}

    def feed(self, raw, at):
        if not isinstance(raw,str) or len(raw.encode())>262144:
            self.store.invalidate('INVALID_CATALOGUE_FRAME',at);return
        try:parsed=json.loads(raw)
        except ValueError:
            self.store.invalidate('MALFORMED_JSON_FRAME',at);return
        for message in parsed if isinstance(parsed,list) else [parsed]:
            if not isinstance(message,dict):
                self.store.invalidate('INVALID_CATALOGUE_MESSAGE',at);continue
            condition=str(message.get('market',message.get('condition_id',''))).lower()
            if condition not in self.selected:
                # A known token under a wrong condition must not retain continuity.
                changes=message.get('price_changes',[]) if message.get('event_type')=='price_change' else [message]
                for item in changes if isinstance(changes,list) else []:
                    token=str(item.get('asset_id','')) if isinstance(item,dict) else ''
                    if token in self.store.tokens:self.store.invalidate('CONDITION_MISMATCH',at,token)
                self.counters['unsubscribed_messages']+=1;continue
            reference=self.store.discovery_reference(next(iter(self.store.markets[condition]['tokens'].values())))
            received=self.store.c.execute('select received_ms from inputs where id=?',(reference[0],)).fetchone()[0]
            if at<received:
                self.store.invalidate('BEFORE_GAMMA_RECEIPT',at)
                self.counters['before_metadata_receipt']+=1;continue
            if not self.store.active(condition,at):
                self.counters['closed_or_previous_day']+=1;continue
            process(canonical(message),at,self.store.markets[condition],self.store,self.per_market[condition])
            self.counters['routed_messages']+=1


def discover_city(city, *, clock=None, fetch=None):
    """One bounded Gamma response shared by every bucket of this city."""
    from urllib.request import Request, urlopen
    from urllib.parse import urlencode
    from dataclasses import replace
    from .discovery import GAMMA, select
    clock=clock or (lambda:datetime.now(timezone.utc));fetch=fetch or urlopen
    at=clock();cfg=Config(city=city)
    day=datetime.fromisoformat(cfg.day(at))
    months=('january','february','march','april','may','june','july','august','september','october','november','december')
    slug=f'highest-temperature-in-{EUROPE_CITIES[city][0]}-on-{months[day.month-1]}-{day.day}-{day.year}'
    url=GAMMA+'?'+urlencode({'slug':slug})
    with fetch(Request(url,headers={'User-Agent':'Holy-Weather/0.1 public-observation'}),timeout=12) as response:
        body=response.read(4_000_001);status=response.status
    if status!=200 or len(body)>4_000_000:raise ValueError('GAMMA_HTTP_OR_BODY_BOUND')
    received=clock();doc=json.loads(body)
    receipt=dict(url=url,http=status,requested_at=at.isoformat(),received_at=received.isoformat(),sha256=sha(body),raw_body=body.decode())
    if doc==[]:return [],receipt,[{'reason':'EXACT_EVENT_NOT_PUBLISHED'}]
    if not isinstance(doc,list) or len(doc)!=1 or doc[0].get('slug')!=slug:raise ValueError('EXACT_EVENT_REQUIRED')
    candidates=doc[0].get('markets',[])
    if not isinstance(candidates,list) or len(candidates)>64:raise ValueError('GAMMA_CATALOGUE_BOUND')
    markets=[];skipped=[]
    for item in candidates:
        if not isinstance(item,dict):raise ValueError('INVALID_MARKET_OBJECT')
        bucket=item.get('groupItemTitle','')
        try:
            if item.get('version') not in (None,'v1'):raise ValueError('UNSUPPORTED_MARKET_VERSION')
            markets.append(select(doc,replace(cfg,bucket=bucket),now=received))
        except (ValueError,KeyError,TypeError) as exc:
            skipped.append({'bucket':str(bucket)[:80],'reason':str(exc)[:160]})
    if len(markets)>16:raise ValueError('CITY_MARKET_BOUND')
    if len({m['condition_id'] for m in markets})!=len(markets):raise ValueError('DUPLICATE_MARKET_IDENTITY')
    return markets,receipt,skipped


class DayDiscovery:
    """Revalidate on restart/date change; bounded retries only for known unavailability."""
    def __init__(self, store, discover, clock):
        self.store=store;self.discover=discover;self.clock=clock
        self.days={};self.selected=set();self.results={};self.retry={}

    def due(self):
        at=self.clock()
        return local_days(at)!=self.days or any(at>=v for v in self.retry.values())

    def refresh(self, stopped=lambda:False):
        s=self.store
        for city,day in local_days(self.clock()).items():
            if stopped():break
            at=self.clock()
            # Date may change while earlier cities are being fetched.
            day=local_days(at)[city]
            if self.days.get(city)==day and at<self.retry.get(city,float('inf')):continue
            self.days[city]=day;self.retry.pop(city,None)
            station=EUROPE_CITIES[city][1]
            self.selected={c for c in self.selected if s.markets[c]['station']!=station}
            s.expire_closed_deliveries(at);s.retire(at)
            self.selected.intersection_update(s.markets)
            key=DAY_PREFIX+city
            row=s.c.execute('select value from meta where key=?',(key,)).fetchone()
            previous=json.loads(row[0]) if row else {}
            if previous.get('day','')>day:
                self.results[city]={'verified_buckets':0,'reason':'LOCAL_DATE_REGRESSION'};continue
            same=previous.get('day')==day
            if same and previous.get('status') in ('IN_PROGRESS','BLOCKED'):
                self.results[city]={'verified_buckets':0,'reason':'DISCOVERY_'+previous['status']};continue
            attempts=previous.get('attempts',0) if same and previous.get('status')!='VERIFIED' else 0
            if attempts>=3:
                self.results[city]={'verified_buckets':0,'reason':'DISCOVERY_ATTEMPTS_EXHAUSTED'};continue
            if same and previous.get('next_ms',0)>at:
                self.retry[city]=previous['next_ms'];continue
            state=dict(day=day,attempts=attempts+1,status='IN_PROGRESS',at_ms=at)
            s.set_meta(key,state)
            try:
                markets,receipt,skipped=self.discover(city)
                if receipt:
                    state['receipt_input_id']=s.retain_discovery(receipt)
                state['skipped']=skipped
                if len(markets)>16:raise ValueError('CITY_MARKET_BOUND')
                received=int(datetime.fromisoformat(receipt['received_at']).timestamp()*1000) if markets else at
                for market in markets:
                    if (market['station']!=station or market['timezone']!=EUROPE_CITIES[city][2]
                            or market['date']!=day or local_days(self.clock())[city]!=day
                            or local_days(received)[city]!=day or received>self.clock()):
                        raise ValueError('DISCOVERY_CITY_DATE_OR_RECEIPT_MISMATCH')
                for market in markets:s.bind(market,receipt)
                self.selected.update(m['condition_id'].lower() for m in markets)
                invalid=any(x.get('reason') not in ('CONTRACT_NOT_OPEN','EXACT_EVENT_NOT_PUBLISHED') for x in skipped)
                state.update(status='VERIFIED' if markets else 'BLOCKED' if invalid else 'UNAVAILABLE')
                self.results[city]={'verified_buckets':len(markets),'skipped':skipped}
            except (OSError,ValueError,KeyError,TypeError) as exc:
                from urllib.error import HTTPError
                blocked=isinstance(exc,HTTPError) and exc.code in (401,403,429)
                # Rule/identity/parser errors are not transient transport errors.
                blocked=blocked or not isinstance(exc,OSError)
                state.update(status='BLOCKED' if blocked else 'UNAVAILABLE',reason=type(exc).__name__+':'+str(exc)[:160])
                self.results[city]={'verified_buckets':0,'reason':state['reason']}
            if state['status']=='UNAVAILABLE' and state['attempts']<3:
                state['next_ms']=self.clock()+300000
                self.retry[city]=state['next_ms']
            s.set_meta(key,state)
        s.expire_closed_deliveries(self.clock(),self.selected)


def observe_catalogue(cfg, *, connect=None, discover=discover_city, transport=None, token=None,
                      clock=None, monotonic=time.monotonic, stop=None, wait=None):
    """Finite session with local-day revalidation; no service or extended deadline."""
    import threading
    from websockets.exceptions import WebSocketException
    from .app import WS,millis
    from .telegram import Sender
    if connect is None:
        from websockets.sync.client import connect
    clock=clock or millis;stop=stop or threading.Event();wait=wait or stop.wait
    if cfg.telegram_enabled and not token:raise ValueError('EXPLICIT_TELEGRAM_TOKEN_REQUIRED')
    sender=Sender(token,**({'transport':transport} if transport else {}),minimum_interval_ms=3100) if cfg.telegram_enabled else None
    result={'cities':{},'state':'PREPARING','telegram_enabled':cfg.telegram_enabled,'weather_enabled':False}
    with CatalogueStore(cfg) as store:
        try:
            deadline=monotonic()+cfg.duration_seconds
            days=DayDiscovery(store,discover,clock)
            stopped=lambda:stop.is_set() or monotonic()>=deadline
            days.refresh(stopped)
            selected=days.selected
            result['cities']=days.results
            router=Router(store,selected)
            result['state']='COMPLETED_WINDOW'
            reconnects=0
            while not stop.is_set() and monotonic()<deadline:
                if days.due():
                    store.gap('LOCAL_DAY_OR_DISCOVERY_REFRESH',clock())
                    days.refresh(stopped)
                    selected=days.selected;result['cities']=days.results
                    router.selected=set(selected)
                    for condition in selected:router.per_market.setdefault(condition,Counter())
                tokens=[t for t,c in store.tokens.items() if c in selected and store.active(c,clock())]
                if not tokens:
                    result['state']='NO_VERIFIED_CURRENT_MARKETS'
                    if wait(min(1,max(0,deadline-monotonic()))):break
                    continue
                result['state']='COMPLETED_WINDOW'
                try:
                    with connect(WS,proxy=None,open_timeout=10,close_timeout=2,ping_interval=10,ping_timeout=20,
                                 max_size=262144,max_queue=16,compression=None) as ws:
                        tokens=[t for t in tokens if store.active(store.tokens[t],clock())]
                        if days.due():continue
                        if not tokens:
                            result['state']='ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED';break
                        store.connection_started(clock())
                        ws.send(canonical({'type':'market','assets_ids':tokens[:16],'custom_feature_enabled':True}))
                        subscribed=min(16,len(tokens));live_tokens=set(tokens[:16]);next_subscription=monotonic()+1
                        last_ping=monotonic();last_pong=last_ping;next_stats=last_ping
                        while not stop.is_set() and monotonic()<deadline:
                            at=clock()
                            if days.due():break
                            if subscribed<len(tokens) and monotonic()>=next_subscription:
                                batch=[t for t in tokens[subscribed:subscribed+16] if store.active(store.tokens[t],at)]
                                if batch:ws.send(canonical({'operation':'subscribe','assets_ids':batch,'custom_feature_enabled':True}))
                                live_tokens.update(batch)
                                subscribed+=16;next_subscription=monotonic()+1
                            if not any(store.active(c,at) for c in selected):
                                result['state']='ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED';break
                            closed=sorted(t for t in live_tokens if not store.active(store.tokens[t],at))
                            for start in range(0,len(closed),16):
                                ws.send(canonical({'operation':'unsubscribe','assets_ids':closed[start:start+16]}))
                            live_tokens.difference_update(closed)
                            store.expire_closed_deliveries(at)
                            if sender:sender.tick(store,at)
                            if monotonic()>=next_stats:
                                store.set_meta('catalogue_counters',{'router':dict(router.counters),'markets':{k:dict(v) for k,v in router.per_market.items()}})
                                next_stats=monotonic()+10
                            if monotonic()-last_pong>25:raise TimeoutError('APPLICATION_HEARTBEAT_TIMEOUT')
                            if monotonic()-last_ping>=10:ws.send('PING');last_ping=monotonic()
                            try:raw=ws.recv(timeout=min(1,max(.01,deadline-monotonic())))
                            except TimeoutError:continue
                            if raw=='PONG':last_pong=monotonic();continue
                            router.feed(raw,clock())
                except (OSError,TimeoutError,WebSocketException) as exc:
                    store.gap('CATALOGUE_NETWORK_GAP',clock())
                    result.update(error=type(exc).__name__,detail=str(exc)[:240])
                    # Denied access / protocol and oversized-frame errors are not
                    # transient outages. Never bypass them through reconnects.
                    from websockets.exceptions import ConnectionClosed
                    close=getattr(exc,'rcvd',None) or getattr(exc,'sent',None)
                    transient=isinstance(exc,(OSError,TimeoutError,ConnectionClosed))
                    if close is not None and close.code in (1008,1009):transient=False
                    if not transient:
                        result['state']='NETWORK_REFUSED_NO_RETRY';break
                    if reconnects>=cfg.max_reconnects:
                        result['state']='NETWORK_GAP_NO_RETRY' if not cfg.max_reconnects else 'RECONNECT_BUDGET_EXHAUSTED';break
                    delay=min(2**reconnects,8,max(0,deadline-monotonic()))
                    if delay<=0:
                        result['state']='DEADLINE_DURING_GAP';break
                    reconnects+=1
                    if wait(delay):break
                    if monotonic()>=deadline:
                        result['state']='DEADLINE_DURING_GAP';break
                    continue
                else:
                    if days.due() or not any(store.active(c,clock()) for c in selected):continue
                    break
            store.gap('CATALOGUE_SESSION_ENDED',clock())
            result['reconnects']=reconnects
            if sender:sender.close(store)
            if stop.is_set():result['state']='SIGNAL_STOP'
            result['counters']={'router':dict(router.counters),'markets':{k:dict(v) for k,v in router.per_market.items()}}
            result['price_events']=store.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]
            result['deliveries']={r[0]:r[1] for r in store.c.execute("select json_extract(value,'$.status'),count(*) from meta where key like 'alert_delivery_v1:%' group by 1")}
            result['sqlite_integrity']=store.c.execute('pragma integrity_check').fetchone()[0]
            result['foreign_key_errors']=len(store.c.execute('pragma foreign_key_check').fetchall())
            result['data_bytes']=store.usage()
            return result
        finally:
            if sender:sender.close(store)


def main():
    import argparse
    import signal
    import threading
    from .telegram import token_from_env
    parser=argparse.ArgumentParser()
    parser.add_argument('--data-dir',required=True)
    parser.add_argument('--seconds',type=int,default=120)
    parser.add_argument('--send',action='store_true')
    parser.add_argument('--price-notification-filter',action='store_true',
                        help='Wait for ask-only bid confirmation and batch same-side bucket updates (30s)')
    args=parser.parse_args();stop=threading.Event()
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:stop.set())
    cfg=CatalogueConfig(data_dir=args.data_dir,duration_seconds=args.seconds,telegram_enabled=args.send,
                        price_notification_filter=args.price_notification_filter)
    result=observe_catalogue(cfg,token=token_from_env() if args.send else None,stop=stop)
    print(json.dumps(result,ensure_ascii=False,sort_keys=True))
    return 0 if result['state']=='COMPLETED_WINDOW' else 2


if __name__=='__main__':raise SystemExit(main())
