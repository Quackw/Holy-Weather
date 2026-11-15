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


@dataclass(frozen=True)
class CatalogueConfig(Config):
    city: str = 'london'
    bucket: str = 'catalogue'
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
            for row in self.c.execute('select value from meta where key like ?', (PREFIX+'%',)):
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
        prior=self.markets.get(condition)
        if prior and prior!=market:raise StorageStop('MARKET_IDENTITY_OR_RULE_DRIFT')
        if condition not in self.markets and len(self.markets)>=MAX_MARKETS:
            raise StorageStop('CATALOGUE_MARKET_BOUND')
        if any(t in self.tokens and self.tokens[t]!=condition for t in market['tokens'].values()):
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

    def market_for_token(self, token):
        return self.markets[self.tokens[token]]

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
        market=self.markets[condition]
        today=datetime.fromtimestamp(at/1000,timezone.utc).astimezone(ZoneInfo(market['timezone'])).date().isoformat()
        return today==market['date'] and not self.c.execute('select 1 from meta where key=?',('market_closed:'+condition,)).fetchone()

    def expire_closed_deliveries(self, at):
        rows=self.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' and json_extract(value,'$.status')='PENDING'").fetchall()
        expired=[]
        for row in rows:
            value=json.loads(row['value'])
            event=self.c.execute('select token from events where id=?',(value['event_id'],)).fetchone()
            if event and not self.active(self.tokens[event[0]],at):
                value.update(status='EXPIRED',completed_ms=at,reason='MARKET_CLOSED_OR_LOCAL_DATE_ENDED')
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
    receipt=dict(url=url,http=status,requested_at=at.isoformat(),received_at=received.isoformat(),sha256=sha(body),raw_body=body.decode())
    return markets,receipt,skipped


def observe_catalogue(cfg, *, connect=None, discover=discover_city, transport=None, token=None,
                      clock=None, monotonic=time.monotonic, stop=None):
    """One finite session. No reconnect, rollover, service or credential discovery."""
    import threading
    from websockets.exceptions import WebSocketException
    from .app import WS,millis
    from .telegram import Sender
    if connect is None:
        from websockets.sync.client import connect
    clock=clock or millis;stop=stop or threading.Event()
    if cfg.telegram_enabled and not token:raise ValueError('EXPLICIT_TELEGRAM_TOKEN_REQUIRED')
    sender=Sender(token,**({'transport':transport} if transport else {}),minimum_interval_ms=3100) if cfg.telegram_enabled else None
    result={'cities':{},'state':'PREPARING','telegram_enabled':cfg.telegram_enabled,'weather_enabled':False}
    with CatalogueStore(cfg) as store:
        try:
            # Reopening requires the same pinned catalogue; discovery revalidates it.
            selected=set()
            for city in EUROPE_CITIES:
                if stop.is_set():break
                try:
                    markets,receipt,skipped=discover(city)
                    for market in markets:store.bind(market,receipt);selected.add(market['condition_id'].lower())
                    result['cities'][city]={'verified_buckets':len(markets),'skipped':skipped}
                except (OSError,ValueError,KeyError,TypeError) as exc:
                    result['cities'][city]={'verified_buckets':0,'reason':type(exc).__name__+':'+str(exc)[:160]}
            if not selected:
                result['state']='NO_VERIFIED_OPEN_MARKETS';return result
            # Do not resubscribe historical or currently unverified entries on restart.
            tokens=[t for t,c in store.tokens.items() if c in selected and store.active(c,clock())]
            if not tokens:
                result['state']='NO_CURRENT_LOCAL_DATE_MARKETS';return result
            router=Router(store,selected)
            deadline=monotonic()+cfg.duration_seconds
            result['state']='COMPLETED_WINDOW'
            try:
                with connect(WS,proxy=None,open_timeout=10,close_timeout=2,ping_interval=10,ping_timeout=20,
                             max_size=262144,max_queue=16,compression=None) as ws:
                    store.connection_started(clock())
                    ws.send(canonical({'type':'market','assets_ids':tokens[:16],'custom_feature_enabled':True}))
                    subscribed=16;next_subscription=monotonic()+1
                    last_ping=monotonic();last_pong=last_ping;next_stats=last_ping
                    while not stop.is_set() and monotonic()<deadline:
                        at=clock()
                        if subscribed<len(tokens) and monotonic()>=next_subscription:
                            ws.send(canonical({'operation':'subscribe','assets_ids':tokens[subscribed:subscribed+16],'custom_feature_enabled':True}))
                            subscribed+=16;next_subscription=monotonic()+1
                        if not any(store.active(c,at) for c in selected):
                            result['state']='ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED';break
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
                result.update(state='NETWORK_GAP_NO_RETRY',error=type(exc).__name__,detail=str(exc)[:240])
            finally:
                store.gap('CATALOGUE_SESSION_ENDED',clock())
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
    args=parser.parse_args();stop=threading.Event()
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:stop.set())
    cfg=CatalogueConfig(data_dir=args.data_dir,duration_seconds=args.seconds,telegram_enabled=args.send)
    result=observe_catalogue(cfg,token=token_from_env() if args.send else None,stop=stop)
    print(json.dumps(result,ensure_ascii=False,sort_keys=True))
    return 0 if result['state']=='COMPLETED_WINDOW' else 2


if __name__=='__main__':raise SystemExit(main())
