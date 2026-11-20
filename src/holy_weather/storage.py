"""One owner, transactional inputs/decisions/state, bounded private SQLite."""
from contextlib import contextmanager
from copy import deepcopy
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import time
import zlib
from .core import canonical, evolve, reset, sha, quote_clock_status, cached_price

APP_ID = 0x48574D31
SCHEMA = '''
CREATE TABLE market(id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL, receipt TEXT NOT NULL);
CREATE TABLE inputs(id INTEGER PRIMARY KEY, identity TEXT NOT NULL UNIQUE, received_ms INTEGER NOT NULL, raw BLOB NOT NULL, raw_sha TEXT NOT NULL);
CREATE TABLE samples(input_id INTEGER PRIMARY KEY REFERENCES inputs(id), token TEXT NOT NULL, kind TEXT NOT NULL, source_ms INTEGER NOT NULL, received_ms INTEGER NOT NULL, bid TEXT, ask TEXT, bid_size TEXT, ask_size TEXT, trade TEXT, trade_size TEXT, snapshot_trade TEXT, status TEXT NOT NULL);
CREATE TABLE state(token TEXT PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE latest(token TEXT NOT NULL, kind TEXT NOT NULL, input_id INTEGER NOT NULL REFERENCES inputs(id), PRIMARY KEY(token,kind));
CREATE TABLE events(id TEXT PRIMARY KEY, input_id INTEGER NOT NULL REFERENCES inputs(id), token TEXT NOT NULL, received_ms INTEGER NOT NULL, type TEXT NOT NULL, status TEXT NOT NULL, reason TEXT, payload TEXT NOT NULL);
CREATE TABLE event_inputs(event_id TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE, input_id INTEGER NOT NULL REFERENCES inputs(id), PRIMARY KEY(event_id,input_id));
CREATE TABLE diagnostics(id INTEGER PRIMARY KEY, received_ms INTEGER NOT NULL, category TEXT NOT NULL, reason TEXT NOT NULL);
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE INDEX input_time ON inputs(received_ms);
CREATE INDEX event_time ON events(received_ms);
'''


class StorageStop(RuntimeError):
    pass


class Store:
    active_delivery_source = "meta"

    def __init__(self, cfg, *, session=True):
        self.cfg, self.closed = cfg, False
        self._rule_hash = cfg.rule_hash()  # Config is frozen for this sole writer.
        self._research_cache = None
        if getattr(cfg,'region',None)=='europe':
            from .research_cache import ResearchCache
            self._research_cache = ResearchCache()
        self.seen_tops={}
        self.connected=False
        self.ready_tokens=set()
        self.price_alert_ready=set()
        self.pending_baselines={}
        self.root = Path(cfg.data_dir).absolute()
        if self.root.is_symlink():
            raise StorageStop('SYMLINK_DATA_DIRECTORY')
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        marker = self.root / '.holy-weather-owned'
        if not marker.exists():
            if any(self.root.iterdir()):
                raise StorageStop('NONEMPTY_UNOWNED_DIRECTORY')
            marker.write_text('HOLY_WEATHER_STAGE1\n')
        if marker.is_symlink() or marker.read_text() != 'HOLY_WEATHER_STAGE1\n':
            raise StorageStop('OWNERSHIP_MARKER')
        if (self.root/'.writer.lock').is_symlink():
            raise StorageStop('SYMLINK_WRITER_LOCK')
        self.lock = (self.root / '.writer.lock').open('a')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close(); raise StorageStop('WRITER_ALREADY_RUNNING')
        self.db_path = self.root/'monitor.sqlite3'
        self.reserve = min(8*1024*1024, cfg.storage_bytes//2)
        try:
            self._guard()
        except Exception:
            self.lock.close();raise
        new = not self.db_path.exists()
        if self.db_path.is_symlink():
            raise StorageStop('SYMLINK_DATABASE')
        self.c = sqlite3.connect(self.db_path, timeout=1, isolation_level=None)
        self.c.row_factory = sqlite3.Row
        try:
            if not new:
                app_id=self.c.execute('pragma application_id').fetchone()[0]
                version=self.c.execute('pragma user_version').fetchone()[0]
                empty=not self.c.execute("select name from sqlite_master where name not like 'sqlite_%'").fetchall()
                if app_id==0 and version==0 and empty:
                    new=True  # proven empty, owned bootstrap interrupted before commit
                elif app_id!=APP_ID:
                    raise StorageStop('FOREIGN_DATABASE')
                elif version!=1:
                    raise StorageStop('UNKNOWN_SCHEMA')
            if self.c.execute('pragma quick_check').fetchone()[0]!='ok':
                raise StorageStop('SQLITE_INTEGRITY_CHECK_FAILED')
            self.c.execute('pragma foreign_keys=ON')
            self.c.execute('pragma journal_mode=WAL')
            self.c.execute('pragma synchronous=FULL')
            self.c.execute('pragma temp_store=MEMORY')
            self.c.execute('pragma cache_size=-4096')
            self.c.execute('pragma wal_autocheckpoint=128')
            self.c.execute('pragma journal_size_limit=1048576')
            pages = (cfg.storage_bytes-self.reserve)//self.c.execute('pragma page_size').fetchone()[0]
            self.c.execute(f'pragma max_page_count={pages}')
            if new:
                self.c.executescript('BEGIN;'+SCHEMA+f'PRAGMA application_id={APP_ID};PRAGMA user_version=1;COMMIT;')
            if self.c.execute('pragma user_version').fetchone()[0] != 1:
                raise StorageStop('UNKNOWN_SCHEMA')
            current = self.c.execute("select value from meta where key='rule_hash'").fetchone()
            if current and current[0] != cfg.rule_hash():
                raise StorageStop('CONFIG_RULE_DRIFT_USE_NEW_DATA_DIRECTORY')
            self.c.execute("insert or ignore into meta values('rule_hash',?)", (cfg.rule_hash(),))
            from .history import Seen
            self.history_seen=Seen(self.c)
            if session:
                self.gap('PROCESS_START_OR_RESTART', int(time.time()*1000))
        except Exception:
            self.c.close(); self.lock.close(); raise

    def usage(self):
        paths = list(self.root.iterdir())
        if len(paths) > 32 or any(p.is_symlink() or not p.is_file() for p in paths):
            raise StorageStop('UNEXPECTED_DATA_DIRECTORY_CONTENT')
        return sum(p.stat().st_size for p in paths)

    def _guard(self):
        if self.usage() >= self.cfg.storage_bytes-self.reserve:
            raise StorageStop('STORAGE_BUDGET_CAPTURE_STOPPED')

    def _checkpoint_wal(self):
        wal = self.root/'monitor.sqlite3-wal'
        if wal.exists() and wal.stat().st_size > 1024*1024:
            self.c.execute('pragma wal_checkpoint(truncate)')

    def _flush_receive(self):
        batch = getattr(self, '_receive_batch', None)
        if not batch or batch['broken'] or not batch['pending']:
            return
        if batch['active']:
            raise StorageStop('RECEIVE_COMMIT_INSIDE_INPUT')
        try:
            self.c.execute('commit')
        except BaseException:
            batch['broken'] = True
            if self.c.in_transaction:
                self.c.execute('rollback')
            raise
        finally:
            batch['pending'] = batch['bytes'] = 0
        self._checkpoint_wal()
        self._guard()

    @contextmanager
    def receive_batch(self):
        """EU receive slice only. No waits or sender effects inside this scope."""
        if getattr(self.cfg, 'region', None) != 'europe':
            yield
            return
        if getattr(self, '_receive_batch', None) or self.c.in_transaction:
            raise StorageStop('NESTED_RECEIVE_BATCH')
        batch = dict(pending=0, bytes=0, active=False, broken=False)
        self._receive_batch = batch
        try:
            yield
        finally:
            try:
                if not batch['broken']:
                    self._flush_receive()
            finally:
                self._receive_batch = None

    @contextmanager
    def _input_transaction(self, sample, raw_bytes):
        batch = getattr(self, '_receive_batch', None)
        budget = None
        if batch and sample['kind'] != 'market_resolved' and raw_bytes <= 16*1024:
            row = self.c.execute('select length(cast(payload as blob)) from state where token=?',
                                 (sample['token'],)).fetchone()
            state_bytes = row[0] if row else 0
            if state_bytes <= 64*1024:
                budget = raw_bytes + state_bytes + 16*1024
        if budget is None:
            with self.transaction():
                yield
            return
        if batch['bytes'] + budget > 256*1024:
            self._flush_receive()
        try:
            self._guard()
        except BaseException:
            batch['broken'] = True
            if self.c.in_transaction:
                self.c.execute('rollback')
            batch['pending'] = batch['bytes'] = 0
            raise
        try:
            if not self.c.in_transaction:
                self.c.execute('begin immediate')
            self.c.execute('savepoint hw_receive_input')
        except BaseException:
            batch['broken'] = True
            batch['pending'] = batch['bytes'] = 0
            if self.c.in_transaction:
                self.c.execute('rollback')
            raise
        batch['active'] = True
        try:
            yield
        except BaseException:
            try:
                self.c.execute('rollback to hw_receive_input')
                self.c.execute('release hw_receive_input')
            except BaseException:
                batch['broken'] = True
                if self.c.in_transaction:
                    self.c.execute('rollback')
                raise
            finally:
                batch['active'] = False
            if batch['pending']:
                self._flush_receive()
            elif self.c.in_transaction:
                self.c.execute('rollback')
            raise
        else:
            try:
                self.c.execute('release hw_receive_input')
            except BaseException:
                batch['broken'] = True
                batch['pending'] = batch['bytes'] = 0
                if self.c.in_transaction:
                    self.c.execute('rollback')
                raise
            finally:
                batch['active'] = False
            batch['pending'] += 1
            batch['bytes'] += budget
            wal = self.root/'monitor.sqlite3-wal'
            if (batch['pending'] >= 4 or
                    wal.exists() and wal.stat().st_size > 1024*1024):
                self._flush_receive()

    @contextmanager
    def transaction(self):
        # Queue claim/ACK, GAP, metadata and weather writes are durable barriers.
        self._flush_receive()
        self._guard(); self.c.execute('begin immediate')
        try:
            yield
            self.c.execute('commit')
        except Exception:
            if self.c.in_transaction:
                self.c.execute('rollback')
            raise
        self._checkpoint_wal()

    def bind(self, market, receipt):
        with self.transaction():
            prior = self.c.execute('select payload from market where id=1').fetchone()
            # Metadata receipt times may refresh. Contract identity/rules cannot.
            if prior and json.loads(prior[0]) != market:
                raise StorageStop('MARKET_IDENTITY_OR_RULE_DRIFT')
            summary={k:v for k,v in receipt.items()if k!='raw_body'}
            self.c.execute('insert or replace into market values(1,?,?)', (canonical(market), canonical(summary)))
            raw=canonical(receipt).encode();identity='GAMMA:'+sha(raw)
            at=int(__import__('datetime').datetime.fromisoformat(receipt['received_at'].replace('Z','+00:00')).timestamp()*1000) if receipt.get('received_at') else int(time.time()*1000)
            self.c.execute('insert or ignore into inputs(identity,received_ms,raw,raw_sha)values(?,?,?,?)',
                           (identity,at,zlib.compress(raw,3),sha(raw)))
            ident=self.c.execute('select id from inputs where identity=?',(identity,)).fetchone()[0]
            self.c.execute('insert or replace into latest values(?,?,?)',('__MARKET__','discovery',ident))

    def market_for_token(self, token):
        row=self.c.execute('select payload from market where id=1').fetchone()
        market=json.loads(row[0]) if row else None
        if not market or token not in market['tokens'].values():
            raise StorageStop('TOKEN_WITHOUT_BOUND_MARKET')
        return market

    def discovery_reference(self, token):
        return self.c.execute("select input_id from latest where token='__MARKET__' and kind='discovery'").fetchone()

    def closed_key(self, sample):
        return 'market_closed'

    def diagnostic(self, category, reason, at):
        with self.transaction():
            self.c.execute('insert into diagnostics(received_ms,category,reason) values(?,?,?)', (at, category, reason[:300]))
            self.c.execute('delete from diagnostics where id not in (select id from diagnostics order by id desc limit 512)')

    def connection_started(self, at):
        self.gap('CONNECTION_BASELINE', at)
        self.connected=True
        bound=self.c.execute('select payload from market where id=1').fetchone()
        if bound:
            self.pending_baselines={t:time.monotonic() for t in json.loads(bound[0])['tokens'].values()}

    def gap(self, reason, at):
        self.connected=False
        self.ready_tokens.clear()
        self.price_alert_ready.clear()
        self.pending_baselines.clear()
        previous_tops=dict(self.seen_tops)
        self.seen_tops.clear()
        with self.transaction():
            from .alert_queue import settle_waits
            settle_waits(self,at,gap=reason)
            for row in self.c.execute('select token,payload from state').fetchall():
                state=reset(json.loads(row['payload']))
                state['high_source']=max(state['high_source'],previous_tops.get(row['token'],{}).get('source_ms',-1))
                self.c.execute('update state set payload=? where token=?', (canonical(state), row['token']))
            self.c.execute('insert into diagnostics(received_ms,category,reason) values(?,?,?)', (at, 'GAP', reason[:300]))
            self.c.execute('delete from diagnostics where id not in (select id from diagnostics order by id desc limit 512)')

    def invalidate(self,reason,at,token=None):
        """Data continuity failed; transport may still be open. Await a new baseline."""
        bound=self.c.execute('select payload from market where id=1').fetchone()
        targets={token} if token else ({r[0] for r in self.c.execute('select token from state')} |
                  (set(json.loads(bound[0])['tokens'].values()) if bound else set()))
        with self.transaction():
            for target in targets:
                from .alert_queue import settle_waits
                settle_waits(self,at,gap=reason,token=target)
                row=self.c.execute('select payload from state where token=?',(target,)).fetchone()
                if row:
                    state=reset(json.loads(row[0]))
                    state['high_source']=max(state['high_source'],self.seen_tops.get(target,{}).get('source_ms',-1))
                    self.c.execute('update state set payload=? where token=?',(canonical(state),target))
            self.c.execute('insert into diagnostics(received_ms,category,reason) values(?,?,?)',
                           (at,'GAP',('DATA:'+reason+(':'+token if token else ':ALL'))[:300]))
            self.c.execute('delete from diagnostics where id not in (select id from diagnostics order by id desc limit 512)')
        for target in targets:
            self.price_alert_ready.discard(target)
            self.ready_tokens.discard(target);self.seen_tops.pop(target,None)
            if self.connected:self.pending_baselines.setdefault(target,time.monotonic())

    def recovery_due(self,seconds):
        return self.connected and bool(self.pending_baselines) and time.monotonic()-min(self.pending_baselines.values())>=seconds

    def known_duplicate(self,sample):
        return (self.seen_tops.get(sample.get('token'),{}).get('identity')==sample['id'] or
                self.history_seen.contains(sample['id']) or
                bool(self.c.execute('select 1 from inputs i join samples s on s.input_id=i.id where i.identity=?',(sample['id'],)).fetchone()))

    def quote_status(self, research_status):
        # evolve reached this research-only bound after validating the quote's
        # clocks/order/ask. Keep its failure in samples; PRICE validates bid too.
        return 'FRESH' if self.cfg.price_alert_enabled and research_status=='ENGINE_POINT_BOUND' else research_status

    def apply(self, sample):
        raw = canonical(sample['raw']).encode()
        if len(raw) > 262144:
            raise ValueError('INPUT_BOUND')
        if self.history_seen.contains(sample['id']):
            return {'status':'DUPLICATE','events':[],'raw_bytes':0,'compressed_bytes':0}
        with self._input_transaction(sample, len(raw)):
            prior = self.c.execute('select id from inputs where identity=?', (sample['id'],)).fetchone()
            if prior:
                return {'status': 'DUPLICATE', 'events': [], 'raw_bytes':0,'compressed_bytes':0}
            compressed=zlib.compress(raw,3)
            cur = self.c.execute('insert into inputs(identity,received_ms,raw,raw_sha) values(?,?,?,?)',
                                 (sample['id'], sample['received_ms'], compressed, sha(raw)))
            input_id = cur.lastrowid
            if sample['kind'] == 'market_resolved':
                from .alert_queue import settle_waits
                condition=str(sample['raw'].get('market',sample['raw'].get('condition_id',''))).lower()
                waiting=self.c.execute("select distinct json_extract(value,'$.token') from meta where key like 'alert_delivery_v1:%' and json_extract(value,'$.status')='WAITING_BID'").fetchall()
                for row in waiting:
                    if self.market_for_token(row[0])['condition_id'].lower()==condition:
                        settle_waits(self,sample['received_ms'],gap='MARKET_RESOLVED',token=row[0])
                self.c.execute("insert or replace into meta values(?,'true')",(self.closed_key(sample),))
                return {'status': 'MARKET_RESOLVED', 'events': []}
            if self.c.execute("select 1 from meta where key=?",(self.closed_key(sample),)).fetchone():
                return {'status': 'AFTER_RESOLUTION', 'events': []}
            sample = dict(sample, input_id=input_id)
            row = self.c.execute('select payload from state where token=?', (sample['token'],)).fetchone()
            cache=getattr(self,'_research_cache',None)
            if cache:previous,hint=cache.take(sample['token'],row[0] if row else None)
            else:previous,hint=(json.loads(row[0]) if row else None),None
            quote=sample['kind']!='last_trade_price'
            awaiting=quote and sample['token'] not in self.ready_tokens
            if awaiting and previous:
                previous=reset(previous)
            europe = getattr(self.cfg,'region',None)=='europe'
            state, events, status = evolve(previous, sample, self.cfg, owned=europe and not awaiting,
                                          decimal_parser=cached_price if europe else None, _extrema=hint)
            if status=='ENGINE_POINT_BOUND' and self.cfg.price_alert_enabled:
                # The accepted source clock must survive the research reset/GAP.
                state['high_source']=sample['source_ms']
            if awaiting and sample['kind']=='price_change' and status=='FRESH':
                state, events, status = reset(previous if previous is not None else state), [], 'AWAITING_BASELINE'
            state_raw=cache.encode(state,hint) if cache else canonical(state)
            self.c.execute('insert or replace into state values(?,?)', (sample['token'], state_raw))
            keys = ('token','kind','source_ms','received_ms','bid','ask','bid_size','ask_size','trade','trade_size','snapshot_trade')
            self.c.execute('insert into samples values(?,?,?,?,?,?,?,?,?,?,?,?,?)', (input_id, *(sample[k] for k in keys), status))
            category = 'trade' if sample['kind']=='last_trade_price' else 'quote'
            self.c.execute('insert or replace into latest values(?,?,?)', (sample['token'], category, input_id))
            for event in events:
                if self.cfg.weather_context:
                    from .weather import context
                    event['weather']=context(self.weather_state(),sample['received_ms'],min(p['received_ms'] for p in event['evidence']))
                if self.cfg.lamp_context:
                    from .lamp import context as lamp_context
                    market=json.loads(self.c.execute('select payload from market where id=1').fetchone()[0])
                    event['lamp']=lamp_context(self.lamp_state(),event['weather'],market,sample['received_ms'])
                    event['prices']=self.price_context(sample['received_ms'])
                ident = sha([sample['id'], self._rule_hash, event['type'], event.get('level'), event.get('direction'), event['status']])
                self.c.execute('insert into events values(?,?,?,?,?,?,?,?)', (ident, input_id, sample['token'], sample['received_ms'], event['type'], event['status'], event.get('reason'), canonical(event)))
                for ref in {p['input_id'] for p in event['evidence']}:
                    self.c.execute('insert into event_inputs values(?,?)', (ident, ref))
                for ref in set(event.get('weather',{}).get('input_ids',[])) | set(event.get('lamp',{}).get('input_ids',[])):
                    self.c.execute('insert or ignore into event_inputs values(?,?)',(ident,ref))
                for price in event.get('prices',{}).values():
                    if price.get('input_id'):
                        self.c.execute('insert or ignore into event_inputs values(?,?)',(ident,price['input_id']))
                discovery=self.discovery_reference(sample['token'])
                if discovery:self.c.execute('insert or ignore into event_inputs values(?,?)',(ident,discovery[0]))
            research_status=status
            status=self.quote_status(research_status)
            if self.cfg.price_alert_enabled:
                from .price_alert import record
                events += record(self,sample,status)
                from .alert_queue import settle_waits
                settle_waits(self,sample['received_ms'],sample=sample,status=status)
            result={'status': status, 'research_status':research_status, 'events': events,'raw_bytes':len(raw),'compressed_bytes':len(compressed)}
        if cache:cache.keep(sample['token'],state_raw,state,hint)
        if cache:
            # Only evidence points can alias the privately owned research state.
            for event in result['events']:
                if event.get('evidence'):event['evidence']=deepcopy(event['evidence'])
        # Readiness follows this input's atomic SQL writes; the receiver flushes
        # pending inputs before housekeeping or any sender side effect.
        if quote:
            if status=='FRESH' and sample['kind'] in ('book','best_bid_ask'):
                self.ready_tokens.add(sample['token'])
                self.pending_baselines.pop(sample['token'],None)
            elif status!='FRESH':
                self.ready_tokens.discard(sample['token'])
                if self.connected:self.pending_baselines.setdefault(sample['token'],time.monotonic())
        return result

    def lamp_state(self,market=None):
        key='lamp_context_v1'
        if getattr(self.cfg,'us_lamp_enabled',False):
            if market is None:raise ValueError('LAMP_MARKET_REQUIRED')
            key+=':'+market['station']+':'+market['date']
        row=self.c.execute('select value from meta where key=?',(key,)).fetchone()
        return json.loads(row[0]) if row else {}

    def lamp_status(self):
        return self.lamp_state().get('status','NOT_RECEIVED')

    def record_lamp(self,receipt,market,available_ms,*,markets=None):
        from .lamp import update
        regional=getattr(self.cfg,'us_lamp_enabled',False)
        if not self.cfg.lamp_context and not regional:raise ValueError('LAMP_NOT_ENABLED')
        key='lamp_context_v1'+(':'+market['station']+':'+market['date'] if regional else '')
        previous_lamp=self.lamp_state(market)
        if (self.cfg.lamp_alert_enabled or regional) and available_ms<previous_lamp.get("poll_available_ms",-1):
            raise ValueError("LAMP_ALERT_CLOCK_REVERSED")
        state=update(previous_lamp,receipt,market,available_ms)
        raw=canonical(dict(receipt=receipt,available_ms=available_ms,status=state['status'],duplicate=state['duplicate'])).encode()
        compressed=zlib.compress(raw,3)
        with self.transaction():
            used=self.c.execute("select coalesce(sum(length(raw)),0) from inputs where identity like 'LAMP:%'").fetchone()[0]
            if used+len(compressed)>1024*1024:
                self.c.execute('insert or replace into meta values(?,?)',
                               (key,canonical(dict(previous_lamp,status='LAMP_STATE_BOUND'))))
                return
            identity='LAMP:'+sha(raw)
            self.c.execute('insert or ignore into inputs(identity,received_ms,raw,raw_sha) values(?,?,?,?)',
                           (identity,receipt['received_ms'],compressed,sha(raw)))
            ident=self.c.execute('select id from inputs where identity=?',(identity,)).fetchone()[0]
            if not state['duplicate']:state['input_id']=ident
            state['poll_input_id']=ident
            suffix=':'+key if regional else ''
            labels=('__LAMP__'+suffix,'__LAMP_POLL__'+suffix)
            self.c.execute('delete from latest where token in (?,?)',labels)
            for label,ref in zip(labels,(state['input_id'],ident)):
                self.c.execute('insert into latest values(?,?,?)',(label,'lamp',ref))
            self.c.execute('insert or replace into meta values(?,?)',(key,canonical(state)))
            if regional and self.cfg.catalogue_metar_enabled:
                from .city_peak import record
                record(self, [market] if markets is None else markets, state, available_ms)
            elif self.cfg.lamp_alert_enabled or regional:
                from .lamp_alert import record
                for contract in [market] if markets is None else markets:
                    if (contract['station'],contract['date'])!=(market['station'],market['date']):
                        raise ValueError('LAMP_CONTRACT_STATION_DATE_MISMATCH')
                    record(self,contract,state,available_ms,verified_markets=markets or (),
                           key='lamp_alert_v1:'+contract['condition_id'].lower() if regional else 'lamp_alert_v1')

    def price_context(self,at_ms,market=None):
        if market is None:
            bound=self.c.execute('select payload from market where id=1').fetchone()
            if not bound:return {}
            market=json.loads(bound[0])
        snapshot=self.observation_snapshot(now_ms=at_ms,market=market)
        rows={r['token']:r for r in snapshot['latest'] if r['kind']!='last_trade_price'}
        result={}
        for side,token in market['tokens'].items():
            r=rows.get(token)
            if not r or r['received_ms']>at_ms:
                result[side]={'ask':None,'status':'NOT_AVAILABLE'}
            else:
                result[side]={k:r[k]for k in ('input_id','bid','ask','source_ms','received_ms','freshness_now','status','quote_usable_now')}
            result[side]['observation_state']=snapshot['observation']['sides'][side]
        return result

    def forecast_card(self,at_ms):
        from .weather import context as weather_context
        from .lamp import context as lamp_context
        bound=self.c.execute('select payload from market where id=1').fetchone()
        if not bound:return {'status':'MARKET_NOT_BOUND'}
        market=json.loads(bound[0]);weather=weather_context(self.weather_state(),at_ms,at_ms)
        return dict(at_ms=at_ms,kind='INFORMATIONAL_CONTEXT_NOT_TRADE_SIGNAL',weather=weather,
                    lamp=lamp_context(self.lamp_state(),weather,market,at_ms),prices=self.price_context(at_ms))

    def weather_state(self):
        row=self.c.execute("select value from meta where key='weather_context_v1'").fetchone()
        return json.loads(row[0]) if row else {}

    def weather_status(self):
        return self.weather_state().get('status','NOT_RECEIVED')

    def record_weather(self, receipt, market, available_ms):
        from .weather import update
        if not self.cfg.weather_context:
            raise ValueError('WEATHER_NOT_ENABLED')
        state,flags=update(self.weather_state(),receipt,market,available_ms)
        raw=canonical(dict(receipt=receipt,available_ms=available_ms,report_flags=flags)).encode()
        compressed=zlib.compress(raw,3)
        with self.transaction():
            used=self.c.execute("select coalesce(sum(length(raw)),0) from inputs where identity like 'AWC:%'").fetchone()[0]
            if used+len(compressed)>4*1024*1024:
                # Stop weather capture only, never silently expand the shared budget.
                self.c.execute("insert or replace into meta values('weather_context_v1',?)",
                               (canonical(dict(self.weather_state(),status='WEATHER_STATE_BOUND')),))
                return
            identity='AWC:'+sha(raw)
            self.c.execute('insert or ignore into inputs(identity,received_ms,raw,raw_sha) values(?,?,?,?)',
                           (identity,receipt['received_ms'],compressed,sha(raw)))
            input_id=self.c.execute('select id from inputs where identity=?',(identity,)).fetchone()[0]
            state['poll_input_id']=input_id
            for report in state.get('records',[]):
                if 'input_id' not in report:report['input_id']=input_id
                if report.get('conflict') and report.get('conflict_input_id') is None:report['conflict_input_id']=input_id
            # Protect the current projection from explicit input retention, using
            # the existing latest index. Referenced event evidence is immutable.
            self.c.execute("delete from latest where kind='weather'")
            for report in state.get('records',[]):
                self.c.execute('insert into latest values(?,?,?)',
                               ('__WEATHER__:'+str(report['observed_ms']),'weather',report['input_id']))
                if report.get('conflict_input_id'):
                    self.c.execute('insert into latest values(?,?,?)',
                                   ('__WEATHER_CONFLICT__:'+str(report['observed_ms']),'weather',report['conflict_input_id']))
            self.c.execute('insert into latest values(?,?,?)',('__WEATHER_POLL__','weather',input_id))
            self.c.execute("insert or replace into meta values('weather_context_v1',?)",(canonical(state),))

    def set_meta(self, key, value):
        with self.transaction():
            self.c.execute('insert or replace into meta values(?,?)', (key, canonical(value)))

    def reject(self, raw, reason, at, token=None, *, invalidate_state=True):
        value=canonical(raw).encode()
        if len(value)>262144:
            value=canonical({'rejected_sha256':sha(value),'reason':'REJECTED_INPUT_TOO_LARGE'}).encode()
        with self.transaction():
            if not self.history_seen.contains(sha(value)):
                self.c.execute('insert or ignore into inputs(identity,received_ms,raw,raw_sha) values(?,?,?,?)',
                               (sha(value),at,zlib.compress(value,3),sha(value)))
        if invalidate_state:self.invalidate('INVALID_INPUT:'+reason,at,token)

    def stats(self, *, now_ms=None):
        now_ms=int(time.time()*1000) if now_ms is None else now_ms
        result = {'inputs': self.c.execute('select count(*) from inputs').fetchone()[0],
                'samples': self.c.execute('select count(*) from samples').fetchone()[0],
                'events': {r[0]: r[1] for r in self.c.execute('select status,count(*) from events group by status')},
                'sample_states': {r[0]: r[1] for r in self.c.execute('select status,count(*) from samples group by status')},
                'data_bytes': self.usage(), 'budget_bytes': self.cfg.storage_bytes,
                'sqlite_integrity': self.c.execute('pragma quick_check').fetchone()[0],
                'foreign_key_errors': len(self.c.execute('pragma foreign_key_check').fetchall()),
                'compressed_input_bytes':self.c.execute('select coalesce(sum(length(raw)),0) from inputs').fetchone()[0]}
        result.update(self.observation_snapshot(now_ms=now_ms))
        if self.cfg.weather_context:result['weather_status']=self.weather_status()
        if self.cfg.lamp_context:result['forecast_card']=self.forecast_card(now_ms)
        return result

    def observation_snapshot(self, *, now_ms=None, market=None):
        # Periodic health checks read only latest rows, not the entire history.
        now_ms=int(time.time()*1000) if now_ms is None else now_ms
        result={'latest': [dict(r) for r in self.c.execute("select s.* from latest l join samples s on s.input_id=l.input_id where l.kind!='price_alert' order by s.token,s.kind")]}
        for row in result['latest']:
            row['age_now_ms']=now_ms-row['source_ms']
            row['receipt_age_now_ms']=now_ms-row['received_ms']
            ages=(row['age_now_ms'],row['receipt_age_now_ms'])
            clock_status=quote_clock_status(self.cfg,row['source_ms'],row['received_ms'],now_ms)
            row['freshness_now']='CLOCK_UNVERIFIED' if clock_status=='FUTURE' else clock_status
            if self.cfg.source_clock_tolerance_ms:
                row['source_lead_ms']=row['source_ms']-row['received_ms']
                row['source_clock_tolerance_ms']=self.cfg.source_clock_tolerance_ms
            row['quote_usable_now']=(row['kind']!='last_trade_price' and self.quote_status(row['status'])=='FRESH'
                                     and row['ask'] is not None and row['bid'] is not None and row['freshness_now']=='FRESH')
        if market is None:
            bound=self.c.execute('select payload from market where id=1').fetchone()
            market=json.loads(bound[0]) if bound else None
        tokens=market['tokens'] if market else {}
        quotes={r['token']:r for r in result['latest'] if r['kind']!='last_trade_price'}
        closed_key=self.closed_key({'kind':'market_resolved','raw':{'market':market['condition_id']}}) if market else 'market_closed'
        closed=bool(self.c.execute('select 1 from meta where key=?',(closed_key,)).fetchone())
        sides={}
        for side, token in tokens.items():
            row=quotes.get(token)
            sides[side]=('MARKET_RESOLVED' if closed else 'CONNECTION_UNCONFIRMED' if not self.connected else
                         'AWAITING_BASELINE' if token not in self.ready_tokens else
                         'QUOTE_UNUSABLE' if not row or not row['quote_usable_now'] else 'READY')
        result['observation']={'state':('MARKET_RESOLVED' if closed else 'CONNECTION_UNCONFIRMED' if not self.connected else
                              'QUOTES_FRESH' if len(sides)==2 and all(v=='READY' for v in sides.values()) else 'PARTIAL_OR_UNUSABLE'),
                              'sides':sides}
        return result

    def prune(self, now_ms, *, apply=False):
        """Own data only. Unaudited events retain 30d; unreferenced input retains 24h.

        Delivery-audited events, referenced evidence and latest snapshots are retained.
        Freed SQLite pages are reused; this does not promise physical shrinking.
        """
        cutoff = now_ms-24*3600*1000
        query = '''select id from inputs where received_ms<? and id not in
                   (select input_id from event_inputs union select input_id from events union select input_id from latest
                    union select json_extract(p.value,'$.input_id') from events e, json_each(e.payload,'$.prices') p
                    where json_extract(p.value,'$.input_id') is not null)'''
        candidates = [r[0] for r in self.c.execute(query, (cutoff,))]
        if apply:
            with self.transaction():
                # Clear expired comparison windows before explicit maintenance.
                for row in self.c.execute('select token,payload from state').fetchall():
                    self.c.execute('update state set payload=? where token=?', (canonical(reset(json.loads(row['payload']))), row['token']))
                self.c.execute("delete from events where received_ms<? and json_extract(payload,'$.delivery_admission') is null and id not in (select json_extract(value,'$.event_id') from meta where key like 'alert_delivery_v1:%')", (now_ms-30*24*3600*1000,))
                self.c.execute('delete from samples where input_id in ('+query+')', (cutoff,))
                self.c.execute('delete from inputs where id in ('+query+')', (cutoff,))
        return {'eligible_unreferenced_inputs': len(candidates), 'applied': apply}

    def close(self):
        if not self.closed:
            self._flush_receive()
            self.c.execute('pragma wal_checkpoint(truncate)')
            self.history_seen.close()
            self.c.close(); self.lock.close(); self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
