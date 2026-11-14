"""One owner, transactional inputs/decisions/state, bounded private SQLite."""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import time
import zlib
from .core import canonical, evolve, reset, sha

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
    def __init__(self, cfg, *, session=True):
        self.cfg, self.closed = cfg, False
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

    @contextmanager
    def transaction(self):
        self._guard(); self.c.execute('begin immediate')
        try:
            yield
            self.c.execute('commit')
        except Exception:
            if self.c.in_transaction:
                self.c.execute('rollback')
            raise
        wal = self.root/'monitor.sqlite3-wal'
        if wal.exists() and wal.stat().st_size > 1024*1024:
            self.c.execute('pragma wal_checkpoint(truncate)')

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
                bool(self.c.execute('select 1 from inputs i join samples s on s.input_id=i.id where i.identity=?',(sample['id'],)).fetchone()))

    def apply(self, sample):
        raw = canonical(sample['raw']).encode()
        if len(raw) > 262144:
            raise ValueError('INPUT_BOUND')
        with self.transaction():
            prior = self.c.execute('select id from inputs where identity=?', (sample['id'],)).fetchone()
            if prior:
                return {'status': 'DUPLICATE', 'events': [], 'raw_bytes':0,'compressed_bytes':0}
            compressed=zlib.compress(raw,3)
            cur = self.c.execute('insert into inputs(identity,received_ms,raw,raw_sha) values(?,?,?,?)',
                                 (sample['id'], sample['received_ms'], compressed, sha(raw)))
            input_id = cur.lastrowid
            if sample['kind'] == 'market_resolved':
                self.c.execute("insert or replace into meta values('market_closed','true')")
                return {'status': 'MARKET_RESOLVED', 'events': []}
            if self.c.execute("select 1 from meta where key='market_closed'").fetchone():
                return {'status': 'AFTER_RESOLUTION', 'events': []}
            sample = dict(sample, input_id=input_id)
            row = self.c.execute('select payload from state where token=?', (sample['token'],)).fetchone()
            previous = json.loads(row[0]) if row else None
            quote=sample['kind']!='last_trade_price'
            awaiting=quote and sample['token'] not in self.ready_tokens
            if awaiting and previous:
                previous=reset(previous)
            state, events, status = evolve(previous, sample, self.cfg)
            if awaiting and sample['kind']=='price_change' and status=='FRESH':
                state, events, status = reset(previous if previous is not None else state), [], 'AWAITING_BASELINE'
            self.c.execute('insert or replace into state values(?,?)', (sample['token'], canonical(state)))
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
                ident = sha([sample['id'], self.cfg.rule_hash(), event['type'], event.get('level'), event.get('direction'), event['status']])
                self.c.execute('insert into events values(?,?,?,?,?,?,?,?)', (ident, input_id, sample['token'], sample['received_ms'], event['type'], event['status'], event.get('reason'), canonical(event)))
                for ref in {p['input_id'] for p in event['evidence']}:
                    self.c.execute('insert into event_inputs values(?,?)', (ident, ref))
                for ref in set(event.get('weather',{}).get('input_ids',[])) | set(event.get('lamp',{}).get('input_ids',[])):
                    self.c.execute('insert or ignore into event_inputs values(?,?)',(ident,ref))
                for price in event.get('prices',{}).values():
                    if price.get('input_id'):
                        self.c.execute('insert or ignore into event_inputs values(?,?)',(ident,price['input_id']))
                discovery=self.c.execute("select input_id from latest where kind='discovery'").fetchone()
                if discovery:self.c.execute('insert or ignore into event_inputs values(?,?)',(ident,discovery[0]))
            if self.cfg.price_alert_enabled:
                from .price_alert import record
                events += record(self,sample,status)
            result={'status': status, 'events': events,'raw_bytes':len(raw),'compressed_bytes':len(compressed)}
        # Readiness changes only after the input/state transaction commits.
        if quote:
            if status=='FRESH' and sample['kind'] in ('book','best_bid_ask'):
                self.ready_tokens.add(sample['token'])
                self.pending_baselines.pop(sample['token'],None)
            elif status!='FRESH':
                self.ready_tokens.discard(sample['token'])
                if self.connected:self.pending_baselines.setdefault(sample['token'],time.monotonic())
        return result

    def lamp_state(self):
        row=self.c.execute("select value from meta where key='lamp_context_v1'").fetchone()
        return json.loads(row[0]) if row else {}

    def lamp_status(self):
        return self.lamp_state().get('status','NOT_RECEIVED')

    def record_lamp(self,receipt,market,available_ms):
        from .lamp import update
        if not self.cfg.lamp_context:raise ValueError('LAMP_NOT_ENABLED')
        state=update(self.lamp_state(),receipt,market,available_ms)
        raw=canonical(dict(receipt=receipt,available_ms=available_ms,status=state['status'],duplicate=state['duplicate'])).encode()
        compressed=zlib.compress(raw,3)
        with self.transaction():
            used=self.c.execute("select coalesce(sum(length(raw)),0) from inputs where identity like 'LAMP:%'").fetchone()[0]
            if used+len(compressed)>1024*1024:
                self.c.execute("insert or replace into meta values('lamp_context_v1',?)",
                               (canonical(dict(self.lamp_state(),status='LAMP_STATE_BOUND')),))
                return
            identity='LAMP:'+sha(raw)
            self.c.execute('insert or ignore into inputs(identity,received_ms,raw,raw_sha) values(?,?,?,?)',
                           (identity,receipt['received_ms'],compressed,sha(raw)))
            ident=self.c.execute('select id from inputs where identity=?',(identity,)).fetchone()[0]
            if not state['duplicate']:state['input_id']=ident
            state['poll_input_id']=ident
            self.c.execute("delete from latest where kind='lamp'")
            for label,ref in (('__LAMP__',state['input_id']),('__LAMP_POLL__',ident)):
                self.c.execute('insert into latest values(?,?,?)',(label,'lamp',ref))
            self.c.execute("insert or replace into meta values('lamp_context_v1',?)",(canonical(state),))

    def price_context(self,at_ms):
        bound=self.c.execute('select payload from market where id=1').fetchone()
        if not bound:return {}
        market=json.loads(bound[0]);snapshot=self.observation_snapshot(now_ms=at_ms)
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

    def reject(self, raw, reason, at, token=None):
        value=canonical(raw).encode()
        if len(value)>262144:
            value=canonical({'rejected_sha256':sha(value),'reason':'REJECTED_INPUT_TOO_LARGE'}).encode()
        with self.transaction():
            self.c.execute('insert or ignore into inputs(identity,received_ms,raw,raw_sha) values(?,?,?,?)',
                           (sha(value),at,zlib.compress(value,3),sha(value)))
        self.invalidate('INVALID_INPUT:'+reason,at,token)

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

    def observation_snapshot(self, *, now_ms=None):
        # Periodic health checks read only latest rows, not the entire history.
        now_ms=int(time.time()*1000) if now_ms is None else now_ms
        result={'latest': [dict(r) for r in self.c.execute("select s.* from latest l join samples s on s.input_id=l.input_id where l.kind!='price_alert' order by s.token,s.kind")]}
        for row in result['latest']:
            row['age_now_ms']=now_ms-row['source_ms']
            row['receipt_age_now_ms']=now_ms-row['received_ms']
            ages=(row['age_now_ms'],row['receipt_age_now_ms'])
            row['freshness_now']=('CLOCK_UNVERIFIED' if min(ages)<0 or row['source_ms']>row['received_ms'] else
                                  'STALE' if max(ages)>self.cfg.quote_age_seconds*1000 else 'FRESH')
            row['quote_usable_now']=(row['kind']!='last_trade_price' and row['status']=='FRESH'
                                     and row['ask'] is not None and row['freshness_now']=='FRESH')
        market=self.c.execute('select payload from market where id=1').fetchone()
        tokens=json.loads(market[0])['tokens'] if market else {}
        quotes={r['token']:r for r in result['latest'] if r['kind']!='last_trade_price'}
        closed=bool(self.c.execute("select 1 from meta where key='market_closed'").fetchone())
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
        """Own data only. Events retain 30d; unreferenced input retains 24h.

        Referenced evidence and latest token snapshots are never deleted.
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
                self.c.execute('delete from events where received_ms<?', (now_ms-30*24*3600*1000,))
                self.c.execute('delete from samples where input_id in ('+query+')', (cutoff,))
                self.c.execute('delete from inputs where id in ('+query+')', (cutoff,))
        return {'eligible_unreferenced_inputs': len(candidates), 'applied': apply}

    def close(self):
        if not self.closed:
            self.c.execute('pragma wal_checkpoint(truncate)')
            self.c.close(); self.lock.close(); self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
