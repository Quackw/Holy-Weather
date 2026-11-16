"""Explicit closed-writer history checkpoints. No scheduler or live pruning.

Full original SQLite is archived first. A schema-v1 working projection keeps all
notification events/state/evidence and recent research. Archived identities remain
an exact bounded duplicate index, not a probabilistic filter.
"""
import base64
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import zlib
from .core import canonical

INDEX='history_seen_v1:'
PART='history_checkpoint_v1:'
MAX_INDEX_BYTES=96*1024*1024
MAX_SNAPSHOT_BYTES=256*1024*1024
TABLES=('market','inputs','samples','state','latest','events','event_inputs','diagnostics','meta')


def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def sync_file(path):
    with Path(path).open('rb') as f:os.fsync(f.fileno())


def sync_dir(path):
    fd=os.open(path,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


class Seen:
    def __init__(self,c):
        self.buckets={};total=0
        for key,value in c.execute('select key,value from meta where key like ?',(INDEX+'%',)):
            prefix=key[len(INDEX):]
            if not re.fullmatch('[0-9a-f]{2}',prefix):raise ValueError('HISTORY_INDEX_KEY')
            v=json.loads(value)
            if type(v.get('count')) is not int or v['count']<=0 or v['count']*32>MAX_INDEX_BYTES:raise ValueError('HISTORY_INDEX_BOUND')
            compressed=base64.b64decode(v['data'],validate=True)
            d=zlib.decompressobj();raw=d.decompress(compressed,v['count']*32+1)
            if not d.eof or d.unused_data or d.unconsumed_tail or len(raw)!=v['count']*32:raise ValueError('HISTORY_INDEX_ENCODING')
            if hashlib.sha256(raw).hexdigest()!=v['sha256']:raise ValueError('HISTORY_INDEX_SHA')
            total+=len(raw)
            if total>MAX_INDEX_BYTES:raise ValueError('HISTORY_INDEX_BOUND')
            previous=None
            for n in range(0,len(raw),32):
                x=raw[n:n+32]
                if x.hex()[:2]!=prefix or previous is not None and previous>=x:raise ValueError('HISTORY_INDEX_ORDER')
                previous=x
            self.buckets[prefix]=raw
    def contains(self,identity):
        if not isinstance(identity,str) or not re.fullmatch('[0-9a-f]{64}',identity):return False
        raw=self.buckets.get(identity[:2],b'');target=bytes.fromhex(identity);lo=0;hi=len(raw)//32
        while lo<hi:
            mid=(lo+hi)//2
            if raw[mid*32:(mid+1)*32]<target:lo=mid+1
            else:hi=mid
        return raw[lo*32:(lo+1)*32]==target
    def add(self,c,identities):
        additions={}
        for identity in identities:
            if not re.fullmatch('[0-9a-f]{64}',identity):raise ValueError('HISTORY_IDENTITY')
            additions.setdefault(identity[:2],set()).add(bytes.fromhex(identity))
        total=sum(map(len,self.buckets.values()))
        for prefix,extra in additions.items():
            old=self.buckets.get(prefix,b'')
            # Only one shard is expanded at a time; the resident index stays packed.
            raw=b''.join(sorted(extra|{old[n:n+32] for n in range(0,len(old),32)}))
            total+=len(raw)-len(old)
            if total>MAX_INDEX_BYTES:raise ValueError('HISTORY_INDEX_BOUND')
            self.buckets[prefix]=raw
            v=dict(count=len(raw)//32,sha256=hashlib.sha256(raw).hexdigest(),data=base64.b64encode(zlib.compress(raw,9)).decode())
            c.execute('insert or replace into meta values(?,?)',(INDEX+prefix,canonical(v)))


def references(value):
    todo=[value]
    while todo:
        item=todo.pop()
        if isinstance(item,dict):
            for k,v in item.items():
                if k=='input_id' or k.endswith('_input_id'):
                    if v is not None:
                        if type(v) is not int or v<=0:raise ValueError('HISTORY_INPUT_REFERENCE')
                        yield v
                elif k=='input_ids':
                    if not isinstance(v,list) or any(type(x) is not int or x<=0 for x in v):raise ValueError('HISTORY_INPUT_REFERENCE')
                    yield from v
                elif isinstance(v,(dict,list)):todo.append(v)
        elif isinstance(item,list):todo.extend(item)


def check(c):
    from .storage import APP_ID
    if c.execute('pragma application_id').fetchone()[0]!=APP_ID or c.execute('pragma user_version').fetchone()[0]!=1:raise ValueError('HISTORY_SCHEMA')
    if set(x[0] for x in c.execute("select name from sqlite_master where type='table' and name not like 'sqlite_%'"))!=set(TABLES):raise ValueError('HISTORY_TABLES')
    if c.execute('pragma integrity_check').fetchone()[0]!='ok' or c.execute('pragma foreign_key_check').fetchall():raise ValueError('HISTORY_INTEGRITY')


def fingerprint(c):
    result={}
    for table in TABLES:
        h=hashlib.sha256();n=0
        for row in c.execute('select * from '+table+' order by rowid'):
            h.update(repr(tuple(row)).encode());n+=1
        result[table]=dict(rows=n,sha256=h.hexdigest())
    return result


def unpack(archive,destination,expected):
    path=Path(destination)
    if path.exists() or path.is_symlink():raise ValueError('HISTORY_DEST_EXISTS')
    h=hashlib.sha256();size=0
    with gzip.open(archive,'rb') as a,path.open('xb') as b:
        while block:=a.read(1024*1024):
            size+=len(block)
            if size>MAX_SNAPSHOT_BYTES:raise ValueError('HISTORY_SNAPSHOT_BOUND')
            h.update(block);b.write(block)
    if h.hexdigest()!=expected:raise ValueError('HISTORY_ARCHIVE_SHA')


def prepare(data_dir,destination,*,expected_sha,cutoff_ms=None):
    """Prepare only. Original is never written, renamed, checkpointed or pruned."""
    from .storage import SCHEMA,APP_ID
    source=Path(data_dir).absolute();dest=Path(destination).absolute();db=source/'monitor.sqlite3'
    if source.is_symlink() or db.is_symlink() or (source/'.writer.lock').is_symlink():raise ValueError('HISTORY_SYMLINK')
    if (source/'.holy-weather-owned').read_text()!='HOLY_WEATHER_STAGE1\n':raise ValueError('HISTORY_OWNER')
    if dest.exists() or source==dest or source in dest.parents:raise ValueError('HISTORY_DESTINATION')
    if not 0<db.stat().st_size<=MAX_SNAPSHOT_BYTES:raise ValueError('HISTORY_SNAPSHOT_BOUND')
    if shutil.disk_usage(dest.parent).free<5*db.stat().st_size+128*1024*1024:raise ValueError('HISTORY_SPACE')
    with (source/'.writer.lock').open('rb') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        wal=source/'monitor.sqlite3-wal'
        if wal.exists() and wal.stat().st_size:raise ValueError('HISTORY_NONEMPTY_WAL')
        if digest(db)!=expected_sha:raise ValueError('HISTORY_SOURCE_DRIFT')
        a=sqlite3.connect(db.as_uri()+'?mode=ro&immutable=1',uri=True)
        try:
            check(a);seen=Seen(a);last=a.execute('select max(received_ms) from inputs').fetchone()[0]
            if last is None:raise ValueError('HISTORY_EMPTY')
            cutoff=last-900000 if cutoff_ms is None else cutoff_ms
            if type(cutoff) is not int or cutoff>last-900000:raise ValueError('HISTORY_RETENTION_MINIMUM')
            dest.mkdir(mode=0o700);snapshot=dest/'snapshot.sqlite3';archive=dest/'history.sqlite3.gz'
            b=sqlite3.connect(snapshot);a.backup(b);check(b)
            old=fingerprint(a)
            if fingerprint(b)!=old:raise ValueError('HISTORY_BACKUP_MISMATCH')
            b.close();os.chmod(snapshot,0o600);backup_sha=digest(snapshot);snapshot_sha=expected_sha
            # Writer is locked out and WAL is empty: preserve the exact original
            # artifact bytes as well as the independently checked Backup API copy.
            with db.open('rb') as f,archive.open('xb') as z:
                with gzip.GzipFile(fileobj=z,mode='wb',mtime=0,compresslevel=6) as g:shutil.copyfileobj(f,g,1024*1024)
            os.chmod(archive,0o600)
            unpack(archive,dest/'restore-proof.sqlite3',snapshot_sha)
            b=sqlite3.connect((dest/'restore-proof.sqlite3').as_uri()+'?mode=ro&immutable=1',uri=True);check(b)
            if fingerprint(b)!=old:raise ValueError('HISTORY_RESTORE_MISMATCH')
            b.close();os.chmod(dest/'restore-proof.sqlite3',0o600)
            candidate=dest/'candidate.sqlite3';c=sqlite3.connect(candidate,isolation_level=None)
            try:
                c.execute('pragma temp_store=MEMORY');c.executescript('begin;'+SCHEMA+f'pragma application_id={APP_ID};pragma user_version=1;commit;')
                c.execute('attach database ? as original',(db.as_uri()+'?mode=ro&immutable=1',))
                c.executescript('create temp table keep_events(id text primary key);create temp table keep_inputs(id integer primary key);')
                c.execute("insert into keep_events select id from original.events where received_ms>=? or type not in ('CROSS','MOVE','RETURN') or json_extract(payload,'$.delivery_admission') is not null",(cutoff,))
                for raw, in a.execute("select value from meta where key like 'alert_delivery_v1:%'"):
                    q=json.loads(raw)
                    for ident in q.get('event_ids',[q['event_id']]):c.execute('insert or ignore into keep_events values(?)',(ident,))
                c.execute("insert into keep_inputs select id from original.inputs where received_ms>=? or length(identity)!=64 or identity glob '*[^0-9a-f]*'",(cutoff,))
                c.execute('insert or ignore into keep_inputs select max(id) from original.inputs')
                c.execute('insert or ignore into keep_inputs select input_id from original.events where id in keep_events')
                c.execute('insert or ignore into keep_inputs select input_id from original.event_inputs where event_id in keep_events')
                c.execute('insert or ignore into keep_inputs select input_id from original.latest')
                # Every retained JSON reference stays resolvable, including current
                # model windows, forecast/weather projections and confirmations.
                queries=['select payload from original.state','select value from original.meta where key!=\'rule_hash\'',
                    'select payload from original.events where id in keep_events','select payload from original.market','select receipt from original.market']
                for query in queries:
                    for raw, in c.execute(query).fetchall():
                        for ident in references(json.loads(raw)):c.execute('insert or ignore into keep_inputs values(?)',(ident,))
                absent=c.execute('select count(*) from keep_inputs where id not in (select id from original.inputs)').fetchone()[0]
                if absent:raise ValueError('HISTORY_DANGLING_REFERENCE')
                removed=c.execute('select count(*) from original.inputs where id not in keep_inputs').fetchone()[0]
                if not removed:raise ValueError('HISTORY_NOTHING_ARCHIVABLE')
                c.execute('begin')
                for table in TABLES:
                    where=(' where id in keep_inputs' if table=='inputs' else
                           ' where input_id in keep_inputs' if table=='samples' else
                           ' where id in keep_events' if table=='events' else
                           ' where event_id in keep_events' if table=='event_inputs' else '')
                    # Preserve ROWID as well as public input/event identifiers.
                    info=c.execute('pragma table_info('+table+')').fetchall();names=[x[1] for x in info]
                    integer_pk=any(x[5] and x[2].upper()=='INTEGER' for x in info)
                    columns=','.join(names) if integer_pk else 'rowid,'+','.join(names)
                    c.execute('insert into '+table+'('+columns+') select '+columns+' from original.'+table+where+' order by rowid')
                identities=(x[0] for x in c.execute('select identity from original.inputs where id not in keep_inputs').fetchall())
                seen.add(c,identities)
                manifest=dict(version=1,source_sha256=expected_sha,snapshot_sha256=snapshot_sha,backup_api_sha256=backup_sha,archive_sha256=digest(archive),cutoff_ms=cutoff,
                    removed_inputs=removed,removed_research_events=old['events']['rows']-c.execute('select count(*) from events').fetchone()[0],
                    source_fingerprint=old,archive='history.sqlite3.gz',schema=1)
                c.execute('insert into meta values(?,?)',(PART+snapshot_sha,canonical({k:v for k,v in manifest.items() if k!='source_fingerprint'})))
                c.execute('commit');check(c);Seen(c)
                for table in TABLES:
                    if table in ('inputs','samples','events','event_inputs'):
                        # Selected immutable rows must be byte-identical to source.
                        if c.execute('select count(*) from (select * from '+table+' except select * from original.'+table+')').fetchone()[0]:raise ValueError('HISTORY_ROW_CHANGED:'+table)
                    elif table=='meta':
                        if c.execute('select count(*) from (select * from original.meta where key not like ? except select * from meta)',(INDEX+'%',)).fetchone()[0]:raise ValueError('HISTORY_STATE_CHANGED:meta')
                    elif c.execute('select count(*) from (select * from original.'+table+' except select * from '+table+')').fetchone()[0]:raise ValueError('HISTORY_STATE_CHANGED:'+table)
                manifest['candidate_fingerprint']=fingerprint(c)
            finally:c.close()
            os.chmod(candidate,0o600)
            manifest.update(candidate_sha256=digest(candidate),candidate_bytes=candidate.stat().st_size,archive_bytes=archive.stat().st_size,source_unchanged=digest(db)==expected_sha)
            if not manifest['source_unchanged']:raise ValueError('HISTORY_SOURCE_CHANGED')
            (dest/'MANIFEST.json').write_text(canonical(manifest)+'\n');os.chmod(dest/'MANIFEST.json',0o600)
            for p in (candidate,archive,dest/'MANIFEST.json'):sync_file(p)
            sync_dir(dest)
            return manifest
        finally:a.close()


def restore_history(active_db,part,destination):
    """Isolated proof/inverse only: never adopt or rewind state/queue/new records."""
    active=Path(active_db).absolute();part=Path(part).absolute();dest=Path(destination).absolute()
    if dest.exists() or active.is_symlink() or part.is_symlink():raise ValueError('HISTORY_RESTORE_DEST')
    wal=active.with_name(active.name+'-wal')
    if wal.exists() and wal.stat().st_size:raise ValueError('HISTORY_NONEMPTY_WAL')
    m=json.loads((part/'MANIFEST.json').read_text());archive=part/'history.sqlite3.gz'
    if digest(archive)!=m['archive_sha256']:raise ValueError('HISTORY_ARCHIVE_SHA')
    dest.mkdir(mode=0o700);unpack(archive,dest/'archived.sqlite3',m['snapshot_sha256'])
    a=sqlite3.connect(active.as_uri()+'?mode=ro&immutable=1',uri=True);c=sqlite3.connect(dest/'restored.sqlite3');a.backup(c);a.close()
    try:
        c.execute('attach database ? as old',((dest/'archived.sqlite3').as_uri()+'?mode=ro&immutable=1',));c.execute('begin')
        # State and delivery receipts are from the new active DB, never the past.
        for table,pk in [('inputs','id'),('samples','input_id'),('events','id')]:
            names=[x[1] for x in c.execute('pragma table_info('+table+')')]
            if table in ('inputs','samples'):
                changed=' or '.join('a.'+n+' is not b.'+n for n in names)
                if c.execute('select 1 from '+table+' a join old.'+table+' b on a.'+pk+'=b.'+pk+' where '+changed+' limit 1').fetchone():raise ValueError('HISTORY_RESTORE_CONFLICT')
            c.execute('insert into '+table+' select * from old.'+table+' where '+pk+' not in (select '+pk+' from '+table+')')
        c.execute('insert or ignore into event_inputs select * from old.event_inputs');c.commit();check(c)
        result=fingerprint(c)
    finally:c.close()
    for p in dest.iterdir():os.chmod(p,0o600)
    return result


def main():
    import argparse
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    a=sub.add_parser('prepare');a.add_argument('--data-dir',required=True);a.add_argument('--destination',required=True);a.add_argument('--expected-sha',required=True)
    a=sub.add_parser('restore');a.add_argument('--active-db',required=True);a.add_argument('--part',required=True);a.add_argument('--destination',required=True)
    args=p.parse_args();os.umask(0o077)
    result=(prepare(args.data_dir,args.destination,expected_sha=args.expected_sha) if args.command=='prepare' else
            restore_history(args.active_db,args.part,args.destination))
    print(canonical(result))



def adopt(data_dir,part,*,cleanup=False):
    """Explicit one-time adoption; caller must authorize checkpoint maintenance.

    No process control. The original writer lock is retained across atomic replace.
    Only this part's verified redundant scratch copies may be removed if requested.
    """
    data=Path(data_dir).absolute();part=Path(part).absolute();db=data/'monitor.sqlite3'
    m=json.loads((part/'MANIFEST.json').read_text());candidate=part/'candidate.sqlite3'
    if (data/'.holy-weather-owned').read_text()!='HOLY_WEATHER_STAGE1\n':raise ValueError('HISTORY_OWNER')
    if any(p.is_symlink() for p in (data,db,part,candidate,data/'.writer.lock')):raise ValueError('HISTORY_SYMLINK')
    if (part/'ADOPT_INTENT.json').exists():raise ValueError('HISTORY_ADOPTION_ALREADY_ATTEMPTED')
    with (data/'.writer.lock').open('rb') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        wal=data/'monitor.sqlite3-wal'
        if wal.exists() and wal.stat().st_size:raise ValueError('HISTORY_NONEMPTY_WAL')
        if digest(db)!=m['source_sha256'] or digest(candidate)!=m['candidate_sha256']:raise ValueError('HISTORY_ADOPT_DRIFT')
        if digest(part/'history.sqlite3.gz')!=m['archive_sha256']:raise ValueError('HISTORY_ARCHIVE_SHA')
        c=sqlite3.connect(candidate.as_uri()+'?mode=ro&immutable=1',uri=True)
        try:check(c);Seen(c)
        finally:c.close()
        # Leave the same reserve as the writer, without changing its 256MiB cap.
        if candidate.stat().st_size+1024*1024>=MAX_SNAPSHOT_BYTES-8*1024*1024:raise ValueError('HISTORY_NO_HEADROOM')
        intent=dict(source_sha256=m['source_sha256'],candidate_sha256=m['candidate_sha256'])
        with (part/'ADOPT_INTENT.json').open('x') as f:f.write(canonical(intent))
        os.chmod(part/'ADOPT_INTENT.json',0o600)
        for p in (candidate,part/'history.sqlite3.gz',part/'MANIFEST.json',part/'ADOPT_INTENT.json'):sync_file(p)
        sync_dir(part)
        os.replace(candidate,db)
        sync_dir(data);sync_dir(part)
        if digest(db)!=m['candidate_sha256']:raise ValueError('HISTORY_ADOPT_UNKNOWN')
        (part/'ADOPTED.json').write_text(canonical(intent));os.chmod(part/'ADOPTED.json',0o600)
        sync_file(part/'ADOPTED.json');sync_dir(part)
        if cleanup:
            for name,expected in [('snapshot.sqlite3',m['backup_api_sha256']),('restore-proof.sqlite3',m['snapshot_sha256'])]:
                p=part/name
                if p.is_symlink() or p.stat().st_nlink!=1 or p.stat().st_uid!=os.getuid() or digest(p)!=expected:raise ValueError('HISTORY_CLEANUP_DRIFT')
            for name in ('snapshot.sqlite3','restore-proof.sqlite3'):(part/name).unlink()
            sync_dir(part)
    return intent


if __name__=='__main__':main()
