"""Immutable sorted SHA files and verified copies of closed history parts."""
from dataclasses import dataclass, asdict
import hashlib
import heapq
import json
import os
from pathlib import Path
import re
import shutil
import stat
import time

from .core import canonical

FILE_INDEX='history_seen_file_v1'
MiB=1024**2
GiB=1024**3


@dataclass(frozen=True)
class HistoryStorage:
    directory: str
    spool_bytes: int=64*GiB
    history_bytes: int=192*GiB
    free_reserve_bytes: int=16*GiB
    closed_days: int=2
    mountpoint: str | None=None

    def __post_init__(self):
        path=Path(self.directory)
        if not path.is_absolute() or '..' in path.parts:raise ValueError('HISTORY_STORAGE_DIRECTORY')
        if self.mountpoint is not None:
            mount=Path(self.mountpoint)
            if not mount.is_absolute() or mount not in path.parents:raise ValueError('HISTORY_STORAGE_MOUNTPOINT')
        for name,lo,hi in (('spool_bytes',4*GiB,256*GiB),('history_bytes',8*GiB,1024*GiB),
                           ('free_reserve_bytes',2*GiB,128*GiB),('closed_days',2,30)):
            value=getattr(self,name)
            if type(value) is not int or not lo<=value<=hi:raise ValueError('HISTORY_STORAGE_BOUND:'+name)

    def value(self):return asdict(self)


def private(path,directory=False):
    value=path.lstat()
    if (path.is_symlink() or value.st_uid!=os.getuid() or value.st_mode&0o077
            or (stat.S_ISDIR(value.st_mode) if directory else stat.S_ISREG(value.st_mode)) is False):
        raise ValueError('HISTORY_FILE_PERMISSIONS')


def sync_dir(path):
    fd=os.open(path,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


def checksum(path):
    with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()


def payload(part,name):
    """Local manifest/locator remains after a verified payload moves to the disk."""
    part=Path(part)
    if name not in ('seen.sha256','seen.delta.sha256','history.sqlite3.gz'):raise ValueError('HISTORY_FILE_NAME')
    path=part/name
    if path.exists():
        private(path)
        return path
    locator=part/'MIRRORED.json';private(locator)
    value=json.loads(locator.read_bytes());cold=Path(value['directory'])
    private(cold,True)
    if checksum(cold/'MANIFEST.json')!=value['manifest_sha256']:
        raise ValueError('HISTORY_COLD_MANIFEST')
    path=cold/name;private(path)
    return path


class FileIndex:
    """Exact binary search on disk. Resident memory is independent of total IDs."""
    def __init__(self,value,*,archive_root=None,index_path=None,allow_empty=False):
        self.fd=None
        if (set(value)!={'part','count','sha256'} or type(value['count']) is not int
                or value['count']<(0 if allow_empty else 1) or not re.fullmatch('[0-9a-f]{64}',value['sha256'])):
            raise ValueError('HISTORY_FILE_INDEX_REFERENCE')
        part=Path(value['part'])
        if not part.is_absolute() or part.is_symlink():raise ValueError('HISTORY_FILE_INDEX_PATH')
        if archive_root is not None:part=Path(archive_root)/part.name
        path=Path(index_path) if index_path is not None else payload(part,'seen.sha256')
        private(path)
        self.count=value['count']
        self.fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        try:
            if os.fstat(self.fd).st_size!=self.count*32:raise ValueError('HISTORY_FILE_INDEX_LENGTH')
            digest=hashlib.sha256();previous=None
            for raw in self.values():
                if previous is not None and previous>=raw:raise ValueError('HISTORY_FILE_INDEX_ORDER')
                digest.update(raw);previous=raw
            if digest.hexdigest()!=value['sha256']:raise ValueError('HISTORY_FILE_INDEX_SHA')
        except BaseException:self.close();raise

    def values(self):
        offset=0
        while offset<self.count*32:
            block=os.pread(self.fd,min(MiB,self.count*32-offset),offset)
            if not block or len(block)%32:raise ValueError('HISTORY_FILE_INDEX_READ')
            for n in range(0,len(block),32):yield block[n:n+32]
            offset+=len(block)

    def contains(self,target):
        lo=0;hi=self.count
        while lo<hi:
            mid=(lo+hi)//2;value=os.pread(self.fd,32,mid*32)
            if len(value)!=32:raise ValueError('HISTORY_FILE_INDEX_READ')
            if value<target:lo=mid+1
            else:hi=mid
        return lo<self.count and os.pread(self.fd,32,lo*32)==target

    def close(self):
        if self.fd is not None:os.close(self.fd);self.fd=None

    def __del__(self):self.close()


def write_index(part,*streams,name='seen.sha256',allow_empty=False):
    part=Path(part);path=part/name;count=0;previous=None;digest=hashlib.sha256()
    with path.open('xb') as f:
        os.fchmod(f.fileno(),0o600)
        for raw in heapq.merge(*streams):
            if type(raw) is not bytes or len(raw)!=32:raise ValueError('HISTORY_FILE_INDEX_VALUE')
            if previous is not None and raw<previous:raise ValueError('HISTORY_FILE_INDEX_ORDER')
            if raw==previous:continue
            f.write(raw);digest.update(raw);count+=1;previous=raw
        f.flush();os.fsync(f.fileno())
    if not count and not allow_empty:raise ValueError('HISTORY_FILE_INDEX_EMPTY')
    sync_dir(part)
    return {'part':str(part),'count':count,'sha256':digest.hexdigest()}


def difference(values,previous):
    old=iter(previous);current=next(old,None)
    for value in values:
        while current is not None and current<value:current=next(old,None)
        if current!=value:yield value


def materialize_index(part,destination,*,archive_root=None):
    """Restore a historical index from verified deltas into a new private folder."""
    part=Path(part);dest=Path(destination)
    if dest.exists() or dest.is_symlink():raise ValueError('HISTORY_INDEX_RESTORE_DESTINATION')
    reference=json.loads((part/'MANIFEST.json').read_bytes())['file_index']
    if shutil.disk_usage(dest.parent).free<3*reference['count']*32+128*MiB:
        raise ValueError('HISTORY_INDEX_RESTORE_SPACE')
    segments=[];current=reference;visited=set()
    while current:
        location=Path(current['part']) if archive_root is None else Path(archive_root)/Path(current['part']).name
        if location in visited:raise ValueError('HISTORY_INDEX_RESTORE_CYCLE')
        visited.add(location);private(location,True)
        manifest=json.loads((location/'MANIFEST.json').read_bytes())
        if manifest['file_index']!=current:raise ValueError('HISTORY_INDEX_RESTORE_PARENT')
        if (location/'seen.sha256').is_file():
            segments.append((current,location/'seen.sha256'));break
        delta=manifest['file_index_delta']
        segments.append((delta,payload(location,'seen.delta.sha256')))
        current=manifest['file_index_parent']
    def values(item):
        ref,path=item;index=FileIndex(ref,index_path=path,allow_empty=True)
        try:yield from index.values()
        finally:index.close()
    dest.mkdir(mode=0o700);runs=segments;temporary=[];sequence=0
    while len(runs)>32:
        following=[]
        for start in range(0,len(runs),32):
            sequence+=1;name='merge-'+str(sequence)
            ref=write_index(dest,*(values(item) for item in runs[start:start+32]),name=name,allow_empty=True)
            path=dest/name;temporary.append(path);following.append((ref,path))
        runs=following
    ref=write_index(dest,*(values(item) for item in runs))
    restored=FileIndex(reference,index_path=dest/'seen.sha256')
    restored.close()
    for path in temporary:path.unlink()
    sync_dir(dest)
    return {'file':str(dest/'seen.sha256'),'reference':reference}


def retire_payloads(part,verified):
    cold=Path(verified['directory']);private(cold,True)
    if checksum(cold/'MANIFEST.json')!=verified['manifest_sha256']:
        raise ValueError('HISTORY_COLD_MANIFEST')
    for name in ('history.sqlite3.gz','seen.sha256','seen.delta.sha256'):
        p=part/name
        if not p.exists():continue
        private(p)
        expected=verified.get('materialized_index',{}).get('sha256') if name=='seen.sha256' else None
        if expected is None:
            target=cold/name;private(target);expected=verified['files'][name]
            if checksum(target)!=expected:raise ValueError('HISTORY_COLD_RETIRE_DRIFT')
        elif checksum(cold/'seen.delta.sha256')!=verified['materialized_index']['delta_sha256']:
            raise ValueError('HISTORY_COLD_RETIRE_DELTA')
        if p.stat().st_nlink!=1 or checksum(p)!=expected:
            raise ValueError('HISTORY_COLD_RETIRE_DRIFT')
        p.unlink()
    sync_dir(part)


def offload(parts,policy,*,keep=(),max_bytes=512*MiB,max_seconds=90):
    """Copy first. Retire only exact verified payloads, keeping local locators."""
    destination=Path(policy.directory);started=time.monotonic();copied=0;done=[]
    try:
        if policy.mountpoint is not None and (not os.path.ismount(policy.mountpoint)
                or Path(policy.mountpoint).stat().st_dev!=destination.stat().st_dev):
            raise OSError('HISTORY_DISK_NOT_MOUNTED')
        private(destination,True)
        total=sum(p.stat().st_size for p in destination.rglob('*') if p.is_file())
        for part in sorted(Path(parts).glob('part-*')):
            if part.name in keep or not (part/'ADOPTED.json').is_file():continue
            if (part/'MIRRORED.json').exists():
                if any((part/name).exists() for name in ('history.sqlite3.gz','seen.sha256','seen.delta.sha256')):
                    retire_payloads(part,json.loads((part/'MIRRORED.json').read_bytes()))
                continue
            private(part,True)
            scratch=part/'.MIRRORED.tmp'
            if scratch.exists():
                private(scratch)
                if scratch.stat().st_nlink!=1 or scratch.stat().st_size>16*1024:raise ValueError('HISTORY_COLD_LOCATOR_PARTIAL')
                scratch.unlink()
            names=[p.name for p in part.iterdir() if p.is_file()]
            if set(names)-{'MANIFEST.json','ADOPTED.json','ADOPT_INTENT.json','seen.sha256','seen.delta.sha256','history.sqlite3.gz'}:
                raise ValueError('HISTORY_COLD_UNEXPECTED_FILES')
            manifest=json.loads((part/'MANIFEST.json').read_bytes())
            materialized=None
            if 'file_index_delta' in manifest:
                materialized={'sha256':manifest['file_index']['sha256'],'delta_sha256':manifest['file_index_delta']['sha256']}
                if json.loads((part/'ADOPTED.json').read_bytes()).get('file_index')!=materialized:
                    raise ValueError('HISTORY_COLD_INDEX_PROOF')
                if checksum(part/'seen.sha256')!=materialized['sha256'] or checksum(part/'seen.delta.sha256')!=materialized['delta_sha256']:
                    raise ValueError('HISTORY_COLD_INDEX_DRIFT')
                names.remove('seen.sha256')
            hashes={name:checksum(part/name) for name in names}
            sizes={name:(part/name).stat().st_size for name in names}
            needed=sum(sizes.values())
            if copied+needed>max_bytes or time.monotonic()-started>=max_seconds:break
            cold=destination/(part.name+'-'+hashes['MANIFEST.json'][:16])
            existing=sum(p.stat().st_size for p in cold.iterdir() if p.is_file()) if cold.is_dir() else 0
            if total-existing+needed+16*1024>policy.history_bytes:raise OSError('HISTORY_DISK_QUOTA')
            if shutil.disk_usage(destination).free<needed+policy.free_reserve_bytes:raise OSError('HISTORY_DISK_RESERVE')
            intent={'version':1,'files':hashes,'bytes':sizes}
            if not cold.exists():cold.mkdir(mode=0o700)
            private(cold,True)
            if not (cold/'COPYING.json').exists():
                if {p.name for p in cold.iterdir()}- {'.copy-intent.tmp'}:raise ValueError('HISTORY_COLD_UNKNOWN_COPY')
                temp=cold/'.copy-intent.tmp'
                if temp.exists():
                    private(temp)
                    if temp.stat().st_nlink!=1 or temp.stat().st_size>16*1024:raise ValueError('HISTORY_COLD_COPY_INTENT')
                    temp.unlink()
                with temp.open('x') as f:
                    os.fchmod(f.fileno(),0o600);f.write(canonical(intent));f.flush();os.fsync(f.fileno())
                os.replace(temp,cold/'COPYING.json')
                sync_dir(cold);sync_dir(destination)
            if json.loads((cold/'COPYING.json').read_bytes())!=intent:raise ValueError('HISTORY_COLD_COPY_DRIFT')
            for name in names:
                target=cold/name
                if target.exists():
                    private(target)
                    if checksum(target)!=hashes[name]:raise ValueError('HISTORY_COLD_CONTENT_DRIFT')
                    continue
                temp=cold/('.'+name+'.partial')
                if temp.exists():
                    private(temp)
                    if temp.stat().st_nlink!=1 or temp.stat().st_size>sizes[name]:raise ValueError('HISTORY_COLD_PARTIAL')
                    temp.unlink()  # Explicitly resumable private scratch, source retained.
                with (part/name).open('rb') as a,temp.open('xb') as b:
                    os.fchmod(b.fileno(),0o600)
                    while block:=a.read(MiB):
                        if time.monotonic()-started>=max_seconds:raise TimeoutError('HISTORY_COLD_COPY_DEADLINE')
                        b.write(block)
                    b.flush();os.fsync(b.fileno())
                if checksum(temp)!=hashes[name]:raise ValueError('HISTORY_COLD_COPY_SHA')
                os.replace(temp,target);sync_dir(cold)
            verified={'directory':str(cold),'manifest_sha256':hashes['MANIFEST.json'],'files':hashes}
            if materialized:verified['materialized_index']=materialized
            local=part/'MIRRORED.json';scratch=part/'.MIRRORED.tmp'
            with scratch.open('x') as f:
                os.fchmod(f.fileno(),0o600);f.write(canonical(verified));f.flush();os.fsync(f.fileno())
            os.replace(scratch,local)
            sync_dir(part);sync_dir(cold);sync_dir(destination)
            retire_payloads(part,verified)
            copied+=needed
            total=total-existing+sum(p.stat().st_size for p in cold.iterdir() if p.is_file())
            done.append(part.name)
        return {'status':'READY','copied_bytes':copied,'parts':done,'history_bytes':total}
    except (OSError,ValueError) as error:
        return {'status':'SPOOLING','reason':str(error) if type(error) in (ValueError,OSError,TimeoutError) and str(error).startswith('HISTORY_') else type(error).__name__,
                'copied_bytes':copied,'parts':done}
