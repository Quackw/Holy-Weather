"""One bounded NOAA download for the US catalogue; SQLite stays on its owner."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import queue
import re
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from .core import US_CITIES, canonical, sha
from .lamp import MAX_BODY, nomads_source
from .weather import millis

KEY='us_lamp_poll_v1'
MAX_DOWNLOAD=6*1024*1024
PUBLICATION_WAIT_MS=10*60*1000
RETRY_MS=5*60*1000
HEADER=re.compile(rb'\s*([A-Z0-9]{4})\s+GFS LAMP GUIDANCE\s+\d{1,2}/\d{1,2}/\d{4}\s+\d{4} UTC\s*')


def cycle_at(at):
    ready=datetime.fromtimestamp((at-PUBLICATION_WAIT_MS)/1000,timezone.utc)
    cycle=ready.replace(minute=30,second=0,microsecond=0)
    if cycle>ready:cycle-=timedelta(hours=1)
    return int(cycle.timestamp()*1000)


def fetch_cycle(issue_ms,*,fetch=urlopen,clock=millis):
    """Stream one collective; retain only ten original station blocks and SHA."""
    source=nomads_source(issue_ms);requested=clock()
    stations={v[1] for v in US_CITIES.values()}
    blocks={};current=None;size=0;digest=hashlib.sha256();started=time.monotonic()
    try:
        with fetch(Request(source,headers={'User-Agent':'Holy-Weather/US-LAMP bounded-observation'}),timeout=5) as response:
            if response.status!=200:raise ValueError('LAMP_HTTP_STATUS')
            if hasattr(response,'geturl') and response.geturl()!=source:raise ValueError('LAMP_SOURCE_REDIRECT')
            modified=response.headers.get('Last-Modified')
            length=response.headers.get('Content-Length')
            if length is not None and not 0<int(length)<=MAX_DOWNLOAD:raise ValueError('LAMP_DOWNLOAD_BOUND')
            while line:=response.readline(1025):
                if time.monotonic()-started>10:raise TimeoutError('LAMP_TOTAL_TIMEOUT')
                size+=len(line);digest.update(line)
                if size>MAX_DOWNLOAD or len(line)>1024:raise ValueError('LAMP_DOWNLOAD_BOUND')
                header=HEADER.fullmatch(line)
                if header:
                    current=header[1].decode()
                    if current in stations:
                        if current in blocks:raise ValueError('LAMP_DUPLICATE_STATION')
                        blocks[current]=bytearray()
                if current in stations:
                    blocks[current].extend(line)
                    if len(blocks[current])>MAX_BODY:raise ValueError('LAMP_STATION_BOUND')
                if not line.strip():current=None
            if length is not None and size!=int(length):raise ValueError('LAMP_TRUNCATED_DOWNLOAD')
        received=clock();file_sha=digest.hexdigest()
        receipts={}
        for station in sorted(stations):
            raw=bytes(blocks.get(station,b''))
            receipts[station]=dict(source=source,requested_ms=requested,received_ms=received,http=200,
                status='OK' if raw else 'MISSING_STATION',raw_body=raw.decode('ascii'),sha256=sha(raw),
                file_sha256=file_sha,file_bytes=size,last_modified=modified)
        return dict(receipts=receipts,status='OK',terminal=False,bytes=size,file_sha256=file_sha)
    except HTTPError as exc:
        status='ACCESS_DENIED' if exc.code in (401,403,429) else 'NOT_PUBLISHED' if exc.code==404 else 'HTTP_ERROR'
        terminal=exc.code in (401,403,429) or 400<=exc.code<500 and exc.code!=404
    except (OSError,URLError) as exc:
        status='NETWORK_ERROR';terminal=False
    except (ValueError,UnicodeError) as exc:
        status='INVALID_DATA';terminal=True
    received=clock()
    receipts={s:dict(source=source,requested_ms=requested,received_ms=received,status=status,terminal=terminal)
              for s in stations}
    return dict(receipts=receipts,status=status,terminal=terminal,bytes=size)


class Collection:
    """Persist the request schedule; a single HTTP thread never touches SQLite."""
    def __init__(self,fetcher=fetch_cycle):
        self.fetcher=fetcher;self.results=queue.Queue(maxsize=1);self.thread=None;self.state=None;self.closed=False

    def tick(self,store,selected,at):
        if self.closed:return
        if self.state is None:
            row=store.c.execute('select value from meta where key=?',(KEY,)).fetchone()
            self.state=json.loads(row[0]) if row else dict(next_ms=0,attempts=0,blocked=False)
        markets=[m for c,m in store.markets.items() if c in selected and store.active(c,at)]
        independent=store.cfg.us_weather_continuity_enabled
        try:result=self.results.get_nowait()
        except queue.Empty:result=None
        if result is not None:
            if any(r['received_ms']>at for r in result['receipts'].values()):
                self.results.put_nowait(result);return
            statuses={}
            for station,receipt in result['receipts'].items():
                contracts=[m for m in markets if m['station']==station]
                if contracts or independent:
                    _,_,zone=next(v for v in US_CITIES.values() if v[1]==station)
                    descriptor=contracts[0] if contracts else dict(station=station,timezone=zone,
                        date=datetime.fromtimestamp(at/1000,ZoneInfo(zone)).date().isoformat())
                    store.record_lamp(receipt,descriptor,at,markets=contracts)
                    state=store.lamp_state(descriptor)
                    statuses[station]=dict(status=state['status'],reason=state.get('reason'),issue_ms=state.get('issue_ms'))
            self.state.update(status=result['status'],blocked=result['terminal'],
                              bytes=result.get('bytes'),file_sha256=result.get('file_sha256'),stations=statuses)
            if result['status']=='OK' and any(v['status']!='OK' for v in statuses.values()):
                self.state['status']='PARTIAL'
            if result['status']=='OK':
                prior=self.state.get('last_success_ms')
                self.state['missing_issues']=max(0,(self.state['cycle_ms']-prior)//3600000-1) if prior is not None else None
                self.state['last_success_ms']=self.state['cycle_ms']
                self.state['next_ms']=self.state['cycle_ms']+3600000+PUBLICATION_WAIT_MS
            store.set_meta(KEY,self.state)
        if self.state['blocked'] or at<self.state['next_ms'] or not (markets or independent):return
        if self.thread is not None and self.thread.is_alive():return
        cycle=cycle_at(at)
        attempts=self.state['attempts'] if self.state.get('cycle_ms')==cycle else 0
        if attempts>=3:return
        self.state.update(cycle_ms=cycle,attempts=attempts+1,next_ms=at+RETRY_MS,status='REQUESTING')
        store.set_meta(KEY,self.state)
        def request():
            try:result=self.fetcher(cycle)
            except Exception:
                result=dict(receipts={},status='WORKER_ERROR',terminal=True,bytes=0)
            self.results.put_nowait(result)
        self.thread=threading.Thread(target=request,daemon=True,name='us-lamp-one-request')
        self.thread.start()

    def close(self):
        self.closed=True
