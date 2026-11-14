"""Opt-in Telegram adapter. One claim before one HTTP attempt; never retry uncertainty."""
import argparse
from datetime import datetime,timezone
from decimal import Decimal
from html import escape,unescape
import json
import os
from pathlib import Path
import queue
import re
import threading
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
from .core import CITIES,Config,canonical,sha
from .lamp import c_to_f, MAX_AGE_MS
from .storage import Store,StorageStop
from .weather import WEATHER_AGE_MS,millis

TARGET='@holy_weather'
PREFIX='telegram_delivery:'
MAX_DELIVERIES=1024


def number(value):
    return format(Decimal(str(value)).normalize(),'f')


def age(ms):
    if ms is None:return 'N/A'
    if ms<0:return 'CLOCK ERROR'
    return '<1s' if ms<1000 else str(ms//1000)+'s' if ms<60000 else str(ms//60000)+'m'


def prepare(store,event_id,*,test=False,at_ms=None):
    if type(test) is not bool:raise ValueError('TEST_FLAG_BOOLEAN')
    at_ms=millis() if at_ms is None else at_ms
    row=store.c.execute('select * from events where id=?',(event_id,)).fetchone()
    if not row or row['status']!='DETECTED':raise ValueError('DETECTED_EVENT_REQUIRED')
    if at_ms<row['received_ms']:raise ValueError('EVENT_NOT_YET_RECEIVED')
    event=json.loads(row['payload']);market=json.loads(store.c.execute('select payload from market where id=1').fetchone()[0])
    side=next((side for side,token in market['tokens'].items() if token==row['token']),None)
    if not side:raise ValueError('EVENT_TOKEN_MISMATCH')
    url=market['url'];parsed=urlparse(url)
    if parsed.scheme!='https' or parsed.netloc!='polymarket.com' or len(parsed.path.strip('/').split('/'))!=3 or parsed.query or parsed.fragment:
        raise ValueError('EXACT_BUCKET_URL_REQUIRED')
    points=event.get('evidence',[])
    if len(points)<2:raise ValueError('PRICE_EVIDENCE_REQUIRED')
    latest=points[-1];before=points[-2] if event['type']=='RETURN' else points[0]
    quote=event.get('prices',{}).get(side,{})
    if quote.get('ask')!=latest['ask'] or quote.get('source_ms')!=latest['source_ms'] or quote.get('status')!='FRESH':
        raise ValueError('QUOTE_EVIDENCE_MISMATCH')
    if not test and (quote.get('observation_state')!='READY' or store.observation_snapshot(now_ms=at_ms)['observation']['sides'].get(side)!='READY'):
        raise ValueError('QUOTE_NOT_CONFIRMED')
    clock=row['received_ms'] if test else at_ms
    if not 0<=clock-latest['source_ms']<=store.cfg.quote_age_seconds*1000 or latest['source_ms']>latest['received_ms']:
        raise ValueError('QUOTE_STALE_OR_CLOCK_ERROR')
    weather=event.get('weather',{});lamp=event.get('lamp',{});last=weather.get('latest')
    if last and (last['station']!=market['station'] or max(last['received_ms'],last['available_ms'])>=row['received_ms']):
        raise ValueError('WEATHER_IDENTITY_OR_CAUSALITY')
    if lamp.get('issue_ms') is not None and max(lamp['received_ms'],lamp['available_ms'])>=row['received_ms']:
        raise ValueError('LAMP_CAUSALITY')
    temp_age=clock-last['observed_ms'] if last else None
    suffix='' if temp_age is not None and 0<=temp_age<=WEATHER_AGE_MS else ' (STALE)' if last else ''
    current='N/A' if not last else number(c_to_f(last['temperature_c']))+'°F'+suffix
    observed=weather.get('observed_max_c');seen='N/A' if observed is None else number(c_to_f(observed))+'°F'
    lamp_status=lamp.get('status','MISSING')
    if lamp_status=='OK' and not 0<=clock-lamp['issue_ms']<=MAX_AGE_MS:lamp_status='STALE'
    peak=lamp.get('future_peak_f') if lamp_status=='OK' else None
    if peak is None:forecast='N/A ('+escape(lamp_status)+')'
    else:forecast=number(peak)+'°F'
    delta=lamp.get('peak_minus_observed_max_f') if peak is not None else None
    remaining='N/A' if delta is None else ('+' if Decimal(delta)>=0 else '')+number(delta)+'°F'
    city=next(k for k,v in CITIES.items() if v[1]==market['station']).replace('-',' ').title()
    day=datetime.fromisoformat(market['date']);months=('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')
    kyiv=datetime.fromtimestamp(row['received_ms']/1000,timezone.utc).astimezone(ZoneInfo('Europe/Kyiv'))
    lines=[('<b>TEST · ' if test else '<b>')+'HOLY WEATHER · '+escape(city)+' · '+months[day.month-1]+' '+str(day.day)+'</b>',
           '<a href="'+escape(url,quote=True)+'"><b>'+escape(market['bucket'])+' · '+side+'</b></a>',
           'Ask: <b>'+number(Decimal(before['ask'])*100)+'¢ → '+number(Decimal(latest['ask'])*100)+'¢</b>',
           'Latest METAR: <b>'+current+'</b> · Observed max: <b>'+seen+'</b>',
           'Remaining LAMP peak: <b>'+forecast+'</b> · Above observed max: <b>'+remaining+'</b>',
           '<b>'+kyiv.strftime('%H:%M')+' Kyiv</b> · Quote age: '+escape(age(clock-latest['source_ms']))+' · METAR age: '+escape(age(temp_age))]
    if weather.get('provider_status') not in (None,'OK'):lines.append('AWC: '+escape(weather['provider_status']))
    if test:lines.append('TEST · Recorded event; ages shown at event time.')
    text='\n'.join(lines)
    if len(text)>3500:raise ValueError('CARD_BOUND')
    identity=store.c.execute('select identity from inputs where id=?',(row['input_id'],)).fetchone()[0]
    key=PREFIX+sha([TARGET,market['condition_id'],row['token'],identity,test])
    body=dict(event_id=event_id,input_identity=identity,target=TARGET,test=test,prepared_ms=at_ms,event_ms=row['received_ms'],source_ms=latest['source_ms'],max_quote_age_ms=store.cfg.quote_age_seconds*1000,key=key,html=text)
    return dict(body,preview_sha256=sha(body))


def claim(store,preview,*,now_ms=None):
    now_ms=millis() if now_ms is None else now_ms
    fresh=prepare(store,preview['event_id'],test=preview['test'],at_ms=preview['prepared_ms'])
    if fresh!=preview:raise ValueError('PREVIEW_CHANGED')
    if not preview['test'] and not 0<=now_ms-preview['source_ms']<=store.cfg.quote_age_seconds*1000:
        raise ValueError('QUOTE_EXPIRED_BEFORE_SEND')
    with store.transaction():
        if store.c.execute('select 1 from meta where key=?',(preview['key'],)).fetchone():return False
        count=store.c.execute("select count(*) from meta where key like 'telegram_delivery:%'").fetchone()[0]
        if count>=MAX_DELIVERIES:raise ValueError('TELEGRAM_LEDGER_BOUND')
        record=dict(status='UNKNOWN',input_identity=preview['input_identity'],event_id=preview['event_id'],preview_sha256=preview['preview_sha256'],target=TARGET,test=preview['test'],claimed_ms=now_ms)
        store.c.execute('insert into meta values(?,?)',(preview['key'],canonical(record)))
        # The compact claim survives normal event retention; no retry after uncertainty.
    return True


def post(token,preview,*,fetch=urlopen,clock=millis):
    """No logs of URL/token/response description; no retry, even on timeout/429."""
    if not preview['test'] and not 0<=clock()-preview['source_ms']<=preview['max_quote_age_ms']:
        return {'status':'REJECTED','reason':'QUOTE_EXPIRED_BEFORE_HTTP'}
    try:
        body=json.dumps(dict(chat_id=TARGET,text=preview['html'],parse_mode='HTML',link_preview_options={'is_disabled':True},disable_notification=preview['test'])).encode()
        request=Request('https://api.telegram.org/bot'+token+'/sendMessage',data=body,headers={'Content-Type':'application/json'},method='POST')
        with fetch(request,timeout=5)as r:
            raw=r.read(65537)
        if len(raw)>65536:return {'status':'UNKNOWN','reason':'RESPONSE_BOUND'}
        data=json.loads(raw)
        if data.get('ok') is not True:return {'status':'REJECTED','reason':'API_REJECTED'}
        m=data.get('result',{});chat=m.get('chat',{})
        if type(m.get('message_id')) is not int or m['message_id']<=0 or chat.get('username','').lower()!=TARGET[1:] or chat.get('type')!='channel' or type(chat.get('id')) is not int or type(m.get('date')) is not int or m['date']<=0:
            return {'status':'UNKNOWN','reason':'ACK_UNVERIFIED'}
        plain=unescape(re.sub(r'<[^>]+>','',preview['html']))
        link=unescape(re.search(r'<a href="([^"]+)">',preview['html'])[1])
        if m.get('text')!=plain or not any(e.get('type')=='text_link' and e.get('url')==link for e in m.get('entities',[])):
            return {'status':'UNKNOWN','reason':'ACK_CONTENT_UNVERIFIED'}
        return {'status':'SENT','message_id':m['message_id'],'chat_id':chat.get('id'),'telegram_date':m.get('date'),
                'response_text_sha256':sha(plain.encode()),'bucket_link_verified':True}
    except HTTPError as exc:
        return {'status':'UNKNOWN','reason':'HTTP_'+str(exc.code)}
    except Exception as exc:
        return {'status':'UNKNOWN','reason':type(exc).__name__}


def token_from_env():
    token=os.environ.get('HOLY_WEATHER_TELEGRAM_TOKEN','')
    if not re.fullmatch(r'\d+:[A-Za-z0-9_-]{20,}',token):raise ValueError('TELEGRAM_TOKEN_NOT_CONFIGURED')
    return token


def finish(store,preview,result,at_ms):
    with store.transaction():
        row=store.c.execute('select value from meta where key=?',(preview['key'],)).fetchone()
        record=json.loads(row[0])
        if record['status']!='UNKNOWN':raise ValueError('TELEGRAM_ALREADY_FINAL')
        record.update(result,completed_ms=at_ms,event_to_completion_ms=at_ms-preview['event_ms'] if at_ms>=preview['event_ms'] else None,
                      attempt_to_completion_ms=at_ms-record['claimed_ms'] if at_ms>=record['claimed_ms'] else None)
        store.c.execute('update meta set value=? where key=?',(canonical(record),preview['key']))
    return record


class Sender:
    """One HTTP worker; all claim/ack writes stay with the existing SQLite owner."""
    def __init__(self,token,transport=post):
        self.token=token;self.transport=transport;self.thread=None;self.queue=queue.Queue(maxsize=1);self.stopped=False;self.status='IDLE'
    def tick(self,store,at_ms):
        try:preview,result,completed=self.queue.get_nowait()
        except queue.Empty:pass
        else:
            finish(store,preview,result,completed)
            self.status=result['status']
            if result['status']!='SENT':self.stopped=True
        if self.stopped or (self.thread and self.thread.is_alive()):return
        rows=store.c.execute("""select id from events where status='DETECTED' and received_ms>=? and (select identity from inputs where inputs.id=events.input_id) not in
            (select json_extract(value,'$.input_identity') from meta where key like 'telegram_delivery:%'
             and json_extract(value,'$.test')=0 and json_extract(value,'$.input_identity') is not null)
            order by received_ms,id limit 32""",(at_ms-store.cfg.quote_age_seconds*1000,)).fetchall()
        for row in rows:
            try:preview=prepare(store,row[0],at_ms=at_ms)
            except ValueError:continue
            try:
                if not claim(store,preview,now_ms=at_ms):continue
            except ValueError as exc:
                self.status=str(exc);self.stopped=True;return
            self.status='SENDING'
            def send():
                try:result=self.transport(self.token,preview)
                except Exception as exc:result={'status':'UNKNOWN','reason':type(exc).__name__}
                self.queue.put_nowait((preview,result,millis()))
            self.thread=threading.Thread(target=send,daemon=True,name='telegram-one-request');self.thread.start();break
    def close(self,store=None):
        self.stopped=True
        if store is not None:self.tick(store,millis())
        if self.status=='SENDING':self.status='UNKNOWN'


def main(argv=None):
    parser=argparse.ArgumentParser();parser.add_argument('command',choices=['preview','send'])
    parser.add_argument('--config',default='config.lamp.example.toml');parser.add_argument('--event-id');parser.add_argument('--test',action='store_true')
    parser.add_argument('--preview-file');parser.add_argument('--preview-sha')
    a=parser.parse_args(argv)
    try:
        with Store(Config.load(a.config),session=False)as store:
            if a.command=='preview':result=prepare(store,a.event_id,test=a.test)
            else:
                if not a.preview_file or not a.preview_sha:raise ValueError('EXACT_PREVIEW_REQUIRED')
                path=Path(a.preview_file)
                if path.stat().st_size>16384:raise ValueError('PREVIEW_BOUND')
                preview=json.loads(path.read_text())
                if preview.get('preview_sha256')!=a.preview_sha:raise ValueError('PREVIEW_SHA')
                token=token_from_env()
                if not claim(store,preview):result={'status':'ALREADY_ATTEMPTED_NO_RETRY'}
                else:result=finish(store,preview,post(token,preview),millis())
            print(json.dumps(result,ensure_ascii=False));return 0
    except (ValueError,StorageStop)as exc:
        print(json.dumps({'status':'REFUSED','reason':str(exc)}));return 2


if __name__=='__main__':raise SystemExit(main())
