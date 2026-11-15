"""One durable, bounded queue for independent PRICE/LAMP events; no new schema."""
import json
from decimal import Decimal
from datetime import datetime,timezone,timedelta
from html import escape
from urllib.parse import urlparse
from .core import canonical,sha,CITIES

PREFIX='alert_delivery_v1:'
TARGET='@holy_weather'
LIMIT=1024


def num(v):return format(Decimal(str(v)).normalize(),'f')
def stamp(ms):return datetime.fromtimestamp(ms/1000,timezone.utc).strftime('%H:%M:%S UTC')


def day_expiry(expires,day,zone):
    from zoneinfo import ZoneInfo
    next_day=(datetime.fromisoformat(day)+timedelta(days=1)).replace(tzinfo=ZoneInfo(zone))
    return min(expires,int(next_day.timestamp()*1000)-1)


def card(event,market,at):
    url=event['market_url'];p=urlparse(url)
    if p.scheme!='https' or p.netloc!='polymarket.com' or len(p.path.strip('/').split('/'))!=3 or p.query or p.fragment:raise ValueError('EXACT_BUCKET_LINK_REQUIRED')
    city=next(k for k,v in CITIES.items() if v[1]==market['station']).replace('-',' ').title()
    link='<a href="'+escape(url,quote=True)+'">'+escape(event['bucket'])+'</a>'
    market_link='<a href="'+escape(url,quote=True)+'">Market ↗</a>'
    title=escape(city)+' · '+escape(event['bucket'])
    if event['type']=='PRICE_ALERT':
        a,b=event['previous'],event['current']
        lines=['📈 <b>PRICE ALERT</b>',escape(city)+' · '+event['side']+' · '+escape(event['bucket']),
               'Midpoint: <b>'+num(Decimal(a['midpoint'])*100)+'% → '+num(Decimal(b['midpoint'])*100)+'%</b>',
               'Crossed: '+ ' · '.join(num(Decimal(t)*100)+'%' for t in event['thresholds']),market_link]
        expires=b['source_ms']+90000
    else:
        f=event['current_forecast']
        reason='Crossed boundary: '+', '.join(event['crossed_boundaries_f'])+'°F' if event['crossed_boundaries_f'] else 'Peak change: '+num(event['change_f'])+'°F'
        lines=['☀️ <b>LAMP ALERT</b>',title,
               'Forecast peak: <b>'+num(event['previous_peak_f'])+'°F → '+num(event['future_peak_f'])+'°F</b>',reason,market_link]
        expires=f['issue_ms']+7200000
    text='\n'.join(lines)
    if len(text)>3500:raise ValueError('CARD_BOUND')
    return text,expires


def enqueue(store,event_id,event,market,at):
    """Called in the same transaction as its originating event."""
    row=store.c.execute('select payload from events where id=?',(event_id,)).fetchone()
    if not row:raise ValueError('DELIVERY_EVENT_MISSING')
    saved=json.loads(row[0])
    # A previous refusal is final: never backfill it when capacity reappears.
    if 'delivery_admission' in saved:return
    def admission(status,reason):
        saved['delivery_admission']=dict(status=status,reason=reason,at_ms=at)
        store.c.execute('update events set payload=? where id=?',(canonical(saved),event_id))
    if not store.cfg.alert_queue_enabled:
        admission('NOT_QUEUED','QUEUE_DISABLED');return
    key=PREFIX+sha([TARGET,event['type'],event_id])
    if store.c.execute('select 1 from meta where key=?',(key,)).fetchone():return
    # Keep final receipts for audit/dedupe; only unresolved work consumes slots.
    # Unknown/malformed statuses remain capacity-consuming (fail closed).
    count=store.c.execute("select count(*) from meta where key like ? and coalesce(json_extract(value,'$.status'),'') not in ('SENT','REJECTED','EXPIRED')",(PREFIX+'%',)).fetchone()[0]
    if count>=LIMIT:
        # Event remains durable; fail closed on delivery without stopping capture.
        store.c.execute("insert or replace into meta values('alert_queue_status',?)",(canonical({'status':'FULL','limit':LIMIT,'at_ms':at}),))
        admission('NOT_QUEUED','QUEUE_FULL');return
    html,expires=card(event,market,at)
    if event['type']=='PRICE_ALERT':expires=min(event['current']['source_ms'],event['current']['received_ms'])+store.cfg.quote_age_seconds*1000
    # Keep the contract-day limit valid between claim and the final HTTP check.
    expires=day_expiry(expires,market['date'],market['timezone'])
    value=dict(status='PENDING',track=event['type'],event_id=event_id,event_ms=at,html=html,
               expires_ms=expires,target=TARGET,market_date=market['date'],timezone=market['timezone'])
    store.c.execute('insert into meta values(?,?)',(key,canonical(value)))
    admission('QUEUED','ACCEPTED')
    store.c.execute("delete from meta where key='alert_queue_status'")


def preview(store,key,*,test=False):
    row=store.c.execute('select value from meta where key=?',(key,)).fetchone()
    if not row:raise ValueError('QUEUE_ENTRY_MISSING')
    v=json.loads(row[0]);body=dict(key=key,target=TARGET,event_id=v['event_id'],event_ms=v['event_ms'],
        html=('TEST · '+v['html']+'\nTEST · Preview only; not live market data.' if test else v['html']),
        test=test,expires_ms=day_expiry(v['expires_ms'],v['market_date'],v['timezone']))
    return dict(body,preview_sha256=sha(body))


def claim(store,p,at):
    from zoneinfo import ZoneInfo
    if preview(store,p['key'],test=p['test'])!=p:raise ValueError('PREVIEW_CHANGED')
    with store.transaction():
        v=json.loads(store.c.execute('select value from meta where key=?',(p['key'],)).fetchone()[0])
        if v['status']!='PENDING':return False
        today=datetime.fromtimestamp(at/1000,timezone.utc).astimezone(ZoneInfo(v['timezone'])).date().isoformat()
        if not p['test'] and (at<v['event_ms'] or at>p['expires_ms'] or today!=v['market_date']):
            v.update(status='EXPIRED',completed_ms=at)
            store.c.execute('update meta set value=? where key=?',(canonical(v),p['key']));return False
        v.update(status='UNKNOWN',claimed_ms=at,preview_sha256=p['preview_sha256'])
        store.c.execute('update meta set value=? where key=?',(canonical(v),p['key']))
    return True


def finish(store,p,result,at):
    with store.transaction():
        v=json.loads(store.c.execute('select value from meta where key=?',(p['key'],)).fetchone()[0])
        if v['status']!='UNKNOWN':raise ValueError('DELIVERY_ALREADY_FINAL')
        v.update(result,completed_ms=at,attempt_ms=max(0,at-v['claimed_ms']))
        store.c.execute('update meta set value=? where key=?',(canonical(v),p['key']))
    return v
