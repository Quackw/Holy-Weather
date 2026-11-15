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
PRICE_BATCH_MS=30000


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


def batch_card(store,ids,market,at):
    events=[json.loads(store.c.execute('select payload from events where id=?',(i,)).fetchone()[0]) for i in ids]
    if len(events)==1:return card(events[0],market,at)[0]
    city=next(k for k,v in CITIES.items() if v[1]==market['station']).replace('-',' ').title()
    lines=['📈 <b>PRICE ALERT</b>',escape(city)+' · '+escape(market['bucket'])]
    for side in ('YES','NO'):
        group=[e for e in events if e['side']==side]
        if not group:continue
        levels=sorted({t for e in group for t in e['thresholds']},key=Decimal)
        lines.append(side+' midpoint: <b>'+num(Decimal(group[0]['previous']['midpoint'])*100)+'% → '+num(Decimal(group[-1]['current']['midpoint'])*100)+'%</b>')
        lines.append('Crossed: '+' · '.join(num(Decimal(t)*100)+'%' for t in levels))
    if len({e['side'] for e in events})>1:lines.append('Both sides moved')
    lines.append('<a href="'+escape(market['url'],quote=True)+'">Market ↗</a>')
    return '\n'.join(lines)


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
    filtered=store.cfg.price_notification_filter and event['type']=='PRICE_ALERT'
    waiting=filtered and Decimal(event['current']['bid'])<=Decimal(event['previous']['bid'])
    key=PREFIX+sha([TARGET,event['type'],event_id])
    if store.c.execute('select 1 from meta where key=?',(key,)).fetchone():return
    html,expires=card(event,market,at)
    if event['type']=='PRICE_ALERT':expires=min(event['current']['source_ms'],event['current']['received_ms'])+store.cfg.quote_age_seconds*1000
    # Keep the contract-day limit valid between claim and the final HTTP check.
    expires=day_expiry(expires,market['date'],market['timezone'])
    if filtered and not waiting:
        row=store.c.execute("select key,value from meta where key like ? and json_extract(value,'$.status')='PENDING' and json_extract(value,'$.delivery_notes') is null and json_extract(value,'$.confirmation') is null and json_extract(value,'$.batch_market')=? and json_extract(value,'$.side')=? and json_extract(value,'$.batch_until_ms')>? order by json_extract(value,'$.event_ms') limit 1",(PREFIX+'%',market['url'],event['side'],at)).fetchone()
        if row:
            v=json.loads(row[1])
            if v['event_ms']<=at<=v['expires_ms'] and len(v['event_ids'])<10:
                v['event_ids'].append(event_id)
                v['expires_ms']=min(v['expires_ms'],expires)
                v['not_before_ms']=min(v['not_before_ms'],max(at,v['expires_ms']-1000))
                v['html']=batch_card(store,v['event_ids'],market,at)
                store.c.execute('update meta set value=? where key=?',(canonical(v),row[0]))
                admission('QUEUED','BATCHED')
                saved['delivery_admission']['queue_key']=row[0]
                store.c.execute('update events set payload=? where id=?',(canonical(saved),event_id))
                return
    # Keep final receipts for audit/dedupe; only unresolved work consumes slots.
    # Unknown/malformed statuses remain capacity-consuming (fail closed).
    count=store.c.execute("select count(*) from meta where key like ? and coalesce(json_extract(value,'$.status'),'') not in ('SENT','REJECTED','EXPIRED','MERGED')",(PREFIX+'%',)).fetchone()[0]
    if count>=LIMIT:
        # Event remains durable; fail closed on delivery without stopping capture.
        store.c.execute("insert or replace into meta values('alert_queue_status',?)",(canonical({'status':'FULL','limit':LIMIT,'at_ms':at}),))
        admission('NOT_QUEUED','QUEUE_FULL');return
    value=dict(status='PENDING',track=event['type'],event_id=event_id,event_ms=at,html=html,
               expires_ms=expires,target=TARGET,market_date=market['date'],timezone=market['timezone'])
    if filtered:
        value.update(event_ids=[event_id],side=event['side'],token=event['token'],batch_market=market['url'],batch_until_ms=at+PRICE_BATCH_MS,
                     not_before_ms=min(at+PRICE_BATCH_MS,max(at,expires-1000)))
    if waiting:
        value.update(status='WAITING_BID',wait_until_ms=min(at+store.cfg.price_confirmation_seconds*1000,expires),
                     original_bid=event['current']['bid'],original_source_ms=event['current']['source_ms'],
                     thresholds=event['thresholds'],confirmation='WAITING')
    store.c.execute('insert into meta values(?,?)',(key,canonical(value)))
    admission('QUEUED','WAITING_BID' if waiting else 'ACCEPTED')
    store.c.execute("delete from meta where key='alert_queue_status'")


def settle_waits(store,at,*,sample=None,status=None,gap=None,token=None):
    """Caller owns transaction. Delivery-only evidence; never change PRICE state."""
    from .core import quote_clock_status
    from .price_alert import midpoint
    rows=store.c.execute("select key,value from meta where key like ? and json_extract(value,'$.status')='WAITING_BID'",(PREFIX+'%',)).fetchall()
    for key,raw in rows:
        v=json.loads(raw)
        if token is not None and v['token']!=token:continue
        reason=None
        if gap:
            v.update(status='REJECTED',reason='CONFIRMATION_GAP:'+gap,confirmation='GAP',completed_ms=at)
        elif sample and sample['token']==v['token'] and sample['kind']!='last_trade_price' and status!='FRESH':
            v.update(status='REJECTED',reason='CONFIRMATION_INVALID_QUOTE:'+str(status),confirmation='GAP',completed_ms=at)
        elif at>v['expires_ms']:
            v.update(status='EXPIRED',reason='CONFIRMATION_EXPIRED',confirmation='EXPIRED',completed_ms=at)
        else:
            if sample and sample['token']==v['token'] and v['event_ms']<at<=v['wait_until_ms'] and store.connected and status=='FRESH' and sample['kind'] in ('book','best_bid_ask'):
                price=midpoint(sample['bid'],sample['ask'])
                sentinel=sample['kind']=='best_bid_ask' and (sample['bid']=='0' or sample['ask']=='1')
                if price is not None and not sentinel and sample['source_ms']>v['original_source_ms'] and quote_clock_status(store.cfg,sample['source_ms'],sample['received_ms'],at)=='FRESH' and Decimal(sample['bid'])>Decimal(v['original_bid']) and Decimal(price)>=max(map(Decimal,v['thresholds'])):
                    reason='LATER_BID_CONFIRMATION'
                    v['confirmation_quote']={k:sample[k] for k in ('input_id','source_ms','received_ms','bid','ask')}
                    store.c.execute('insert or ignore into event_inputs values(?,?)',(v['event_id'],sample['input_id']))
            if reason:
                v.update(status='PENDING',confirmation='CONFIRMED',reason=reason,confirmed_ms=at)
            elif at>=v['wait_until_ms']:
                v.update(confirmation='UNCONFIRMED',reason='NO_BID_CONFIRMATION_WITHIN_WINDOW',completed_ms=at)
                if any(Decimal(t)>=Decimal('0.70') for t in v['thresholds']):
                    v.update(status='PENDING')
                    v['html']+='\nAsk-only · not confirmed by bid'
                else:v.update(status='REJECTED')
            else:continue
        store.c.execute('update meta set value=? where key=?',(canonical(v),key))


def advance_waits(store,at):
    with store.transaction():
        settle_waits(store,at)
        # Only settled, unclaimed same-side work can combine. UNKNOWN is immutable.
        rows=store.c.execute("select key,value from meta where key like ? and json_extract(value,'$.status')='PENDING' and json_extract(value,'$.batch_market') is not null order by json_extract(value,'$.event_ms'),key",(PREFIX+'%',)).fetchall()
        groups={}
        for key,raw in rows:
            v=json.loads(raw)
            if at>v['expires_ms']:
                v.update(status='EXPIRED',reason='DELIVERY_EXPIRED',completed_ms=at)
                store.c.execute('update meta set value=? where key=?',(canonical(v),key));continue
            identity=(v['batch_market'],v.get('side'))
            previous=groups.get(identity)
            if previous and v['event_ms']<previous[1]['batch_until_ms'] and len(previous[1]['event_ids'])+len(v['event_ids'])<=10:
                k,a=previous
                a['event_ids']+=v['event_ids']
                a['expires_ms']=min(a['expires_ms'],v['expires_ms'])
                a['not_before_ms']=min(a['not_before_ms'],max(at,a['expires_ms']-1000))
                notes=a.setdefault('delivery_notes',{})
                if a.get('confirmation'):notes[a['event_id']]=a['confirmation']
                notes.update(v.get('delivery_notes',{}))
                if v.get('confirmation'):notes[v['event_id']]=v['confirmation']
                event=json.loads(store.c.execute('select payload from events where id=?',(a['event_id'],)).fetchone()[0])
                market=store.market_for_token(event['token'])
                a['html']=batch_card(store,a['event_ids'],market,at)
                if 'UNCONFIRMED' in notes.values():a['html']+='\nIncludes ask-only · not confirmed by bid'
                v.update(status='MERGED',queue_key=k,completed_ms=at)
                store.c.execute('update meta set value=? where key=?',(canonical(a),k))
                store.c.execute('update meta set value=? where key=?',(canonical(v),key))
            else:groups[identity]=(key,v)



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
        if not p['test'] and at<v.get('not_before_ms',v['event_ms']):return False
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
