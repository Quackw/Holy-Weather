"""Independent midpoint notifications; caller owns the input transaction."""
from decimal import Decimal, localcontext
import json
from .core import canonical,sha,quote_clock_status

THRESHOLDS=('0.50','0.55','0.60','0.70','0.80')


def midpoint(bid,ask):
    if bid is None or ask is None:return None
    with localcontext() as ctx:
        ctx.prec=220
        b,a=Decimal(bid),Decimal(ask)
        if not b.is_finite() or not a.is_finite() or not 0<=b<=a<=1:return None
        return str((b+a)/2)


def record(store,sample,status):
    token=sample['token']
    if sample['kind'] not in ('book','best_bid_ask'):
        if sample['kind']!='last_trade_price' and status!='FRESH':store.price_alert_ready.discard(token)
        return []
    key='price_alert_v1:'+token
    row=store.c.execute('select value from meta where key=?',(key,)).fetchone()
    state=json.loads(row[0]) if row else {'fired':[],'previous':None}
    price=midpoint(sample['bid'],sample['ask'])
    # Netflix BBO sentinel policy; full books provide explicit levels instead.
    sentinel=sample['kind']=='best_bid_ask' and (sample['bid']=='0' or sample['ask']=='1')
    if not store.connected or status!='FRESH' or price is None or sentinel:
        store.price_alert_ready.discard(token)
        reason='UNCONFIRMED_OR_INVALID_MIDPOINT'
    else:
        previous=state['previous']
        baseline=token not in store.price_alert_ready or previous is None or quote_clock_status(store.cfg,previous['source_ms'],previous['received_ms'],sample['received_ms'])!='FRESH'
        levels=[] if baseline else [t for t in THRESHOLDS if t not in state['fired'] and Decimal(previous['midpoint'])<Decimal(t)<=Decimal(price)]
        current={k:sample[k] for k in ('input_id','id','source_ms','received_ms','bid','ask')}
        current['midpoint']=price
        events=[]
        if levels:
            market=store.market_for_token(token)
            side=next(s for s,t in market['tokens'].items() if t==token)
            ident=sha(['PRICE_ALERT_V1',market['condition_id'],token,levels])
            event=dict(type='PRICE_ALERT',status='DETECTED',track='PRICE',side=side,token=token,
                       thresholds=levels,kind='MULTI_CROSS' if len(levels)>1 else 'CROSS',
                       metric='DECIMAL_MIDPOINT',previous=previous,current=current,market_url=market['url'],bucket=market['bucket'])
            if store.cfg.source_clock_tolerance_ms:
                event['clock_policy']={'max_source_lead_ms':store.cfg.source_clock_tolerance_ms,'previous_source_lead_ms':previous['source_ms']-previous['received_ms'],'current_source_lead_ms':sample['source_ms']-sample['received_ms'],'timestamps_rewritten':False}
            store.c.execute('insert into events values(?,?,?,?,?,?,?,?)',(ident,sample['input_id'],token,sample['received_ms'],'PRICE_ALERT','DETECTED',None,canonical(event)))
            refs={previous['input_id'],sample['input_id']}
            discovery=store.discovery_reference(token)
            if discovery:refs.add(discovery[0])
            for ref in refs:store.c.execute('insert into event_inputs values(?,?)',(ident,ref))
            from .alert_queue import enqueue
            enqueue(store,ident,event,market,sample['received_ms'])
            state['fired']+=levels;events.append(event)
        state['previous']=current
        store.c.execute('insert or replace into meta values(?,?)',(key,canonical(state)))
        store.c.execute('insert or replace into latest values(?,?,?)',('__PRICE_ALERT__'+token,'price_alert',sample['input_id']))
        store.price_alert_ready.add(token)
        reason='BASELINE' if baseline else 'CROSS' if levels else 'NO_NEW_THRESHOLD'
        if events:return events
    if reason=='NO_NEW_THRESHOLD':return []
    store.c.execute('insert into diagnostics(received_ms,category,reason) values(?,?,?)',(sample['received_ms'],'PRICE_ALERT',reason+':'+token))
    store.c.execute('delete from diagnostics where id not in (select id from diagnostics order by id desc limit 512)')
    return []
