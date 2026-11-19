"""Independent forecast-triggered events, within the existing writer transaction."""
from decimal import Decimal
import json
from .core import canonical,sha
from .lamp import MAX_AGE_MS,bucket_context,c_to_f
from .weather import context as weather_context

KEY='lamp_alert_v1'


def remaining(state,market,at):
    if not state:return {},'FIRST_BULLETIN'
    if state.get('status')!='OK':return {},state.get('status','MISSING')
    if max(state['received_ms'],state['available_ms'])>at:return {},'NOT_YET_AVAILABLE'
    if not 0<=at-state['issue_ms']<=MAX_AGE_MS:return {},'STALE_OR_FUTURE'
    hours={h['valid_ms']:h['temperature_f'] for h in state['hours'] if h['valid_ms']>at and h['local_time'][:10]==market['date']}
    if not hours:return {},'NO_REMAINING_HOURS'
    if any(t is None for t in hours.values()):return {},'PARTIAL_TMP'
    return hours,'OK'


def record(store,market,state,at,*,key=KEY,verified_markets=()):
    row=store.c.execute('select value from meta where key=?',(key,)).fetchone()
    saved=json.loads(row[0]) if row else dict(previous=None,seen=[],threshold=store.cfg.lamp_peak_change_f,last_at=None)
    if saved['threshold']!=store.cfg.lamp_peak_change_f:raise ValueError('LAMP_ALERT_THRESHOLD_CHANGED')
    if saved['last_at'] is not None and at<saved['last_at']:raise ValueError('LAMP_ALERT_CLOCK_REVERSED')
    saved['last_at']=at
    current,why=remaining(state,market,at)
    previous=saved['previous'];body=state.get('body_sha256')
    reason=why
    if why!='OK':saved['previous']=None
    elif body in saved['seen']:
        # Previously received guidance is not a new issue and cannot heal a gap.
        return
    else:
        if len(saved['seen'])>=256:raise ValueError('LAMP_ALERT_LEDGER_BOUND')
        saved['seen'].append(body)
        old,old_status=remaining(previous,market,at)
        reason='BASELINE' if previous is None else 'INCOMPARABLE_'+old_status
        if old_status=='OK' and old.keys()==current.keys():
            before=max(map(Decimal,old.values()));after=max(map(Decimal,current.values()))
            boundary=bucket_context(market,None,None);crossed=[]
            if boundary['status']=='VERIFIED':
                for k in ('lower_f','upper_f'):
                    if boundary[k] is None:continue
                    x=Decimal(boundary[k])
                    if before<x<=after or after<x<=before:crossed.append(str(x))
            significant=abs(after-before)>=Decimal(saved['threshold'])
            reason='NO_MATERIAL_CHANGE'
            if crossed or significant:
                weather=weather_context(store.weather_state(),at,at)
                prices=store.price_context(at,market=market)
                event=dict(type='LAMP_ALERT',track='LAMP',status='DETECTED',
                    reason='BOUNDARY_AND_PEAK_CHANGE' if crossed and significant else 'BOUNDARY_CROSS' if crossed else 'PEAK_CHANGE',
                    previous_peak_f=str(before),future_peak_f=str(after),change_f=str(after-before),
                    threshold_f=saved['threshold'],crossed_boundaries_f=crossed,bucket_boundaries=boundary,
                    previous_forecast=previous,current_forecast=state,compared_hours=sorted(current),
                    weather=weather,prices=prices,observed_max_f=c_to_f(weather.get('observed_max_c')),
                    market_url=market['url'],bucket=market['bucket'],station=market['station'],at_ms=at,
                    market_reaction='NOT_ESTABLISHED',trade_signal=False)
                ident=sha(['LAMP_ALERT_V1',market['condition_id'],body])
                token=next(iter(market['tokens'].values()))
                store.c.execute('insert into events values(?,?,?,?,?,?,?,?)',(ident,state['input_id'],token if key!=KEY else '__LAMP__',at,'LAMP_ALERT','DETECTED',event['reason'],canonical(event)))
                refs={previous['input_id'],state['input_id']}|set(weather.get('input_ids',[]))
                refs|={q['input_id'] for q in prices.values() if q.get('input_id')}
                discovery=store.discovery_reference(token)
                if discovery:refs.add(discovery[0])
                for ref in refs:store.c.execute('insert into event_inputs values(?,?)',(ident,ref))
                from .alert_queue import enqueue
                enqueue(store,ident,event,market,at,verified_markets=verified_markets)
                reason=event['reason']
        elif old_status=='OK':reason='INCOMPARABLE_REMAINING_HOURS'
        saved['previous']=state
    store.c.execute('insert or replace into meta values(?,?)',(key,canonical(saved)))
    label='__LAMP_ALERT__'+(':'+key if key!=KEY else '')
    store.c.execute('delete from latest where token=?',(label,))
    if saved['previous']:
        store.c.execute('insert into latest values(?,?,?)',(label,'lamp_alert',saved['previous']['input_id']))
    store.c.execute('insert into diagnostics(received_ms,category,reason) values(?,?,?)',(at,'LAMP_ALERT',reason))
    store.c.execute('delete from diagnostics where id not in (select id from diagnostics order by id desc limit 512)')
