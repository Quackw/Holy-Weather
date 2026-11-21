"""Accepted pre-transport-recovery function from PR53 base 516f66f.

Kept for compatible current-data rollback. Original function SHA256: d6777a02d7457dae399b47d8c1bd32516f656cbd932f2c4a44dab18b011686dd.
"""
import json
from holy_weather.core import canonical
from holy_weather.recovery import RecoveryStop


def prepare_resume(store,identity,at,*,carry_pending_price=False,carry_pending_lamp=False):
    """Keep eligible unclaimed PRICE/LAMP work across a clean window handoff."""
    disabled=False
    with store.transaction():
        previous=store.c.execute("select value from meta where key='price_recovery_control_v1'").fetchone()
        if previous and json.loads(previous[0])!=identity:raise RecoveryStop('RECOVERY_CONTROL_BINDING')
        store.c.execute("insert or ignore into meta values('price_recovery_control_v1',?)",(canonical(identity),))
        pending=[]
        for key,raw in store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'"):
            v=json.loads(raw)
            if v['status']=='UNKNOWN' or v.get('claimed_ms') is not None and v['status']!='SENT':disabled=True
            if v['status'] in ('PENDING','WAITING_BID'):
                if v.get('track') not in ('PRICE_ALERT','LAMP_ALERT'):raise RecoveryStop('RECOVERY_UNEXPECTED_PENDING_TRACK')
                pending.append((key,v))
        for key,v in pending:
            eligible_track=carry_pending_price or carry_pending_lamp and v['track']=='LAMP_ALERT'
            if (eligible_track and not disabled and v['status']=='PENDING' and v.get('claimed_ms') is None
                    and v.get('confirmation') is None and v.get('delivery_notes') is None
                    and v['event_ms']<=at<=v['expires_ms']):
                event=store.c.execute('select token from events where id=? and type=?',(v['event_id'],v['track'])).fetchone()
                if event:
                    market=store.market_for_token(event[0])
                    if (market['date']==v['market_date'] and market['timezone']==v['timezone']
                            and store.active(market['condition_id'].lower(),at)):
                        # DayDiscovery revalidates the contract before the sender runs.
                        continue
            v.update(status='EXPIRED',reason='NO_BACKLOG_ON_RECOVERY',completed_ms=at)
            store.c.execute('update meta set value=? where key=?',(canonical(v),key))
    return disabled
