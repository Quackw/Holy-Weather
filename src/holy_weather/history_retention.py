"""Move settled old catalogue state into the full archive, keeping live obligations."""
from datetime import datetime,timedelta,timezone
import json
from zoneinfo import ZoneInfo
from .core import sha

CATALOGUE='price_catalogue_v1:'
DELIVERY='alert_delivery_v1:'
TERMINAL={'SENT','REJECTED','EXPIRED','MERGED'}


def selection(c,at_ms,days):
    """Unknown receipts and their markets remain active, even after their date."""
    values=dict(c.execute("select key,value from meta where key not like 'history_seen_v1:%'"))
    retired={}
    for key,raw in values.items():
        if not key.startswith(CATALOGUE):continue
        market=json.loads(raw)
        today=datetime.fromtimestamp(at_ms/1000,timezone.utc).astimezone(ZoneInfo(market['timezone'])).date()
        if market['date']<(today-timedelta(days=days)).isoformat():retired[key]=market
    token_keys={token:key for key,market in retired.items() for token in market['tokens'].values()}
    for key,raw in values.items():
        if not key.startswith(DELIVERY):continue
        value=json.loads(raw)
        if value.get('status') in TERMINAL:continue
        token=value.get('token')
        if not token:
            event=c.execute('select token from events where id=?',(value.get('event_id'),)).fetchone()
            token=event[0] if event else None
        key=token_keys.get(token)
        if key:retired.pop(key,None)
    conditions={m['condition_id'].lower() for m in retired.values()}
    tokens={token for m in retired.values() for token in m['tokens'].values()}
    station_days={m['station']+':'+m['date'] for m in retired.values()}
    station_days-={m['station']+':'+m['date'] for k,raw in values.items()
                  if k.startswith(CATALOGUE) and k not in retired for m in [json.loads(raw)]}
    meta=set(retired)
    for key,raw in values.items():
        if key.startswith('price_alert_v1:') and key.removeprefix('price_alert_v1:') in tokens:meta.add(key)
        if key.startswith('market_closed:') and key.removeprefix('market_closed:') in conditions:meta.add(key)
        if key.startswith('lamp_alert_v1:') and key.removeprefix('lamp_alert_v1:') in conditions:meta.add(key)
        if key.startswith('lamp_context_v1:') and key.removeprefix('lamp_context_v1:') in station_days:meta.add(key)
        if key.startswith(DELIVERY):
            value=json.loads(raw)
            event=c.execute('select token from events where id=?',(value.get('event_id'),)).fetchone()
            if value.get('status') in TERMINAL and event and event[0] in tokens:meta.add(key)
        if key.startswith('price_catalogue_day_refusal_v1:'):
            value=json.loads(raw)
            if value.get('at_ms',at_ms)>=0 and value.get('at_ms',at_ms)<at_ms-days*86400000:meta.add(key)
    # Older audit bundles remain in the source snapshot. Keep the newest two
    # links of each namespace for the current in-flight transition/rotation.
    for prefix in ('price_control_audit_v1:','price_transition_v1:'):
        audit=[]
        for key,raw in values.items():
            if key.startswith(prefix):
                value=json.loads(raw);at=value.get('at_ms',value.get('state',{}).get('last_ms',at_ms))
                audit.append((at,key))
        for at,key in sorted(audit)[:-2]:
            if at<at_ms-days*86400000:meta.add(key)
    latest=set(tokens)|{'__PRICE_ALERT__'+t for t in tokens}|{'__MARKET__'+c for c in conditions}
    latest|={'__LAMP_ALERT__:lamp_alert_v1:'+c for c in conditions}
    latest|={prefix+':lamp_context_v1:'+day for day in station_days for prefix in ('__LAMP__','__LAMP_POLL__')}
    return {'meta':meta,'tokens':tokens,'latest':latest}


def retired_token_hashes(candidate):
    """A retired token can never be registered again as a new contract."""
    hashes=set()
    for raw, in candidate.execute('''select o.value from original.meta o left join main.meta n on n.key=o.key
        where o.key like 'price_catalogue_v1:%' and n.key is null'''):
        market=json.loads(raw)
        hashes.update(bytes.fromhex(sha(['RETIRED_PRICE_TOKEN_V1',token])) for token in market['tokens'].values())
    return iter(sorted(hashes))
