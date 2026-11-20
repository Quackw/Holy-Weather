"""Opt-in US warming notice and hourly temperature revisions; one city, one owner.

This delivery policy does not change the original 2 F LAMP detector. Only actual
temperature differences count, on identical future UTC hours and a verified day.
"""
from datetime import datetime, time
from decimal import Decimal
from zoneinfo import ZoneInfo
import zlib

from . import catalogue_metar, city_peak, lamp_alert
from .alert_queue import enqueue
from .core import canonical, sha
from .lamp import MAX_AGE_MS, bucket_context
from .lamp_card import position

PREFIX='us_lamp_hourly_v2:'
POLICY='US_WARMING_HOURLY_V2'
HOUR=city_peak.HOUR
WARMING_LEAD_MS=HOUR  # One forecast-hour lead, not a forecast of minute precision.


def record(store, markets, forecast, at):
    """Called inside the owner's transaction; event, state and queue are atomic."""
    if not markets or forecast.get('status')!='OK':return
    market=markets[0];station=market['station'];day=market['date'];key=PREFIX+station
    saved=catalogue_metar.load(store,key)
    if saved.get('day','')>day:return
    legacy=catalogue_metar.load(store,'us_lamp_city_v1:'+station)
    def save():store.c.execute('insert or replace into meta values(?,?)',(key,canonical(saved)))
    if saved.get('day')!=day:
        if saved:
            raw=canonical(dict(type='US_LAMP_HOURLY_DAY',station=station,state=saved)).encode()
            store.c.execute('insert or ignore into inputs(identity,received_ms,raw,raw_sha) values(?,?,?,?)',
                            (sha(raw),at,zlib.compress(raw,3),sha(raw)))
        saved=dict(day=day,baseline_ms=forecast.get('issue_ms'),status='BASELINE',
                   initial_event_id=legacy.get('event_id') if legacy.get('day')==day else None)
        save()
        return  # First new issue/day is a baseline, including enablement mid-day.
    if at<saved.get('last_at_ms',0):raise ValueError('LAMP_HOURLY_CLOCK_REVERSED')
    if at<saved.get('next_review_ms',0):return
    weather=catalogue_metar.load(store,catalogue_metar.PREFIX+station)
    view,status=city_peak.outlook(weather,forecast,market,at,require_warming=False)
    if not view:
        saved.update(status=status,last_at_ms=at,next_review_ms=at+60000);save()
        return
    hours,_=lamp_alert.remaining(forecast,market,at)
    snapshot=dict(at_ms=at,issue_ms=forecast['issue_ms'],
                  input_id=forecast['input_id'],body_sha256=forecast['body_sha256'],source=forecast['source'],
                  received_ms=forecast['received_ms'],available_ms=forecast['available_ms'],
                  hours={str(h):temp for h,temp in hours.items()},
                  current_f=view['current_f'],observed_ms=view['observed_ms'],future_peak_f=view['future_peak_f'])
    previous=saved.get('snapshot')
    local=datetime.fromtimestamp(at/1000,ZoneInfo(market['timezone']))
    daytime=city_peak.LOCAL_SEND_START<=local.hour<city_peak.LOCAL_SEND_END
    reason=None;changes=[];metar_changes=[]
    if not saved.get('initial_event_id'):
        if Decimal(view['future_peak_f'])<Decimal(view['observed_max_f']):status='PEAK_ALREADY_OBSERVED'
        elif forecast['issue_ms']>saved['baseline_ms'] and Decimal(view['warming_f'])>0:
            start=min(h for h,temp in hours.items() if Decimal(temp)>Decimal(view['current_f']))
            if daytime and start-WARMING_LEAD_MS<=at<view['peak_ms']:
                reason='WARMING_START';view['warming_start_ms']=start
            else:status='WAITING_WARMING_START' if daytime else 'WAITING_LOCAL_SEND_WINDOW'
    elif previous is None:
        status='HOURLY_BASELINE'  # Never replay changes from before policy enablement.
    elif at-previous['at_ms']>MAX_AGE_MS or at-previous['issue_ms']>MAX_AGE_MS:
        status='HOURLY_GAP_BASELINE'
    else:
        old={int(h):temp for h,temp in previous['hours'].items() if int(h)>at}
        if old.keys()!=hours.keys():status='INCOMPARABLE_REMAINING_HOURS'
        else:
            changes=[h for h,temp in hours.items() if Decimal(temp)!=Decimal(old[h])]
            from .lamp import c_to_f
            metar_changes=[dict(observed_ms=r['observed_ms'],temperature_f=c_to_f(r['temperature_c']))
                           for r in weather['records'] if r['observed_ms']>previous['observed_ms']
                           and Decimal(c_to_f(r['temperature_c']))!=Decimal(previous['current_f'])]
            metar_changed=bool(metar_changes)
            if daytime and (changes or metar_changed):
                reason='FORECAST_AND_METAR_TEMPERATURE_CHANGE' if changes and metar_changed else (
                    'FORECAST_TEMPERATURE_CHANGE' if changes else 'METAR_TEMPERATURE_CHANGE')
            else:status='NO_TEMPERATURE_CHANGE' if daytime else 'WAITING_LOCAL_SEND_WINDOW'
    if reason:
        matches=[m for m in markets if m['station']==station and m['date']==day
                 and m['event_id']==market['event_id'] and m['metadata_state']=='OPEN_AT_DISCOVERY_RECEIPT'
                 and position(Decimal(view['daily_peak_f']),bucket_context(m,None,None))=='внутри']
        proof=store.discovery_reference(matches[0]['tokens']['YES']) if len(matches)==1 else None
        if not proof or not store.active(matches[0]['condition_id'].lower(),at):status='NO_UNIQUE_VERIFIED_BUCKET';reason=None
        elif store.c.execute('select received_ms from inputs where id=?',(proof[0],)).fetchone()[0]>at:
            status='MARKET_NOT_AVAILABLE';reason=None
    if reason:
        market=matches[0]
        event_id=sha([POLICY,station,day,'INITIAL' if reason=='WARMING_START' else at//HOUR])
        closes=int(datetime.combine(local.date(),time(city_peak.LOCAL_SEND_END),ZoneInfo(market['timezone'])).timestamp()*1000)
        event=dict(type='LAMP_ALERT',track='LAMP',status='DETECTED',reason=reason,station=station,at_ms=at,
                   market_url=market['url'],bucket=market['bucket'],city_outlook=view,current_forecast=forecast,
                   metar=weather,notification_policy=POLICY,previous_review=previous,
                   changed_forecast_hours=changes,metar_changes=metar_changes if previous and reason!='WARMING_START' else [],
                   delivery_window=dict(policy=POLICY,closes_ms=closes))
        store.c.execute('insert into events values(?,?,?,?,?,?,?,?)',
                        (event_id,forecast['input_id'],market['tokens']['YES'],at,'LAMP_ALERT','DETECTED',reason,canonical(event)))
        refs={forecast['input_id'],proof[0]}
        if previous:refs.add(previous['input_id'])
        store.c.executemany('insert into event_inputs values(?,?)',[(event_id,ref) for ref in refs])
        enqueue(store,event_id,event,market,at)
        if reason=='WARMING_START':
            saved['initial_event_id']=event_id
            # Preserve the original daily ledger/ID if it already exists.
            store.c.execute('insert or replace into meta values(?,?)',('us_lamp_city_v1:'+station,
                canonical(dict(legacy,day=day,baseline_ms=saved['baseline_ms'],status='RECORDED',event_id=event_id))))
        saved['last_event_id']=event_id;status='RECORDED'
    saved.update(status=status,last_at_ms=at,snapshot=snapshot)
    # Before the initial notice check the warming start as each source arrives;
    # afterwards combine the two sources into at most one hourly city review.
    saved['next_review_ms']=at+HOUR if saved.get('initial_event_id') else at+60000
    save()


def review(store, selected, at):
    """An hourly review also runs when the new NOAA issue failed to arrive."""
    for _,station,_ in store.cfg.cities.values():
        markets=[m for c,m in store.markets.items() if c in selected and m['station']==station and store.active(c,at)]
        saved=catalogue_metar.load(store,PREFIX+station)
        if markets and (saved.get('day')!=markets[0]['date'] or at>=saved.get('next_review_ms',0)):
            forecast=store.lamp_state(markets[0])
            if forecast.get('status')=='OK':
                with store.transaction():record(store,markets,forecast,at)
