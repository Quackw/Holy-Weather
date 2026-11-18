"""Original hourly LAMP, not an observed or modelled daily Tmax."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from html.parser import HTMLParser
import re
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
from .core import sha, CITIES, US_CITIES
from .weather import WeatherPoller, millis, station_check

SOURCE='https://lamp.mdl.nws.noaa.gov/cgi-bin/getlav.pl?sta=KLGA'
MAX_BODY=65536
MAX_AGE_MS=2*60*60*1000  # explicit hourly-guidance context policy, not market TTL
NOMADS='https://nomads.ncep.noaa.gov/pub/data/nccf/com/lmp/prod/'


def nomads_source(issue_ms):
    issued=datetime.fromtimestamp(issue_ms/1000,timezone.utc)
    return NOMADS+issued.strftime('lmp.%Y%m%d/lmp.t%H30z.lavtxt.ascii')


class _Pre(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True); self.blocks=[]; self.inside=False
    def handle_starttag(self,tag,attrs):
        if tag.lower()=='pre':self.blocks.append('');self.inside=True
    def handle_endtag(self,tag):
        if tag.lower()=='pre':self.inside=False
    def handle_data(self,data):
        if self.inside:self.blocks[-1]+=data


def source_for(station):
    if station not in {v[1] for v in US_CITIES.values()}:raise ValueError('LAMP_STATION')
    return f'https://lamp.mdl.nws.noaa.gov/cgi-bin/getlav.pl?sta={station}'


def fetch_lamp(*,station='KLGA',fetch=urlopen,clock=millis):
    source=source_for(station)
    requested=clock()
    try:
        with fetch(Request(source,headers={'User-Agent':'Holy-Weather/0.3 bounded-KLGA-research'}),timeout=5)as response:
            status=response.status;body=response.read(MAX_BODY+1)
        received=clock()
        if len(body)>MAX_BODY:
            return dict(source=source,requested_ms=requested,received_ms=received,status='BODY_BOUND',terminal=True)
        return dict(source=source,requested_ms=requested,received_ms=received,http=status,
                    status='OK' if status==200 else 'NO_DATA' if status==204 else 'HTTP_ERROR',
                    terminal=status not in (200,204),raw_body=body.decode('utf-8'),sha256=sha(body))
    except HTTPError as exc:
        return dict(source=source,requested_ms=requested,received_ms=clock(),http=exc.code,
                    status='ACCESS_DENIED' if exc.code in (401,403,429) else 'HTTP_ERROR',terminal=True)
    except (OSError,URLError,UnicodeError,ValueError)as exc:
        return dict(source=source,requested_ms=requested,received_ms=clock(),status='NETWORK_OR_DECODE_ERROR',error=type(exc).__name__,terminal=True)


def decode(receipt,market,available_ms):
    station_check(market)
    requested,received=receipt['requested_ms'],receipt['received_ms']
    native=re.fullmatch(re.escape(NOMADS)+r'lmp\.\d{8}/lmp\.t\d{2}30z\.lavtxt\.ascii',receipt['source'])
    if receipt['source']!=source_for(market['station']) and not native:raise ValueError('LAMP_SOURCE')
    if any(type(t)is not int for t in (requested,received,available_ms)) or not 0<=requested<=received<=available_ms:
        raise ValueError('LAMP_RECEIPT_CLOCK')
    state=dict(status=receipt['status'],received_ms=received,available_ms=available_ms,
               source=receipt['source'],hours=[],issue_ms=None,units='F',day=market['date'])
    if receipt['status']!='OK':return state
    raw=receipt['raw_body'];body=raw.encode()
    if len(body)>MAX_BODY or sha(body)!=receipt['sha256']:raise ValueError('LAMP_BODY_SHA_OR_BOUND')
    if native:
        text=raw
        pattern=r'\s*([A-Z0-9]{4})\s+GFS LAMP GUIDANCE\s+(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{2})(30) UTC\s*'
    else:
        parser=_Pre();parser.feed(raw)
        if len(parser.blocks)!=1:raise ValueError('LAMP_SINGLE_BULLETIN_REQUIRED')
        text=parser.blocks[0]
        pattern=r'\s*([A-Z0-9]{4})\s+.+?\bGFS LAMP (\d{2})(\d{2}) UTC\s+(\d{1,2})/(\d{1,2})/(\d{4})\s*'
    lines=text.splitlines()
    headers=[re.fullmatch(pattern,line)for line in lines]
    headers=[h for h in headers if h]
    if len(headers)!=1 or headers[0][1]!=market['station']:raise ValueError('LAMP_STATION_OR_HEADER')
    h=headers[0]
    issued=(datetime(int(h[4]),int(h[2]),int(h[3]),int(h[5]),int(h[6]),tzinfo=timezone.utc) if native else
            datetime(int(h[6]),int(h[4]),int(h[5]),int(h[2]),int(h[3]),tzinfo=timezone.utc))
    issued_ms=int(issued.timestamp()*1000)
    if issued_ms>received:raise ValueError('LAMP_FUTURE_ISSUE')
    if native and receipt['source']!=nomads_source(issued_ms):raise ValueError('LAMP_SOURCE_ISSUE_MISMATCH')
    state.update(issue_ms=issued_ms,issue_clock='BULLETIN_HEADER',raw_bulletin=text,body_sha256=sha(body))
    def row(label):
        found=[re.fullmatch(r'\s*'+label+r' (.*)',line)for line in lines]
        found=[m[1].rstrip() for m in found if m]
        if len(found)>1:raise ValueError('LAMP_DUPLICATE_ROW')
        return found[0] if found else None
    utc,tmp=row('UTC'),row('TMP')
    if tmp is None:return dict(state,status='MISSING_TMP')
    if utc is None or len(utc)%3 or len(tmp)!=len(utc) or len(utc)//3 not in (25,38):
        raise ValueError('LAMP_HOURLY_COLUMNS')
    hours=[]
    first=issued.replace(minute=0)+timedelta(hours=1)
    for i in range(len(utc)//3):
        valid=first+timedelta(hours=i)
        hour=utc[i*3:i*3+3].strip();value=tmp[i*3:i*3+3].strip()
        if not re.fullmatch(r'\d{2}',hour) or int(hour)!=valid.hour:raise ValueError('LAMP_HOUR_SEQUENCE')
        if not re.fullmatch(r'-?\d{1,3}',value):raise ValueError('LAMP_TMP_VALUE')
        number=int(value)
        if number!=999 and not -150<=number<=150:raise ValueError('LAMP_TMP_RANGE')
        hours.append(dict(valid_ms=int(valid.timestamp()*1000),local_time=valid.astimezone(ZoneInfo(market['timezone'])).isoformat(),
                          temperature_f=None if number==999 else str(number)))
    return dict(state,hours=hours,status='OK')


def update(previous,receipt,market,available_ms):
    try:state=decode(receipt,market,available_ms)
    except (ValueError,KeyError,TypeError,OverflowError)as exc:
        state=dict(status='INVALID_DATA',reason=str(exc),source=receipt['source'],hours=[],issue_ms=None,
                   received_ms=receipt['received_ms'],available_ms=available_ms)
    previous=previous or {}
    state['duplicate']=False
    high=previous.get('highest_issue_ms',previous.get('issue_ms'))
    if state['status']=='OK' and high is not None:
        if state['issue_ms']<high:
            state.update(status='ISSUE_REGRESSION',hours=[])
        elif state.get('body_sha256')==previous.get('body_sha256') and previous.get('status')=='OK':
            # Re-fetching a cycle must not reset either forecast availability or age.
            state=dict(previous,duplicate=True)
    candidates=[x for x in (high,state.get('issue_ms')) if x is not None]
    state['highest_issue_ms']=max(candidates) if candidates else None
    state['poll_received_ms']=receipt['received_ms'];state['poll_available_ms']=available_ms
    return state


def c_to_f(value):
    return None if value is None else str(Decimal(value)*Decimal('1.8')+32)


def bucket_context(market,observed_f,peak_f):
    if 'half rounded up' not in market.get('rules',''):return {'status':'UNVERIFIED_RULES'}
    m=re.fullmatch(r'(-?\d+)-(-?\d+)°F',market['bucket'])
    tail=re.fullmatch(r'(-?\d+)°F or (below|higher)',market['bucket'])
    if m and int(m[2])>=int(m[1]):
        low,high=Decimal(m[1])-Decimal('.5'),Decimal(m[2])+Decimal('.5')
    elif tail:
        low=Decimal(tail[1])-Decimal('.5') if tail[2]=='higher' else None
        high=Decimal(tail[1])+Decimal('.5') if tail[2]=='below' else None
    else:return {'status':'UNSUPPORTED_BUCKET'}
    result=dict(status='VERIFIED',label=market['bucket'],lower_f=str(low) if low is not None else None,
                upper_f=str(high) if high is not None else None,
                lower_inclusive=True,upper_inclusive=False,rule='NEAREST_WHOLE_F_HALF_UP',rules_sha256=market['rules_sha256'])
    if observed_f is not None:
        obs=Decimal(observed_f)
        result.update(lower_minus_observed_f=str(low-obs) if low is not None else None,
                      upper_minus_observed_f=str(high-obs) if high is not None else None,
                      next_boundary_f=next((str(b)for b in (low,high)if b is not None and b>obs),None))
    if peak_f is not None:
        result.update(lower_minus_peak_f=str(low-Decimal(peak_f)) if low is not None else None,
                      upper_minus_peak_f=str(high-Decimal(peak_f)) if high is not None else None)
    return result


def context(state,weather,market,event_ms):
    state=state or {};status=state.get('status','NOT_RECEIVED');peak=None;remaining=[]
    available=max(state.get('received_ms',event_ms),state.get('available_ms',event_ms))<event_ms
    refs=set()
    if not available:status='NOT_YET_AVAILABLE' if state else 'NOT_RECEIVED'
    elif state.get('input_id'):refs.add(state['input_id'])
    if available and status=='OK':
        age=event_ms-state['issue_ms']
        if age<0:status='FUTURE_ISSUE'
        elif age>MAX_AGE_MS:status='STALE'
        else:
            remaining=[h for h in state['hours'] if h['valid_ms']>event_ms and h['local_time'][:10]==market['date']]
            if not remaining:status='NO_REMAINING_HOURS'
            elif any(h['temperature_f']is None for h in remaining):status='PARTIAL_TMP'
            else:peak=str(max(Decimal(h['temperature_f'])for h in remaining))
    # Poll errors/evidence can be referenced only if already received/available.
    if max(state.get('poll_received_ms',event_ms),state.get('poll_available_ms',event_ms))<event_ms and state.get('poll_input_id'):
        refs.add(state['poll_input_id'])
    seen=c_to_f(weather.get('observed_max_c'));last=weather.get('latest')
    delta=str(Decimal(peak)-Decimal(seen)) if peak is not None and seen is not None else None
    return dict(status=status,station=market['station'],source=source_for(market['station']),issue_ms=state.get('issue_ms') if available else None,
                received_ms=state.get('received_ms') if available else None,available_ms=state.get('available_ms') if available else None,
                hours=remaining,future_peak_f=peak,metar_temperature_f=c_to_f(last['temperature_c'])if last else None,
                observed_max_f=seen,peak_minus_observed_max_f=delta,lamp_below_observed_max=Decimal(delta)<0 if delta is not None else None,
                peak_times=[h['local_time']for h in remaining if h['temperature_f']==peak]if peak else [],
                max_scope='FUTURE_HOURLY_POINTS_ON_MARKET_LOCAL_DAY',official_daily_tmax=None,
                observation_freshness=weather.get('freshness','MISSING'),observation_coverage=weather.get('completeness','UNKNOWN'),
                bucket=bucket_context(market,seen,peak),input_ids=sorted(refs),trade_signal=False)


class LampPoller(WeatherPoller):
    source=SOURCE
    interval=900
    record_method='record_lamp'
    status_method='lamp_status'
    failed_states=('INVALID_DATA','LAMP_STATE_BOUND','ISSUE_REGRESSION')
    def __init__(self,fetcher=fetch_lamp,monotonic=time.monotonic):
        super().__init__(fetcher,monotonic)


def format_card(card):
    """Text preview only; no messaging or recommendation path."""
    w=card['weather'];f=card['lamp'];b=f['bucket']
    def number(value,unit='°F'):return 'н/д' if value is None else str(value)+' '+unit
    def stamp(value):return 'н/д' if value is None else datetime.fromtimestamp(value/1000,timezone.utc).isoformat()
    last=w.get('latest')
    prices=[]
    for side in ('YES','NO'):
        p=card['prices'].get(side,{})
        value='н/д' if p.get('ask')is None else str(Decimal(p['ask'])*100)+'¢'
        prices.append(side+' ask '+value+' ['+p.get('status','NOT_AVAILABLE')+', '+p.get('freshness_now','UNKNOWN')+', '+p.get('observation_state','UNCONFIRMED')+']')
    distance=None
    if b.get('next_boundary_f') is not None and f.get('observed_max_f') is not None:
        distance=str(Decimal(b['next_boundary_f'])-Decimal(f['observed_max_f']))
    return '\n'.join([
        f.get('station','н/д')+' · информационный контекст · '+stamp(card['at_ms']),
        'METAR: '+number(last['temperature_c'],'°C') if last else 'METAR: н/д',
        'Наблюдаемый максимум: '+number(f['observed_max_f'])+' (только полученные наблюдения)',
        'Будущий почасовой пик LAMP: '+number(f['future_peak_f'])+' ['+f['status']+']',
        'Пик LAMP минус наблюдаемый максимум: '+number(f['peak_minus_observed_max_f']),
        'Бакет '+b.get('label','н/д')+': ['+b.get('lower_f','?')+', '+b.get('upper_f','?')+') °F; до следующей границы '+number(distance),
        ' | '.join(prices),
        'LAMP: заголовок '+stamp(f['issue_ms'])+'; получен '+stamp(f['received_ms']),
        'Погода: '+w['freshness']+' / '+w['completeness']+'; источник '+w.get('provider_status','UNKNOWN')+'. Не вероятность и не сигнал к покупке.'
    ])
