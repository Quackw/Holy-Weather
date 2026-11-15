"""One explicit Gamma contract. No catalogue crawl or CLOB REST fallback."""
from datetime import datetime, timezone
import json
import re
from urllib.parse import urlencode, urlparse, parse_qs
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
from .core import CITIES, EUROPE_CITIES, sha

GAMMA = 'https://gamma-api.polymarket.com/events'


def select(document, cfg, *, now):
    slug_city, station, zone = CITIES[cfg.city]
    day = cfg.day(now)
    local_day = now.astimezone(ZoneInfo(zone)).date().isoformat()
    if day != local_day:
        raise ValueError('NOT_CURRENT_STATION_LOCAL_DATE')
    date = datetime.strptime(day, '%Y-%m-%d')
    # Locale-independent month names, including on non-English Linux hosts.
    months = ('january','february','march','april','may','june','july','august','september','october','november','december')
    event_slug = f'highest-temperature-in-{slug_city}-on-{months[date.month-1]}-{date.day}-{date.year}'
    events = [e for e in document if e.get('slug') == event_slug]
    if len(events) != 1:
        raise ValueError('EXACT_EVENT_MISSING_OR_AMBIGUOUS')
    event = events[0]
    if event.get('active') is not True or event.get('closed') is not False:
        raise ValueError('EVENT_NOT_OPEN')
    markets = [m for m in event.get('markets', []) if m.get('groupItemTitle') == cfg.bucket]
    if len(markets) != 1:
        raise ValueError('EXACT_BUCKET_MISSING_OR_AMBIGUOUS')
    m = markets[0]
    if any(m.get(k) is not True for k in ('active', 'acceptingOrders', 'enableOrderBook')) or m.get('closed') is not False:
        raise ValueError('CONTRACT_NOT_OPEN')
    description = m.get('description', '')
    source = m.get('resolutionSource', '')
    parsed = urlparse(source)
    if parsed.scheme != 'https' or parsed.netloc != 'aviationweather.gov' or parsed.path != '/api/data/metar' or parse_qs(parsed.query).get('ids') != [station]:
        raise ValueError('UNSUPPORTED_RESOLUTION_SOURCE_OR_STATION')
    unit = 'Celsius' if cfg.city in EUROPE_CITIES else 'Fahrenheit'
    required = (station, 'routine', 'METAR', 'local time', unit, 'rounded to the nearest whole degree')
    if cfg.city in EUROPE_CITIES:
        if 'whole degrees Celsius' not in description or 'whole Fahrenheit' in description or not re.fullmatch(r'-?\d+°C(?: or (?:below|higher))?',cfg.bucket):
            raise ValueError('UNREVIEWED_CELSIUS_BUCKET_OR_RULES')
    if not all(x in description for x in required) or 'half rounded up' not in description:
        raise ValueError('UNREVIEWED_RULES')
    stamp = f"{date.day} {months[date.month-1][:3].title()} '{date.year%100:02d}"
    if stamp not in description:
        raise ValueError('RULE_DATE_MISMATCH')
    condition = m.get('conditionId', '')
    if not re.fullmatch(r'0x[a-fA-F0-9]{64}', condition):
        raise ValueError('INVALID_CONDITION')
    def array(value):
        return json.loads(value) if isinstance(value, str) else value
    outcomes, tokens = array(m.get('outcomes')), array(m.get('clobTokenIds'))
    if not isinstance(outcomes, list) or not isinstance(tokens, list) or len(tokens) != 2 or len(outcomes) != 2:
        raise ValueError('BINARY_OUTCOMES_REQUIRED')
    labels = [str(x).upper() for x in outcomes]
    if set(labels) != {'YES','NO'} or len(set(tokens)) != 2 or not all(isinstance(t, str) and t.isdigit() and 0 < int(t) < 2**256 for t in tokens):
        raise ValueError('TOKEN_OUTCOME_IDENTITY')
    return {'market_id': str(m['id']), 'event_id': str(event['id']), 'condition_id': condition,
            'tokens': dict(zip(labels,tokens)), 'date': day, 'station': station, 'timezone': zone,
            'bucket': cfg.bucket, 'rules': description, 'rules_sha256': sha(description.encode()),
            'resolution_source': source, 'metadata_state':'OPEN_AT_DISCOVERY_RECEIPT',
            'url': 'https://polymarket.com/event/'+event_slug+'/'+m['slug']}


def discover(cfg, *, clock=lambda:datetime.now(timezone.utc), fetch=urlopen):
    at = clock(); city = CITIES[cfg.city][0]; date = datetime.strptime(cfg.day(at), '%Y-%m-%d')
    months = ('january','february','march','april','may','june','july','august','september','october','november','december')
    slug = f'highest-temperature-in-{city}-on-{months[date.month-1]}-{date.day}-{date.year}'
    url = GAMMA+'?'+urlencode({'slug':slug})
    with fetch(Request(url,headers={'User-Agent':'Holy-Weather/0.1 public-observation'}), timeout=12) as response:
        body = response.read(4_000_001); status = response.status
    if status != 200 or len(body) > 4_000_000:
        raise ValueError('GAMMA_HTTP_OR_BODY_BOUND')
    received = clock(); document = json.loads(body)
    if not isinstance(document,list):
        raise ValueError('GAMMA_EVENT_LIST_REQUIRED')
    market = select(document,cfg,now=received)
    receipt = {'url':url,'http':status,'requested_at':at.isoformat(),'received_at':received.isoformat(),
               'sha256':sha(body),'raw_body':body.decode('utf-8')}
    return market,receipt
