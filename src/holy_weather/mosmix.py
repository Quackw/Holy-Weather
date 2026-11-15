"""Bounded original DWD point forecast decoder; not wired to live alerts yet."""
from datetime import datetime,timezone
from decimal import Decimal
import io
import zipfile
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo
from .core import sha

STATIONS={'EGLC':('P0478','Europe/London'),'LFPB':('07150','Europe/Paris')}
MAX_COMPRESSED=1024*1024
MAX_XML=4*1024*1024


def decode(body,*,station,requested_ms,received_ms,at_ms):
    if station not in STATIONS:raise ValueError('MOSMIX_STATION')
    if any(type(t)is not int for t in (requested_ms,received_ms,at_ms)) or not 0<=requested_ms<=received_ms<=at_ms:
        raise ValueError('MOSMIX_CLOCK')
    if len(body)>MAX_COMPRESSED:raise ValueError('MOSMIX_BODY_BOUND')
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        entries=z.infolist()
        if len(entries)!=1 or not entries[0].filename.endswith('.kml') or entries[0].file_size>MAX_XML:
            raise ValueError('MOSMIX_ARCHIVE_BOUND')
        raw=z.read(entries[0])
    if b'<!DOCTYPE' in raw or b'<!ENTITY' in raw:raise ValueError('MOSMIX_XML_ENTITY')
    root=ET.fromstring(raw)
    def elements(name):return [e for e in root.iter() if e.tag.split('}')[-1]==name]
    issues=elements('IssueTime');points=elements('Placemark');steps=elements('TimeStep')
    if len(issues)!=1 or len(points)!=1 or not 1<=len(steps)<=300:raise ValueError('MOSMIX_STRUCTURE')
    def clock(text):
        d=datetime.fromisoformat(text.replace('Z','+00:00'))
        if d.tzinfo is None or d.utcoffset().total_seconds()!=0:raise ValueError('MOSMIX_UTC_REQUIRED')
        return int(d.timestamp()*1000)
    issue=clock(issues[0].text)
    if issue>received_ms:raise ValueError('MOSMIX_FUTURE_ISSUE')
    point=points[0];names=[e.text for e in point if e.tag.split('}')[-1]=='name']
    if names!=[STATIONS[station][0]]:raise ValueError('MOSMIX_STATION_IDENTITY')
    times=[clock(e.text) for e in steps]
    if any(b-a!=3600000 for a,b in zip(times,times[1:])):raise ValueError('MOSMIX_HOURLY_SEQUENCE')
    forecasts=[e for e in point.iter() if e.tag.split('}')[-1]=='Forecast' and any(k.split('}')[-1]=='elementName' and v=='TTT' for k,v in e.attrib.items())]
    values=[]
    if len(forecasts)>1:raise ValueError('MOSMIX_DUPLICATE_TTT')
    if forecasts:
        rows=[e for e in forecasts[0] if e.tag.split('}')[-1]=='value']
        if len(rows)!=1:raise ValueError('MOSMIX_TTT_ROW')
        values=(rows[0].text or '').split()
        if len(values)!=len(times):raise ValueError('MOSMIX_TTT_LENGTH')
    hours=[]
    for ms,v in zip(times,values):
        temp=None if v=='-' else Decimal(v)-Decimal('273.15')
        if temp is not None and (not temp.is_finite() or not -100<=temp<=70):raise ValueError('MOSMIX_TEMPERATURE')
        hours.append({'valid_ms':ms,'temperature_c':str(temp) if temp is not None else None})
    day=datetime.fromtimestamp(at_ms/1000,ZoneInfo(STATIONS[station][1])).date()
    remaining=[h for h in hours if h['valid_ms']>at_ms and datetime.fromtimestamp(h['valid_ms']/1000,ZoneInfo(STATIONS[station][1])).date()==day]
    peak=max((Decimal(h['temperature_c']) for h in remaining),default=None) if remaining and all(h['temperature_c'] is not None for h in remaining) else None
    # No freshness policy is invented here: readiness needs a separate reviewed
    # source-specific policy, not the LAMP two-hour limit or HTTP receipt time.
    return {'source':'DWD_MOSMIX_L','station':station,'point_id':names[0],'issue_ms':issue,
            'requested_ms':requested_ms,'received_ms':received_ms,'available_ms':at_ms,
            'sha256':sha(body),'status':'DECODED' if values else 'MISSING_TTT',
            'hours':hours,'remaining_hours':remaining,'freshness':'NOT_QUALIFIED',
            'future_peak_c':str(peak) if peak is not None else None,
            'alert_enabled':False}
