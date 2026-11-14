"""Offline KLGA previews only. No messaging, network or live-engine changes."""
import argparse
from decimal import Decimal
import json
from pathlib import Path
from .core import Config, sha
from .lamp import context as lamp_context
from .replay import replay


class Tracks:
    """One deterministic replay run; independent immutable evidence keys per track."""
    def __init__(self):
        self.records=[]; self.keys=set(); self.previous=None

    def emit(self, track, identity, **fields):
        key=sha([track,identity])
        if key in self.keys:return
        if len(self.records)>=1000:raise ValueError('RESEARCH_REPORT_BOUND')
        self.keys.add(key)
        self.records.append(dict(track=track,key=key,**fields))

    def observe(self, store, market, at):
        if market['station']!='KLGA':raise ValueError('RESEARCH_KLGA_ONLY')
        card=store.forecast_card(at)
        common=dict(at_ms=at,market_url=market['url'],bucket=market['bucket'],
                    condition_id=market['condition_id'],context=card)
        # Research interval is explicitly frozen: 0 < ask <= 70 cents.
        # First eligible quote per side/contract in this replay, not a lock verdict.
        for side,q in card['prices'].items():
            if q.get('ask') is not None and 0<Decimal(q['ask'])<=Decimal('.70'):
                ready=q.get('observation_state')=='READY' and q.get('quote_usable_now')
                status='PRICE_WATCH' if ready else 'WITHHELD'
                reason='ASK_AT_OR_BELOW_70C_OUTCOME_UNVERIFIED' if ready else q.get('observation_state','QUOTE_UNCONFIRMED')
                self.emit('LOCK',[market['condition_id'],side,status,reason],
                          status=status,reason=reason,
                          side=side,ask=q['ask'],official_result='NOT_VERIFIED',**common)
        state=store.lamp_state()
        if not state or max(state.get('received_ms',at),state.get('available_ms',at))>=at:return
        identity=[market['condition_id'],state.get('body_sha256'),state.get('issue_ms'),state.get('status')]
        key=sha(['LAMP',identity])
        if key in self.keys:return
        current=card['lamp'];previous=self.previous
        fields=dict(status='BASELINE',reason='FIRST_AVAILABLE_BULLETIN',market_reaction='UNKNOWN_NOT_MEASURED')
        if current['status']!='OK':
            fields.update(status='UNAVAILABLE',reason=current['status'])
            self.previous=None
        elif previous:
            old=lamp_context(previous,card['weather'],market,at)
            a={h['valid_ms']:h['temperature_f'] for h in old['hours']}
            b={h['valid_ms']:h['temperature_f'] for h in current['hours']}
            if old['status']!='OK' or not a or a.keys()!=b.keys():
                fields.update(status='INCOMPARABLE',reason='STALE_OR_DIFFERENT_REMAINING_HORIZON')
            else:
                before=max(map(Decimal,a.values()));after=max(map(Decimal,b.values()))
                boundaries=current['bucket']
                crossings=[]
                if boundaries['status']=='VERIFIED':
                    for name in ('lower_f','upper_f'):
                        v=Decimal(boundaries[name])
                        if (before<v<=after) or (after<v<=before):crossings.append(str(v))
                fields.update(status='PEAK_CHANGED' if before!=after else 'NO_PEAK_CHANGE',
                              reason='MATCHED_REMAINING_HOURS',before_peak_f=str(before),after_peak_f=str(after),
                              crossed_boundaries_f=crossings,previous_bulletin_sha256=previous.get('body_sha256'))
        self.emit('LAMP',identity,bulletin_sha256=state.get('body_sha256'),**fields,**common)
        if current['status']=='OK':self.previous=json.loads(json.dumps(state))


def render(record):
    title='🔒 LOCK ALERT' if record['track']=='LOCK' else '☀️ LAMP ALERT'
    lines=[title+' · New York · '+record['status'],record['bucket']+' · '+record['market_url']]
    if record['track']=='LOCK':lines.append(record['side']+' ask: '+str(Decimal(record['ask'])*100)+'¢ · Outcome unverified')
    else:
        lines.append('Remaining peak: '+record.get('before_peak_f','N/A')+' → '+record.get('after_peak_f',record['context']['lamp'].get('future_peak_f') or 'N/A')+'°F')
        lines.append('Market reaction: UNKNOWN')
    lines.append('Reason: '+record['reason'])
    return '\n'.join(lines)


def run(cfg,manifest):
    tracks=Tracks()
    result=replay(cfg,manifest,observer=tracks.observe)
    result['research']=dict(mode='OFFLINE_PREVIEW',lock_rule='FIRST_ELIGIBLE_SIDE_PER_CONTRACT_0_LT_ASK_LE_070',
                            records=tracks.records,cards=[render(r)for r in tracks.records],
                            limitations=['NO_OFFICIAL_VERIFIER','NO_LIKELY_LOCK_RULE','NO_MARKET_LAG_MEASUREMENT','NO_DELIVERY'])
    return result


def main():
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--manifest',required=True)
    a=p.parse_args();cfg=Config.load(a.config)
    if not cfg.weather_context or not cfg.lamp_context:raise ValueError('WEATHER_AND_LAMP_REQUIRED')
    print(json.dumps(run(cfg,Path(a.manifest)),ensure_ascii=False,indent=2))


if __name__=='__main__':main()
