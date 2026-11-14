from copy import deepcopy
from holy_weather.research import Tracks,render
from holy_weather.lamp import context
from test_lamp import parse,bulletin,observed,AT

class Saved:
    def __init__(self,market):
        self.market=market;self.state={};self.ready=True;self.ask='.70'
    def lamp_state(self):return self.state
    def forecast_card(self,at):
        return dict(at_ms=at,weather=observed(),lamp=context(self.state,observed(),self.market,at),
                    prices={'YES':dict(ask=self.ask,quote_usable_now=self.ready,observation_state='READY' if self.ready else 'AWAITING_BASELINE')})

def test_independent_dedupe_and_matched_hours(market):
    s=Saved(market);t=Tracks();s.state=parse(bulletin(),market)
    t.observe(s,market,AT+2);t.observe(s,market,AT+3)
    assert [r['track'] for r in t.records]==['LOCK','LAMP']
    s.state=parse(bulletin([48]*25),market,AT+100)
    t.observe(s,market,AT+102)
    r=t.records[-1]
    assert r['status']=='PEAK_CHANGED' and r['crossed_boundaries_f']==['47.5']
    assert '☀️ LAMP ALERT' in render(r)
    assert r['market_reaction']=='UNKNOWN_NOT_MEASURED'

def test_no_unconfirmed_quote_or_future_lamp(market):
    s=Saved(market);s.ready=False;s.state=parse(bulletin(),market);t=Tracks()
    t.observe(s,market,AT)
    assert t.records[0]['status']=='WITHHELD'
    t.observe(s,market,AT+2)
    assert [r['track']for r in t.records]==['LOCK','LAMP']

def test_expired_peak_not_forecast_revision(market):
    s=Saved(market);t=Tracks();s.state=parse(bulletin([99,99]+[46]*23),market)
    t.observe(s,market,AT+2)
    # Same future values after first hour expires; different bulletin hash only.
    s.state=deepcopy(s.state);s.state['body_sha256']='changed'
    t.observe(s,market,AT+3600001)
    assert t.records[-1]['status']=='NO_PEAK_CHANGE'

def test_invalid_and_recovery_baseline(market):
    s=Saved(market);t=Tracks();s.state=parse(bulletin(),market)
    t.observe(s,market,AT+2)
    s.state=parse(bulletin().replace(' TMP ',' XXX '),market,AT+100)
    t.observe(s,market,AT+102)
    assert t.records[-1]['status']=='UNAVAILABLE'
    s.state=parse(bulletin([48]*25),market,AT+200)
    t.observe(s,market,AT+202)
    assert t.records[-1]['status']=='BASELINE'

def test_price_threshold_and_immutable_record(market):
    s=Saved(market);t=Tracks();s.ask='.71';t.observe(s,market,AT+1)
    assert not t.records
    s.ask='.70';t.observe(s,market,AT+2)
    s.ask='.5';t.observe(s,market,AT+3)
    assert len(t.records)==1 and t.records[0]['ask']=='.70'
    assert t.records[0]['official_result']=='NOT_VERIFIED'
