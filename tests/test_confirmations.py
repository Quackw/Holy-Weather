from collections import Counter
import json
from holy_weather import app
from holy_weather.storage import Store


def delta(market,at,ask='0.699',size='25'):
    return {'event_type':'price_change','market':market['condition_id'],'timestamp':str(at),
            'price_changes':[{'asset_id':market['tokens']['YES'],'price':'0.60','size':size,'side':'BUY','best_bid':'0.60','best_ask':ask}]}


def test_unchanged_confirmations_are_bounded_and_keep_latest_clock(cfg,market):
    c=Counter();at=1794650000000
    with Store(cfg)as s:
        for i in range(31):app.process(json.dumps(delta(market,at+i*1000,size=str(i+1))),at+i*1000,market,s,c)
        assert s.stats()['samples']==3
        assert c['coalesced_unchanged_top_confirmations']==28
        assert s.stats()['latest'][0]['source_ms']==at+30000
        assert not s.stats()['events']


def test_price_changes_are_not_dropped_by_confirmation_coalescing(cfg,market,make_raw):
    c=Counter();at=1794650000000
    with Store(cfg)as s:
        app.process(json.dumps(make_raw('0.699',at-1,bid='0.60')),at-1,market,s,c)
        for i,ask in enumerate(['0.699','0.701','0.699','0.702']):app.process(json.dumps(delta(market,at+i*1000,ask)),at+i*1000,market,s,c)
        assert s.stats()['samples']==4
        assert s.stats()['events']=={'DETECTED':1,'SUPPRESSED':2}


def test_reversed_coalesced_clock_does_not_create_a_false_event(cfg,market):
    c=Counter();at=1794650000000
    with Store(cfg)as s:
        for offset,ask in [(0,'0.699'),(1000,'0.699'),(500,'0.99'),(2000,'0.98')]:
            app.process(json.dumps(delta(market,at+offset,ask)),at+2000,market,s,c)
        assert c['OUT_OF_ORDER']==1
        assert not s.stats()['events']
