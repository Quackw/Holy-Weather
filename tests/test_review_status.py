from collections import Counter
import json
import pytest
from holy_weather.app import process
from holy_weather.core import normalize
from holy_weather.storage import Store

AT=1794650000000


def setup(store, market):
    store.bind(market, {'synthetic': True})
    store.connection_started(AT)


def quote(store, market, make_raw, token, at=AT, received=None, **kw):
    process(json.dumps(make_raw(at=at, token=token, **kw)), at if received is None else received,
            market, store, Counter())


def test_current_observation_requires_both_independent_baselines(cfg, market, make_raw):
    with Store(cfg) as store:
        setup(store, market)
        quote(store, market, make_raw, market['tokens']['YES'])
        status=store.stats(now_ms=AT)['observation']
        assert status['sides']=={'YES':'READY','NO':'AWAITING_BASELINE'}
        quote(store, market, make_raw, market['tokens']['NO'])
        assert store.stats(now_ms=AT)['observation']['state']=='QUOTES_FRESH'
        store.gap('DISCONNECT', AT+1)
        stats=store.stats(now_ms=AT+2)
        assert all(row['freshness_now']=='FRESH' for row in stats['latest'])
        assert stats['observation']['state']=='CONNECTION_UNCONFIRMED'
        store.connection_started(AT+3)
        quote(store, market, make_raw, market['tokens']['YES'], at=AT+4)
        assert store.stats(now_ms=AT+4)['observation']['sides']['NO']=='AWAITING_BASELINE'
        # A trade is not a quote baseline.
        trade={'event_type':'last_trade_price','market':market['condition_id'],
               'asset_id':market['tokens']['NO'],'timestamp':str(AT+5),'price':'0.5','size':'1','side':'BUY'}
        process(json.dumps(trade),AT+5,market,store,Counter())
        assert store.stats(now_ms=AT+5)['observation']['sides']['NO']=='AWAITING_BASELINE'
    with Store(cfg,session=False) as store:
        assert store.stats(now_ms=AT+6)['observation']['state']=='CONNECTION_UNCONFIRMED'


@pytest.mark.parametrize('case', ['missing','future','stale','out_of_order','receipt_future','expired'])
def test_saved_timestamp_alone_is_insufficient(cfg, market, make_raw, case):
    with Store(cfg) as store:
        setup(store, market)
        for token in market['tokens'].values():quote(store,market,make_raw,token)
        token=market['tokens']['YES'];now=AT+10
        if case=='missing':quote(store,market,make_raw,token,at=AT+1,ask=None)
        elif case=='future':quote(store,market,make_raw,token,at=AT+2,received=AT+1)
        elif case=='stale':quote(store,market,make_raw,token,at=AT+1,received=AT+90002);now=AT+90002
        elif case=='out_of_order':
            store.apply(normalize(make_raw(at=AT-1,token=token),AT+1,market))
        elif case=='receipt_future':quote(store,market,make_raw,token,at=AT+1,received=AT+100,kind='book')
        else:now=AT+90001
        stats=store.stats(now_ms=now)
        assert stats['observation']['state']!='QUOTES_FRESH'
        assert not next(r for r in stats['latest'] if r['token']==token)['quote_usable_now']


def test_resolution_overrides_recent_quotes(cfg, market, make_raw):
    with Store(cfg) as store:
        setup(store,market)
        for token in market['tokens'].values():quote(store,market,make_raw,token)
        process(json.dumps({'event_type':'market_resolved','market':market['condition_id']}),AT+1,market,store,Counter())
        assert store.stats(now_ms=AT+2)['observation']['state']=='MARKET_RESOLVED'


def test_delta_without_full_baseline_is_not_ready(cfg,market):
    with Store(cfg) as store:
        setup(store,market)
        for token in market['tokens'].values():
            process(json.dumps({'event_type':'price_change','market':market['condition_id'],'timestamp':str(AT),
                'price_changes':[{'asset_id':token,'best_bid':'0.4','best_ask':'0.5'}]}),AT,market,store,Counter())
        assert set(store.stats(now_ms=AT)['observation']['sides'].values())=={'AWAITING_BASELINE'}
