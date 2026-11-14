"""A busy connection isn't proof of fresh quotes for the subscribed contract."""
from collections import Counter
from dataclasses import replace
import json
from holy_weather import app
from holy_weather.storage import Store

AT=1794650000000


def send(store,market,counters,message,at):
    app.process(json.dumps(message),at,market,store,counters)


def setup(store,market,make_raw,counters):
    store.bind(market,{'synthetic':True});store.connection_started(AT)
    for token in market['tokens'].values():
        send(store,market,counters,make_raw('0.49',AT,token=token,kind='book'),AT)


def test_new_market_broadcasts_do_not_refresh_quotes_or_write_rows(cfg,market,make_raw):
    counters=Counter()
    with Store(replace(cfg,price_alert_enabled=True)) as store:
        setup(store,market,make_raw,counters)
        before=store.c.total_changes
        samples=[tuple(r) for r in store.c.execute('select * from samples')]
        for i in range(1000):
            at=AT+i*100
            send(store,market,counters,{'event_type':'new_market','market':'0x'+'b'*64,
                 'timestamp':str(at),'assets_ids':['987','654']},at)
        assert counters['received_book']==2 and counters['ignored_new_market']==1000
        assert counters['ignored_depth_or_control']==1000
        assert store.c.total_changes==before
        assert samples==[tuple(r) for r in store.c.execute('select * from samples')]
        snapshot=store.observation_snapshot(now_ms=AT+100000)
        assert snapshot['observation']['state']=='PARTIAL_OR_UNUSABLE'
        assert all(r['freshness_now']=='STALE' for r in snapshot['latest'])
        assert not store.c.execute('select 1 from events').fetchone()


def test_real_quotes_resume_after_quiet_period_without_crossing_stale_baseline(cfg,market,make_raw):
    counters=Counter()
    with Store(replace(cfg,price_alert_enabled=True,alert_queue_enabled=True)) as store:
        setup(store,market,make_raw,counters)
        later=AT+91000
        for token in market['tokens'].values():
            send(store,market,counters,make_raw('0.80',later-10,token=token,bid='0.78'),later)
        assert store.observation_snapshot(now_ms=later)['observation']['state']=='QUOTES_FRESH'
        assert not store.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()
        send(store,market,counters,make_raw('0.82',later+1000,bid='0.80'),later+1010)
        row=store.c.execute("select payload from events where type='PRICE_ALERT'").fetchone()
        event=json.loads(row[0])
        assert event['thresholds']==['0.80'] and event['side']=='YES'
        assert event['current']['source_ms']==later+1000
        assert event['current']['received_ms']==later+1010
        assert counters['received_best_bid_ask']==3
        assert store.c.execute("select count(*) from meta where key like 'alert_delivery_v1:%'").fetchone()[0]==1


def test_ignored_types_use_bounded_counter_names(cfg,market):
    counters=Counter()
    with Store(cfg) as store:
        before=store.c.total_changes
        for i in range(100):
            send(store,market,counters,{'event_type':'unrecognized_'+str(i)},AT+i)
        send(store,market,counters,{'event_type':'tick_size_change'},AT+101)
        assert set(counters)=={'wire_bytes','frames','ignored_depth_or_control',
                               'ignored_unknown_type','ignored_tick_size_change'}
        assert counters['ignored_unknown_type']==100
        assert store.c.total_changes==before


def test_gamma_indicative_prices_cannot_fill_missing_websocket_quotes(cfg,gamma,make_raw):
    from holy_weather.discovery import select
    from conftest import NOW
    gamma[0]['markets'][0].update(bestBid=0.85,bestAsk=0.94,outcomePrices='["0.895","0.105"]')
    market=select(gamma,cfg,now=NOW);counters=Counter()
    with Store(replace(cfg,price_alert_enabled=True,alert_queue_enabled=True)) as store:
        store.bind(market,{'synthetic':True,'raw_document':gamma});store.connection_started(AT)
        send(store,market,counters,make_raw('0.01',AT,bid=None,kind='book'),AT)
        send(store,market,counters,make_raw(None,AT,bid='0.99',token=market['tokens']['NO'],kind='book'),AT)
        prices=store.price_context(AT+1)
        assert prices['YES']['bid'] is None and prices['YES']['ask']=='0.01'
        assert prices['NO']['bid']=='0.99' and prices['NO']['ask'] is None
        assert store.observation_snapshot(now_ms=AT+1)['observation']['state']=='PARTIAL_OR_UNUSABLE'
        assert not store.c.execute("select 1 from events where type='PRICE_ALERT'").fetchone()
        assert not store.c.execute("select 1 from meta where key like 'alert_delivery_v1:%'").fetchone()
