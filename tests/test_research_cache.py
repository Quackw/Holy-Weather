from copy import deepcopy
import json
import pytest
from holy_weather import core
from holy_weather.research_cache import ResearchCache
from test_catalogue import AT


def test_cached_evolution_and_serialization_are_exact_across_expiry_and_trade():
    cfg=core.Config(window_seconds=3);cache=ResearchCache();prior=None
    for n in range(1200):
        raw=core.canonical(prior)if prior else None
        state,hint=cache.take('token',raw)
        sample=dict(kind='last_trade_price'if n%11==0 else'best_bid_ask',ask=('0.001','0.10','0.100','0.55','0.70','0.99','1')[n%7],source_ms=AT+n*100,received_ms=AT+n*100,input_id=n+1)
        expected=core.evolve(prior,sample,cfg)
        actual=core.evolve(state,sample,cfg,owned=True,decimal_parser=core.cached_price,_extrema=hint)
        assert actual==expected
        encoded=cache.encode(actual[0],hint);assert encoded==core.canonical(expected[0])
        cache.keep('token',encoded,actual[0],hint);prior=deepcopy(expected[0])


def test_public_event_mutation_cannot_override_cached_sql_state(gamma,tmp_path):
    from dataclasses import replace
    from holy_weather import catalogue,history
    from test_catalogue import setup
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0];proof=[]
    for enabled in(False,True):
        with catalogue.CatalogueStore(replace(cfg,data_dir=str(tmp_path/str(enabled))),session=False)as store:
            if not enabled:store._research_cache=None
            store.bind(m,receipt);store.connection_started(AT)
            def apply(value,n):
                raw=dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens']['YES'],timestamp=str(AT+n),best_bid=value,best_ask=value)
                return store.apply(core.normalize(raw,AT+n,m))
            apply('0.49',1);result=apply('0.71',2)
            for event in result['events']:
                if event.get('evidence'):event['evidence'][0]['ask']='0.01'
            apply('0.72',3);proof.append(history.fingerprint(store.c))
    assert proof[0]==proof[1]


def test_sqlite_change_invalidates_cache_and_nested_points_encode_exactly():
    cache=ResearchCache();state=dict(points=[dict(ask='0.49',source_ms=1,received_ms=1,input_id=1,extra={'proof':[1]})],gates={},excursion=None,last_source=1)
    _,hint=cache.take('token',None);raw=cache.encode(state,hint);cache.keep('token',raw,state,hint)
    actual,hint=cache.take('token',raw);actual['points'][0]['extra']['proof'].append(2)
    assert cache.encode(actual,hint)==core.canonical(actual)
    cache.keep('token',cache.encode(actual,hint),actual,hint)
    changed=dict(actual,last_source=9);changed_raw=core.canonical(changed)
    restored,_=cache.take('token',changed_raw);assert restored==changed


def test_cache_eviction_and_oversize_fall_back_to_exact_sql_state(monkeypatch):
    from holy_weather import research_cache as module
    monkeypatch.setattr(module,'_LIMIT',8192);cache=ResearchCache()
    for n in range(100):
        state=dict(points=[],gates={},excursion=None,last_source=n)
        _,hint=cache.take(str(n),None);raw=cache.encode(state,hint);cache.keep(str(n),raw,state,hint)
        assert cache.bytes<=8192
    assert len(cache.items)<100
    state=dict(points=[],gates={},excursion=None,last_source=1,padding='x'*10000)
    _,hint=cache.take('big',None);raw=cache.encode(state,hint);cache.keep('big',raw,state,hint)
    assert 'big'not in cache.items and cache.take('big',raw)[0]==state


def test_unordered_or_expired_points_use_exact_filter_and_earliest_origin():
    cfg=core.Config(window_seconds=3);cache=ResearchCache()
    prior=dict(points=[dict(ask='0.40',source_ms=AT+20,received_ms=AT+20,input_id=1),dict(ask='0.49',source_ms=AT,received_ms=AT,input_id=2)],gates={},excursion=None,last_source=AT+20)
    _,hint=cache.take('token',None);raw=cache.encode(prior,hint);cache.keep('token',raw,prior,hint)
    for n in(25,3005,6005):
        state,hint=cache.take('token',raw)
        sample=dict(kind='best_bid_ask',ask='0.71',source_ms=AT+n,received_ms=AT+n,input_id=n)
        expected=core.evolve(json.loads(raw),sample,cfg)
        actual=core.evolve(state,sample,cfg,owned=True,decimal_parser=core.cached_price,_extrema=hint)
        assert actual==expected
        raw=cache.encode(actual[0],hint);assert raw==core.canonical(expected[0]);cache.keep('token',raw,actual[0],hint)
