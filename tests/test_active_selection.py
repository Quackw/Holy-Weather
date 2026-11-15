from copy import deepcopy
import gzip
import importlib.util
import json
from pathlib import Path
import pytest
from conftest import NOW

spec=importlib.util.spec_from_file_location('active_selection',Path(__file__).resolve().parents[1]/'tools/select_active.py')
active=importlib.util.module_from_spec(spec);spec.loader.exec_module(active)
AT=1794650000000


def another(market):
    m=deepcopy(market);m['condition_id']='0x'+'b'*64;m['tokens']={'YES':'33333','NO':'44444'};return m


def quote(market,at=AT,side='YES',bid='0.48',ask='0.52',kind='best_bid_ask'):
    data=dict(event_type=kind,market=market['condition_id'],asset_id=market['tokens'][side],timestamp=str(at))
    if kind=='book':data.update(bids=[] if bid is None else [dict(price=bid,size='25')],asks=[] if ask is None else [dict(price=ask,size='20')])
    else:data.update(best_bid=bid,best_ask=ask)
    return data


def send(selector,message,at=AT,n=1):selector.feed(json.dumps(message),at,n)


def delta(market,at=AT):
    return dict(event_type='price_change',market=market['condition_id'],timestamp=str(at),price_changes=[
        dict(asset_id=market['tokens']['YES'],price='0.48',size='20',side='BUY',best_bid='0.48',best_ask='0.52')])


def test_selection_uses_actual_quotes_instead_of_gamma_ranking(market):
    first=deepcopy(market);second=another(market)
    first.update(bestBid=0.49,bestAsk=0.51,volume24hrClob=999999)
    second.update(bestBid=None,bestAsk=None,volume24hrClob=0)
    selector=active.NativeSelection([first,second])
    send(selector,[quote(first,bid=None,kind='book'),quote(second,kind='book')])
    assert selector.choose(AT) is None
    send(selector,quote(second,AT+1),AT+1,2)
    chosen=selector.choose(AT+1)
    assert chosen['market']['condition_id']==second['condition_id']
    assert chosen['quotes']['YES']['bid']=='0.48' and chosen['quotes']['YES']['ask']=='0.52'
    assert chosen['quotes']['YES']['source_ms']==AT+1 and chosen['selected_at_ms']==AT+1


@pytest.mark.parametrize('bid,ask',[('0','0.52'),('0.48','1'),(None,'0.52'),('0.48',None),('0.60','0.50')])
def test_missing_sentinel_or_crossed_top_never_selects(market,bid,ask):
    selector=active.NativeSelection([market]);send(selector,quote(market,bid=bid,ask=ask))
    send(selector,quote(market,AT+1,bid=bid,ask=ask),AT+1,2)
    assert selector.choose(AT+1) is None


@pytest.mark.parametrize('source',[AT-90001,AT+1])
def test_stale_or_future_full_quote_does_not_select(market,source):
    selector=active.NativeSelection([market]);send(selector,quote(market,source))
    send(selector,quote(market,source+1),AT+1,2)
    assert selector.choose(AT+1) is None


def test_price_change_cannot_seed_baseline_even_with_full_top(market):
    selector=active.NativeSelection([market])
    send(selector,quote(market,AT-133000,kind='book'))
    send(selector,delta(market,AT),AT,2)
    assert selector.choose(AT) is None
    send(selector,quote(market,AT+1),AT+1,3)
    assert selector.choose(AT+1) is None
    send(selector,delta(market,AT+2),AT+2,4)
    assert selector.choose(AT+2)['quotes']['YES']['baseline']['kind']=='best_bid_ask'


def test_confirmations_preserve_clocks_and_duplicate_does_not_refresh(market):
    selector=active.NativeSelection([market]);send(selector,quote(market));raw=quote(market,AT+1)
    send(selector,raw,AT+1,2);send(selector,raw,AT+1000,3)
    assert selector.choose(AT+1000)['quotes']['YES']['received_ms']==AT+1
    assert selector.choose(AT+90002) is None
    send(selector,delta(market,AT+90003),AT+90004,4)
    chosen=selector.choose(AT+90004)
    assert chosen['quotes']['YES']['source_ms']==AT+90003
    assert chosen['quotes']['YES']['baseline']['source_ms']==AT


def test_gap_and_reversed_source_need_new_full_baseline(market):
    selector=active.NativeSelection([market]);send(selector,quote(market))
    send(selector,quote(market,AT+1),AT+1,2);assert selector.choose(AT+1)
    send(selector,quote(market,AT-1,bid='0.6',ask='0.7'),AT+1,2)
    assert selector.choose(AT+1) is None
    send(selector,delta(market,AT+2),AT+2,3);assert selector.choose(AT+2) is None
    send(selector,quote(market,AT+3),AT+3,4);assert selector.choose(AT+3) is None
    send(selector,delta(market,AT+4),AT+4,5);assert selector.choose(AT+4)
    selector.gap();send(selector,delta(market,AT+5),AT+5,6)
    assert selector.choose(AT+5) is None


def test_closed_market_and_wrong_condition_are_not_reactivated(market):
    selector=active.NativeSelection([market]);send(selector,quote(market));send(selector,quote(market,AT+1),AT+1,2)
    bad=quote(market,AT+1);bad['market']='0x'+'c'*64
    send(selector,bad,AT+1,2);assert selector.choose(AT+1) is None
    send(selector,quote(market,AT+2),AT+2,3);send(selector,quote(market,AT+3),AT+3,4)
    send(selector,dict(event_type='market_resolved',market=market['condition_id']),AT+3,4)
    send(selector,quote(market,AT+4),AT+4,5)
    assert selector.choose(AT+4) is None


def test_side_independence_and_malformed_frame(market):
    selector=active.NativeSelection([market]);send(selector,quote(market));send(selector,quote(market,side='NO',bid=None))
    send(selector,quote(market,AT+1),AT+1,3)
    assert set(selector.choose(AT+1)['quotes'])=={'YES'}
    selector.feed('bad JSON {',AT+1,3);assert selector.choose(AT+1) is None


def test_selection_memory_and_candidate_bounds(market):
    selector=active.NativeSelection([market])
    for i in range(300):send(selector,quote(market,AT+i),AT+i,i+1)
    assert len(selector.seen[market['tokens']['YES']])==256
    with pytest.raises(ValueError,match='SELECTION_MARKET_BOUND'):active.NativeSelection([market]*17)
    with pytest.raises(ValueError,match='SELECTION_IDENTITY_AMBIGUOUS'):active.NativeSelection([market,market])


def test_metadata_verification_never_requires_gamma_prices(cfg,gamma):
    gamma[0]['markets'][0].update(bestBid=None,bestAsk=None,version='v1')
    markets=active.verified_markets(gamma,cfg,NOW);assert len(markets)==1
    gamma[0]['markets'][0]['resolutionSource']='https://aviationweather.gov/api/data/metar?ids=KJFK'
    assert not active.verified_markets(gamma,cfg,NOW)


def test_old_market_day_cannot_be_selected_despite_fresh_prices(market):
    market=dict(market,date='2026-11-13')
    selector=active.NativeSelection([market]);send(selector,quote(market))
    assert selector.choose(AT) is None


def test_trade_does_not_refresh_an_old_quote(market):
    selector=active.NativeSelection([market]);send(selector,quote(market));send(selector,quote(market,AT+1),AT+1,2)
    assert selector.choose(AT+1)
    send(selector,dict(event_type='last_trade_price',market=market['condition_id'],asset_id=market['tokens']['YES'],
                       timestamp=str(AT+90002),price='0.50',size='20',side='BUY'),AT+90002,3)
    assert selector.choose(AT+90002) is None


def test_reversed_receipt_invalidates_even_when_source_clock_advances(market):
    selector=active.NativeSelection([market]);send(selector,quote(market));send(selector,quote(market,AT+1),AT+10,2)
    assert selector.choose(AT+10)
    send(selector,quote(market,AT+2),AT+2,3)
    assert selector.choose(AT+10) is None and selector.counters['receipt_clock_reversed']==1


def test_scan_subscribes_candidates_once_and_selects_native_quote(market,tmp_path):
    second=another(market);sent=[];tick=[0.0]
    class Socket:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def send(self,value):sent.append(json.loads(value))
        def recv(self,timeout):
            tick[0]+=.1;at=AT+int(tick[0]*1000)
            return json.dumps([quote(market,at,bid=None),quote(second,at)])
    result=active.scan([market,second],tmp_path/'selection',seconds=1,settle_seconds=0,
                       connect=lambda *args,**kwargs:Socket(),clock=lambda:AT+int(tick[0]*1000),monotonic=lambda:tick[0])
    assert result['state']=='SELECTED' and result['rest_book_reads']==0 and result['events_created']==0
    assert result['selection']['market']['condition_id']==second['condition_id']
    assert len(sent)==1 and set(sent[0]['assets_ids'])==set(market['tokens'].values())|set(second['tokens'].values())
    assert len(gzip.decompress((tmp_path/'selection/wire.jsonl.gz').read_bytes()).splitlines())==2


def test_scan_denial_has_no_retry_or_candidate_fallback(market,tmp_path):
    calls=[]
    def deny(*args,**kwargs):calls.append(1);raise PermissionError('synthetic')
    result=active.scan([market],tmp_path/'deny',seconds=1,connect=deny)
    assert result['state']=='STOPPED_NO_RETRY' and len(calls)==1 and result['selection'] is None


def test_provider_error_cannot_leave_an_old_candidate_ready(market):
    selector=active.NativeSelection([market]);send(selector,quote(market));send(selector,quote(market,AT+1),AT+1,2)
    assert selector.choose(AT+1)
    with pytest.raises(ValueError,match='PROVIDER_ERROR_NO_RETRY'):send(selector,dict(type='error',error='synthetic denied'),AT+2,3)
    assert selector.choose(AT+2) is None


def test_initial_snapshot_without_new_native_confirmation_is_not_activity(market,tmp_path):
    tick=[0.0]
    class Socket:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def send(self,value):pass
        def recv(self,timeout):tick[0]+=.1;return json.dumps(quote(market))
    result=active.scan([market],tmp_path/'quiet',seconds=1,settle_seconds=0,
        connect=lambda *args,**kwargs:Socket(),clock=lambda:AT+int(tick[0]*1000),monotonic=lambda:tick[0])
    assert result['state']=='NO_CONFIRMED_ACTIVE_NATIVE_QUOTE' and result['selection'] is None
    assert result['counters']['duplicate']>=1 and not (tmp_path/'quiet/data').exists()
