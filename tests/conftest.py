from dataclasses import replace
from datetime import datetime, timezone
import json
import pytest
from ci_stacks import pytest_addoption, pytest_configure, pytest_runtest_protocol, pytest_unconfigure
from holy_weather.core import Config
from holy_weather.discovery import select

NOW=datetime(2026,11,14,12,tzinfo=timezone.utc)


@pytest.fixture
def gamma():
    rules="Highest temperature at LaGuardia (KLGA) on 14 Nov '26. The routine METAR uses station local time. Fahrenheit is rounded to the nearest whole degree with exact half rounded up."
    market={'id':'123','conditionId':'0x'+'a'*64,'groupItemTitle':'46-47°F','slug':'highest-temperature-in-nyc-on-november-14-2026-46-47f',
            'active':True,'closed':False,'acceptingOrders':True,'enableOrderBook':True,'description':rules,
            'resolutionSource':'https://aviationweather.gov/api/data/metar?ids=KLGA&format=raw',
            'outcomes':'["Yes","No"]','clobTokenIds':'["12345","67890"]'}
    return [{'id':'10','slug':'highest-temperature-in-nyc-on-november-14-2026','active':True,'closed':False,'markets':[market]}]


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=str(tmp_path/'owned'))


@pytest.fixture
def market(gamma,cfg):
    return select(gamma,cfg,now=NOW)


@pytest.fixture
def make_raw(market):
    def make(ask='0.49',at=1794650000000,token=None,bid='0.48',kind='best_bid_ask'):
        r={'event_type':kind,'market':market['condition_id'],'asset_id':token or market['tokens']['YES'],'timestamp':str(at)}
        if kind=='book':
            r.update(bids=[] if bid is None else [{'price':bid,'size':'25'}],asks=[] if ask is None else [{'price':ask,'size':'20'}])
        else:
            r.update(best_bid=bid,best_ask=ask)
        return r
    return make
