"""The opt-in queue keeps its timers alive when real sending is disabled."""
from collections import deque
from dataclasses import replace
import json
import pytest
from holy_weather.catalogue import CatalogueStore,observe_catalogue
from test_catalogue import setup,AT


@pytest.mark.parametrize('high',[True,False])
def test_quiet_stream_settles_bid_wait_with_telegram_off(gamma,tmp_path,monkeypatch,high):
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0];ticks=[0.0];stores=[];checked=[]
    def forbidden_sender(*a,**kw):raise AssertionError('TELEGRAM_DISABLED_MUST_NOT_CONSTRUCT_SENDER')
    monkeypatch.setattr('holy_weather.telegram.Sender',forbidden_sender)
    original=CatalogueStore.__init__
    def capture(self,*args,**kw):original(self,*args,**kw);stores.append(self)
    monkeypatch.setattr(CatalogueStore,'__init__',capture)
    def quote(bid,ask,ms):return json.dumps(dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens']['YES'],timestamp=str(AT+ms),best_bid=bid,best_ask=ask))
    seq=deque([quote('0.56','0.80',100) if high else quote('0.48','0.50',100),quote('0.56','0.85',200) if high else quote('0.48','0.54',200)])
    class WS:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def send(self,raw):pass
        def recv(self,timeout):
            if ticks[0]>=10 and not checked:
                q=json.loads(stores[0].c.execute("select value from meta where key like 'alert_delivery_v1:%'").fetchone()[0]);checked.append(q)
                assert q['status']==('PENDING' if high else 'REJECTED')
                assert q['confirmation']=='UNCONFIRMED'
                assert q['reason']=='NO_BID_CONFIRMATION_WITHIN_WINDOW'
            if seq:ticks[0]+=.1;return seq.popleft()
            ticks[0]+=1;return 'PONG'
    result=observe_catalogue(replace(cfg,duration_seconds=12,telegram_enabled=False,price_notification_filter=True,price_confirmation_seconds=8),discover=lambda city:([m],receipt,[]) if city=='london' else ([],{},[]),connect=lambda *a,**k:WS(),clock=lambda:AT+round(ticks[0]*1000),monotonic=lambda:ticks[0])
    assert checked and result['telegram_enabled'] is False
    assert result['price_events']==1 and result['sqlite_integrity']=='ok'
