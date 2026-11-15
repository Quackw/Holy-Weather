"""Finite grouped transport recovery, existing detector and durable queue."""
from collections import deque
from dataclasses import replace
import json
import pytest
from holy_weather.catalogue import observe_catalogue,CatalogueStore
from test_catalogue import setup,AT

def message(m,token,price,at,kind='best_bid_ask'):
    return json.dumps(dict(event_type=kind,market=m['condition_id'],asset_id=token,timestamp=str(at),best_bid=price,best_ask=price))

@pytest.mark.parametrize('interrupt',[False,True])
def test_242_tokens_every_group_receives_baseline_and_crossing(gamma,tmp_path,interrupt):
    cfg,cities=setup(gamma,tmp_path);markets=[];by_city={};tokens={}
    from holy_weather.core import EUROPE_CITIES
    for i,(city,(base,receipt)) in enumerate(zip(EUROPE_CITIES,cities)):
        by_city[city]=([],receipt,[])
        for b in range(11):
            n=i*11+b+1;m=dict(base,condition_id='0x'+f'{n:064x}',tokens={'YES':str(1000+2*n),'NO':str(1001+2*n)},bucket=f'{b}°C')
            markets.append(m);by_city[city][0].append(m)
            for t in m['tokens'].values():tokens[t]=m
    ticks=[0.0];subscriptions=[];pending=deque();connections=[];broken=[False]
    class WS:
        def __enter__(self):
            connections.append(1);pending.clear();return self
        def __exit__(self,*a):pass
        def send(self,raw):
            if raw=='PING':pending.append('PONG');return
            v=json.loads(raw);assert len(v['assets_ids'])<=16
            subscriptions.extend(v['assets_ids'])
            for t in v['assets_ids']:pending.extend([(t,'0.49'),(t,'0.51')])
        def recv(self,timeout):
            ticks[0]+=.02 if pending else .2
            if interrupt and not broken[0] and len(subscriptions)>=32:
                broken[0]=True;raise OSError('gap during grouped subscription')
            if pending:
                x=pending.popleft()
                if x=='PONG':return x
                t,p=x;return message(tokens[t],t,p,AT+int(ticks[0]*1000))
            raise TimeoutError()
    result=observe_catalogue(replace(cfg,duration_seconds=25),discover=lambda city:by_city[city],connect=lambda *a,**k:WS(),clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0],wait=lambda delay:ticks.__setitem__(0,ticks[0]+delay))
    assert len(set(subscriptions))==242
    assert len(subscriptions)==(274 if interrupt else 242)
    assert len(connections)==(2 if interrupt else 1)
    assert result['price_events']==242 and result['deliveries']=={'PENDING':242}
    assert result['sqlite_integrity']=='ok' and result['foreign_key_errors']==0
    with CatalogueStore(cfg,session=False) as s:
        assert s.c.execute('select count(distinct token) from samples').fetchone()[0]==242
        assert s.c.execute("select count(distinct token) from events where type='PRICE_ALERT'").fetchone()[0]==242
        assert s.usage()<8*1024*1024


def test_reconnect_resubscribes_baseline_and_preserves_fired_thresholds(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0];t=m['tokens']['YES']
    ticks=[0.0];calls=[];sent=[]
    class WS:
        def __init__(self,index):self.index=index;self.seq=deque(['0.49','0.51'] if index==0 else ['0.81','0.49','0.61','0.61'])
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,raw):
            if raw!='PING':sent.append(json.loads(raw))
        def recv(self,timeout):
            ticks[0]+=.1
            if self.seq:return message(m,t,self.seq.popleft(),AT+int(ticks[0]*1000))
            if self.index==0:raise OSError('synthetic disconnect')
            return 'PONG'
    def connect(*a,**k):calls.append(1);return WS(len(calls)-1)
    def wait(delay):ticks[0]+=delay;return False
    discover=lambda city:([m],receipt,[]) if city=='london' else ([],receipt,[])
    r=observe_catalogue(replace(cfg,duration_seconds=5,max_reconnects=1),discover=discover,connect=connect,clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0],wait=wait)
    assert len(calls)==2 and r['reconnects']==1
    assert len(sent)==2 and sent[0]==sent[1]
    assert r['price_events']==2
    with CatalogueStore(cfg,session=False) as s:
        es=[json.loads(x[0]) for x in s.c.execute("select payload from events where type='PRICE_ALERT'")]
        assert sorted(x['thresholds'] for x in es)==[['0.50'],['0.55','0.60']]
        assert s.c.execute("select count(*) from meta where key like 'alert_delivery_v1:%'").fetchone()[0]==2
        assert s.c.execute("select count(*) from diagnostics where reason='CATALOGUE_NETWORK_GAP'").fetchone()[0]==1

@pytest.mark.parametrize('budget',[0,2])
def test_reconnect_budget_does_not_extend_deadline(gamma,tmp_path,budget):
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0];ticks=[0.0];calls=[]
    def connect(*a,**k):calls.append(1);raise OSError('offline')
    def wait(delay):ticks[0]+=delay;return False
    r=observe_catalogue(replace(cfg,duration_seconds=10,max_reconnects=budget),discover=lambda city:([m],receipt,[]) if city=='london' else ([],receipt,[]),connect=connect,clock=lambda:AT,monotonic=lambda:ticks[0],wait=wait)
    assert len(calls)==budget+1 and ticks[0]<=10
    assert r['state']==('NETWORK_GAP_NO_RETRY' if not budget else 'RECONNECT_BUDGET_EXHAUSTED')
    assert r['price_events']==0


def test_protocol_refusal_does_not_retry(gamma,tmp_path):
    from websockets.exceptions import InvalidMessage
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0];calls=[]
    def connect(*a,**k):calls.append(1);raise InvalidMessage('refused')
    r=observe_catalogue(cfg,discover=lambda city:([m],receipt,[]) if city=='london' else ([],receipt,[]),connect=connect,clock=lambda:AT)
    assert len(calls)==1 and r['state']=='NETWORK_REFUSED_NO_RETRY'

def test_resolved_market_unsubscribes_without_interrupting_other(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,2);a,b=[x[0] for x in markets];ticks=[0.0];sent=[]
    seq=deque([json.dumps({'event_type':'market_resolved','market':a['condition_id']}),message(b,b['tokens']['YES'],'0.49',AT+200),message(b,b['tokens']['YES'],'0.51',AT+300)])
    class WS:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,raw):
            if raw!='PING':sent.append(json.loads(raw))
        def recv(self,timeout):ticks[0]+=.1;return seq.popleft() if seq else 'PONG'
    def discover(city):
        if city=='london':return [a],markets[0][1],[]
        if city=='paris':return [b],markets[1][1],[]
        return [],{},[]
    r=observe_catalogue(replace(cfg,duration_seconds=2),discover=discover,connect=lambda *a,**k:WS(),clock=lambda:AT+int(ticks[0]*1000),monotonic=lambda:ticks[0])
    unsub=[x for x in sent if x.get('operation')=='unsubscribe']
    assert len(unsub)==1 and set(unsub[0]['assets_ids'])==set(a['tokens'].values())
    assert r['price_events']==1 and r['state']=='COMPLETED_WINDOW'


def test_deadline_during_backoff_no_new_connection(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0];ticks=[0.0];calls=[]
    def connect(*a,**k):calls.append(1);raise OSError('offline')
    r=observe_catalogue(replace(cfg,duration_seconds=1,max_reconnects=3),discover=lambda city:([m],receipt,[]) if city=='london' else ([],{},[]),connect=connect,clock=lambda:AT,monotonic=lambda:ticks[0],wait=lambda delay:ticks.__setitem__(0,ticks[0]+delay))
    assert len(calls)==1 and r['state']=='DEADLINE_DURING_GAP'

def test_midnight_during_connect_never_subscribes_old_day(gamma,tmp_path):
    from datetime import datetime,timedelta
    from zoneinfo import ZoneInfo
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0]
    midnight=int((datetime.fromisoformat(m['date'])+timedelta(days=1)).replace(tzinfo=ZoneInfo(m['timezone'])).timestamp()*1000)
    at=[midnight-1];sent=[]
    class WS:
        def __enter__(self):at[0]=midnight;return self
        def __exit__(self,*a):pass
        def send(self,raw):sent.append(raw)
    r=observe_catalogue(cfg,discover=lambda city:([m],receipt,[]) if city=='london' else ([],{},[]),connect=lambda *a,**k:WS(),clock=lambda:at[0])
    assert not sent and r['state']=='ALL_MARKETS_CLOSED_OR_LOCAL_DATE_ENDED'
