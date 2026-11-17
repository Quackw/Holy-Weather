"""Real local-day/router/SQLite logic, synthetic Gamma and managed UTC clocks."""
from dataclasses import replace
from datetime import datetime,timezone
import json
import pytest
from holy_weather.catalogue import CatalogueConfig,CatalogueStore,DayDiscovery,Router,observe_catalogue,local_days
from holy_weather.core import Config,US_CITIES
from holy_weather.discovery import select
from test_catalogue import setup,feed,AT


def ms(value):return int(datetime.fromisoformat(value).timestamp()*1000)


def discover_us(gamma,city,at):
    now=datetime.fromtimestamp(at/1000,timezone.utc);day=Config(city=city).day(now);d=datetime.fromisoformat(day)
    slug,station,_=US_CITIES[city];month=d.strftime('%B').lower()
    doc=json.loads(json.dumps(gamma).replace('nyc',slug).replace('KLGA',station)
        .replace('november-14-2026',f'{month}-{d.day}-{d.year}').replace("14 Nov '26",f"{d.day} {month[:3].title()} '{d.year%100:02d}"))
    n=d.toordinal()*100+list(US_CITIES).index(city)
    doc[0]['markets'][0].update(conditionId='0x'+f'{n:064x}',clobTokenIds=json.dumps([str(n*2),str(n*2+1)]))
    return [select(doc,Config(city=city),now=now)],dict(received_at=now.isoformat(),raw_body=json.dumps(doc)),[]


@pytest.mark.parametrize('start',['2026-03-09T03:59:59+00:00','2026-11-02T04:59:59+00:00','2026-11-17T04:59:59+00:00'])
def test_ny_rolls_before_la_with_new_verified_baseline_and_old_history(gamma,tmp_path,start):
    cfg=CatalogueConfig(region='us',city='new-york',data_dir=str(tmp_path/'us'));at=[ms(start)]
    calls=[]
    def discover(city):
        calls.append((city,local_days(at[0],US_CITIES)[city]))
        return discover_us(gamma,city,at[0]) if city in ('new-york','los-angeles') else ([],{},[])
    with CatalogueStore(cfg) as s:
        days=DayDiscovery(s,discover,lambda:at[0]);days.refresh()
        ny=next(m for m in s.markets.values() if m['station']=='KLGA');la=next(m for m in s.markets.values() if m['station']=='KLAX')
        s.connection_started(at[0]);router=Router(s,days.selected)
        feed(router,ny,'0.49',at[0]+1);feed(router,ny,'0.71',at[0]+2)
        old_event=s.c.execute("select id from events where type='PRICE_ALERT'").fetchone()[0]
        at[0]+=1000;days.refresh();s.connection_started(at[0]);router=Router(s,days.selected)
        new=next(m for m in s.markets.values() if m['station']=='KLGA')
        assert new['date']>ny['date'] and new['tokens']!=ny['tokens']
        assert la['condition_id'] in s.markets and new['date']!=la['date']
        feed(router,new,'0.81',at[0]+1)  # New contract high price is baseline.
        feed(router,la,'0.81',at[0]+2)   # Reconnected older contract also baseline.
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==1
        assert s.c.execute('select id from events where id=?',(old_event,)).fetchone()
        assert json.loads(s.c.execute("select value from meta where key=?",('price_alert_v1:'+ny['tokens']['YES'],)).fetchone()[0])['fired']==['0.50','0.55','0.60','0.70']
        assert [day for c,day in calls if c=='los-angeles']==[la['date']]
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok' and not s.c.execute('pragma foreign_key_check').fetchall()


@pytest.mark.parametrize('values',[
    ['2026-03-08T06:59:59+00:00','2026-03-08T07:00:00+00:00'],
    ['2026-11-01T05:59:59+00:00','2026-11-01T06:00:00+00:00']])
def test_dst_hour_jump_or_repeat_does_not_roll_contract_twice(values):
    days=[local_days(ms(v),US_CITIES) for v in values]
    assert days[0]==days[1]


def test_health_distinguishes_live_depth_and_independent_price_confirmations(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0]
    with CatalogueStore(cfg) as s:
        s.bind(m,receipt);s.connection_started(AT);router=Router(s)
        feed(router,m,'0.49',AT+1);feed(router,m,'0.51',AT+2,side='NO')
        value=s.runtime_status(AT+3);assert value['cities']['london']==dict(contracts=1,fresh_quotes=2,price_ready=2)
        for at in (AT+30000,AT+60000,AT+91003):
            raw=dict(event_type='price_change',market=m['condition_id'],timestamp=str(at),
                     price_changes=[dict(asset_id=m['tokens']['YES'],best_bid='0.49',best_ask='0.49',price='0.1',size='1',side='BUY')])
            router.feed(json.dumps(raw),at)
        value=s.runtime_status(AT+91004)['cities']['london']
        assert value==dict(contracts=1,fresh_quotes=1,price_ready=0)
        feed(router,m,'0.81',AT+91005,side='NO')
        assert s.runtime_status(AT+91006)['cities']['london']==dict(contracts=1,fresh_quotes=2,price_ready=1)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==0
        s.gap('synthetic disconnect',AT+91007)
        assert s.runtime_status(AT+91008)['cities']['london']==dict(contracts=1,fresh_quotes=0,price_ready=0)
