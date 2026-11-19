"""Offline old → candidate → old data compatibility; never sends or controls services."""
import json
from pathlib import Path
import sys
from types import SimpleNamespace

from holy_weather import us_lamp
from holy_weather.alert_queue import claim, finish, preview
from holy_weather.catalogue import CatalogueConfig, CatalogueStore, Router
from holy_weather.history import fingerprint
from holy_weather.recovery import code_identity

phase,path=sys.argv[1:];root=Path(path);fixture=json.loads((root/'fixture.json').read_bytes())
cfg=CatalogueConfig(region='us',city='new-york',data_dir=str(root/'data'),us_lamp_enabled=True)
m=fixture['market'];at=fixture['at'];clock=at if phase=='seed' else at+3600000+(20 if phase=='rollback' else 0)


def feed(store,price,n):
    Router(store).feed(json.dumps(dict(event_type='best_bid_ask',market=m['condition_id'],
        asset_id=m['tokens']['YES'],timestamp=str(clock+n),best_bid=price,best_ask=price)),clock+n)


def receipt(store,status,now):
    key=store.c.execute("select key from meta where key like 'alert_delivery_v1:%' and json_extract(value,'$.status')='PENDING' and json_extract(value,'$.track')='PRICE_ALERT'").fetchone()[0]
    card=preview(store,key);assert claim(store,card,now)
    finish(store,card,dict(status=status,**({'message_id':10} if status=='SENT' else {})),now+1)


with CatalogueStore(cfg,session=False) as store:
    if phase=='seed':
        store.bind(m,fixture['receipt'])
    else:
        assert json.loads(json.dumps(fingerprint(store.c)))==json.loads((root/'fingerprint.json').read_bytes())
    store.connection_started(clock)
    if phase=='seed':
        feed(store,'0.49',1);feed(store,'0.56',2);receipt(store,'SENT',clock+3)
        store.record_lamp(fixture['lamp_first'],m,fixture['lamp_first']['received_ms'])
    elif phase=='candidate':
        feed(store,'0.49',1);feed(store,'0.71',2);receipt(store,'UNKNOWN',clock+3)
        store.record_lamp(fixture['lamp_second'],m,fixture['lamp_second']['received_ms'])
        collector=us_lamp.Collection();collector.state=dict(next_ms=clock+300000,attempts=1,blocked=False,status='REQUESTING')
        collector.thread=SimpleNamespace(is_alive=lambda:True)
        collector.close(store,clock+6)
    else:
        collector=us_lamp.Collection(lambda issue:(_ for _ in ()).throw(AssertionError('early retry')))
        collector.tick(store,set(store.markets),clock);collector.close()
        feed(store,'0.81',1);feed(store,'0.49',2);feed(store,'0.81',3)
        feed(store,'0.49',4);feed(store,'0.81',5)
    assert store.c.execute('pragma integrity_check').fetchone()[0]=='ok'
    assert not store.c.execute('pragma foreign_key_check').fetchall()
    assert store.c.execute('pragma user_version').fetchone()[0]==1
    queue={k:json.loads(v) for k,v in store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")}
    if phase!='seed':
        old=json.loads((root/'queue.json').read_bytes())
        assert all(queue[k]==v for k,v in old.items())
    price=store.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]
    lamp=store.c.execute("select count(*) from events where type='LAMP_ALERT'").fetchone()[0]
    assert price=={'seed':1,'candidate':2,'rollback':3}[phase]
    assert lamp==(phase!='seed')
    fired=json.loads(store.c.execute('select value from meta where key=?',('price_alert_v1:'+m['tokens']['YES'],)).fetchone()[0])['fired']
    assert len(fired)=={'seed':2,'candidate':4,'rollback':5}[phase]
    (root/'fingerprint.json').write_text(json.dumps(fingerprint(store.c),sort_keys=True))
    (root/'queue.json').write_text(json.dumps(queue,sort_keys=True))
    print(json.dumps(dict(phase=phase,code=code_identity(),price_events=price,lamp_events=lamp,integrity='ok',schema=1,fired=fired,
                         statuses=sorted(v['status'] for v in queue.values()))))
