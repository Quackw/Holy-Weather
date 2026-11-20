"""Synthetic fixture process; deliberate exit tests WAL recovery and queue barriers."""
import json,os,sys
from pathlib import Path
from holy_weather.catalogue import CatalogueConfig,CatalogueStore,Router
from holy_weather.alert_queue import preview,claim

root=Path(sys.argv[1]);phase=sys.argv[2];fixture=json.loads((root/'crash_fixture.json').read_text());cfg=CatalogueConfig(data_dir=fixture['data_dir']);m=fixture['market'];at=fixture['at']
def feed(r,value,n):r.feed(json.dumps(dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens']['NO'],timestamp=str(at+n),best_bid=value,best_ask=value)),at+n)
with CatalogueStore(cfg,session=False)as store:
    store.connection_started(at);router=Router(store)
    with store.receive_batch():
        feed(router,'0.49',1);feed(router,'0.81',2)
        if phase=='before_commit':os._exit(71)
    if phase=='after_claim':
        keys=[key for key,raw in store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")if json.loads(raw)['status']=='PENDING'];assert len(keys)==1
        assert claim(store,preview(store,keys[0]),at+3)
    os._exit(71)
