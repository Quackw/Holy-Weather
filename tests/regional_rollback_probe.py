"""Offline subprocess probe, executed with an explicitly selected source tree."""
import json,sys,sqlite3
from pathlib import Path
from datetime import datetime,timezone
from holy_weather.catalogue import CatalogueConfig,CatalogueStore,Router
from holy_weather.core import Config
from holy_weather.discovery import select

region,phase,root=sys.argv[1:];root=Path(root)
city='london' if region=='europe' else 'new-york'
kwargs={'city':city,'data_dir':str(root/'data')}
if region=='us':kwargs['region']='us'
cfg=CatalogueConfig(**kwargs)
now=datetime(2026,11,14,12,tzinfo=timezone.utc);at=int(now.timestamp()*1000)
def saved(c):
 return {t:[[{'hex':v.hex()} if isinstance(v,bytes) else v for v in r] for r in c.execute('select * from '+t+' order by rowid')] for t in ('events','event_inputs','meta','samples','inputs')}
def feed(s,m,value,offset,side='YES'):
 Router(s).feed(json.dumps(dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens'][side],timestamp=str(at+offset),best_bid=value,best_ask=value)),at+offset)
with CatalogueStore(cfg,session=False) as s:
 if phase=='old':
  m=json.loads((root/'market.json').read_text());receipt={'received_at':now.isoformat(),'raw_body':json.dumps(m)}
  s.bind(m,receipt);s.connection_started(at);feed(s,m,'0.49',1);feed(s,m,'0.56',2)
 else:
  m=next(iter(s.markets.values()))
  previous=json.loads((root/'snapshot.json').read_text())
  assert saved(s.c)==previous,'CODE_SWITCH_CHANGED_HISTORY_OR_QUEUE'
  s.connection_started(at+(10 if phase=='new' else 20))
  base=10 if phase=='new' else 20
  feed(s,m,'0.49',base+1);feed(s,m,'0.56',base+2)
  assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==(1 if phase=='new' else 2),'DUPLICATED_FIRED_THRESHOLD'
  feed(s,m,'0.71' if phase=='new' else '0.81',base+3)
  if phase=='new':
   rows=s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' order by key").fetchall()
   # Explicit synthetic delivery states, no HTTP transport.
   with s.transaction():
    for status,row in zip(('SENT','UNKNOWN'),rows):
     d=json.loads(row['value']);d['status']=status
     s.c.execute('update meta set value=? where key=?',(json.dumps(d),row['key']))
 assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
 assert not s.c.execute('pragma foreign_key_check').fetchall()
 (root/'snapshot.json').write_text(json.dumps(saved(s.c)))
 print(json.dumps({'region':region,'phase':phase,'price_events':s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0],'queue':[json.loads(r[0])['status'] for r in s.c.execute("select value from meta where key like 'alert_delivery_v1:%'")],'integrity':'ok'}))
