"""Offline code switch on current records; selected source comes from PYTHONPATH."""
import json
from pathlib import Path
import sys

from holy_weather import alert_queue, catalogue
from holy_weather.core import canonical
from test_catalogue import AT, feed
from test_us_lamp import receipt

root=Path(sys.argv[1]);phase=sys.argv[2]
cfg=catalogue.CatalogueConfig(region='us',city='los-angeles',us_lamp_enabled=True,
                              data_dir=str(root/'data'))
m,r=json.loads((root/'binding.json').read_text())

def saved(s):
    return {table:[[v.hex() if isinstance(v,bytes) else v for v in row]
                   for row in s.c.execute('select * from '+table+' order by rowid')]
            for table in ('inputs','samples','state','events','event_inputs','meta','latest')}

with catalogue.CatalogueStore(cfg,session=False) as s:
    if phase=='old':s.bind(m,r)
    else:assert saved(s)==json.loads((root/'snapshot.json').read_text()),'SWITCH_CHANGED_CURRENT_DATA'
    at=AT+dict(old=0,new=10000,rollback=20000)[phase]
    s.connection_started(at);router=catalogue.Router(s)
    if phase=='old':
        for side in ['YES','NO']:
            feed(router,m,'0.49',at+1,side);feed(router,m,'0.56',at+2,side)
        s.record_lamp(receipt(m['station']),m,at+3)
        s.record_lamp(receipt(m['station'],48,at+4),m,at+4)
    else:
        if phase=='new':
            from test_us_price_flow import depth, Socket
            router.feed(depth(m,at+1),at+1)
            assert catalogue.USPriceRecovery(0).tick(s,Socket(),{m['tokens']['YES']},at+2,6)==1
        feed(router,m,'0.49',at+3);feed(router,m,'0.56',at+4)
        assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==(2 if phase=='new' else 3)
        feed(router,m,'0.71' if phase=='new' else '0.81',at+5)
    rows=s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%' order by key").fetchall()
    terminal=[]
    for key,raw in rows:
        d=json.loads(raw)
        token=s.c.execute('select token from events where id=?',(d['event_id'],)).fetchone()[0]
        p=alert_queue.preview(s,key)
        if d['track']=='PRICE_ALERT' and phase=='old' and token==m['tokens']['YES']:
            assert alert_queue.claim(s,p,at+6)
            alert_queue.finish(s,p,dict(status='SENT',message_id=123),at+7)
        elif d['track']=='PRICE_ALERT' and phase=='new' and token==m['tokens']['NO']:
            assert alert_queue.claim(s,p,at+6)  # Deliberately unknown transport outcome.
        elif d['status'] in ('SENT','UNKNOWN'):
            assert not alert_queue.claim(s,p,at+6)
            assert s.c.execute('select value from meta where key=?',(key,)).fetchone()[0]==raw
            terminal.append(d['status'])
    assert s.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0]==dict(old=2,new=3,rollback=4)[phase]
    assert s.c.execute("select count(*) from events where type='LAMP_ALERT'").fetchone()[0]==1
    fired=json.loads(s.c.execute('select value from meta where key=?',('price_alert_v1:'+m['tokens']['YES'],)).fetchone()[0])['fired']
    assert len(fired)==dict(old=2,new=4,rollback=5)[phase]
    assert s.c.execute('pragma user_version').fetchone()[0]==1
    assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
    assert not s.c.execute('pragma foreign_key_check').fetchall()
    (root/'snapshot.json').write_text(canonical(saved(s)))
    print(canonical(dict(phase=phase,price=dict(old=2,new=3,rollback=4)[phase],fired=len(fired),
                         queue=[json.loads(v)['status'] for _,v in s.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")],
                         schema=1,integrity='ok')))
