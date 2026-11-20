"""US-only offline equality and bounded memory for the existing research cache."""
from dataclasses import replace
import pytest
from holy_weather import catalogue,history
from test_catalogue import AT,feed
from test_us_price_flow import us_market

@pytest.mark.parametrize('lamp_enabled',[False,True])
def test_us_cache_preserves_evidence_across_updates_and_gap(gamma,tmp_path,lamp_enabled):
    cities=list(catalogue.US_CITIES)[:10]
    pairs=[us_market(gamma,tmp_path,city,number=n+1) for n,city in enumerate(cities)]
    cfg=pairs[0][0];proof=[];sizes=[]
    for cached in (False,True):
        with catalogue.CatalogueStore(replace(cfg,us_lamp_enabled=lamp_enabled,data_dir=str(tmp_path/str(cached))),session=False) as s:
            if not cached:s._research_cache=None
            for _,market,receipt in pairs:s.bind(market,receipt)
            s.connection_started(AT);r=catalogue.Router(s)
            for n in range(500):
                m=pairs[n%10][1];side='YES' if n%2 else 'NO';feed(r,m,('0.49','0.56','0.71','0.81')[(n//10)%4],AT+n+1,side)
                if n==249:s.gap('CONTROL',AT+n+2);s.connection_started(AT+n+2)
            history.check(s.c);proof.append(history.fingerprint(s.c));sizes.append((s._research_cache.bytes,len(s._research_cache.items)) if s._research_cache else None)
    assert proof[0]==proof[1] and sizes[1][0]<=16*1024*1024 and sizes[1][1]<=64 and sizes[1][0]>0
