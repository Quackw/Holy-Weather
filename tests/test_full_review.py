from dataclasses import replace
from datetime import datetime,timezone
import json
import pytest
from holy_weather import weather,lamp
from holy_weather.core import normalize,sha
from holy_weather.storage import Store
from holy_weather.replay import replay
from test_weather import AT,report,receipt


@pytest.mark.parametrize('row',[None,[],42])
def test_malformed_awc_row_is_explicit_not_exception(market,row):
    state,flags=weather.update({},receipt([row]),market,AT+1)
    assert state['status']=='INVALID_DATA' and not state['records']


def test_awc_decimal_temperature_must_match_raw_t_group(market):
    row=report(6.4);row['rawOb']+=' RMK T00611017'
    state,_=weather.update({},receipt([row]),market,AT+1)
    assert state['status']=='INVALID_DATA'


@pytest.mark.parametrize('temp,raw',[(-1.7,'T10171022'),(6.1,'T00611017')])
def test_awc_matching_precise_temperature_kept(market,temp,raw):
    row=report(temp);row['rawOb']+=' RMK '+raw
    parsed=weather.decode(receipt([row]),market,AT+1)
    assert parsed[0]['temperature_c']==str(temp)


def test_card_distinguishes_disconnection_from_quote_age(cfg,market,make_raw):
    with Store(replace(cfg,weather_context=True,lamp_context=True))as s:
        s.bind(market,{'synthetic':True});s.connection_started(AT)
        for side in ('YES','NO'):
            s.apply(normalize(make_raw('0.49',AT+1,token=market['tokens'][side]),AT+1,market))
        assert s.price_context(AT+2)['YES']['observation_state']=='READY'
        s.gap('TEST_DISCONNECT',AT+3)
        card=s.forecast_card(AT+4)
        assert card['prices']['YES']['freshness_now']=='FRESH'
        assert card['prices']['YES']['observation_state']=='CONNECTION_UNCONFIRMED'
        assert 'CONNECTION_UNCONFIRMED' in lamp.format_card(card)


@pytest.mark.parametrize('legacy',[False,True])
def test_event_opposite_quote_evidence_survives_retention(cfg,market,make_raw,legacy):
    with Store(replace(cfg,weather_context=True,lamp_context=True))as s:
        s.bind(market,{'synthetic':True})
        s.apply(normalize(make_raw('0.4',AT+1,token=market['tokens']['NO'],bid='0.3'),AT+1,market))
        for p,t in [('0.49',AT+10),('0.60',AT+20)]:result=s.apply(normalize(make_raw(p,t),t,market))
        opposite=result['events'][0]['prices']['NO']['input_id']
        s.apply(normalize(make_raw('0.41',AT+30,token=market['tokens']['NO'],bid='0.3'),AT+30,market))
        if legacy:s.c.execute('delete from event_inputs where input_id=?',(opposite,))
        s.prune(AT+2*86400000,apply=True)
        assert s.c.execute('select 1 from inputs where id=?',(opposite,)).fetchone()


def test_unknown_replay_kind_refused_before_db(cfg,tmp_path,gamma):
    root=tmp_path/'corpus';root.mkdir();g=root/'gamma.json';s=root/'stream.jsonl'
    iso=lambda t:datetime.fromtimestamp(t/1000,timezone.utc).isoformat()
    g.write_text(json.dumps(gamma));s.write_text(json.dumps({'kind':'unknown','received_at':iso(AT+1)})+'\n')
    m=root/'manifest.json';m.write_text(json.dumps({'market_date':'2026-11-14','gamma':{'path':'gamma.json','sha256':sha(g.read_bytes()),'received_at':iso(AT)},'stream':{'path':'stream.jsonl','sha256':sha(s.read_bytes())}}))
    with pytest.raises(ValueError,match='REPLAY_ENVELOPE_KIND'):replay(cfg,m)
    from pathlib import Path
    assert not Path(cfg.data_dir).exists()
