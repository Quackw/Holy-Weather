from dataclasses import replace
from datetime import datetime, timezone
import json

import pytest

from holy_weather.alert_queue import card, preview
from holy_weather.catalogue import CatalogueConfig, CatalogueStore
from holy_weather.discovery import select
from test_us_lamp import receipt


@pytest.fixture
def la_event(gamma, tmp_path):
    now = datetime(2026, 11, 18, 16, tzinfo=timezone.utc)
    at = int(now.timestamp()*1000)
    cfg = CatalogueConfig(city='los-angeles', region='us', data_dir=str(tmp_path/'data'), us_lamp_enabled=True)
    document = json.loads(json.dumps(gamma).replace('nyc', 'los-angeles')
                          .replace('KLGA', 'KLAX').replace('november-14', 'november-18')
                          .replace("14 Nov", "18 Nov").replace('LaGuardia', 'Los Angeles'))
    old = document[0]['markets'].pop()
    for i, bucket in enumerate(('66-67°F', '68-69°F')):
        document[0]['markets'].append(dict(old, groupItemTitle=bucket, id=str(10+i),
            slug=document[0]['slug']+'-'+bucket[:5]+'f', conditionId='0x'+f'{i+1:064x}',
            clobTokenIds=json.dumps([str(2*i+1), str(2*i+2)])))
    markets = [select(document, replace(cfg, city='los-angeles', bucket=m['groupItemTitle']), now=now)
               for m in document[0]['markets']]
    neighbor, original = markets
    with CatalogueStore(cfg, session=False) as store:
        for market in markets:
            store.bind(market, dict(received_at=now.isoformat(), raw_body=json.dumps(document)))
        start = at-1800000
        first = receipt('KLAX', 68, at, issue=start)
        second = receipt('KLAX', 67, at+3600000, issue=start+3600000)
        store.record_lamp(first, original, at, markets=markets)
        store.record_lamp(second, original, at+3600000, markets=markets)
        key, expiry, payload = store.c.execute("""
            select m.key, json_extract(m.value,'$.expires_ms'), e.payload
            from meta m join events e on e.id=json_extract(m.value,'$.event_id')
            where m.key like 'alert_delivery_v1:%' and e.token=?""", (original['tokens']['YES'],)).fetchone()
        event = json.loads(payload)
        assert (event['reason'], event['change_f'], event['threshold_f']) == ('BOUNDARY_CROSS', '-1', '2')
        assert expiry == start + 3600000 + 7200000
        yield store, original, neighbor, event, key, at, markets, second


def test_original_link_and_frozen_historical_card(la_event):
    store, original, neighbor, event, key, at, markets, second = la_event
    expected = ('☀️ <b>LAMP · Лос-Анджелес</b>\n18 ноября · аэропорт KLAX · прогноз NOAA\n\n'
                'Прогноз пика до конца дня снизился с 68°F до 67°F.\n'
                'Новый прогноз ниже диапазона 68–69°F и попадает в 66–67°F.\n\n'
                f'<a href="{original["url"]}">Открыть рынок 68–69°F ↗</a>')
    assert preview(store, key)['html'] == expected
    # Existing queued text and its hash remain frozen, even on a duplicate bulletin.
    store.c.execute("update meta set value=json_set(value,'$.html',?) where key=?",
                    ('☀️ <b>LAMP ALERT</b>\nHistorical card', key))
    frozen = preview(store, key)
    cfg = store.cfg
    store.close()
    with CatalogueStore(cfg, session=False) as store:
        store.record_lamp(second, original, at+3600001, markets=markets)
        assert preview(store, key) == frozen


def test_optional_context_requires_matching_evidence(la_event):
    _, original, neighbor, event, _, at, _, _ = la_event
    for candidates in ([], [dict(neighbor, station='KLGA')], [dict(neighbor, date='2026-11-19')],
                       [dict(neighbor, metadata_state='UNVERIFIED')], [dict(neighbor, rules='')],
                       [neighbor, dict(neighbor, condition_id='another')]):
        assert '66–67' not in card(event, original, at, verified_markets=candidates)[0]
    latest = dict(station='KLAX', local_date=original['date'])
    with_obs = dict(event, observed_max_f='65.0', weather=dict(station='KLAX', latest=latest))
    assert 'Максимум среди полученных наблюдений: 65°F.' in card(with_obs, original, at)[0]
    for weather in [dict(station='KLGA', latest=latest),
                    dict(station='KLAX', latest=dict(latest, local_date='2026-11-17'))]:
        assert 'наблюдений' not in card(dict(with_obs, weather=weather), original, at)[0]
    with pytest.raises(ValueError, match='EXACT_BUCKET_LINK_REQUIRED'):
        card(dict(event, market_url=neighbor['url']), original, at)
