"""Accepted clean-window carry must keep its confirmation and notes barriers."""
import json

import pytest

from holy_weather import catalogue, recovery
from holy_weather.core import canonical
from test_catalogue import AT, feed
from test_us_lamp import setup, receipt


@pytest.mark.parametrize('track', ['PRICE_ALERT', 'LAMP_ALERT'])
@pytest.mark.parametrize('field', ['confirmation', 'delivery_notes'])
def test_legacy_confirmation_or_notes_are_never_carried(gamma, tmp_path, track, field):
    cfg, markets = setup(gamma, tmp_path)
    market, discovery = markets[0]
    with catalogue.CatalogueStore(cfg, session=False) as store:
        store.bind(market, discovery)
        if track == 'PRICE_ALERT':
            store.connection_started(AT)
            router = catalogue.Router(store)
            feed(router, market, '0.49', AT)
            feed(router, market, '0.71', AT + 1)
        else:
            store.record_lamp(receipt(market['station']), market, AT)
            store.record_lamp(receipt(market['station'], 48, AT + 1), market, AT + 1)
        key, raw = store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'").fetchone()
        value = json.loads(raw)
        assert value['track'] == track and value['status'] == 'PENDING'
        value[field] = {'legacy': 'retained'}
        store.c.execute('update meta set value=? where key=?', (canonical(value), key))
        expiry = value['expires_ms']
        assert not recovery.prepare_resume(store, {'test': field}, AT + 2, carry_pending_price=True, carry_pending_lamp=True)
        final = json.loads(store.c.execute('select value from meta where key=?', (key,)).fetchone()[0])
        assert final['status'] == 'EXPIRED' and final['reason'] == 'NO_BACKLOG_ON_RECOVERY'
        assert final['expires_ms'] == expiry and final[field] == value[field]
        assert final.get('claimed_ms') is None
