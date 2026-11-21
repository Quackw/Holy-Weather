"""Offline queue, new-event delivery, restart and rollback evidence."""
from dataclasses import replace
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys

import pytest

from holy_weather import alert_queue, recovery, telegram
from holy_weather.core import canonical, sha
from holy_weather.storage import Store
from test_alert_queue import config, keys, seed
from test_lamp_alert import put
from test_price_alert import AT, send

PREFIX = 'transport_unknown_quarantine_v1:'


def receipt(store, key):
    return store.c.execute('select value from meta where key=?', (key,)).fetchone()[0]


def make_unknown(store, market, make_raw, track, reason):
    seed(store, market, make_raw)
    key = next(k for k in keys(store) if json.loads(receipt(store, k))['track'] == track)
    preview = alert_queue.preview(store, key)
    assert alert_queue.claim(store, preview, AT + 20001)
    alert_queue.finish(store, preview, {'status': 'UNKNOWN', 'reason': reason}, AT + 20002)
    return key, preview, receipt(store, key)


@pytest.mark.parametrize('track', ['PRICE_ALERT', 'LAMP_ALERT'])
@pytest.mark.parametrize('reason', ['URLError', 'HTTP_502'])
def test_known_transport_unknown_allows_only_new_work(cfg, market, make_raw, track, reason):
    with Store(config(cfg)) as store:
        key, old_preview, raw = make_unknown(store, market, make_raw, track, reason)
        old_events = dict(store.c.execute('select id,payload from events'))
        assert not recovery.prepare_resume(store, {'test': 'transport'}, AT + 20003,
                                           carry_pending_price=True, carry_pending_lamp=True)
        assert receipt(store, key) == raw
        assert not alert_queue.claim(store, old_preview, AT + 20004)
        assert all(json.loads(receipt(store, k))['status'] != 'PENDING' for k in keys(store))
        marker_key = PREFIX + json.loads(raw)['event_id']
        marker_raw = receipt(store, marker_key)
        marker = json.loads(marker_raw)
        assert marker['decision'] == 'KEEP_UNKNOWN_NO_RETRY'
        assert marker['receipt_sha256'] == sha(raw.encode()) and marker['key'] == key
        assert dict(store.c.execute('select id,payload from events')) == old_events
        send(store, market, make_raw('0.82', AT + 21000, bid='0.80'), AT + 21000)
        fresh = next(k for k in keys(store) if json.loads(receipt(store, k))['status'] == 'PENDING')
        calls = []
        sender = telegram.Sender('synthetic', lambda token, p: (calls.append(p['key']) or {'status': 'SENT', 'message_id': 731}))
        sender.tick(store, AT + 21001)
        assert sender.thread is not None
        sender.thread.join(2)
        assert not sender.thread.is_alive()
        sender.tick(store, AT + 21002)
        sender.close(store)
        assert calls == [fresh] and json.loads(receipt(store, fresh))['status'] == 'SENT'
        assert receipt(store, key) == raw and receipt(store, marker_key) == marker_raw
        assert not recovery.prepare_resume(store, {'test': 'transport'}, AT + 22000,
                                           carry_pending_price=True, carry_pending_lamp=True)
        assert receipt(store, marker_key) == marker_raw
        assert store.c.execute('pragma user_version').fetchone()[0] == 1
        assert store.c.execute('pragma integrity_check').fetchone()[0] == 'ok'
        assert not store.c.execute('pragma foreign_key_check').fetchall()


@pytest.mark.parametrize('change', [
    {'reason': 'ACK_UNVERIFIED'}, {'reason': 'HTTP_429'}, {'reason': 'HTTP_401'},
    {'reason': 'HTTP_503'}, {'completed_ms': None}, {'claimed_ms': None},
    {'claimed_ms': True}, {'completed_ms': AT}, {'message_id': 10},
    {'preview_sha256': 'invalid'}, {'track': 'OTHER'},
])
def test_uncertain_or_malformed_failure_still_blocks(cfg, market, make_raw, change):
    with Store(config(cfg)) as store:
        key, preview, raw = make_unknown(store, market, make_raw, 'PRICE_ALERT', 'HTTP_502')
        value = json.loads(raw); value.update(change)
        raw = canonical(value)
        store.c.execute('update meta set value=? where key=?', (raw, key))
        assert recovery.prepare_resume(store, {'test': 'blocked'}, AT + 20003)
        assert receipt(store, key) == raw
        assert not store.c.execute('select 1 from meta where key like ?', (PREFIX + '%',)).fetchone()
        assert not alert_queue.claim(store, preview, AT + 20004)


def test_unconfirmed_other_send_blocks_without_partial_quarantine(cfg, market, make_raw):
    with Store(config(cfg)) as store:
        key, _, raw = make_unknown(store, market, make_raw, 'PRICE_ALERT', 'HTTP_502')
        other = next(k for k in keys(store) if k != key)
        assert alert_queue.claim(store, alert_queue.preview(store, other), AT + 20003)
        assert recovery.prepare_resume(store, {'test': 'other'}, AT + 20004)
        assert receipt(store, key) == raw
        assert not store.c.execute('select 1 from meta where key like ?', (PREFIX + '%',)).fetchone()


def test_quarantine_and_backlog_expiry_roll_back_together(cfg, market, make_raw):
    with Store(config(cfg)) as store:
        make_unknown(store, market, make_raw, 'PRICE_ALERT', 'HTTP_502')
        before = dict(store.c.execute('select key,value from meta'))
        store.c.execute("create temp trigger quarantine_fault before insert on meta when new.key like 'transport_unknown_quarantine_v1:%' begin select raise(abort,'quarantine fixture'); end")
        with pytest.raises(sqlite3.IntegrityError, match='quarantine fixture'):
            recovery.prepare_resume(store, {'test': 'atomic'}, AT + 20003)
        assert dict(store.c.execute('select key,value from meta')) == before








def test_transport_failure_keeps_current_sender_stopped(cfg, market, make_raw):
    with Store(config(cfg)) as store:
        seed(store,market,make_raw);calls=[]
        sender=telegram.Sender('synthetic',lambda token,p:(calls.append(p['key']) or {'status':'UNKNOWN','reason':'HTTP_502'}))
        sender.tick(store,AT+20000);sender.thread.join(2);sender.tick(store,AT+20001)
        sender.tick(store,AT+20002);sender.close(store)
        assert len(calls)==1 and sender.stopped


@pytest.mark.parametrize('change', [{'receipt_sha256': '0' * 64}, {'at_ms': AT + 99000}, {'decision': 'RETRY'}])
def test_existing_quarantine_drift_blocks(cfg, market, make_raw, change):
    with Store(config(cfg)) as store:
        key, _, raw = make_unknown(store, market, make_raw, 'PRICE_ALERT', 'HTTP_502')
        assert not recovery.prepare_resume(store, {'test': 'marker'}, AT + 20003)
        marker_key = PREFIX + json.loads(raw)['event_id']
        value = json.loads(receipt(store, marker_key)); value.update(change)
        store.c.execute('update meta set value=? where key=?', (canonical(value), marker_key))
        assert recovery.prepare_resume(store, {'test': 'marker'}, AT + 20004)
        assert receipt(store, key) == raw


def test_restart_keeps_receipt_and_old_code_rollback_relatches(cfg, market, make_raw):
    cfg = config(cfg)
    with Store(cfg) as store:
        key, preview, raw = make_unknown(store, market, make_raw, 'LAMP_ALERT', 'URLError')
        assert not recovery.prepare_resume(store, {'test': 'rollback'}, AT + 20003)
    with Store(cfg) as store:
        assert receipt(store, key) == raw
        assert not recovery.prepare_resume(store, {'test': 'rollback'}, AT + 20004)
        if os.environ.get('HW_ACCEPTED_RECOVERY'):
            path = Path(os.environ['HW_ACCEPTED_RECOVERY'])
            spec = importlib.util.spec_from_file_location('holy_weather.accepted_before_transport_recovery', path)
            old = importlib.util.module_from_spec(spec); sys.modules[spec.name] = old; spec.loader.exec_module(old)
        else:
            import transport_recovery_previous as old
        assert old.prepare_resume(store, {'test': 'rollback'}, AT + 20005)
        assert receipt(store, key) == raw and not alert_queue.claim(store, preview, AT + 20006)
        assert store.c.execute('pragma integrity_check').fetchone()[0] == 'ok'


def test_clean_later_window_keeps_eligible_new_pending(gamma, tmp_path):
    from holy_weather.catalogue import CatalogueStore, Router
    from test_price_recovery import seed as catalogue_seed, feed
    plan,market=catalogue_seed(gamma,tmp_path)
    with CatalogueStore(replace(plan.config,price_notification_filter=False)) as store:
        store.connection_started(AT);router=Router(store)
        feed(router,market,'0.49',AT+1);feed(router,market,'0.71',AT+2)
        key=keys(store)[0];preview=alert_queue.preview(store,key)
        assert alert_queue.claim(store,preview,AT+3)
        alert_queue.finish(store,preview,{'status':'UNKNOWN','reason':'HTTP_502'},AT+4)
        raw=receipt(store,key)
        assert not recovery.prepare_resume(store, {'test': 'carry'}, AT + 5)
        feed(router,market,'0.82',AT+10000)
        pending = next(k for k in keys(store) if json.loads(receipt(store, k))['status'] == 'PENDING')
        before = receipt(store, pending)
        assert not recovery.prepare_resume(store, {'test': 'carry'}, AT + 10001,
                                           carry_pending_price=True, carry_pending_lamp=True)
        assert receipt(store, pending) == before and receipt(store, key) == raw


@pytest.mark.parametrize('field', ['confirmation', 'delivery_notes'])
def test_transport_quarantine_keeps_existing_carry_refusals(gamma, tmp_path, field):
    from holy_weather.catalogue import CatalogueStore, Router
    from test_price_recovery import seed as catalogue_seed, feed
    plan,market=catalogue_seed(gamma,tmp_path)
    with CatalogueStore(replace(plan.config,price_notification_filter=False)) as store:
        store.connection_started(AT);router=Router(store)
        feed(router,market,'0.49',AT+1);feed(router,market,'0.71',AT+2)
        key=keys(store)[0];preview=alert_queue.preview(store,key)
        assert alert_queue.claim(store,preview,AT+3)
        alert_queue.finish(store,preview,{'status':'UNKNOWN','reason':'HTTP_502'},AT+4)
        unknown=receipt(store,key)
        assert not recovery.prepare_resume(store,{'test':'carry-refusal'},AT+5)
        feed(router,market,'0.82',AT+10000)
        pending=next(k for k in keys(store)if json.loads(receipt(store,k))['status']=='PENDING')
        value=json.loads(receipt(store,pending));value[field]={'retained':'prior decision'}
        store.c.execute('update meta set value=? where key=?',(canonical(value),pending))
        assert not recovery.prepare_resume(store,{'test':'carry-refusal'},AT+10001,carry_pending_price=True)
        result=json.loads(receipt(store,pending))
        assert result['status']=='EXPIRED' and result['reason']=='NO_BACKLOG_ON_RECOVERY'
        assert receipt(store,key)==unknown
