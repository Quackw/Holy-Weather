"""Exercise quiet-stream queue deadlines through the single-market observer."""
from collections import deque
from dataclasses import replace
from datetime import datetime
import json
import pytest
from holy_weather import app, telegram
from holy_weather.storage import Store
from conftest import NOW


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('high', [False, True])
@pytest.mark.parametrize('quiet', ['pong', 'timeout'])
def test_observe_settles_quiet_bid_wait(cfg, market, monkeypatch, enabled, high, quiet):
    ticks = [0.0]; stores = []; checked = []; transports = []
    at = int(NOW.timestamp() * 1000)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)

    clock = lambda: at + round(ticks[0] * 1000)
    monkeypatch.setattr(app, 'datetime', FixedDatetime)
    monkeypatch.setattr(app, 'discover', lambda _: (market, {'synthetic': True}))
    monkeypatch.setattr(app.time, 'monotonic', lambda: ticks[0])
    monkeypatch.setattr(app, 'millis', clock)
    monkeypatch.setattr(telegram, 'millis', clock)

    original_store = Store.__init__
    def capture(self, *args, **kwargs):
        original_store(self, *args, **kwargs); stores.append(self)
    monkeypatch.setattr(Store, '__init__', capture)

    def forbidden(*args, **kwargs):
        transports.append(True)
        raise AssertionError('NO_TOKEN_OR_TELEGRAM_WITH_SENDING_DISABLED')

    if enabled:
        monkeypatch.setattr(telegram, 'token_from_env', lambda: 'synthetic-no-credential')
        original_sender = telegram.Sender.__init__
        def paced_sender(self, *args, **kwargs):
            original_sender(self, *args, **kwargs)
            # Exercise the real ON tick without sending the resulting preview.
            self.next_send_ms = at + 100000
        monkeypatch.setattr(telegram.Sender, '__init__', paced_sender)
    else:
        monkeypatch.setattr(telegram, 'token_from_env', forbidden)
        monkeypatch.setattr(telegram, 'Sender', forbidden)

    def quote(bid, ask, ms, side='YES'):
        return json.dumps(dict(event_type='best_bid_ask', market=market['condition_id'],
                               asset_id=market['tokens'][side], timestamp=str(at + ms),
                               best_bid=bid, best_ask=ask))

    sequence = deque([
        quote('0.56', '0.80', 100) if high else quote('0.48', '0.50', 100),
        quote('0.30', '0.32', 200, 'NO'),
        quote('0.56', '0.85', 300) if high else quote('0.48', '0.54', 300),
    ])

    class Socket:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def send(self, raw): pass
        def recv(self, timeout):
            if ticks[0] >= 9 and not checked:
                store = stores[0]
                value = json.loads(store.c.execute(
                    "select value from meta where key like 'alert_delivery_v1:%'"
                ).fetchone()[0]); checked.append(value)
                assert value['status'] == ('PENDING' if high else 'REJECTED')
                assert value['confirmation'] == 'UNCONFIRMED'
                assert value['reason'] == 'NO_BID_CONFIRMATION_WITHIN_WINDOW'
                assert value['wait_until_ms'] <= value['completed_ms'] <= value['wait_until_ms'] + 1000
                if high: assert 'Ask-only · not confirmed by bid' in value['html']
                assert store.c.execute("select count(*) from events where type='PRICE_ALERT'").fetchone()[0] == 1
            if sequence:
                ticks[0] += .1
                return sequence.popleft()
            ticks[0] += 1
            if quiet == 'timeout': raise TimeoutError()
            return 'PONG'

    result = app.observe(replace(cfg, duration_seconds=12, price_alert_enabled=True,
                                alert_queue_enabled=True, telegram_enabled=enabled,
                                price_notification_filter=True),
                         connect=lambda *args, **kwargs: Socket(), telegram_transport=forbidden)
    assert checked and not transports
    assert result['state'] == 'COMPLETED_WINDOW'
    assert not result['counters'].get('baseline_recovery_gaps')
    assert result['storage']['sqlite_integrity'] == 'ok'
    assert result['storage']['foreign_key_errors'] == 0
