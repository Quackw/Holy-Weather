# Telegram cards — opt-in, independent, no production activation

Target: @holy_weather. English HTML cards link directly to the verified bucket and show side, before/after ask, latest METAR, received observed maximum, remaining original LAMP peak, signed difference, Kyiv event time and data ages. There is no recommendation footer. Missing/stale weather is explicit; original forecasts and prices are not recomputed. Quote evidence must match the event's exact token and stored source timestamp.

## Safe defaults

Telegram is disabled by default. `telegram_enabled=true` requires both existing weather-context flags. The bot token is read only from `HOLY_WEATHER_TELEGRAM_TOKEN`; it is never stored in SQLite, logs, examples or Git. The token is not included in error text. No existing POLYMETEO sender, service or credentials are modified.

The current PR does not authorize or perform deployment or real messages. Before the first test in the public channel, review the exact preview and obtain explicit permission. Supply access locally through the environment, not by pasting a token into a chat or command log. Channel posting rights are not established by code tests.

## Delivery semantics

One existing SQLite writer creates a durable UNKNOWN claim before a single HTTP attempt. A background worker handles only HTTPS; the owner records the returned acknowledgement. All event types caused by the same token/input update share one delivery key, so a MOVE plus CROSS does not generate duplicate cards. Keys use the immutable input identity, not a reusable SQLite row number.

`SENT` requires an acknowledgement with message ID, channel identity, Telegram date, exact rendered text and bucket link. Errors or uncertain outcomes do not trigger automatic retries. UNKNOWN survives restart and prevents another attempt for that update. This favors avoiding duplicates over guaranteed delivery: a crash after claiming but before sending can leave an unsent UNKNOWN. No exactly-once network guarantee is claimed.

Ordinary delivery requires a confirmed current side, valid event quote and the unchanged 90-second source-age limit. A pending send is claimed only after checks; quote age is checked again immediately before HTTP. Missing or stale AWC/LAMP does not stop market collection; the card shows its status. A Telegram error disables that sender instance without stopping the monitor. Restart does not retry its existing claims. No mass historical backfill is performed.

The existing meta table stores a maximum of 1,024 compact delivery records within the existing database budget. Reaching this cap disables delivery explicitly; it does not delete history or increase storage limits. No new database or migration. Normal retention may age out old events; their compact delivery claims remain. Successful acknowledgement stores event-to-completion and attempt-to-completion durations; these are API acknowledgement timings, not proof that a human viewed the message.

At finite shutdown, an already completed result can be recorded; an unfinished request remains UNKNOWN, with no retry. A five-second HTTP socket timeout bounds individual network operations; no persistent service is added.

## Preview and one reviewed test

On an isolated copy of saved data, with its matching config:

```sh
python -m holy_weather.telegram preview --config /absolute/path/test.toml --event-id EVENT_ID --test
```

Save that exact JSON outside Git. A test card has an explicit TEST title and says that it is a recorded event with ages measured at event time. This is the only mode that permits an old event; it cannot masquerade as a fresh market notification.

Only after permission for that preview and locally configured credentials:

```sh
python -m holy_weather.telegram send --config /absolute/path/test.toml --preview-file /absolute/path/PREVIEW.json --preview-sha EXACT_PREVIEW_SHA
```

The sender regenerates the preview from immutable event data and compares it with the supplied JSON before claiming. A modified body, destination or expired ordinary quote is refused. `send` is a single explicit action, not a scheduler. The normal monitor integration remains disabled until separately enabled in a finite run.

## Qualification

202 tests cover formatting, exact bucket/side, HTML escaping, duplicate event types, restart/UNKNOWN, timeout, refusal, wrong-channel ACK, bounds, stale/disconnected data, preview tampering, delivery timing and full-monitor continuation during Telegram failure. All HTTP/Telegram responses in tests are synthetic. A saved real Dallas event was rendered on a database copy; original SHA is unchanged. Linux offline qualification and GitHub CI are required before review.

No real Telegram request, channel-permission check or delivery has occurred in this task. Real delivery remains pending explicit user approval and local token availability.

Protocol reference: https://core.telegram.org/bots/api#sendmessage (JSON POST, HTML parse mode, link-preview control, Message acknowledgement).

Prior-code compatibility was checked on a separate Dallas database copy: accepted PR-4 code read and wrote the database with the new delivery claim present; the new code then reopened it and refused a repeated claim. Original inputs and source SHA were unchanged. No schema migration or HTTP request was involved.
