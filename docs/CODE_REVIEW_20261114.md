# Full source review — 2026-11-14

Reviewed all eight Python modules, the CLI/configuration, storage/retention, replay, weather/forecast parsing, event detection, tests and CI configuration on the ten-city branch. This is a same-agent source review with reproduced defects, not an independent audit or proof of perpetual uptime.

## Confirmed defects and minimal fixes

1. **AWC malformed entries**: JSON arrays containing null, a list or a number raised AttributeError, escaping the weather validation path. Require report objects and return INVALID_DATA without taking down market processing.
2. **Precise METAR temperature**: validation checked only the rounded body temperature, so a decoded 6.4°C could pass alongside raw T0061… (6.1°C). When a precise T group exists, require exact agreement; duplicate groups are rejected. No new forecast formula is introduced.
3. **Card connection state**: a recently saved quote could display FRESH after disconnection without the observation state in the preview. Preserve quote-age status and separately include per-side READY / CONNECTION_UNCONFIRMED / other observation state. Also expose the weather provider status alongside observation freshness.
4. **Opposite-side event evidence**: prices embedded in an event could reference an opposite-side input not protected by event_inputs. A later quote followed by explicit retention could remove that original artifact. Protect both price references for new events; retention also recognizes embedded price references in old events without rewriting their payloads or migrating the schema.
5. **Unknown replay records**: unknown envelope kinds/control events were silently ignored. Reject them during whole-stream prevalidation, before opening the destination database.

The initial focused reproduction had 7 failing and 2 passing tests. After fixes and a legacy-event retention case, the full suite has 172 passing tests locally and on Linux Python 3.13.5. Compile and tracked-source checks pass. No detector thresholds, cooldown, hysteresis, 90-second quote limit, forecast calculation, schema or budget were changed.

## Real-data regression

Replayed the existing saved streams for all ten cities twice into new isolated directories. Each pair is deterministic; original database hashes remain unchanged. AWC decoding remains OK for all ten under the new precise-temperature check. Dallas retains one detected crossing and one suppressed crossing. Original out-of-order evidence remains explicit. The recorded live observations are not repeated or relabelled as new live checks.

Linux offline bundle SHA256: 2a2e2216ca20f45dccaf4c1d2bee71dd41430c2413ff34997685c575897e3bec. Raw artifacts and receipts remain ignored under evidence/full-review-20261114 and evidence/ten-city-20261114.

## Limits

No remaining blocker was found for review of the finite ten-city monitor within this scope. This does not establish continuous ten-city operation, recovery under every possible provider payload, forecast accuracy or trading advantage. One explicit contract per finite run and no automatic date rollover remain intentional. No production, main, original history, service or Telegram changes were made.
