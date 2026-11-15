# PRICE time contract

Source and actual receipt timestamps remain distinct and are never rewritten.
PR12 retains strict source-not-after-receipt checking and the 90-second freshness
limit. PR14 separately contains the accepted bounded 100 ms catalogue policy
and reordered-confirmation repair; it is not merged into this foundation.

Market date and expiry use the exact airport IANA time zone. Offline regressions
cover 23/24/25-hour days, local midnight and queue expiry for all 11 profiles.
Before reconnecting the finite observer checks local date again. New connection
or restart requires a confirmed baseline, never a crossing through missing data.

Historical Gamma receipt must precede replayed events. Saved three-city replays
check this and retain original source SHA. No current freshness is inferred from
a historic timestamp. Deferred forecast time work is preserved on the weather
experiments branch, not included here.
