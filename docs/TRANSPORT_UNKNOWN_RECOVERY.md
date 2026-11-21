# Recover future work after a completed transport failure

A terminal UNKNOWN receipt never establishes delivery and must never be retried.
A completed URLError or HTTP_502 request currently stops the sender for the
remaining worker window. At the next existing worker boundary, the recovery
transaction can now quarantine that exact retained receipt and allow future work.
The receipt, event, source evidence and fired thresholds remain unchanged.

The recovery requires an UNKNOWN row for PRICE_ALERT or LAMP_ALERT with valid
claim/completion times and event/preview fingerprints, no message ID, and one of
the two recognized transport reasons. It records KEEP_UNKNOWN_NO_RETRY in a
separate fingerprint-bound metadata record. On first quarantine it expires the
existing pending backlog in the same transaction. Later clean windows retain
eligible new work under the existing rules. No UNKNOWN is promoted to SENT.

Other unconfirmed claims, rejected requests, unexpected ACKs, invalid metadata
or a changed quarantine record keep the sender blocked. The running sender's
stop behavior, HTTP retry policy, recipient, message formats, thresholds,
expiry, reconnect accounting and SQLite schema remain unchanged.

This is recovery for subsequent events, not proof that the failed message was
absent from Telegram, a transport-capacity fix, or authorization to deploy.
Rolling back compatible code on the current database re-latches the sender;
never restore an older database over later receipts. Actual delivery must be
verified on naturally occurring eligible events after a separately approved
installation. Offline tests use isolated databases and synthetic transports.
