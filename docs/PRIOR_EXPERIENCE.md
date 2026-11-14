# Prior experience and provenance

Before implementation the November 14 POLYMETEO monitor report, its PR3 source
candidate and the available HOLY NFX price/stream/registry/sender principles were
studied. The 50,270 snapshots / 469 events are historical experiment results,
not claims about this new application.

Lessons applied: separate bid/ask/trade clocks and sizes; first snapshot is a
baseline; explicitly retain gaps; source uniqueness and durable state; keep raw
event evidence; suppress 69.9↔70.1¢ threshold chatter; bound the data directory.
An HTTP403 from a separate REST endpoint is not fixed by fabricating a current
status or sending a request to another identity/address.

No application source was copied from POLYMETEO or NFX. No reuse license for
their application code was assumed. This is a clean Python implementation of
the stated monitoring rules, with synthetic tests. Existing real records are
accessed only through separately copied, SHA-bound research artifacts and the
generic replay manifest; no working database is shared or opened.

The single runtime dependency is websockets 17.1, BSD-3-Clause. Its published
package supplies its original license. pytest and its test dependencies are
development-only, version-constrained in requirements-dev.lock. No package from
the NFX dependency tree, no Node runtime and no production credentials are used.
