# Bounded qualification plan — not a running collection

Read [source audit](EUROPE_SOURCE_AUDIT.md) first. One project, two directions:
PRICE and FORECAST; US FORECAST uses NOAA LAMP, European source name is explicit.
Historical `LAMP_ALERT` rows and keys stay unchanged. No renaming migration in
this audit. No LOCK, own Tmax model, probabilities, orders or new services.

## Five small PRs, no automatic merges

1. **Source audit (this PR):** current repository findings, official access/licence
   matrix, coordinate conflicts and bounded qualification proposal. Docs only.
2. **MOSMIX_S ingestion:** shared issue download, streaming decode, station/clock
   checks and persisted provenance using existing ownership/writer/storage patterns.
   First implement and test offline; do not use existing single-point DOM decoder
   on a global archive. Preserve old forecasts across failures/restart. Coordinate
   conflicts must remain explicit, even when parsing succeeds.
3. **Regional sources:** only adapters with justified need and verified original
   access. This may be a small documented rejection/defer decision, not seven new
   adapters. AROME bulk packages do not fit current storage limits. No new account,
   paid subscription or data source substitution without separate agreement.
4. **FORECAST events:** source-specific units/age/cadence, common remaining UTC
   hours, independent queue keys, explicit model in short English preview.
   Proposed European research threshold: **1.0°C** absolute remaining-peak change,
   OR a verified Celsius bucket boundary crossing. This is one bucket-width scale
   for integer-Celsius contracts, not an automatic conversion of 2°F or a claim of
   predictive skill. Freeze configuration and tests before assessing signals.
   Keep accepted US 2°F unchanged. Lower-than-observed forecast remains visible.
5. **Pre-launch:** finite real cycles, process restart, controlled error recovery,
   resource measurements, both exact TEST previews and separate send permission.
   Only afterwards consider an autonomous supervisor/next-contract handoff proposal.

## Proposed first MOSMIX_S native qualification envelope

- One local isolated research directory first; no new remote private-code transfer.
  A later Linux package needs an exact manifest, destination and bounded command.
- Maximum **15 minutes**, **two adjacent already published archives**, each at
  most **48 MiB compressed**; one shared download per issue for all 11 points.
- At most two small index reads (256 KiB each), two archive GETs, no archive retry
  in this initial evidence run. Maximum HTTP payload **97 MiB**. Stop on 401/403/429,
  missing file, drift, malformed archive, size bound or unknown state. A failed
  first run is evidence, not authorization for blind retries.
- Whole directory cap stays **256 MiB**, including two originals, SQLite/WAL/SHM,
  projections and logs; reserve 16 MiB. No uncompressed global XML file on disk.
  Per-source log max 1 MiB; never delete existing evidence to make a run fit.
- Stream HTTP to a new partial file while computing SHA; atomic final rename only
  after bounds/integrity checks. ZIP metadata checked before streaming its member.
  No extraction of paths, no multiple unexpected members, no XML entities/DTD.
- Planned decoded-byte ceiling **512 MiB** and peak RSS ceiling **128 MiB** for
  this qualification (not measured claims). Streaming parser must release every
  completed station and avoid retaining the global tree. If a bound is exceeded,
  stop and report; do not silently enlarge it. Measure actual CPU/time/RSS/I/O.
- Store original archive SHA, byte length, CRC result, source URL, HTTP metadata,
  request/receipt/availability clocks; point projections retain their source SHA.
  One hash-shared archive, not 11 copies. Preserve originals for qualification;
  propose later retention only after replay/restore proof.
- London/Paris decoded first, then remaining point IDs from the same stream.
  London/Moscow remain blocked if their coordinates conflict. Do not replace
  missing station values, nonfinite TTT or horizon gaps with zeros or old values.
- Assert issue identities advance exactly one scheduled cycle. Duplicate SHA is
  not a new issue. Same issue/different SHA is an explicit revision. Older issue
  cannot replace high-water state. A skipped cycle is recorded even if a later
  valid cycle is accepted. Original event evidence is immutable.

This bounded historical pair can prove decoding and same-hour comparison. It
**cannot measure when the files first appeared** or demonstrate scheduled live
recovery. Do not report filename/Last-Modified as first-seen publication proof.

## Separate optional arrival observation (not started)

After the two-file resource gate passes, prepare at most **130 minutes** observing
one expected publication boundary. Poll a small index no faster than every five
minutes, at most 27 times; fetch at most two new complete archives with the same
48 MiB cap. No sleeping inside a writer/market receive loop. Publication is
bracketed by last-absent and first-present receipts; report that uncertainty and
HTTP transfer latency separately. Do not promise second-precision publication.

Failure tests use a labelled injected timeout and the next scheduled opportunity,
not fictitious provider outage evidence. Source-age/grace policy must be based on
measured arrival/cadence and fixed before signal evaluation; HTTP 200 never
refreshes issue age. Never copy NOAA's two-hour TTL onto MOSMIX_L.

## Acceptance and rollback checklist

- Identical remaining UTC hour sets, complete to station-local midnight; preserve
  fall-back duplicate local hours and spring-forward absence; no past-hour peak
  expiry masquerading as forecast change.
- Compare fresh causal issue states, not a forecast retrieved after the event.
- No alert at baseline, duplicate, restart, gap or incomparable horizon. One event
  if both threshold and boundary conditions hold; directions up/down both tested.
- Source collection continues through WS outage and Telegram failure. Consumer
  availability is when the writer persists the result, not worker receipt time.
- Event, evidence references and delivery entry commit or roll back together.
  PRICE and FORECAST dedupe stay independent; UNKNOWN never auto-retries.
- Test real saved files twice with immutable SHA, integrity/FK checks, old-reader
  compatibility where applicable, restart and rollback preserving new inputs.
- Preserve code-only rollback; never restore an old data snapshot over newer
  forecasts, events or queue history. No destructive schema migration.
- Daily autonomous operation is a later gate: next-contract rule/station/token
  verification, fresh baseline, isolated day state, common storage cap, safe
  supervisor, free-space guard and outage recovery all need proof.

Until these pass: overall PARTIAL/BLOCKED, no permanent service, no real sends,
no production READY and no city-added announcement.
