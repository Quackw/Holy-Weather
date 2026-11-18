# US catalogue LAMP

The US catalogue can opt in with `us_lamp_enabled=true` in its existing plan.
The default is false. Europe rejects this flag. Legacy single-contract
AWC/LAMP flags and stored KLGA data keep their existing behavior.

| City | Station | Local timezone |
| --- | --- | --- |
| New York | KLGA | America/New_York |
| Atlanta | KATL | America/New_York |
| Seattle | KSEA | America/Los_Angeles |
| Miami | KMIA | America/New_York |
| Chicago | KORD | America/Chicago |
| Dallas | KDAL | America/Chicago |
| Austin | KAUS | America/Chicago |
| Houston | KHOU | America/Chicago |
| Los Angeles | KLAX | America/Los_Angeles |
| San Francisco | KSFO | America/Los_Angeles |

The existing Gamma discovery must validate each day's native contract and
resolution station before the forecast can produce an event. Every validated
bucket receives its own comparison state. The initial issue for each new
contract is a baseline. Stations and dates never share comparison state.

## Source and schedule

Use original NOAA/NCEP NOMADS hourly `lmp.tHH30z.lavtxt.ascii` files.
The source URL includes its UTC date and issue hour. Preserve the native header,
TMP row, UTC row, station-block SHA, collective SHA, response Last-Modified,
issue time, actual receipt time and owner-thread availability time.
The decoder reads the original NOMADS header directly. Existing MDL station
HTML remains supported.

One bounded HTTP thread streams one collective for all ten stations. It retains
only the requested station blocks. Bounds are 6 MiB downloaded, 1024 bytes per
line, 64 KiB per station, five-second socket timeout and a ten-second elapsed
scan guard. No collective archive is saved in the live directory.

Poll at ten minutes after the nominal issue, allowing for publication delay.
This is a scheduling allowance, not a claim of publication time. Retry a known
temporary failure at five-minute intervals, at most three requests per cycle.
403/429 and invalid source data latch an explicit refusal. The next request
time and attempts survive worker/controller restart. A skipped successful issue
is recorded. HTTP 200 alone does not qualify a station's data.

## Events and delivery

Reuse the original 2°F absolute peak-change rule and verified bucket-boundary
crossings in either direction. Compare identical remaining UTC hours on the
contract's local date. Finite ranges and the below/higher end buckets use the
contract's half-up rounding boundaries.

Use the existing event transaction and PRICE/LAMP queue namespaces. LAMP capture
continues during a known market-socket outage within the finite worker window.
Missing market quotes and METAR remain explicit in the event. This extension
does not start an AWC collector or infer observed Tmax.

SENT and UNKNOWN never requeue. Forecast input, state and events survive restart.
The shared sender's existing pacing, expiry and contract revalidation apply.
Each changed bucket keeps its original event and exact market URL.

## Storage and operation

Schema stays at v1. Directory budget stays at 256 MiB and LAMP inputs at 1 MiB.
The existing closed-worker archive path also triggers at 768 KiB of LAMP inputs
when the plan already authorizes history storage. Unreferenced old receipts move
to the full verified archive. Referenced inputs stay hot. Completed date-scoped
states follow the existing closed-market retention, and UNKNOWN pins its market.
No maintenance runs just because this flag is set.

Disabling the feature preserves events and receipts. The accepted PRICE code
can read/write the same database after removing the opt-in field from its plan.
Keep the new archive reader for archive operations involving retired LAMP inputs.
A code/configuration transition must use the existing verified rebinding process
and requires a separately approved server operation.

Qualification uses synthetic CI cases and retained real source receipts outside
Git. Offline CI does not establish installed server identity, live delivery or
continuous-service readiness. No deployment or real send follows from enabling
this code in a PR.

[NOAA LAMP product inventory](https://www.nco.ncep.noaa.gov/pmb/products/lmp/)
and [MDL NWS webservices](https://vlab.noaa.gov/web/mdl/lamp-nws-webservices).
