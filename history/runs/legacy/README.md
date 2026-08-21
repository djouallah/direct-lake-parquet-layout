# Incomplete run records

Runs that are not a whole generation, kept for reference and read by nothing. The page (`dashboard/app.js`)
skips a record for any of the reasons in its `incomplete()`; this one is here so it cannot even be
offered to it, and so `measure.py`'s floor is not held back by a run nobody will render.

A run that was never TORN DOWN does **not** belong here. That used to send run 30733912205 (duckrun)
to this directory and it was moved back out; it is here now for a different reason — the config it
ran under, see the table.

| record | engine | why |
|---|---|---|
| `2026-08-02T1034Z-30743411308.json` | spark | **No benchmark.** The `bench` job was skipped by a `needs` bug (a job with no `if:` defaults to `success()` over the whole transitive graph, and the skipped `duckdb` matrix poisoned it), so only the ETL half exists. An empty analytics column reads as "querying this engine was free" rather than "nobody measured it". |
| `2026-08-02T0610Z-30733912205.json` | duckrun | **Built at `threads: 1`**, before duckrun 0.4.38 lifted its adapter's `config.threads = 1` pin. Every other engine has always run at 4, so this is a different workload, not a slower one — and `variant()` reads only `layout.config` (vcores, resource profile, NEE), so it would have keyed to the same column as a `threads: 4` duckrun run and been silently superseded rather than distinguished. Removing it also lets `measure.py`'s floor walk forward off a generation nobody will render. |
| `2026-08-11T*` — 31445985164, 31447430982, 31450956154, 31451599140, 31456798377, 31460095071 | duckrun ×4, spark ×2 | **The nyc 43.7M-row generation** (3 landed months, before the archive drained to 41 months / 591.7M). The page shows one source generation and defaults to the biggest; these six were the smaller side of the `?rows=` switch, parked 2026-08-13 so nyc renders one generation only. Nothing scientific was lost: the spark V-Order pair among them (31450956154 / 31451599140 — the 3,371× runs result) was re-measured at 592M by 31463232970 / 31464759095, and the 43.7M numbers survive here and in CLAUDE.md. |
| 30748384735, 30776174056, 30777980085, 30784244593, 30897711643, 31151001360 | iceberg ×6 | **The pre-`1.6.0.dev365` DuckDB writer.** Every one wrote `mart.fct_summary` at DuckDB's old default of **1,172 row groups / 122,851 rows**, an order of magnitude off every other engine; the pin on the iceberg leg made it **3 files / 53 row groups / 2.7M rows** (32444969823, the one record kept). They were parked 2026-08-21 because **row-group count is not part of `layoutKey`** — the power-of-two banding was deleted — so all seven iceberg runs collapsed into ONE layout row, and `groupMid` would have taken a median across two writers' parquet and printed a `53–1,172 RG` span for a difference nobody dispatched. That is precisely the contamination `ENGINES_HIDDEN` used to prevent by hiding the engine outright; hiding is over, so the old generation leaves instead. 30784244593 is doubly disqualified — its `fct_summary` never landed (4 vCores), so it has no mart layout at all. Their numbers are not lost: the 1,172-RG geometry, the 9,288 CU and the `mw` encoding comparison that came out of them are recorded in CLAUDE.md and README.md, which is what they were for. |

It is still a perfectly good raw record of what that run did. It is simply not comparable to a
complete one — an empty analytics column reads as "querying spark was free" — and the page's whole
job is comparison.
