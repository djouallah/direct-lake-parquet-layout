# To do

Open work that needs a decision or a dispatch. Not a wishlist — an item earns a place here by being
something a future session would otherwise have to rediscover.

[README.md](README.md) states the thesis, [LEARNINGS.md](LEARNINGS.md) records the investigations,
[CLAUDE.md](CLAUDE.md) records the rules those imply, [RETROSPECTIVE.md](RETROSPECTIVE.md) what the
exercise cost. This file is what has not been done.

---

## iceberg has a geometry knob now — spend the dispatch on nyc, not aemo

`iceberg_geometry()` turns `row_group_size` / `file_size_mb` into Iceberg table properties that
`duckdb__create_table_as` puts in the CTAS, so the iceberg leg can be dispatched at a chosen
geometry for the first time. **Nothing has been measured through it yet.**

Do nyc. aemo iceberg already writes 53 row groups at 2.7M rows, and CLAUDE.md's own CU-by-row-group
band over 60 duckrun runs is flat from 8 to 73 (1,561-1,765), separating only at 144 (2,190) — so
there is no headroom there. nyc iceberg writes **728 row groups at 0.81M rows** (run 33705511481)
against duckrun's 117 at 5.06M, which is outside every band this repo has measured:

```bash
gh workflow run Benchmark -f dataset=nyc -f engines=iceberg -f cores=8    -f duckrun_auto=false -f row_group_size=5000000 -f file_size_mb=1024
```

⚠️ `duckrun_auto=false` is what hands the geometry over — ON forces `auto`, which emits no property.
It is also the UNSORTED lever, but that costs nothing here: iceberg has no sort to give up.

Read `layout.stats.iceberg.fct_trips` (`num_row_groups` should land near 118) and the `directlake`
CU against 33705511481's 7,955. **Also read the leg's DURATION** — see the timeout note below.

⚠️ **THE FIRST DISPATCH (33733500776) MOVED NOTHING, AND THE CAUSE IS NOW READ OUT OF THE
WRITER'S SOURCE.** nyc at `row_group_size=5000000` wrote 729 row groups at 811,700 rows against the
baseline's 728 at 812,815. `iceberg_insert.hpp:69` hardcodes `batch_size_bytes` to 128 MB, both
thresholds are live, and the first to hit wins — so a rows-only property was inert. Confirmed
independently: 20 BIGINT columns through plain `COPY` at the 128 MB default give **827,392 rows per
row group** against nyc's real 812,815. `iceberg_geometry()` now emits
`write.parquet.row-group-size-bytes` at a fixed 1 GiB alongside the row count, which is measured free
(the 128 MB / 1 GiB duration ratio is 0.75× at 20M rows and 1.14× at 60M).

⚠️ **AND RUN 33739194650 — THE FIRST DISPATCH OF THE FIX — TIMED OUT AND MEASURED NOTHING.**
It hit the 90-minute job cap with `fct_trips` still writing after 86 minutes, against SIX minutes for
the same 591M rows on the two dispatches either side of it; its record carries
`{"num_row_groups": 0, "total_rows": 0, "compression": "EMPTY"}` and the cap truncated the notebook
output, so there is no OOM line and no way to tell a slow write from a hung one. **The geometry is
NOT the suspect** — that was suspected and is retracted on the local scaling measurement above, and
`preserve_insertion_order: false` is set on this leg too, so there is no order-preserving
materialization either. The cap is now 150 minutes so the leg can report; the duration is the
diagnostic. If the re-dispatch lands near 6 minutes it was weather.

**The mechanism is PROVEN** — run 33731443153, on the leg's own `duckdb==1.6.0.dev379`
(core `v2.0.0-alpha39998`), against the real OneLake REST catalog with the leg's own ATTACH
options: `row groups: 4 (asked 250000 rows/group over 1,000,000 rows, expected 4)`.
⚠️ **BUT THAT PROBE COULD NOT HAVE CAUGHT 33733500776, AND HAS BEEN REBUILT.** Its table was
2 narrow columns × 1M rows — roughly 10 MB — so the 128 MB budget never bound and a rows-only
property genuinely worked there. It now writes a 50-column table twice: the shipping PAIR (rows +
1 GiB, must give exactly 4 row groups) beside a rows-only CONTROL (must give MORE, i.e. the byte
default binds). Locally those read 4 and 13. Re-run with
`gh workflow run "DuckDB main smoke" -f onelake=true` after any pin move.

**Expect the geometry alone not to close the gap**, and say so when reporting: iceberg is also
8,961 MB against duckrun's 5,866 and carries `PLAIN_DICTIONARY` with **no RLE**, and neither the
sort nor the encoding is reachable from this leg (README.md has why). A run that moves the row
groups and not the CU is the informative outcome, not a failure.

---

## The non-aemo marts have no trailing ORDER BY, and on iceberg that means unsorted

Separate from the item above and probably the larger lever. `models/aemo/duckdb/marts/fct_summary.sql`
is the ONLY mart with a trailing `ORDER BY`; iceberg's CTAS carries it into stored parquet, and the
commit that widened it to `date, time` took the `time` column from 155.0 MB to 3.4 MB and the table
from 1,129 to 856 MB. Every other dataset gets nothing, which is most of why nyc iceberg is 53%
bigger than duckrun on the same rows — `payment_type` 131.8 MB against 0.1, `store_and_fwd_flag`
89.6 against 0.0, `VendorID` 75.0 against 0.0.

What it costs, and why it is not free: the fairness invariant is **all three trees or none**, so the
sort is paid by duckrun, spark and dwh as well, on 591M rows. duckrun already sorts (its picker
chose `pickup_date, VendorID, store_and_fwd_flag, payment_type` on this mart) so it gains nothing
and pays twice. Decide that trade before dispatching; it is a different experiment from the
geometry one and should not be bundled with it.

---

## tpcds has never been generated — land SF100, by hand, before anything else

`download_tpcds.py` is complete and nobody has ever run it, so `dbt_tpcds_landing/Files` is empty
and every tpcds leg fails on

```
[PATH_NOT_FOUND] Path does not exist: abfss://…/Files/landing/parquet_raw/customer_address
```

**A scheduled run cannot fix this by itself, and that is by design**: `land` only runs a downloader
when `skip_download` is off, and a scheduled run forces it ON so every scheduled build measures the
same archive. Which is why the dataset is DISPATCH-ONLY now — `"scheduled": False` in
`datasets.py` — rather than losing four grid cells a week to rediscovering it. The generation is a
hand dispatch, once per scale factor, and then the dataset behaves like any other.

**THE GENERATION HALF IS PROVEN — run 34550578120, 2026-09-11.** At `cores=32`: 1,116 GiB free
disk, 201 GiB memory, `dsdgen(sf=100)` in 1,398s, raw counts matching the spec on all ten tables,
the null-drop landing **262,082,396** `store_sales` and **142,557,716** `catalog_sales` — both
exactly the paper's Table 4.3.1 — and **0 orphans across all 13 relationships**. 36 minutes end to
end. Nothing about dsdgen, the section 4.5 customisation or the parquet write is open.

**WHAT FAILED WAS THE UPLOAD, AND IT WAS A SIZE LIMIT RATHER THAN WEATHER — now fixed.**
`dr.copy(..., overwrite=True)` becomes duckrun's `single_shot` path, one `Put Blob` with the file
buffered in memory, and OneLake answers a ~400 MB `store_sales` file with a bare 500. All six
`_retry_onelake()` attempts re-ran the identical PUT and failed identically. The per-table copy is
`overwrite=False` now — duckrun's streaming multipart path — which is safe because `wipe()` empties
the folder immediately before it; the archive-log rewrite keeps `overwrite=True` because it really
does replace a blob and is a few KB. **Run 33742041035's "ten OneLake 500s" was almost certainly
this same limit, not the transient it was recorded as.**

The other original failure is closed too: `check_landing.py` refuses an empty archive on the FREE
runner and names the dataset and the dispatch.

```bash
gh workflow run Benchmark -f dataset=tpcds -f engines=duckrun -f cores=32   -f skip_download=false -f download_limit=100 -f build=false -f benchmark=false
```

**SF100, and `cores=32` is not optional.** `download_limit` IS the scale factor here — 262,082,396
`store_sales` rows against SF10's 26,206,837 — and it is the volume the white paper's Direct Lake
findings turn on. dsdgen has no chunking, so the working set is the whole scale factor: the script
refuses below ~260 GiB free under `TMPDIR` and `FABRIC_CORES` is what sizes the node. That refusal
is early and cheap, but it is a refusal — the dispatch has to pass the number, and it is the one
place this repo's standing `cores=8` does not apply. `land` gets 240 minutes on this dataset.

Read `python download_tpcds.py --status` first: it answers "has this already happened" without
creating anything, and a re-run is a no-op once the scale factor is logged (`TPCDS_FORCE=1`
overrides).

`build=false -f benchmark=false` because generating is the whole job: there is nothing to measure
until it has happened, and one dispatch doing both puts a first-ever build behind a first-ever
generation.

**SF100 IS LANDED — run 34555739985, 2026-09-11.** 73 files, 21,403 MB, all ten tables, archive log
written with 72 rows, zero upload retries. That step is done and does not need repeating.

**AND THE BUILD NEEDS `cores=32`, NOT `cores=8`** — run 34559633288 OOM-killed a 62.8 GiB node three
times sorting both facts at once. CLAUDE.md has the measurement; the short form is that tpcds is the
only dataset with TWO large facts and `threads: 4` sorts them concurrently. So the ordinary dispatch
for this dataset is:

```bash
gh workflow run Benchmark -f dataset=tpcds -f engines=duckrun -f cores=32
```

**The other three engines have never been run at all.** dwh and spark do their compute Fabric-side
rather than in the duckrun notebook, so the 62.8 GiB ceiling does not apply to them and `cores` does
not size them — but neither has ever seen this dataset, and `store_sales` at 262M rows is the largest
table either has been asked to write here. Expect to find something.

**The four grid cells go back once a build is green** (flip `scheduled`, add four crons, add four
`DATASET` branches; the grid goes 20 slots to 24 and cell (i,j) still fires on weekday (i+2j)%7, so
no existing cell moves). ⚠️ **Note the scheduled `cores` is 8 for every cell** — putting tpcds on the
grid without resolving that schedules four OOM kills a week.

---

## DuckDB `main` cannot commit an Iceberg CTAS — do not move the pin

`v2.1.0-alpha40144` (source `780c7c743f`) dies on the smoke workflow's plain round-trip:

```
INTERNAL Error: Transformer for rule 'Statement' returned an unexpected type.
  ... IcebergTransaction::Commit ...
```

An assertion failure inside the extension, on `CREATE TABLE onelake.dbo.<t> AS SELECT … FROM
range(1000000)` — no properties, no partitioning, nothing exotic. The last green smoke was
`v2.0.0-alpha38837` (2026-08-24), so it regressed somewhere in between.

**The leg is unaffected**: `fabric_run.py` pins `duckdb==1.6.0.dev379`, whose core is
`v2.0.0-alpha39998`, and that wheel round-trips fine — the property probe in the same workflow runs
on it and passes. So this blocks a PIN MOVE, not today's builds.

The smoke workflow will stay red at the round-trip step until upstream fixes it, and that is the
workflow working. **Check WHICH step failed before reading a red smoke as a problem here** — the
probe step is `if: always()` precisely so a main regression cannot hide the leg's own answer.
Worth an upstream issue with the backtrace, which is in run 33730547105's log.

---

## Does dwh's V-Order move its parquet? Genuinely open, and one dispatch away

`layout.ordering.dwh` reads 100% row-group overlap on every column but `cutoff`, and CLAUDE.md read
that as agreeing with the spark finding that V-Order does not reorder rows. **That finding is
retracted** — it was measured on `fct_summary`, which has no surface to reorder — so the dwh reading
agrees with nothing. Both engines were measured on the one table that could not show an effect.

The taxi pair settled it for spark (3,371x on the most repetitive column). The same experiment
answers dwh, and nothing about it is new work:

```bash
gh workflow run Benchmark -f dataset=nyc -f engines=dwh -f skip_download=true -f dwh_vorder=true
# wait for it to finish — SERIAL, see below
gh workflow run Benchmark -f dataset=nyc -f engines=dwh -f skip_download=true -f dwh_vorder=false
```

Then diff `layout.ordering.dwh.columns` between the two records. Note the warehouse writes no
`add.tags.VORDER`, so `vorder_files` is absent there by design and
`layout.ordering.dwh.vorder_enabled` (the `sys.databases` readback) is what says which arm a run is.

⚠️ `dwh_vorder=false` runs an IRREVERSIBLE `ALTER DATABASE CURRENT SET VORDER = OFF`. That is safe
only because the teardown deletes the warehouse at the end of every run — do not lift this onto one
that outlives its dispatch.

---

## The NYC dataset has never been dispatched

Three runs have now happened — 31447430982 (duckrun), 31450956154 (spark `readHeavyForPBI`) and
31451599140 (spark `writeHeavy`) — all green end to end, and the second and third are the pair that
overturned this repo's V-Order conclusion. What is still untried: **iceberg and dwh have never built
this dataset**, and no run has drained more than three months.

### The first dispatch, and it should be small

```bash
gh workflow run Benchmark -f dataset=nyc -f engines=duckrun -f cores=8 \
   -f skip_download=false -f download_limit=3
```

(This used to pass `-f sort_by=pickup_date,PULocationID`. That input is gone — one field naming one
key could not serve five marts — and duckrun's picker chooses per dataset now, which on the taxi
mart resolves to `pickup_date, VendorID, store_and_fwd_flag, payment_type`.)

Three months is minutes of download and a few million rows. What to read afterwards, in order:

1. **The `land` step's log.** Every `REFUSED <month>` line is a month whose parquet schema lacks a
   core column — see the next item, which is the one open question in the whole change.
2. **`layout.ordering.duckrun.columns` in the run record.** `runs` on `store_and_fwd_flag` and
   `RatecodeID` is the measurement this dataset exists for: those columns are ~99% and ~97% one
   value, which is the RLE surface `fct_summary` never had.
3. **`assert_fct_trips_matches_archive_log`.** It is the only assertion on that table and the only
   detector for a doubled month on the DuckDB pair, which write with `append`.

Then the same dispatch with `-f skip_download=true` to confirm the incremental path is a no-op, and
only then a real drain.

### RESOLVED: 2011 carries `PULocationID`

The archive is documented as 2011-onward because TLC republished all history as parquet and only two
schema eras exist — pre-2011 (lat/lon) and 2011-onward (zone ids). **That was read from a
third-party importer's schema list, not from the files**, and this machine could not reach
CloudFront to check. If TLC did not backfill zone ids into 2011-2016, those months lack
`PULocationID`/`DOLocationID`.

**Answered by run 31445985164**: 2011-01, 2011-02 and 2011-03 all landed with ZERO refused,
43,734,157 rows between them. TLC did backfill the zone ids, so the 2011-onward archive is real and
the ~1.5B row figure stands. The land-time guard is still the thing that would catch a later month
drifting, and it costs nothing to leave in.

If most of 2011-2016 is refused, the honest fix is `NYC_START=2017-01` (an env var the downloader
already reads) plus a line here saying so. The row count drops to ~700M, still 5× `fct_summary`.

### The benchmark half has never run either

`benchmark/fct_trips.SemanticModel` and the NYC DAX suite are written and pinned by tests — the
suite's every table, column and measure is asserted to exist in the template — but no model has been
deployed and no query has run. `deploy_models.template()` refuses a dataset whose `.bim` is missing,
so the failure mode is a refusal rather than a report shaped like a result; that guard has not been
exercised against a real deploy.

Scout it the way the AEMO half is scouted, which costs minutes rather than an hour:

```bash
gh workflow run Benchmark -f dataset=nyc -f engines=duckrun -f build=false \
   -f runs=3 -f think_seconds=0 -f gap_seconds=0
```

### One dataset at a time, and the serial rule is unchanged

**Two `Benchmark` runs must never overlap**, and separate Fabric items per dataset make it *look*
safe to run an aemo and an nyc dispatch together. It is not: one capacity, throttling, two quietly
inflated sets of numbers. See the invariant in [CLAUDE.md](CLAUDE.md); the loop below serialises
against any live run, not just its own.

```bash
for ds in aemo nyc; do
  while gh run list --workflow Benchmark --limit 20 \
        --json status -q '.[].status' | grep -qE 'in_progress|queued|pending'; do sleep 60; done
  gh workflow run Benchmark -f dataset="$ds" -f engines=duckrun -f cores=8
  sleep 30
done
```

---

## The AEMO layout groups are complete, and stay that way

All 17 groups are on *Cost and speed by parquet layout* with a run at `cores=8`, so none is dropped
by the `ETL_VCORES` filter. Nothing is open there. Two things worth keeping, because a new layout
re-opens the same work.

### How a layout group gets onto that section

*Cost and speed by parquet layout* reports build cost at ONE core count (`ETL_VCORES`, `8`), because
build cost tracks the machine and `layoutKey` does not carry `vcores` — a group holding runs from
several machines has a median describing none of them (measured: one duckrun layout reads **9,986 CU
at 8 vCores against 22,547 blended** across 8/16/32/64). **A group with no run at that size leaves
the section entirely**, table and chart alike, with the count named in a note under the table. So a
layout dispatched only at 64 cores is measured and invisible.

A group is keyed on `(V-Order, band of the row-group count, sort columns, engine)`, so a row-group
count in a new power-of-two band opens a group of its own. Filling it is one dispatch:

```bash
gh workflow run Benchmark -f engines=duckrun -f cores=8 -f row_group_size=<rows>
```

`row_group_size` is derived — `<mart rows> ÷ the row groups you want`. That held exactly for all
seven runs that closed this item on aemo: `6000000` → 24 RG, `2000000` → 72, `1000000` → 144.

**⚠️ A NEW SORT KEY IS NO LONGER DISPATCHABLE.** The `sort_by` input is deleted — one field naming
one key could not serve five marts — so `duckrun_auto` is the whole sort control: on means duckrun
picks, off means unsorted at the geometry above. The seven keys in the table below are a closed set,
and `LAYOUTS_SHOWN` hides them from the layout tables anyway.

**⚠️ The scheduled grid does NOT fill these in.** It builds `duckrun_auto` at the form's geometry
defaults, and that group already has 8-core runs. Every other group needs a deliberate dispatch, and
**the nyc groups are all empty**, since nothing has been dispatched there at all.

### What closed it

Seven dispatches at `cores=8`, each landing in the band it was aimed at:

| `sort_by` | `row_group_size` | run | row groups |
|---|---:|---|---:|
| `date,time` | `6000000` | 31260566919 | 24 |
| `date,time` | `2000000` | 31257464517 | 72 |
| `date,time,price` | `1000000` | 31257855850 | 144 |
| `date,time` | `1000000` | 31297053137 | 144 |
| `date,time,price` | `6000000` | 31300101128 | 24 |
| `date,DUID,time` | `6000000` | 31303975708 | 24 |
| `date,time,DUID` | `6000000` | 31308163981 | 24 |

The eighth — `iceberg`'s — needed no dispatch: it already had an 8-core run.
