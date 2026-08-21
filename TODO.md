# To do

Open work that needs a decision or a dispatch. Not a wishlist — an item earns a place here by being
something a future session would otherwise have to rediscover.

[README.md](README.md) states the thesis, [LEARNINGS.md](LEARNINGS.md) records the investigations,
[CLAUDE.md](CLAUDE.md) records the rules those imply, [RETROSPECTIVE.md](RETROSPECTIVE.md) what the
exercise cost. This file is what has not been done.

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
