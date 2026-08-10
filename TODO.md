# To do

Open work that needs a decision or a dispatch. Not a wishlist — an item earns a place here by being
something a future session would otherwise have to rediscover.

[README.md](README.md) states the thesis, [LEARNINGS.md](LEARNINGS.md) records the investigations,
[CLAUDE.md](CLAUDE.md) records the rules those imply, [RETROSPECTIVE.md](RETROSPECTIVE.md) what the
exercise cost. This file is what has not been done.

---

## Nothing is open.

**All 17 layout groups are on *Cost and speed by parquet layout*, complete** — every one has a run at
`cores=8`, so none is dropped by the `ETL_VCORES` filter, and every one carries analytics CU. The
section that used to open this file (7 of 17 excluded, then 4, then 2) is closed by dispatch, not by
a code change.

Two things worth keeping, because the next new layout re-opens the same work.

### How a layout group gets onto that section

*Cost and speed by parquet layout* reports build cost at ONE core count (`ETL_VCORES`, `8`), because
build cost tracks the machine and `layoutKey` does not carry `vcores` — a group holds runs from
several machines and a median over them describes none of them (measured: one duckrun layout reads
**9,986 CU at 8 vCores against 22,547 blended** across 8/16/32/64). **A group with no run at that
size leaves the section entirely**, table and chart alike, with the count named in a note under the
table. So a layout dispatched only at 64 cores is measured and invisible.

A group is keyed on `(V-Order, band of the row-group count, sort columns, engine)`, so a NEW sort key
or a row-group count in a new power-of-two band opens a group of its own. Filling it is one dispatch:

```bash
gh workflow run Benchmark -f engines=duckrun -f cores=8 \
   -f sort_by=<columns> -f row_group_size=<rows>
```

`row_group_size` is derived — `143,980,961 ÷ the row groups you want`. That held exactly for every
one of the seven runs that closed this item: `6000000` → 24 RG, `2000000` → 72, `1000000` → 144.

**⚠️ The nightly does NOT fill these in.** It builds one layout — `sort_by=date,time,price` at
`row_group_size=2000000`, 72 row groups — and that group already has 8-core runs. Every other group
needs a deliberate dispatch.

### Running several — SERIALLY, and there is no other way

**Two `Benchmark` runs must never overlap** (see the invariant in CLAUDE.md: shared capacity gets
throttled, which inflates both runs' numbers silently, and `ensure()` reuses an output item by name
so two duckrun runs would build into one `mart.fct_summary`). The concurrency group is per REF, so it
does not stop `--ref other-branch` — nothing enforces this but the operator.

The queue cannot be pre-loaded either: `cancel-in-progress: false` allows one running plus **one**
pending, and a third dispatch evicts the queued one rather than stacking.

So chain them. Dispatch, wait for that run to finish, dispatch the next:

```bash
# one "sort_by:row_group_size" per layout
for spec in "date,time,DUID:6000000" "date,DUID,time:6000000"; do
  # never dispatch while anything is live — this is the serialisation
  while gh run list --workflow Benchmark --limit 20 \
        --json status -q '.[].status' | grep -qE 'in_progress|queued|pending'; do sleep 60; done
  gh workflow run Benchmark -f engines=duckrun -f cores=8 \
     -f sort_by="${spec%%:*}" -f row_group_size="${spec##*:}"
  sleep 30                                   # let the run register before the next poll
done
```

The `while` is the important line, not the `for`: it waits on ANY live Benchmark run, so the loop
serialises against a nightly or a hand dispatch too, not just against itself. Budget ~1–1.5 h per
iteration — an 8-vCore build is cheaper in CU than a 64-core one but slower on the clock — and
~10,000 CU each.

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

The eighth — `iceberg`'s — needed no dispatch: it already had an 8-core run. That engine spent a
few hours off the dashboard entirely and is back, so the count is 17.
