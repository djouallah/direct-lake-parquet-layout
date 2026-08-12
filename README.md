# Write parquet that Power BI likes

Power BI **Direct Lake** opens a Delta table's parquet directly: the first (cold) query
transcodes row groups into VertiPaq segments, and every query after that scans what the
transcode produced. So query latency — and, more importantly, **capacity-unit consumption** —
belongs to *how the parquet was written*. The engine that wrote it is metadata.

This repo is an educational project that measures exactly that, on real Fabric capacity: two
datasets, four writers producing the **same rows**, one semantic model and one DAX suite over
each, with capacity units (the bill) as the primary metric. dbt is just the convenience tool
that regenerates the data identically per engine; the parquet layout is the subject. The
results are live at **<https://djouallah.github.io/direct-lake-parquet-layout/>**.

The short version, for anyone writing Delta tables that Power BI will read:

## Writing with a Fabric engine? Turn V-Order on

The write-side cost is nearly negligible and the query-side effect is the largest single lever
measured here.

V-Order is a row-reordering plus encoding pass, and what the reordering is worth depends on
**data skew**: wide tables full of repetitive, skewed categorical columns give it long runs to
create; a narrow table of near-unique values gives it nothing. The writer behaves accordingly —
measured on the dataset with heavy skew it reordered the most repetitive column massively, and
on the dataset with nothing to reorder it left the physical row order untouched (within noise
on every column) while still engaging its encodings and shrinking the files 16%. So you don't
pay a reordering penalty on data that can't benefit: the writer is effectively deciding where
the reordering is worth it, which is why "just turn it on" is safe advice rather than a
trade-off to agonize over.

- **Fabric Spark**: only the `readHeavyForPBI` resource profile enables V-Order — the workspace
  default `writeHeavy` sets it off, and `readHeavyForSpark` does not set it at all despite the
  name. Measured on the same data in the same file band, V-Order on vs off is **2.8× the
  analytics CU** (1,332 vs 3,769) — the sharpest experiment on the dashboard. The premium on
  the build side was **~8% of compute CU** (~14% on the taxi dataset) — and on the AEMO build
  the *total* write bill came out cheaper with V-Order, because the profile's 1 GB bins cut
  OneLake write CU to a third and the output itself shrank 16–36%. (The profile flips
  `optimizeWrite` along with V-Order, so the pair compares the whole profile, not the encoder
  alone.)
- **Fabric Warehouse**: V-Order is **on by default** — leave it. `ALTER DATABASE … SET VORDER
  = OFF` is irreversible, and the one run measured with it off billed ~45% more analytics CU
  and wrote 16% larger files (n=1, and the on-arm's own spread is wide — read it as indicative,
  not settled).

## Writing with a third-party writer? Three things approximate it

delta-rs, DuckDB, open-source Spark, Iceberg writers — none of them has a V-Order encoder, and
there is no way to retrofit it short of rewriting the files in Fabric. What was measured here
is that three ordinary knobs recover most of the gap, and two things people tune don't matter.

### 1. Row groups in the millions of rows

Direct Lake maps row groups to VertiPaq segments, and it wants millions of rows in each. The
starkest result in the repo: the same DuckDB, in the same notebook, at the same core count,
running byte-identical SQL, was **3.5× slower cold** (96,503 ms vs 27,785 ms) through the
adapter that let DuckDB's default `ROW_GROUP_SIZE` of 122,880 rows reach OneLake — 1,172
tiny segments instead of ~25 — than through the one that wrote ~5.5M-row groups. The entire
gap is one default constant ([RETROSPECTIVE.md](RETROSPECTIVE.md)).

A sweep on a 144M-row table found a knee around **2–6M rows per row group**; 16M — copying
V-Order's own segment size, and VertiPaq's ceiling — was the *worst* sorted geometry measured.
The full experiment log lives in the
[`fct_summary` model header](models/aemo/duckdb/marts/fct_summary.sql). The practical rule:
never ship a parquet library's default row-group size to a Direct Lake table; declare
something in the millions.

### 2. Keep dictionary encoding on every column

Direct Lake can remap a parquet dictionary into VertiPaq's own; a `PLAIN` column has to be
dictionary-built from raw values at load. One column (`mw`, 144M DOUBLEs) measured 618.6 MB as
`PLAIN` against 423.1 MB with its dictionary — ~200 MB and a rebuild-at-load on a single
column — and the two Spark profiles that give up dictionaries on their large columns are that
engine's two most expensive layouts. Writers differ silently here: Fabric's writers and
delta-rs kept dictionaries on everything measured; OSS-profile Spark and DuckDB's own parquet
writer dropped them on the widest columns, which is exactly where it hurts.

### 3. Sort the table globally — it's most of the V-Order you can have

V-Order is, mechanically, a row-reordering plus encoding pass. VertiPaq inherits the parquet
row order when it transcodes, so a globally sorted table gives longer runs in the resident
columns — which is why warm and hot queries improve, not just cold. Measured with the same
instrument on the taxi dataset, V-Order reordered the most repetitive column to **3,371×
fewer** adjacent-value runs and shrank the table 36%; a global `ORDER BY` in the model is the
same effect under your control.

What sorting is worth tracks the **surface — column count × categorical skew — not row
count**. The 143M-row, five-column AEMO mart shows almost nothing to reorder; the 43.7M-row,
17-column taxi mart, full of 97–99% single-value categoricals, is where the 3,371× comes from.
That is why this repo carries two datasets. And pick the sort key from the query workload
(here, 9 of 25 queries filter on the date dimension; the key leads with `date`), not from what
compresses best: one sort key cut file size a further 30% and bought **statistically zero**
query time or CU — sort for run lengths in the columns your queries touch, don't chase bytes.

### What doesn't matter: file count and file size

File count never separated engines in the CU data — the Warehouse ships 78 files and sits
mid-pack, the small-row-group layout ships ~357 and is the outlier because of its *segments*.
And a 30% smaller file bought no query time (four separate demonstrations in the log). One
real trap hides here: **delta-rs truncates the in-progress row group at the file-size cap**
(measured writing groups at 0.43× their declared rows) — keep the cap comfortably above one
row group's bytes and control size with the row-group knob, not the file knob.

## The lab

Two datasets, chosen as a pair — one with almost no surface for layout to act on, one with a
lot — because the first dataset alone gave a confidently wrong answer about V-Order:

| | `aemo` | `nyc` |
|---|---|---|
| what | Australian electricity market | NYC TLC yellow-taxi trips |
| in | ragged CSV from nemweb | monthly parquet from TLC's CDN |
| out | `mart.fct_summary` — 143M rows, **5 narrow columns**, regular 5-min × DUID grid | `mart.fct_trips` — **17 columns**, ~600M rows built so far |
| shape | near-uniform | four categoricals at 97–99% one value, two Zipfian zone ids |

Four writers produce the same rows from the same landed files, selected by dbt target — so the
only variable left is the parquet each one writes:

| target | engine | parquet writer |
|---|---|---|
| `duckrun` | DuckDB in a Fabric notebook | delta-rs |
| `iceberg` | the same DuckDB, same notebook | DuckDB's writer, via an Iceberg REST catalog |
| `dwh` | Fabric Warehouse (T-SQL) | the warehouse's own (V-Order) |
| `spark` | Fabric Spark (Livy) | parquet-mr (V-Order per resource profile) |

**How it's measured:** one `.bim` semantic model is deployed per engine in Direct Lake mode
with fallback disabled — a query Direct Lake cannot serve fails rather than quietly running on
the SQL endpoint. A 25-query DAX suite runs against a freshly created model: pass 1 is cold
(the transcode), pass 2 warm, the rest hot (median). Capacity units are read per item GUID
from Fabric's own Capacity Metrics model. Methodology detail:
[benchmark/README.md](benchmark/README.md) (the measurement) and
[dashboard/README.md](dashboard/README.md) (how results are grouped and rendered).

**Where to read further:**

- the [live dashboard](https://djouallah.github.io/direct-lake-parquet-layout/) — every run, cost
  and speed per layout
- [RETROSPECTIVE.md](RETROSPECTIVE.md) — what the exercise cost, and what would have to change
  to make it cheaper
- [`fct_summary.sql`](models/aemo/duckdb/marts/fct_summary.sql) — the lab notebook: row-group
  sweeps, sort keys, the delta-rs truncation measurement
- [LEARNINGS.md](LEARNINGS.md) — dbt-adapter and engine war stories (*not* layout)
- [TODO.md](TODO.md) — open questions, with the cost of answering each
- [docs/CI.md](docs/CI.md) — how CI runs this against Fabric, OIDC setup
- [CLAUDE.md](CLAUDE.md) — the working rules, for anyone changing the project

## Methodology

VertiPaq is treated as a **black box**. Nothing here reads engine internals or takes the
documentation's word for what Direct Lake does with a file; the method is plain experiment
design against an opaque system:

- **Change one variable per experiment.** Same landed data, same semantic model, same DAX
  suite; a pair of dispatches differs in a single write knob — row-group size, sort key,
  V-Order, resource profile — and the difference in the bill is attributed to that knob.
- **Measure the layout back from the files, never trust the config.** After every build, the
  parquet footers and the Delta log are read back — row groups, encodings, per-file V-Order
  tags, physical row order. Twice, a declared setting was silently dropped by a writer and
  only the read-back caught it; a run that recorded its config instead of its parquet would
  have published the wrong experiment.
- **The bill is the metric.** Capacity units already price in how much compute each engine was
  handed; latency is reported as context, medians over passes, and runs repeat because a
  shared capacity is noisy.
- **Negative results and retractions stay in the record.** "V-Order does not reorder rows"
  was written down, then overturned by the second dataset and retracted in place; the 16M
  row-group geometry is recorded as the worst measured, and the 30%-smaller file that bought
  nothing is kept as the counterexample to size-chasing. A black box earns conclusions only
  from experiments that could have falsified them.

## Limitations

This is two datasets, one query suite, one capacity — read the numbers as measurements, not
laws:

- **Two datasets** define the whole "surface" axis: one with almost no skew, one with a lot.
  The rule that layout value tracks column count × categorical skew is drawn from exactly these
  two points; your table sits somewhere else on that axis.
- **One 25-query DAX suite** over one semantic model shape. A different workload weights the
  columns — and therefore the sort key — differently.
- **Some cells are thin.** The Warehouse V-Order-off result is a single run; several layout
  groups on the dashboard hold one or two runs, where the median *is* the run. CU on a shared
  capacity is noisy run to run: one dispatch measured 2,629 analytics CU on parquet
  byte-identical to runs reading ~1,330–1,590, because the capacity was busy.
- **The Spark V-Order pair compares resource profiles, not the encoder alone** —
  `readHeavyForPBI` also changes `optimizeWrite` bin size, so file packing moves with it.
- **Everything here is Direct Lake / VertiPaq.** A different reader (Spark, DuckDB, the SQL
  endpoint) pays for parquet differently; these findings do not transfer to it.

## Run it yourself

Quick, offline (DuckDB → local Delta):

```bash
pip install duckrun                      # brings dbt-duckdb, duckdb, deltalake
export FILES_PATH=./landing              # where the script lands raw CSVs
export ONELAKE_TABLES_PATH=./warehouse   # where duckrun writes Delta tables
python download_aemo.py                  # land the raw CSVs once, then:
dbt build --target duckrun               # models + tests, one DAG walk
```

The other engines need their adapter and env vars, then `dbt build --target <name>`:

| target | adapter | key env vars |
|---|---|---|
| `iceberg` | `dbt-duckdb` | `WAREHOUSE_PATH`, `ONELAKE_ENDPOINT`, `ONELAKE_TOKEN`, `FILES_PATH` |
| `dwh` | `dbt-fabric` (Python ≥ 3.12) | `FABRIC_DWH_SERVER`, `FABRIC_DWH_NAME`, `FABRIC_AUTH`, `FILES_PATH` |
| `spark` | `dbt-fabricspark` | `FABRIC_WORKSPACE_ID`, `FABRIC_LAKEHOUSE_ID`, `FABRIC_LAKEHOUSE_NAME`, `FABRIC_AUTH`, `FILES_PATH` |

The dataset is the `DATASET` env var (`aemo` | `nyc`, default `aemo`). Models are written per
dialect under `models/<dataset>/{duckdb,dwh,spark}` and gated in `dbt_project.yml` so exactly
one folder is enabled per (dataset, target) — `dbt parse --target <name>` verifies the gating
offline, no credentials needed.

Tests are deliberately minimal: six assertions on the mart tables (grain uniqueness, key
uniqueness/nullability, a whitespace check on the join key), written once per dialect so every
engine tests the output it just wrote. Every assertion compares a table to itself;
cross-engine agreement is checked by the row-count parity table in CI, nothing else.
