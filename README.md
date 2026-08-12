# Write parquet that Power BI likes

Power BI **Direct Lake** opens a Delta table's parquet directly. The first (cold) query
transcodes each parquet **row group into a VertiPaq segment, one to one**; every query after
that scans the segments the transcoding produced. So query latency — and, more importantly,
**capacity-unit consumption** — is a property of how the parquet was written.

This repo measures exactly that, on real Fabric capacity: two datasets, four writers producing
the **same rows**, one semantic model and one DAX suite over each, capacity units (the bill) as
the primary metric. Results are live at
**<https://djouallah.github.io/direct-lake-parquet-layout/>**.

## How the scan pays for your layout

What a cold query does, per [Microsoft's own account][dl-perf]:

1. The formula engine plans the DAX and issues storage-engine queries.
2. For each column touched and not yet resident, the engine merges that column's per-row-group
   parquet dictionaries into **one global VertiPaq dictionary** — work proportional to
   row-group count, done before the scan can run.
3. If the query joins tables, it builds a **join index per relationship**, which itself loads
   the key columns' dictionaries and the dimension key's segments — a star-schema cold query
   pays for the dimension too.
4. The scan runs across the segments, **remapping each segment's parquet data IDs onto VertiPaq
   IDs** on the way in — a cheap ID swap while the parquet side is dictionary-encoded, a full
   re-encode where it isn't.
5. A warm query on now-resident columns skips all of the above. Under memory pressure the
   engine unloads **segments and join indexes** and rebuilds them on the next query that needs
   them; a rewritten table (`OPTIMIZE`, overwrite) invalidates segments the same way.

The store this fills is the same VertiPaq engine import mode uses ([same page][dl-perf]), so
years of import-mode VertiPaq literature describe the scan too. Three consequences explain
almost every number below:

1. **The scan unit is the segment (= one row group), and the scan pool is sized by cores, not
   by segments.** Microsoft [lays out row groups to match the capacity's cores][dl-perf] and
   warns that uneven row-group sizes unbalance the scan — so a few giant row groups leave cores
   idle: on a 144M-row table, warm query time steps down between 19 and 24 row groups
   (≈5,700 ms → 3,221) and 72 row groups buy nothing over 24. Treat "more row groups than the
   engine has threads" as a floor, not a formula — a strict `ceil(segments ÷ threads)` cost
   model was tested with a deliberate 8-segment run and does not hold, consistent with fetch
   and transcode overlapping the scan.
2. **Cold cost scales with row-group count twice** — every row group is one more local
   dictionary in the merge (step 2) and one more segment to set up and remap (step 4) — so
   segments want **millions of rows each** ([Microsoft says 1–16M][dl-perf]). The same DuckDB,
   same notebook, same SQL was **3.5× slower cold** (96,503 ms vs 27,785) when a library
   default of 122,880 rows per row group reached OneLake: 1,172 tiny segments instead of ~25.
3. **Within a segment the scan is run-length driven.** VertiPaq inherits the parquet row order
   during transcoding, and [VertiScan computes directly on the compressed data][dl-perf], so
   sorted data gives long RLE runs in the resident columns — which is why sorting speeds up
   warm and hot queries, not just the cold transcoding. This is also what V-Order mechanically
   is: a row-reordering plus encoding pass at write time.

Everything below is these facts applied.

[dl-perf]: https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-understand-storage

## Writing with a Fabric engine? Turn V-Order on

V-Order is the sort-and-encode pass done for you, and its worth tracks **surface — column
count × categorical skew — not row count**: on the skewed 17-column taxi mart it collapsed the
most repetitive column to 3,371× fewer runs; on the near-unique 5-column AEMO mart it left row
order untouched and still shrank files 16%. It doesn't reorder what can't benefit, so it is
safe to leave on. Measured cost: ~8% of build compute CU (~14% on taxi); return: up to **2.8×
less analytics CU**.

- **Fabric Spark**: only the `readHeavyForPBI` resource profile enables it — the default
  `writeHeavy` turns it off, and `readHeavyForSpark` doesn't set it at all despite the name.
  Same data, same file band: V-Order on vs off is 1,332 vs 3,769 analytics CU. (The profile
  also flips `optimizeWrite` to 1 GB bins, which on AEMO cut OneLake write CU to a third —
  the total write bill came out *cheaper* with V-Order on.)
- **Fabric Warehouse**: on by default — leave it. `ALTER DATABASE … SET VORDER = OFF` is
  irreversible, and the one run measured with it off billed ~45% more analytics CU and wrote
  16% larger files (n=1 — indicative, not settled).

## Writing with a third-party writer?

delta-rs, DuckDB, open-source Spark, Iceberg writers — none has V-Order. Good practice with
any arbitrary parquet writer:

### 1. Row groups: millions of rows each, and more of them than the engine has threads

Never ship a parquet library's default row-group size to a Direct Lake table — declare
something in the millions. A sweep on the 144M-row table found the knee around **2–6M rows per
row group** (24–72 segments); 16M rows — copying V-Order's own segment size, VertiPaq's
ceiling — was the *worst* sorted geometry measured, because 9 segments starve the scan pool
(mechanic 1). The full sweep log lives in the
[`fct_summary` model header](models/aemo/duckdb/marts/fct_summary.sql).

### 2. Sort the table globally, by what your queries filter

A global `ORDER BY` is most of the V-Order you can have (mechanic 3). Pick the key from the
query workload, not from what compresses best: here 9 of 25 queries filter on the date
dimension, so the key leads with `date`. The counterexample is measured — one alternative key
cut file size a further 30% (543 MB vs 778, n=4 per arm) and bought **statistically zero**
query time or CU. Sort for run lengths in the columns your queries touch; don't chase bytes.

### 3. Keep dictionary encoding on every column

Direct Lake remaps a parquet dictionary straight into VertiPaq's own — [documented][dl-perf] as
a direct remapping of parquet data IDs to VertiPaq IDs when both sides are dictionary-encoded;
a `PLAIN` column forces a re-encode from raw values at load. One 144M-row DOUBLE column
measured 618.6 MB `PLAIN` vs 423.1 MB dictionary-encoded — ~200 MB and a rebuild-at-load on a
single column. Writers
differ silently: Fabric's writers and delta-rs kept dictionaries everywhere; OSS-profile Spark
and DuckDB's own parquet writer dropped them on the widest columns, exactly where it hurts —
those are that engine's two most expensive layouts here.

### A practical example: configuring the delta-rs writer

The three rules above, as an actual [delta-rs](https://github.com/delta-io/delta-rs) writer
profile. The encoding values are the ones [duckrun ships as its default write layout][dr-layout]
— each tuned black-box against a Spark V-Order reference with DAX timings as the signal — and
the two geometry values are this repo's measured knee for the 144M-row table (duckrun's own
defaults are a 16M-row ceiling and 256 MB files):

```python
from deltalake import write_deltalake, WriterProperties, ColumnProperties

wp = WriterProperties(
    compression="SNAPPY",                     # transcoding is decode-bound; ~1.3× ZSTD's size,
                                              #   paid once in storage vs decode on every cold load
    max_row_group_size=6_000_000,             # #1, in ROWS — the knee here; 16M is the ceiling
    dictionary_page_size_limit=32 * 1024**2,  # #3 — THE knob. A column overflows to PLAIN when
                                              #   its dictionary outgrows this; the ~1 MB default
                                              #   is why writers drop the widest columns silently
    data_page_size_limit=1024**2,             # ~1 MB pages; page count is pure reader overhead
    data_page_row_count_limit=1_000_000,      # backstop: an ultra-compressible column otherwise
                                              #   buffers its whole row group as ONE page
    statistics_truncate_length=64,            # row-group min/max only; fat footers help nobody
    default_column_properties=ColumnProperties(
        dictionary_enabled=True, statistics_enabled="CHUNK"),
)
write_deltalake(path, table,                  # #2: ORDER BY your query-filter columns FIRST —
    writer_properties=wp,                     #   no writer property sorts for you
    target_file_size=1024**3)                 # a row group can't span files, so keep this well
                                              #   above one group's bytes (truncation trap below)
```

Two of these are worth restating. The dictionary page limit is the mechanism behind rule #3:
truly unique columns still overflow to PLAIN (correct — their dictionary would be as big as the
data), but the default limit silently de-dictionaries merely-wide columns, exactly the measured
~200 MB `PLAIN` case above; the cost of raising it lands on the *writer's* merge memory
(measured in duckrun's harness: an 18M-row merge at a 128 MB limit hit ~25 GB RSS, 8 MB ~4 GB),
not on the reader. And the statistics stay minimal because Direct Lake
[never reads them at load][dl-perf] — chunk-level min/max is for the other engines sharing the
table.

[dr-layout]: https://djouallah.github.io/duckrun/parquet-layout.html

### What doesn't matter: file count and file size

File count never separated engines in the CU data — the Warehouse ships 78 files and sits
mid-pack; the outlier ships ~357 and loses on its *segments*. A 30% smaller file bought no
query time (four separate demonstrations in the log). Both agree with the documentation:
[Direct Lake does no statistics-based file or row-group skipping at load][dl-perf], so a
smaller or cleverly-partitioned file elides no reads — the sort pays through run lengths
(mechanic 3), never through skipping. One trap: **delta-rs truncates the in-progress row group
at the file-size cap** (measured writing groups at 0.43× their declared rows) — a truncated
group is not just small, it makes segment sizes uneven, which is exactly the
[non-uniform scan load][dl-perf] the docs warn about. Keep the cap comfortably above one row
group's bytes and control size with the row-group knob, not the file knob.

## The lab

Two datasets, chosen as a pair — one with almost no surface for layout to act on, one with a
lot — because the first alone produced a confidently wrong answer about V-Order:

| | `aemo` | `nyc` |
|---|---|---|
| what | Australian electricity market | NYC TLC yellow-taxi trips |
| in | ragged CSV from nemweb | monthly parquet from TLC's CDN |
| out | `mart.fct_summary` — 143M rows, **5 narrow columns**, regular 5-min × DUID grid | `mart.fct_trips` — **17 columns**, ~600M rows built so far |
| shape | near-uniform | four categoricals at 97–99% one value, two Zipfian zone ids |

Four writers produce the same rows from the same landed files, selected by dbt target — the
parquet each writes is the only variable:

| target | engine | parquet writer |
|---|---|---|
| `duckrun` | DuckDB in a Fabric notebook | delta-rs |
| `iceberg` | the same DuckDB, same notebook | DuckDB's writer, via an Iceberg REST catalog |
| `dwh` | Fabric Warehouse (T-SQL) | the warehouse's own (V-Order) |
| `spark` | Fabric Spark (Livy) | parquet-mr (V-Order per resource profile) |

**Measurement:** one `.bim` per engine, Direct Lake with fallback disabled (a query it can't
serve fails, rather than quietly running on the SQL endpoint), deployed fresh so pass 1 is
genuinely cold, pass 2 warm, the rest hot (median). CU is read per item GUID from Fabric's own
Capacity Metrics model. Detail: [benchmark/README.md](benchmark/README.md) and
[dashboard/README.md](dashboard/README.md).

## Method and limits

VertiPaq is treated as a black box: one write knob changes per experiment pair, the layout is
**read back from the parquet footers and Delta log** rather than trusted from config (twice a
declared setting was silently dropped by a writer and only the read-back caught it), the bill
is the metric, and negative results and retractions stay in the record — "V-Order does not
reorder rows" was written down, overturned by the second dataset, and retracted in place.
Every mechanical claim in this document is either one of these black-box measurements (the
number is stated in place) or linked to public documentation — see
[References](#references).

Read the numbers as measurements, not laws:

- **Two datasets** define the whole surface axis; your table sits somewhere else on it.
- **One 25-query DAX suite** — a different workload weights the columns, and therefore the
  sort key, differently.
- **Some cells are thin** — the Warehouse V-Order-off result is one run, and CU on a shared
  capacity is noisy: one dispatch read 2,629 analytics CU on parquet byte-identical to runs
  reading ~1,330–1,590.
- **The Spark V-Order pair compares resource profiles**, not the encoder alone —
  `optimizeWrite` bin size moves with it.
- **Everything here is Direct Lake / VertiPaq.** A different reader pays for parquet
  differently; these findings do not transfer.

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

The dataset is the `DATASET` env var (`aemo` | `nyc`, default `aemo`); models live per dialect
under `models/<dataset>/{duckdb,dwh,spark}`, gated in `dbt_project.yml` so exactly one folder
is enabled per (dataset, target) — `dbt parse --target <name>` verifies the gating offline, no
credentials needed. Tests are six assertions on the mart, written once per dialect so every
engine tests the output it just wrote; cross-engine agreement is the row-count parity table in
CI, nothing else.

**Where to read further:**

- the [live dashboard](https://djouallah.github.io/direct-lake-parquet-layout/) — every run,
  cost and speed per layout
- [RETROSPECTIVE.md](RETROSPECTIVE.md) — what the exercise cost, and what would make it cheaper
- [`fct_summary.sql`](models/aemo/duckdb/marts/fct_summary.sql) — the lab notebook: row-group
  sweeps, sort keys, the delta-rs truncation measurement
- [LEARNINGS.md](LEARNINGS.md) — dbt-adapter and engine war stories (*not* layout)
- [TODO.md](TODO.md) — open questions, with the cost of answering each
- [docs/CI.md](docs/CI.md) — how CI runs this against Fabric, OIDC setup
- [CLAUDE.md](CLAUDE.md) — the working rules, for anyone changing the project

## References

Public documentation grounding the mechanics above; every other claim is a measurement from
this repo's own runs (`history/runs/`, the model-header sweep logs, the
[dashboard](https://djouallah.github.io/direct-lake-parquet-layout/)).

- [Understand Direct Lake query performance](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-understand-storage)
  (Microsoft Learn) — the cold-query pipeline: local→global dictionary merge, parquet-ID→
  VertiPaq-ID remapping and the `PLAIN` re-encode cost, join indexes and what building one
  loads, segments and join indexes unloading under memory pressure, 1–16M-row segment guidance,
  row groups laid out to match cores, non-uniform scan load, and that no parquet/Delta
  statistics are used for skipping at load.
- [Direct Lake overview](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-overview)
  and [How Direct Lake works](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-how-it-works)
  (Microsoft Learn) — framing, transcoding on demand, cold/semiwarm/warm/hot, guardrails.
- [Delta Lake table optimization and V-Order](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order)
  (Microsoft Learn) — V-Order as a write-time sort-and-encode pass.
- Import-mode VertiPaq internals — applicable because Direct Lake fills the same in-memory
  store (first reference): [SQLBI's VertiPaq material](https://www.sqlbi.com/topics/vertipaq/)
  and *The Definitive Guide to DAX* on segments, dictionaries and relationship structures, plus
  the [`DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS` DMV](https://learn.microsoft.com/en-us/analysis-services/instances/use-dynamic-management-views-dmvs-to-monitor-analysis-services)
  that exposes per-segment residency and temperature on a live model.
- [duckrun's parquet layout page](https://djouallah.github.io/duckrun/parquet-layout.html) —
  the delta-rs writer profile above, with the measured rationale per property (SNAPPY vs ZSTD,
  the dictionary-limit / merge-memory trade, page caps) and the black-box tuning loop against a
  Spark V-Order reference.
