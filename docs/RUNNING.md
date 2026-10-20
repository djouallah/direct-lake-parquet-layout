# Running the project

[FINDINGS.md](FINDINGS.md) is about what the measurements found. This is how to run the thing
that produced them. For how CI drives it against Fabric, see [CI.md](CI.md).

## Quick, offline (DuckDB → local Delta)

No Fabric account, no credentials:

```bash
pip install "duckrun[dbt]"               # the adapter, plus duckdb and deltalake
export FILES_PATH=./landing              # where the script lands raw CSVs
export ONELAKE_TABLES_PATH=./warehouse   # where duckrun writes Delta tables
python download_aemo.py                  # land the raw CSVs once, then:
dbt build --target duckrun               # models + tests, one DAG walk
```

## The other three engines

Each needs its adapter and env vars, then `dbt build --target <name>`:

| target | adapter | key env vars |
|---|---|---|
| `iceberg` | `dbt-duckdb` | `WAREHOUSE_PATH`, `ONELAKE_ENDPOINT`, `ONELAKE_TOKEN`, `FILES_PATH` |
| `dwh` | `dbt-fabric` (Python ≥ 3.12) | `FABRIC_DWH_SERVER`, `FABRIC_DWH_NAME`, `FABRIC_AUTH`, `FILES_PATH` |
| `spark` | `dbt-fabricspark` | `FABRIC_WORKSPACE_ID`, `FABRIC_LAKEHOUSE_ID`, `FABRIC_LAKEHOUSE_NAME`, `FABRIC_AUTH`, `FILES_PATH` |

## Datasets and gating

The dataset is the `DATASET` env var (`aemo` | `nyc` | `bts` | `green` | `cms` | `tpcds`, default
`aemo`).

`tpcds` is the one dataset whose input is **generated rather than downloaded**, and generating it is
a **one-off** — done once per scale factor, then read by every engine and every dispatch afterwards.
`download_limit` is the **scale factor** there (1, 10 or 100; `plan` refuses anything else). The
generator is `tpcgen-cli`, pinned exactly (`pip install tpcgen-cli==4.0.0`); it streams one table
part at a time, so it runs wherever the script runs — the `land` runner in CI — and spends no Fabric
compute. From a laptop:

```bash
python download_tpcds.py --status          # what is landed; creates nothing

FILES_PATH=<abfss .../dbt_tpcds_landing/Files> download_limit=10 python download_tpcds.py
```

A re-run is a no-op once the scale factor is logged (`TPCDS_FORCE=1`, the `regenerate` dispatch
input, overrides). Every count is checked before OneLake is touched; the upload is then retried —
six attempts at a widening backoff on a 5xx or a dropped connection, never on a 4xx — because run
33742041035 lost a whole SF100 generation to OneLake 500s on the last step.

Dispatch it:

```bash
# the one-off landing, on the free runner
gh workflow run Benchmark -f dataset=tpcds -f skip_download=false -f download_limit=100 -f build=false -f benchmark=false

# then, once it is landed, an ordinary dispatch
gh workflow run Benchmark -f dataset=tpcds -f engines=duckrun -f cores=8
```

Models live per
dialect under `models/<dataset>/{duckdb,dwh,spark}`, gated in `dbt_project.yml` so exactly one
folder is enabled per (dataset, target). `dbt parse --target <name>` verifies the gating offline —
no credentials needed, and worth doing before spending any capacity, because a gate that disables
everything reports "Nothing to do" and exits 0.

## Tests

Assertions on the mart only, written once per dialect so every engine tests the output it just
wrote in the same DAG walk. A green leg is a self-consistency statement about that engine —
cross-engine agreement is the row-count parity table in CI, and nothing else.

## Offline checks that cost nothing

```bash
python -m pytest .github/scripts/ -q     # run record, provisioning, teardown, stats
python -m pytest benchmark/ -q           # semantic-model templates and the render layer
python -m pytest cu/ -q                  # the capacity-unit ledger
node --test dashboard/app.test.mjs       # the dashboard join, labelling and chart
```
