# Running the project

The [README](../README.md) is about what the measurements found. This is how to run the thing
that produced them. For how CI drives it against Fabric, see [CI.md](CI.md).

## Quick, offline (DuckDB → local Delta)

No Fabric account, no credentials:

```bash
pip install duckrun                      # brings dbt-duckdb, duckdb, deltalake
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

The dataset is the `DATASET` env var (`aemo` | `nyc` | `bts`, default `aemo`). Models live per
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
