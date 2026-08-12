"""The dataset registry — one place that knows what a dataset IS.

Three datasets run through this project, selected by the DATASET env var (a dispatch input):

  aemo   the AEMO NEM electricity pipeline. CSV in, 8 models, mart.fct_summary under layout test —
         143M rows of FIVE narrow columns on a regular 5-minute x DUID grid, i.e. near-uniform.
  nyc    NYC TLC yellow taxi. Parquet in, 4 models, mart.fct_trips under layout test — 17 columns
         of which four sit at 97-99% single-value and two are Zipfian on Manhattan and the airports.
  bts    US DOT/BTS airline on-time performance. Zipped CSV in, 4 models, mart.fct_flights under
         layout test — 22 columns of INDEPENDENT moderate-cardinality categoricals: DayOfWeek is
         seven values near-uniform, Reporting_Airline ~20, Origin/Dest ~350 Zipfian, Tail_Number
         thousands, CancellationCode ~98% NULL.

They exist as POINTS ON ONE SURFACE, and the spread is the experiment — it is what turned a wrong
conclusion into a right one. V-Order reorders rows AND re-encodes them, so what it is worth depends
on the SURFACE: column count x categorical skew. aemo has neither and nyc has both, and measuring
only aemo produced "V-Order does not reorder rows", which is false — see the retraction in
CLAUDE.md. Same instrument, same code, two datasets: 3,371x fewer runs on the most repetitive taxi
column against nothing at all on fct_summary. What made nyc EASY for the optimizer is that its
categoricals are 97-99% single-value — every column can win at once, so the multi-column trade-off
that V-Order's greedy ordering actually is was never exercised. bts is the third point: many
skewed-but-balanced columns that genuinely compete for the sort, which is the canonical BI fact
shape and the regime the other two say nothing about.

WHY THIS FILE EXISTS AT ALL. The Fabric item names were hardcoded in three places that had to agree
by convention: provision.py (which CREATES them), stats.py (which READS them) and
benchmark/engines.py (which DEPLOYS models over them). With one dataset a divergence was a typo you
would notice. With two it is silent and expensive: provision.py creating `dbt_nyc_delta` while
stats.py reads `dbt_delta` records the AEMO lakehouse's layout under the NYC run id, and nothing
anywhere raises. So the map is written once, here.

benchmark/ deliberately cannot import this — that directory is built to be deletable by removing
one folder and one workflow file, and an import from .github/scripts would end it. It carries its
own copy in benchmark/engines.py, and `.github/scripts/test_datasets.py` asserts the two agree for
every dataset, exactly as the stats.py/engines.py mirror was already handled.

ITEM NAMING. The aemo names are byte-identical to what they have always been, so 40+ committed run
records and the CU ledger stay comparable; nyc takes a `dbt_nyc_` PREFIX. Prefix rather than suffix
on purpose: `provision.find()` compares display names with `==` so either would work today, but a
suffix scheme (`dbt_spark_nyc`) makes one name a PREFIX of another, and cu/'s legacy substring
matcher is not the only thing that has ever matched a Fabric item name loosely.
"""
import os
import sys

# dataset -> what its landing lakehouse is called, what each engine's output item is called, which
# table the layout instrumentation profiles, and what a default `sort_by` means on it.
DATASETS = {
    "aemo": {
        "landing": "dbt_landing",
        "dwh_src": "dbt_dwh_src",
        "folder": "benchmark",
        "items": {"duckrun": "dbt_delta", "iceberg": "dbt_iceberg",
                  "spark": "dbt_spark", "dwh": "dbt_dwh"},
        # Pipeline order — the parity table reads top to bottom, so a disagreement in the summary
        # can be traced to the inputs on the rows above it.
        "tables": ["stg_csv_archive_log", "dim_calendar", "dim_duid",
                   "fct_price", "fct_scada", "fct_price_today", "fct_scada_today", "fct_summary"],
        "mart": "fct_summary",
        # The mart's own columns, and the ONLY thing that can validate a `sort_by` before capacity
        # is spent. `plan` already checks the input's SHAPE (comma-separated identifiers); a
        # well-formed name that is not a column of this dataset's mart still dies mid-write, and
        # with two datasets sharing one form that is no longer a typo — it is what happens when a
        # `dataset: nyc` dispatch leaves `sort_by` at the aemo default. Kept here rather than read
        # from the manifest because the manifest only exists inside the notebook.
        "mart_columns": ["date", "time", "DUID", "mw", "price"],
        "sort_by": "auto",
        "download": "download_aemo.py",
        "model_prefix": "aemo_",
    },
    "nyc": {
        "landing": "dbt_nyc_landing",
        "dwh_src": "dbt_nyc_dwh_src",
        "folder": "benchmark",
        "items": {"duckrun": "dbt_nyc_delta", "iceberg": "dbt_nyc_iceberg",
                  "spark": "dbt_nyc_spark", "dwh": "dbt_nyc_dwh"},
        "tables": ["stg_parquet_archive_log", "dim_date", "dim_zone", "fct_trips"],
        "mart": "fct_trips",
        # The 17 core columns plus the two DERIVED ones — `pickup_date` (the date dimension's
        # join key; Direct Lake cannot relate a datetime to a date) and `file`. Mirrors macros/nyc_trip_columns.sql, and
        # `.github/scripts/test_nyc_columns.py` asserts it does.
        "mart_columns": ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                         "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
                         "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",
                         "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
                         "total_amount", "pickup_date", "file"],
        # Pickup time first: every composite query groups or filters through the date relationship.
        # PULocationID second: the widest skewed categorical, and what the selectivity ladder
        # filters on. This is the model's own env_var() fallback and what `plan` NAMES in its error
        # when a dispatch leaves `sort_by` at the other dataset's key — `plan` refuses rather than
        # substituting, because a run that quietly measured a layout other than the one the form
        # described is exactly the failure that reshaped that field.
        "sort_by": "auto",
        "download": "download_nyc_taxi.py",
        "model_prefix": "nyc_",
    },
    "bts": {
        "landing": "dbt_bts_landing",
        "dwh_src": "dbt_bts_dwh_src",
        "folder": "benchmark",
        "items": {"duckrun": "dbt_bts_delta", "iceberg": "dbt_bts_iceberg",
                  "spark": "dbt_bts_spark", "dwh": "dbt_bts_dwh"},
        "tables": ["stg_flights_archive_log", "dim_flight_date", "dim_carrier", "fct_flights"],
        "mart": "fct_flights",
        # The 22 core columns plus `file`. FlightDate is a DATE straight from the source, so unlike
        # nyc there is no derived date column — the dimension join key ships in the file. Mirrors
        # macros/bts_flight_columns.sql, and `.github/scripts/test_bts_columns.py` asserts it does.
        "mart_columns": ["DayOfWeek", "FlightDate", "Reporting_Airline", "Tail_Number",
                         "Flight_Number_Reporting_Airline", "Origin", "Dest", "CRSDepTime",
                         "DepTime", "DepDelay", "DepDel15", "TaxiOut", "TaxiIn", "ArrTime",
                         "ArrDelay", "ArrDel15", "Cancelled", "CancellationCode", "Diverted",
                         "AirTime", "Distance", "DistanceGroup", "file"],
        "sort_by": "auto",
        "download": "download_bts_flights.py",
        "model_prefix": "bts_",
    },
}

ALL = tuple(DATASETS)
DEFAULT = "aemo"

# Which Fabric item KIND each engine writes into. Independent of the dataset — it is a property of
# the adapter, not of the data — so it is not repeated per dataset above.
ENGINE_KIND = {"duckrun": "lakehouses", "iceberg": "lakehouses",
               "spark": "lakehouses", "dwh": "warehouses"}

# What actually wrote the parquet behind each engine's Delta log. Same reasoning: a property of the
# adapter. Mirrored by stats.py's WRITER and benchmark/engines.py's.
WRITER = {"duckrun": "delta-rs", "iceberg": "duckdb (iceberg)",
          "spark": "spark", "dwh": "warehouse"}


def selected(value=None):
    """The dataset this run covers, from DATASET (or an explicit value).

    Refuses an unknown name rather than falling back, and the reason is the sharpest failure this
    change introduced: DATASET is also read by dbt_project.yml's `+enabled` gates, where a typo
    does not raise — it makes EVERY gate false, so `dbt build` reports "Nothing to do", exits 0,
    the leg goes GREEN, and the run records the layout of an empty lakehouse. Every entry point
    that reads the variable comes through here so that outcome is unreachable.

    IT DELIBERATELY DOES NOT STRIP. Stripping would be friendlier and would be WRONG: dbt reads the
    same variable as `env_var('DATASET', 'aemo')`, which does no stripping of its own, so a value
    like `'nyc '` would pass here and still leave every `+enabled` gate false. Anything this
    function accepts must be a value dbt would accept identically, so a stray space has to be fatal
    — in the free `checks` job, before any leg spends capacity."""
    name = value if value is not None else os.environ.get("DATASET") or DEFAULT
    if name not in DATASETS:
        raise SystemExit(f"unknown dataset {name!r}; known: {', '.join(ALL)}")
    return name


def spec(dataset=None):
    return DATASETS[selected(dataset)]


def item(engine, dataset=None):
    """The Fabric item display name holding `engine`'s output for this dataset."""
    items = spec(dataset)["items"]
    if engine not in items:
        raise SystemExit(f"unknown engine {engine!r}; known: {', '.join(items)}")
    return items[engine]


def engines(dataset=None):
    """(engine, item name, item kind) for every engine, in a stable order — the same triple
    stats.py and benchmark/engines.py both key on."""
    items = spec(dataset)["items"]
    return [(e, items[e], ENGINE_KIND[e]) for e in ("duckrun", "iceberg", "spark", "dwh")]


if __name__ == "__main__":
    # A tiny CLI so the workflow can ask a question without a second copy of the answer. Today that
    # is one question — which downloader to run in the `land` job — and it is here rather than as a
    # `${{ }}` conditional in the yaml so that the mapping stays where every other dataset fact
    # lives. Prints ONLY the value, so the caller can use it directly: keep stdout clean.
    if sys.argv[1:] == ["--download"]:
        print(spec()["download"])
    else:
        raise SystemExit(f"usage: {sys.argv[0]} --download")
