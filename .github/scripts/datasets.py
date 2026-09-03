"""The dataset registry — one place that knows what a dataset IS.

Six datasets run through this project, selected by the DATASET env var (a dispatch input):

  aemo   the AEMO NEM electricity pipeline. CSV in, 8 models, mart.fct_summary under layout test —
         143M rows of FIVE narrow columns on a regular 5-minute x DUID grid, i.e. near-uniform.
  nyc    NYC TLC yellow taxi. Parquet in, 4 models, mart.fct_trips under layout test — 17 columns
         of which four sit at 97-99% single-value and two are Zipfian on Manhattan and the airports.
  bts    US DOT/BTS airline on-time performance. Zipped CSV in, 4 models, mart.fct_flights under
         layout test — 22 columns of INDEPENDENT moderate-cardinality categoricals: DayOfWeek is
         seven values near-uniform, Reporting_Airline ~20, Origin/Dest ~350 Zipfian, Tail_Number
         thousands, CancellationCode ~98% NULL.
  green  NYC TLC green taxi. Parquet in, 4 models, mart.fct_green_trips under layout test — the
         same extreme-skew regime as nyc (plus trip_type ~98% one value and ehail_fee ~all NULL)
         on a table an order of magnitude smaller. It exists to test a specific counter-claim:
         that V-Order on green taxi produces BIGGER data, where the yellow pair measured -36%.
  cms    CMS Open Payments general payments. Annual CSV in, 4 models, mart.fct_cms_payments under
         layout test — NINETY-ONE columns, 4x the widest of the others, and the only SPARSE
         surface here: 54 of them are >50% NULL, because CMS models a one-to-many product list as
         five repeated six-column groups whose tail members run 83-99% NULL. It is also the only
         table carrying BOTH skew regimes at once — Nature_of_Payment at 92% single-value (nyc's)
         beside specialty at 302 competing values and the payer id at ~1,000 (bts's).
  tpcds  The TPC-DS subset from the Data Leaps / Microsoft white paper *Modern Power BI
         Architecture Choices for Reporting on Azure Databricks*. DuckDB `dsdgen` in a Fabric
         notebook, 11 models, TWO fact tables, mart.store_sales under layout test — 26M rows at
         SF10 and 262M at SF100.

         THE ONE SYNTHETIC DATASET, AND IT IS A KNOWING EXCEPTION RATHER THAN A DRIFT. Contoso was
         rejected here on exactly this ground and that rejection stands; this one is admitted for a
         different reason, which is not "another point on the surface". It exists to rebuild the
         PAPER'S OWN ROWS inside Fabric, so the Databricks arm in c:/dbx_vertipaq — the same rows
         written with the duckrun parquet-layout profile and mirrored into Fabric — has a V-Order
         reference (spark readHeavyForPBI, and dwh) and a duckrun-layout twin measured by the same
         DAX in the same capacity. Its skew is parameterised per column and independent across
         columns, which is the regime that never exercises the multi-column trade-off, so NEVER
         cite a tpcds number as evidence about skew. Read it as a reproduction, not a measurement
         of what layout is worth on real data.

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

# dataset -> what its landing lakehouse is called, what each engine's output item is called, and
# which table the layout instrumentation profiles.
#
# There was a `sort_by` field here — the key a dispatch of this dataset would use — read by the
# `plan` job's validation and by `stats.py`'s declared-key fallback. Both are gone with the
# `sort_by` dispatch input: one form field naming one key could not serve five marts, so duckrun's
# picker chooses per dataset now and nothing here has to hold an opinion about the key.
#
# `mart_columns` OUTLIVED ITS ONLY CONSUMER and is kept deliberately: `plan` validated a dispatched
# key against it, and nothing does now, but `test_{bts,nyc,green,cms}_columns.py` pin it against the
# staging macros and catch column drift on their own (the cms 91-column count and the
# `Nonccovered` misspelling guard both live there).
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
        # The mart's own columns. This was `plan`'s check that a dispatched `sort_by` named columns
        # of THIS dataset's mart; that input is gone, so nothing reads it at run time now — see the
        # note above `DATASETS` for why it stays.
        "mart_columns": ["date", "time", "DUID", "mw", "price"],
        # Which of `tables` dbt writes into the `landing` schema; the rest live in `mart`. The
        # semantic-model templates read the same two schemas, and test_datasets.py pins this split
        # against each template's own `schemaName` fields — provision.py builds the per-phase
        # Tables shortcuts from it, and a table filed under the wrong schema is a shortcut pointing
        # at a path that does not exist.
        "landing_tables": ["stg_csv_archive_log", "fct_price", "fct_scada",
                           "fct_price_today", "fct_scada_today"],
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
        "landing_tables": ["stg_parquet_archive_log"],
        "download": "download_nyc_taxi.py",
        "model_prefix": "nyc_",
    },
    "green": {
        "landing": "dbt_green_landing",
        "dwh_src": "dbt_green_dwh_src",
        "folder": "benchmark",
        "items": {"duckrun": "dbt_green_delta", "iceberg": "dbt_green_iceberg",
                  "spark": "dbt_green_spark", "dwh": "dbt_green_dwh"},
        "tables": ["stg_green_archive_log", "dim_green_date", "dim_green_zone", "fct_green_trips"],
        "mart": "fct_green_trips",
        # The 20 core columns plus the two DERIVED ones — `pickup_date` (the date dimension's join
        # key; Direct Lake cannot relate a datetime to a date) and `file`. Mirrors
        # macros/green_trip_columns.sql, and `.github/scripts/test_green_columns.py` asserts it
        # does. Green KEEPS congestion_surcharge (present in every month since 2014, NULL before
        # 2019) — the inverse of yellow — and adds trip_type and ehail_fee; the one exclusion is
        # cbd_congestion_fee, 2025-onward only.
        "mart_columns": ["VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
                         "store_and_fwd_flag", "RatecodeID", "PULocationID", "DOLocationID",
                         "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
                         "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge",
                         "total_amount", "payment_type", "trip_type", "congestion_surcharge",
                         "pickup_date", "file"],
        "landing_tables": ["stg_green_archive_log"],
        "download": "download_green_taxi.py",
        "model_prefix": "green_",
    },
    "cms": {
        "landing": "dbt_cms_landing",
        "dwh_src": "dbt_cms_dwh_src",
        "folder": "benchmark",
        "items": {"duckrun": "dbt_cms_delta", "iceberg": "dbt_cms_iceberg",
                  "spark": "dbt_cms_spark", "dwh": "dbt_cms_dwh"},
        "tables": ["stg_cms_archive_log", "dim_cms_date", "dim_cms_payer", "fct_cms_payments"],
        "mart": "fct_cms_payments",
        # ALL 91 SOURCE COLUMNS plus `file`. Date_of_Payment is a DATE straight from the source, so
        # like bts — and unlike nyc and green — there is no derived date column: the dimension join
        # key ships in the file. Mirrors macros/cms_payment_columns.sql, and
        # `.github/scripts/test_cms_columns.py` asserts it does.
        #
        # This is the only mart here that takes its source WHOLE, and that is the point of the
        # dataset: the other four vary skew at 5-22 columns, this one varies WIDTH and adds
        # SPARSITY (54 of the 91 are >50% NULL, because CMS models a one-to-many product list as
        # five repeated six-column groups). Trimming it to the populated columns would delete the
        # surface it was added to measure.
        "mart_columns": [
            "Change_Type", "Covered_Recipient_Type", "Teaching_Hospital_CCN",
            "Teaching_Hospital_ID", "Teaching_Hospital_Name", "Covered_Recipient_Profile_ID",
            "Covered_Recipient_NPI", "Covered_Recipient_First_Name",
            "Covered_Recipient_Middle_Name", "Covered_Recipient_Last_Name",
            "Covered_Recipient_Name_Suffix",
            "Recipient_Primary_Business_Street_Address_Line1",
            "Recipient_Primary_Business_Street_Address_Line2", "Recipient_City",
            "Recipient_State", "Recipient_Zip_Code", "Recipient_Country", "Recipient_Province",
            "Recipient_Postal_Code",
            "Covered_Recipient_Primary_Type_1", "Covered_Recipient_Primary_Type_2",
            "Covered_Recipient_Primary_Type_3", "Covered_Recipient_Primary_Type_4",
            "Covered_Recipient_Primary_Type_5", "Covered_Recipient_Primary_Type_6",
            "Covered_Recipient_Specialty_1", "Covered_Recipient_Specialty_2",
            "Covered_Recipient_Specialty_3", "Covered_Recipient_Specialty_4",
            "Covered_Recipient_Specialty_5", "Covered_Recipient_Specialty_6",
            "Covered_Recipient_License_State_code1", "Covered_Recipient_License_State_code2",
            "Covered_Recipient_License_State_code3", "Covered_Recipient_License_State_code4",
            "Covered_Recipient_License_State_code5",
            "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name",
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID",
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State",
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country",
            "Total_Amount_of_Payment_USDollars", "Date_of_Payment",
            "Number_of_Payments_Included_in_Total_Amount",
            "Form_of_Payment_or_Transfer_of_Value", "Nature_of_Payment_or_Transfer_of_Value",
            "City_of_Travel", "State_of_Travel", "Country_of_Travel",
            "Physician_Ownership_Indicator", "Third_Party_Payment_Recipient_Indicator",
            "Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value",
            "Charity_Indicator", "Third_Party_Equals_Covered_Recipient_Indicator",
            "Contextual_Information", "Delay_in_Publication_Indicator", "Record_ID",
            "Dispute_Status_for_Publication", "Related_Product_Indicator",
            "Covered_or_Noncovered_Indicator_1",
            "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1",
            "Product_Category_or_Therapeutic_Area_1",
            "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1",
            "Associated_Drug_or_Biological_NDC_1", "Associated_Device_or_Medical_Supply_PDI_1",
            "Covered_or_Noncovered_Indicator_2",
            "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2",
            "Product_Category_or_Therapeutic_Area_2",
            "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2",
            "Associated_Drug_or_Biological_NDC_2", "Associated_Device_or_Medical_Supply_PDI_2",
            "Covered_or_Noncovered_Indicator_3",
            "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3",
            "Product_Category_or_Therapeutic_Area_3",
            "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3",
            "Associated_Drug_or_Biological_NDC_3", "Associated_Device_or_Medical_Supply_PDI_3",
            "Covered_or_Noncovered_Indicator_4",
            "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4",
            "Product_Category_or_Therapeutic_Area_4",
            "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4",
            "Associated_Drug_or_Biological_NDC_4", "Associated_Device_or_Medical_Supply_PDI_4",
            "Covered_or_Noncovered_Indicator_5",
            "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5",
            "Product_Category_or_Therapeutic_Area_5",
            "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5",
            "Associated_Drug_or_Biological_NDC_5", "Associated_Device_or_Medical_Supply_PDI_5",
            "Program_Year", "Payment_Publication_Date", "file"],
        "landing_tables": ["stg_cms_archive_log"],
        "download": "download_cms_payments.py",
        "model_prefix": "cms_",
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
        "landing_tables": ["stg_flights_archive_log"],
        "download": "download_bts_flights.py",
        "model_prefix": "bts_",
    },
    "tpcds": {
        "landing": "dbt_tpcds_landing",
        "dwh_src": "dbt_tpcds_dwh_src",
        "folder": "benchmark",
        "items": {"duckrun": "dbt_tpcds_delta", "iceberg": "dbt_tpcds_iceberg",
                  "spark": "dbt_tpcds_spark", "dwh": "dbt_tpcds_dwh"},
        # Pipeline order: the log, then the eight dimensions, then the two facts. Both facts are
        # here and only ONE of them is the `mart` -- see below.
        "tables": ["stg_tpcds_archive_log", "date_dim", "item", "store", "promotion", "ship_mode",
                   "catalog_page", "customer_address", "customer_demographics",
                   "store_sales", "catalog_sales"],
        # THE ONLY DATASET WITH TWO FACT TABLES, and `mart` names the one under layout test:
        # store_sales, the paper's largest (262M rows at SF100, the volume its Direct Lake findings
        # turn on). catalog_sales is a full member of `tables` -- parity, shortcuts, the semantic
        # model and the DAX suite all carry it -- it simply is not the table stats.py profiles
        # deeply, because `mart` is singular by design and a second deep profile would double a
        # 10-minute OneLake read for a question the first one answers.
        "mart": "store_sales",
        # store_sales as LANDED: dsdgen's 23 columns plus the paper's `cache_buster`. No `file`
        # column -- this is the one dataset whose facts are full rebuilds rather than file-driven
        # incrementals, because dsdgen emits a whole scale factor at once and there is no arrival
        # order to increment along. Mirrors macros/tpcds_columns.sql, and
        # `.github/scripts/test_tpcds_columns.py` asserts it does.
        "mart_columns": [
            "ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk", "ss_customer_sk", "ss_cdemo_sk",
            "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_promo_sk", "ss_ticket_number",
            "ss_quantity", "ss_wholesale_cost", "ss_list_price", "ss_sales_price",
            "ss_ext_discount_amt", "ss_ext_sales_price", "ss_ext_wholesale_cost",
            "ss_ext_list_price", "ss_ext_tax", "ss_coupon_amt", "ss_net_paid",
            "ss_net_paid_inc_tax", "ss_net_profit", "cache_buster"],
        "landing_tables": ["stg_tpcds_archive_log"],
        "download": "download_tpcds.py",
        "model_prefix": "tpcds_",
        # `download_limit` IS THE SCALE FACTOR HERE, not a file count -- the same reinterpretation
        # cms makes for program years. The `plan` job refuses anything outside this tuple before a
        # leg spends capacity: the form's default of 200 would ask dsdgen for SF200, and SF1000
        # needs a node this workspace does not have.
        "download_limits": ("1", "10", "100"),
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


def table_schemas(dataset=None):
    """schema -> the dataset's tables in it, for the two schemas dbt writes (`mart` + `landing`).

    This is what provision.py builds the per-phase Tables shortcuts from, so it must agree with the
    semantic-model template's own `schemaName` fields — test_datasets.py reads each `model.bim` as
    data and asserts exactly that. Derived from `tables` minus `landing_tables` rather than stored
    twice, so a new table added to the pipeline order cannot be forgotten here."""
    s = spec(dataset)
    landing = list(s["landing_tables"])
    return {"mart": [t for t in s["tables"] if t not in landing], "landing": landing}


if __name__ == "__main__":
    # A tiny CLI so the workflow can ask a question without a second copy of the answer. Today that
    # is one question — which downloader to run in the `land` job — and it is here rather than as a
    # `${{ }}` conditional in the yaml so that the mapping stays where every other dataset fact
    # lives. Prints ONLY the value, so the caller can use it directly: keep stdout clean.
    if sys.argv[1:] == ["--download"]:
        print(spec()["download"])
    else:
        raise SystemExit(f"usage: {sys.argv[0]} --download")
