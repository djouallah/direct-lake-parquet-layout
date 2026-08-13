{#-- The columns fct_green_trips reads from the raw TLC green parquet, in file order.

     ONE source of truth for all three dialects — the DuckDB read, the T-SQL SELECT and the Spark
     read all generate their column list from this, exactly as nyc_trip_columns() does for yellow.

     WHY THIS SUBSET, and it is a deliberate 20 rather than TLC's full 21. Green's published
     parquet is more uniform than yellow's: every month from 2014-01 carries the same 20 columns
     (probed over the CDN month by month), so green KEEPS `congestion_surcharge` — present since
     the start, NULL before 2019 — the inverse of yellow, where the column only appears from 2019
     and is excluded. The one casualty is:

       cbd_congestion_fee     appears 2025 onward only — green's `airport_fee` analogue

     What the extra columns buy the experiment: `trip_type` is a ~98% single-value categorical
     (street-hail vs dispatch) and `ehail_fee` is ~all NULL, on top of the same skew yellow has —
     RatecodeID, store_and_fwd_flag, payment_type, VendorID at 97-99% single-value and the two
     LocationIDs Zipfian (Brooklyn/Queens here; green's Manhattan pickups are legally restricted
     to the upper zones).

     THE GUARD IS AT LAND TIME. download_green_taxi.py reads each file's parquet footer and
     REFUSES to archive one whose schema does not carry all of these, so everything under
     parquet_raw/ is readable by one statement per dialect and a schema surprise fails at download
     — free, on a runner — instead of mid-write with Fabric capacity already spent. That script
     holds the same list as CORE_COLUMNS and `.github/scripts/test_green_columns.py` asserts the
     two never drift.

     Not here on purpose: `file`. It is derived per dialect from the source path (parse_filename),
     not read from the parquet. --#}
{%- macro green_trip_columns() -%}
  {{- return([
    'VendorID',
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'ehail_fee',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'trip_type',
    'congestion_surcharge'
  ]) -}}
{%- endmacro -%}

{#-- The target type of each column, per dialect. The raw files are even less consistent than
     yellow's — the same column ships as INT64, INT32 and DOUBLE in different years (RatecodeID,
     passenger_count, payment_type, ehail_fee, congestion_surcharge all drift) — so every column is
     cast explicitly rather than inherited. An inherited type would make the stored table's schema
     depend on which months a dispatch happened to land, and `layout` compares encodings across
     engines by column. --#}
{%- macro green_trip_type(column, dialect) -%}
  {%- set ints = ['VendorID', 'passenger_count', 'RatecodeID',
                  'PULocationID', 'DOLocationID', 'payment_type', 'trip_type'] -%}
  {%- set timestamps = ['lpep_pickup_datetime', 'lpep_dropoff_datetime'] -%}
  {%- set strings = ['store_and_fwd_flag'] -%}
  {%- if column in timestamps -%}
    {{- 'DATETIME2(6)' if dialect == 'fabric' else 'TIMESTAMP' -}}
  {%- elif column in strings -%}
    {#-- Spark has no unlengthed VARCHAR — `CAST(x AS VARCHAR)` is a parse error there, and STRING
         is the type Spark stores anyway. Fabric Warehouse needs an explicit length. --#}
    {%- if dialect == 'fabric' -%}VARCHAR(1)
    {%- elif dialect == 'fabricspark' -%}STRING
    {%- else -%}VARCHAR
    {%- endif -%}
  {%- elif column in ints -%}
    {{- 'INT' -}}
  {%- else -%}
    {{- 'FLOAT' if dialect == 'fabric' else 'DOUBLE' -}}
  {%- endif -%}
{%- endmacro -%}
