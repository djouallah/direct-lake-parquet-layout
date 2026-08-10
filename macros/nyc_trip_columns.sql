{#-- The columns fct_trips reads from the raw TLC yellow parquet, in file order.

     ONE source of truth for all three dialects — the DuckDB read, the T-SQL SELECT and the Spark
     read all generate their column list from this, exactly as each AEMO fact generates its CSV spec
     and its CAST list from one `csv_cols`.

     WHY THIS SUBSET, and it is a deliberate 17 rather than TLC's full 19. The published schema has
     moved over sixteen years and two of the columns are casualties of that:

       congestion_surcharge   appears 2019 onward only
       airport_fee            appears 2022 onward, and ships as `Airport_fee` in some months —
                              a casing difference that a name-based read cannot paper over

     Every column below exists in every file from 2011 on, which is the archive slice this dataset
     lands. Dropping the two costs nothing the benchmark cares about: what makes this dataset worth
     having is 17 columns against fct_summary's 5, and every skewed categorical is still here —
     RatecodeID, store_and_fwd_flag, payment_type and VendorID sit at 97-99% single-value, and the
     two LocationIDs are Zipfian on Manhattan and the airports. That skew is the whole experiment.

     THE GUARD IS AT LAND TIME. download_nyc_taxi.py reads each file's parquet footer and REFUSES to
     archive one whose schema does not carry all of these, so everything under parquet_raw/ is
     readable by one statement per dialect and a schema surprise fails at download — free, on a
     runner — instead of mid-write with Fabric capacity already spent. That script holds the same
     list as CORE_COLUMNS and `.github/scripts/test_nyc_columns.py` asserts the two never drift.

     Not here on purpose: `file`. It is derived per dialect from the source path (parse_filename),
     not read from the parquet. --#}
{%- macro nyc_trip_columns() -%}
  {{- return([
    'VendorID',
    'tpep_pickup_datetime',
    'tpep_dropoff_datetime',
    'passenger_count',
    'trip_distance',
    'RatecodeID',
    'store_and_fwd_flag',
    'PULocationID',
    'DOLocationID',
    'payment_type',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount'
  ]) -}}
{%- endmacro -%}

{#-- The target type of each column, per dialect. The raw files are not consistent about integer
     width or about int-vs-double for the count/id columns (passenger_count and RatecodeID ship as
     BIGINT in some months and DOUBLE in others), so every column is cast explicitly rather than
     inherited — an inherited type would make the stored table's schema depend on which months a
     dispatch happened to land, and `layout` compares encodings across engines by column. --#}
{%- macro nyc_trip_type(column, dialect) -%}
  {%- set ints = ['VendorID', 'passenger_count', 'RatecodeID',
                  'PULocationID', 'DOLocationID', 'payment_type'] -%}
  {%- set timestamps = ['tpep_pickup_datetime', 'tpep_dropoff_datetime'] -%}
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
