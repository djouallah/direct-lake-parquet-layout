{#-- The columns fct_flights reads from the landed BTS on-time parquet, in file order.

     ONE source of truth for all three dialects — the DuckDB read, the T-SQL SELECT and the Spark
     read all generate their column list from this, exactly as fct_trips does from
     nyc_trip_columns() and each AEMO fact does from its csv_cols.

     WHY THIS SUBSET, a deliberate 22 out of BTS's ~110. The full file is mostly redundancy — every
     airport carried four ways (code, id, seq id, city market id), every delay carried three ways
     (minutes, a >=15 flag, a group bucket), and Year/Quarter/Month/DayofMonth restating FlightDate.
     What this dataset exists to measure is the regime the other two do not cover: MANY
     INDEPENDENT, MODERATELY SKEWED categoricals that genuinely compete for V-Order's one sort —
     DayOfWeek is seven values near-uniform, Reporting_Airline ~20 moderately skewed, Origin and
     Dest ~350 each and Zipfian, Tail_Number thousands, CRSDepTime ~1,200 clustered on 5-minute
     marks, CancellationCode ~98% NULL, the flags binary. nyc's categoricals are 97-99% one value,
     so every column there could win at once; here the greedy ordering has to pick losers, and the
     per-column `runs` table is what shows which.

     THE GUARD IS AT LAND TIME. download_bts_flights.py reads each month's CSV header and REFUSES to
     archive one that does not carry all of these, so everything under parquet_raw/flights/ is
     readable by one statement per dialect and a schema surprise fails at download — free, on a
     runner — instead of mid-write with Fabric capacity already spent. That script holds the same
     list as CORE_COLUMNS and `.github/scripts/test_bts_columns.py` asserts the two never drift.

     Not here on purpose: `file`. It is derived per dialect from the source path (parse_filename),
     not read from the parquet. And no derived date column either, unlike nyc's pickup_date:
     FlightDate ships as a DATE, so it IS the dim_flight_date join key. --#}
{%- macro bts_flight_columns() -%}
  {{- return([
    'DayOfWeek',
    'FlightDate',
    'Reporting_Airline',
    'Tail_Number',
    'Flight_Number_Reporting_Airline',
    'Origin',
    'Dest',
    'CRSDepTime',
    'DepTime',
    'DepDelay',
    'DepDel15',
    'TaxiOut',
    'TaxiIn',
    'ArrTime',
    'ArrDelay',
    'ArrDel15',
    'Cancelled',
    'CancellationCode',
    'Diverted',
    'AirTime',
    'Distance',
    'DistanceGroup'
  ]) -}}
{%- endmacro -%}

{#-- The target type of each column, per dialect. The landing normalisation
     (download_bts_flights.py) already wrote these exact types into the parquet — BTS serves CSV
     where the flags ship as '1.00' and the times as '0745' — so the CASTs in the models are no-ops
     over already-canonical data, kept as the explicit declaration that all four engines store the
     same schema. Same contract as nyc_trip_type(). --#}
{%- macro bts_flight_type(column, dialect) -%}
  {%- set ints = ['DayOfWeek', 'Flight_Number_Reporting_Airline', 'CRSDepTime', 'DepTime',
                  'DepDel15', 'ArrTime', 'ArrDel15', 'Cancelled', 'Diverted', 'DistanceGroup'] -%}
  {%- set dates = ['FlightDate'] -%}
  {#-- Fabric Warehouse needs explicit VARCHAR lengths; Spark has no unlengthed VARCHAR at all
       (STRING is what it stores anyway). Reporting_Airline is 2-3 chars but BTS documents the
       unique-carrier code space wider; 8 is cheap headroom on a dictionary-encoded column. --#}
  {%- set strings = {'Reporting_Airline': 8, 'Tail_Number': 16, 'Origin': 4, 'Dest': 4,
                     'CancellationCode': 1} -%}
  {%- if column in dates -%}
    {{- 'DATE' -}}
  {%- elif column in strings -%}
    {%- if dialect == 'fabric' -%}VARCHAR({{ strings[column] }})
    {%- elif dialect == 'fabricspark' -%}STRING
    {%- else -%}VARCHAR
    {%- endif -%}
  {%- elif column in ints -%}
    {{- 'INT' -}}
  {%- else -%}
    {{- 'FLOAT' if dialect == 'fabric' else 'DOUBLE' -}}
  {%- endif -%}
{%- endmacro -%}
