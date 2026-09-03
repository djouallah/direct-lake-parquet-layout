-- The wide raw fact, and the table under layout test on this dataset — stats.py's MART for
-- DATASET=bts, the way mart.fct_summary is for aemo and mart.fct_trips for nyc.
--
-- WHY THIS DATASET, IN ONE PARAGRAPH. nyc showed V-Order's reordering is real (3,371x on its most
-- repetitive column) — but its categoricals are 97-99% single-value, so extreme that every column
-- stays run-friendly under ANY sort: the columns never compete, and the multi-column trade-off
-- that V-Order's greedy ordering actually is was never exercised. This table is the competing
-- case, and the canonical BI fact shape: DayOfWeek (seven values, near-uniform),
-- Reporting_Airline (~20, moderately skewed), Origin and Dest (~350 each, Zipfian), Tail_Number
-- (thousands), CRSDepTime (~1,200, clustered on 5-minute marks), CancellationCode (~98% NULL),
-- binary flags — mutually independent, so sorting for one buys nothing on the others. The
-- interesting output is layout.ordering's per-column `runs`: which columns the optimizer
-- sacrifices, and whether the CU gain survives dividing the sort budget.
--
-- THE WRITE IS `append`, THE SAME KNOWING EXCEPTION fct_trips IS, for the same chain: BTS
-- publishes NO primary key for this data and duplicate rows exist in the source, so no column set
-- is a guaranteed key; the only candidate is the FILE, at ~500K rows per file — not unique on it;
-- duckrun REFUSES a unique_key its source is not unique on (engine.assert_source_unique covers
-- both write paths), so a file-keyed write does not degrade, it does not run; a surrogate row id
-- is free in DuckDB and Spark and impossible in Fabric Warehouse; and delete+insert on duckrun is
-- a fenced full-table overwrite. What bounds the exposure: the teardown means no cross-run
-- incremental state, and assert_fct_flights_matches_archive_log reconciles each file's stored
-- row count against its logged one. iceberg runs this same file byte-identical, per the standing
-- rule for the duckdb tree. See models/nyc/duckdb/marts/fct_trips.sql for the long form.
{%- set pending_files_query -%}
SELECT file_stem FROM {{ ref('stg_flights_archive_log') }}
WHERE source_type = 'flights'
{%- if is_incremental() %}
AND file_stem NOT IN (SELECT DISTINCT file FROM {{ this }})
{%- endif -%}
{%- endset -%}

{%- if execute and flags.WHICH in ('run', 'build', 'retry') -%}
  {%- set files_result = run_query(pending_files_query) -%}
  {%- set pending_files = files_result.columns[0].values() | list if files_result else [] -%}
{%- else -%}
  {#-- Parse time: unknowable. `none` means "assume there is work", so the model renders its real
       body rather than the no-op branch — the compiled SQL a reviewer reads is the one that runs. --#}
  {%- set pending_files = none -%}
{%- endif -%}
{%- set has_files = pending_files is none or pending_files | length > 0 -%}

{%- set cols = bts_flight_columns() -%}

{#-- The geometry knobs are the SAME dispatch inputs the other marts read, so a layout question can
     be asked of any dataset with one workflow. NO DERIVED DATE COLUMN, unlike nyc's pickup_date:
     FlightDate ships as a DATE, so it IS the dim_flight_date join key and nothing needs bridging.

     THE SORT IS `auto` OR NOTHING. There was a `sort_by` input taking a literal column list, and
     it could not serve five marts from one field — its default was the aemo key, so this dataset's
     dispatches existed mainly to override it, and the plan job carried a per-dataset column check
     purely to refuse the mismatch. duckrun's picker profiles this table itself. `auto` must be
     passed as a SCALAR — duckrun raises on ['auto']. The comment sits ABOVE the config tag: dbt
     parses config(...) as an expression and a Jinja comment between its arguments is a parse error
     pointing at the wrong line. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    sort_by=('auto' if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
    max_row_group_size=(none if env_var('DUCKDB_ROW_GROUP_SIZE', 'auto').lower() == 'auto'
                        else env_var('DUCKDB_ROW_GROUP_SIZE', 'auto') | int),
    target_file_size_mb=(none if env_var('DUCKDB_FILE_SIZE_MB', 'auto').lower() == 'auto'
                         else env_var('DUCKDB_FILE_SIZE_MB', 'auto') | int),
    iceberg_properties=iceberg_geometry(),
    pre_hook="SET VARIABLE bts_flight_paths = (SELECT COALESCE(NULLIF(list('{{ get_parquet_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_flights_archive_log') }} WHERE source_type = 'flights'{% if is_incremental() %} AND file_stem NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} ORDER BY archive_path))"
) }}

{% if has_files %}
{#-- A plain read, no union_by_name and no schema merging, because the archive is HOMOGENEOUS by
     construction: download_bts_flights.py rewrites every month's CSV to the canonical 22-column
     parquet schema before uploading. The CASTs below are no-ops over already-canonical data, kept
     as the explicit declaration that all four engines store the same types. --#}
WITH flights AS (
  SELECT *
  FROM read_parquet(
    getvariable('bts_flight_paths'),
    filename = 1,
    hive_partitioning = false
  )
)

SELECT
  {%- for name in cols %}
  CAST({{ name }} AS {{ bts_flight_type(name, 'duckdb') }}) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('filename') }} AS file
FROM flights
{% else %}
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
