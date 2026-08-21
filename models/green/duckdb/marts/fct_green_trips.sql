-- The wide raw fact, and the table under layout test on this dataset — stats.py's MART for
-- DATASET=green, the way mart.fct_trips is for nyc.
--
-- WHY THIS DATASET EXISTS. The yellow pair measured V-Order reordering the most repetitive column
-- 3,371x and shrinking the table 36%; the counter-claim under test here is that on GREEN taxi the
-- same profile produces BIGGER data. Green is the same surface — 20 columns, RatecodeID /
-- store_and_fwd_flag / payment_type / VendorID at 97-99% single-value, the LocationIDs Zipfian,
-- plus trip_type (~98% street-hail) and ehail_fee (~all NULL) — on a far smaller table, so it
-- separates "the surface" from "the row count" one more time.
--
-- THIS MODEL WRITES WITH `append` FOR EXACTLY THE REASONS THE YELLOW fct_trips DOES — read that
-- model's header for the full chain. The short form: TLC trip records have NO natural unique key
-- (duplicate trips are documented source behaviour), the only candidate key is the FILE and the
-- source is not unique on it, duckrun refuses a unique_key its source is not unique on across BOTH
-- write paths, a surrogate row id is impossible on Fabric Warehouse, and delete+insert on duckrun
-- is a fenced full-table overwrite. The pre_hook's file list is the selection guard, the teardown
-- bounds the race window, and assert_fct_green_trips_matches_archive_log is the detector.
--
-- No incremental_predicates: they narrow a keyed write's target read, and there is no keyed write
-- left to narrow. `append` never reads the target at all.
{%- set pending_files_query -%}
SELECT file_stem FROM {{ ref('stg_green_archive_log') }}
WHERE source_type = 'green'
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

{%- set cols = green_trip_columns() -%}

{#-- The geometry knobs are the SAME dispatch inputs the AEMO and NYC marts read, so a layout
     question can be asked of any dataset with one workflow. The key this table WANTS is NYC-shaped
     for the same reasons: pickup_date first (every composite query groups or filters THROUGH the
     date relationship, and a date RLEs where the timestamp would not), then PULocationID (the
     widest skewed categorical, and what the selectivity ladder filters on) — a note for reading
     duckrun's choice, not something a dispatch can ask for.

     THE SORT IS `auto` OR NOTHING: the `sort_by` input taking a literal column list is gone, one
     field naming one key being unable to serve five marts. duckrun's picker profiles this table
     itself, per dataset. --#}
-- pickup_date IS A STORED COLUMN AND IT IS NOT THE month_key MISTAKE. Direct Lake cannot relate a
-- DATETIME column to a DATE dimension key, and it has no calculated columns to bridge one, so a
-- date dimension is only reachable if the fact carries a DATE. This one is read by the
-- relationship every date-grouped query in the suite traverses, and it is one narrow column whose
-- values are near-contiguous under the default sort, so it costs little and RLEs well.
{#-- `auto` is duckrun's own picker: it profiles the data and chooses the key, and it is the
     dispatch default. It must be passed as a SCALAR — duckrun raises on `['auto']`, because a list
     means "these columns are the key". Any other value is a comma-separated column list, and blank
     means no sort at all.

     The comment sits ABOVE the tag, not inside it: dbt parses `config(...)` as an expression, and
     a Jinja comment between its arguments is `invalid syntax for function call expression` — an
     error that points at the `{{ config(` line and says nothing about comments. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    sort_by=('auto' if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
    max_row_group_size=(none if env_var('DUCKDB_ROW_GROUP_SIZE', 'auto').lower() == 'auto'
                        else env_var('DUCKDB_ROW_GROUP_SIZE', 'auto') | int),
    target_file_size_mb=(none if env_var('DUCKDB_FILE_SIZE_MB', 'auto').lower() == 'auto'
                         else env_var('DUCKDB_FILE_SIZE_MB', 'auto') | int),
    pre_hook="SET VARIABLE green_paths = (SELECT COALESCE(NULLIF(list('{{ get_parquet_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_green_archive_log') }} WHERE source_type = 'green'{% if is_incremental() %} AND file_stem NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} ORDER BY archive_path))"
) }}

{% if has_files %}
{#-- A plain read, with no union_by_name and no schema merging, because the archive is HOMOGENEOUS
     by construction: download_green_taxi.py rewrites every month to the canonical 20-column schema
     before uploading it. That normalisation exists for Spark, which refuses a scan whose files
     disagree on a column's type and which TLC's own files do — but the benefit lands here too, as
     one plain statement. The CASTs below are therefore no-ops over already-canonical data, kept as
     the explicit declaration that all four engines store the same types. --#}
WITH trips AS (
  SELECT *
  FROM read_parquet(
    getvariable('green_paths'),
    filename = 1,
    hive_partitioning = false
  )
)

SELECT
  {%- for name in cols %}
  CAST({{ name }} AS {{ green_trip_type(name, 'duckdb') }}) AS {{ name }},
  {%- endfor %}
  CAST(lpep_pickup_datetime AS DATE) AS pickup_date,
  {{ parse_filename('filename') }} AS file
FROM trips
{% else %}
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
