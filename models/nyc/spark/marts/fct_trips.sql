-- The wide raw fact — this dataset's MART, the table under layout test. See the duckdb copy's
-- header for why it is a raw fact and not an aggregate, and why the incremental key is the file.
--
-- Insert-only merge: skip_matched_step drops the WHEN MATCHED branch entirely, so a non-unique key
-- degenerates to exactly "insert the rows of files not already present" and cannot raise a
-- multiple-source-row match error, because there is no matched clause to raise it. Requires
-- file_format='delta'.
--
-- ONE READ SHAPE FOR BOTH BRANCHES, unlike the AEMO facts. Those carry two, because a CSV read with
-- an explicit schema is only reachable from the bare CTAS path: the incremental path builds a
-- PERSISTENT __dbt_tmp view, which may not reference a TEMPORARY VIEW, and Fabric Spark rejects an
-- external CSV table with an explicit schema. None of that applies to parquet — it is
-- self-describing, so `parquet.`path`` needs no schema, and a path scan is not a catalog object, so
-- the persistent tmp view may reference it. Only the PATH differs between the branches: an explicit
-- brace glob of this run's new months, or the bare folder on a first/full-refresh build where
-- everything is new anyway.
--
-- The archive is homogeneous by construction — download_nyc_taxi.py rewrites every month to the
-- canonical 17-column schema before uploading. That normalisation exists FOR THIS LEG: Spark
-- refuses a parquet scan whose files disagree on a column's type ("Failed to merge incompatible
-- data types"), with or without mergeSchema, and TLC's own files disagree — passenger_count and
-- RatecodeID ship as int64 in some years and double in others. The CASTs below are therefore
-- no-ops over already-canonical data, kept as the explicit declaration that all four engines store
-- the same types.
{%- set cols = nyc_trip_columns() -%}
{%- set yellow_root = get_parquet_archive_path() ~ '/yellow' -%}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key=['file'],
    skip_matched_step=true,
    schema='mart'
) }}

-- depends_on: {{ ref('stg_parquet_archive_log') }}

{% set new_files = spark_new_parquet_files('yellow', this) if is_incremental() else [] %}
{#-- Plain (non-trimming) tags: {%- -%} here would eat the newline that ends the depends_on
     comment above and glue the next keyword onto it, commenting the SELECT out. --#}
{% if is_incremental() and new_files | length == 0 %}
{#-- No new months this run: compile to a zero-row no-op (the merge source is empty). --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{% else %}
SELECT
  {%- for name in cols %}
  CAST(t.{{ name }} AS {{ nyc_trip_type(name, 'fabricspark') }}) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('t._metadata.file_name') }} AS file
FROM parquet.`{{ yellow_root }}{% if is_incremental() %}/{{ '{' ~ new_files | join(',') ~ '}' }}{% endif %}` AS t
{% endif %}
