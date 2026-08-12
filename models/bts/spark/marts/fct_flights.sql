-- The wide raw fact — this dataset's MART, the table under layout test. See the duckdb copy's
-- header for why it is a raw fact and why the incremental key is the file.
--
-- Insert-only merge: skip_matched_step drops the WHEN MATCHED branch entirely, so a non-unique key
-- degenerates to exactly "insert the rows of files not already present" and cannot raise a
-- multiple-source-row match error, because there is no matched clause. Requires file_format='delta'.
--
-- ONE READ SHAPE FOR BOTH BRANCHES, exactly as fct_trips: parquet is self-describing, so
-- `parquet.`path`` needs no schema, and a path scan is not a catalog object, so the persistent
-- __dbt_tmp view may reference it. Only the PATH differs between the branches: an explicit brace
-- glob of this run's new months, or the bare folder on a first/full-refresh build where everything
-- is new anyway.
--
-- The archive is homogeneous by construction — download_bts_flights.py rewrites every month's CSV
-- to the canonical 22-column parquet schema before uploading. That normalisation exists FOR THIS
-- LEG above all: Spark refuses a parquet scan whose files disagree on a column's type, and BTS's
-- CSVs would otherwise be read with whatever types each month's inference produced. The CASTs
-- below are no-ops over already-canonical data, kept as the explicit declaration that all four
-- engines store the same types.
{%- set cols = bts_flight_columns() -%}
{%- set flights_root = get_parquet_archive_path() ~ '/flights' -%}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key=['file'],
    skip_matched_step=true,
    schema='mart'
) }}

-- depends_on: {{ ref('stg_flights_archive_log') }}

{% set new_files = spark_new_parquet_files('flights', this, log_model='stg_flights_archive_log') if is_incremental() else [] %}
{#-- Plain (non-trimming) tags: {%- -%} here would eat the newline that ends the depends_on
     comment above and glue the next keyword onto it, commenting the SELECT out. --#}
{% if is_incremental() and new_files | length == 0 %}
{#-- No new months this run: compile to a zero-row no-op (the merge source is empty). --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{% else %}
SELECT
  {%- for name in cols %}
  CAST(t.{{ name }} AS {{ bts_flight_type(name, 'fabricspark') }}) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('t._metadata.file_name') }} AS file
FROM parquet.`{{ flights_root }}{% if is_incremental() %}/{{ '{' ~ new_files | join(',') ~ '}' }}{% endif %}` AS t
{% endif %}
