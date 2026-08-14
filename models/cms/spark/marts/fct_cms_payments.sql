-- The wide raw fact — this dataset's MART, the table under layout test. See the duckdb copy's
-- header for why 91 columns, for the sparsity and dual skew regime this dataset was added to
-- measure, and for why the incremental key is (file, Record_ID) rather than the file alone.
--
-- Insert-only merge: skip_matched_step drops the WHEN MATCHED branch entirely. On green that was
-- load-bearing, because a non-unique key would otherwise raise a multiple-source-row match error;
-- here the key IS unique, so it is simply the cheapest correct spelling — there is no matched work
-- worth doing on append-only data. Requires file_format='delta'.
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
-- The archive is homogeneous by construction — download_cms_payments.py rewrites every program year
-- to the canonical 91-column schema before uploading. That normalisation exists FOR THIS LEG on the
-- taxi datasets, because Spark refuses a parquet scan whose files disagree on a column's type; here
-- the source never disagreed in the first place (the CSV header is byte-identical across
-- PY2019-2025), so the CASTs below are no-ops twice over and are kept as the explicit declaration
-- that all four engines store the same types.
{%- set cols = cms_payment_columns() -%}
{%- set cms_root = get_parquet_archive_path() ~ '/cms' -%}

-- NO DERIVED DATE COLUMN, unlike nyc's and green's pickup_date: Date_of_Payment is already a DATE
-- in the source, so the date dimension's join key ships in the file, the same way bts uses
-- FlightDate.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key=['file', 'Record_ID'],
    skip_matched_step=true,
    schema='mart'
) }}

-- depends_on: {{ ref('stg_cms_archive_log') }}

{% set new_files = spark_new_parquet_files('cms', this, log_model='stg_cms_archive_log') if is_incremental() else [] %}
{#-- Plain (non-trimming) tags: {%- -%} here would eat the newline that ends the depends_on
     comment above and glue the next keyword onto it, commenting the SELECT out. --#}
{% if is_incremental() and new_files | length == 0 %}
{#-- No new months this run: compile to a zero-row no-op (the merge source is empty). --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{% else %}
SELECT
  {%- for name in cols %}
  CAST(t.{{ name }} AS {{ cms_payment_type(name, 'fabricspark') }}) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('t._metadata.file_name') }} AS file
FROM parquet.`{{ cms_root }}{% if is_incremental() %}/{{ '{' ~ new_files | join(',') ~ '}' }}{% endif %}` AS t
{% endif %}
