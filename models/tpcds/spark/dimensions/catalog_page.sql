-- TPC-DS `catalog_page` -- the catalog-page dimension.
--
-- A PASS-THROUGH. The white paper's section 4.5 customisation happens at LAND time, in
-- download_tpcds.py, so the parquet under parquet_raw/catalog_page/ already IS the paper's table: the facts
-- have had every any-null row dropped and carry `cache_buster`, and date_dim carries `d_date_sk_1`
-- and only the 2021-2026 rows. This model selects the columns and nothing else -- no CAST, no
-- derived column, no filter. dsdgen emits one canonical schema at every scale factor, so unlike
-- nyc/green/cms there is no drift to normalise and no source pathology to guard against.
--
-- Columns come from macros/tpcds_columns.sql so all four engines store the same columns in the same
-- order. `.github/scripts/test_tpcds_columns.py` pins that list against the generator's.
--
-- A plain CTAS.
-- `file_format='delta'` makes dbt-fabricspark emit `create or replace table`. `auto_optimize=false`
-- is load-bearing rather than tidy: dbt-fabricspark >= 1.13.0 runs OPTIMIZE after every table build
-- by default, which would rewrite the very files this benchmark measures -- and it downgrades its
-- own failures to warnings, so nothing in the log would say it had happened.
{%- set cols = tpcds_columns('catalog_page') -%}
{{ config(materialized='table', file_format='delta', schema='mart', auto_optimize=false) }}

SELECT
{%- for name in cols %}
  {{ name }}{{ "," if not loop.last }}
{%- endfor %}
FROM parquet.`{{ get_parquet_archive_path() }}/catalog_page`
