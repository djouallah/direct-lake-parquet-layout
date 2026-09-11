-- TPC-DS `customer_address` -- the customer-address dimension.
--
-- A PASS-THROUGH. The white paper's section 4.5 customisation happens at LAND time, in
-- download_tpcds.py, so the parquet under parquet_raw/customer_address/ already IS the paper's table: the facts
-- have had every any-null row dropped and carry `cache_buster`, and date_dim carries `d_date_sk_1`
-- and only the 2021-2026 rows. This model selects the columns and nothing else -- no CAST, no
-- derived column, no filter. dsdgen emits one canonical schema at every scale factor, so unlike
-- nyc/green/cms there is no drift to normalise and no source pathology to guard against.
--
-- Columns come from macros/tpcds_columns.sql so all four engines store the same columns in the same
-- order. `.github/scripts/test_tpcds_columns.py` pins that list against the generator's.
--
-- A plain CTAS. dbt-fabric's `table` materialization builds an intermediate relation and renames
-- it, so a leading comment block is safe here -- unlike a dwh VIEW, which dbt-fabric wraps in
-- EXEC('create view ... as <sql>') where the same comment would swallow the SELECT.
{%- set cols = tpcds_columns('customer_address') -%}
{{ config(materialized='table', schema='mart') }}

SELECT
{%- for name in cols %}
  [{{ name }}]{{ "," if not loop.last }}
{%- endfor %}
FROM {{ openrowset_parquet(get_parquet_archive_path() ~ '/customer_address/*.parquet') }} AS src
