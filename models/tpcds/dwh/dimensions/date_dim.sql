-- TPC-DS `date_dim` -- the date dimension, keyed on the SHIFTED key.
--
-- A PASS-THROUGH. The white paper's section 4.5 customisation happens at LAND time, in
-- download_tpcds.py, so the parquet under parquet_raw/date_dim/ already IS the paper's table: the facts
-- have had every any-null row dropped and carry `cache_buster`, and date_dim carries `d_date_sk_1`
-- and only the 2021-2026 rows. This model selects the columns and nothing else -- no CAST, no
-- derived column, no filter. dsdgen emits one canonical schema at every scale factor, so unlike
-- nyc/green/cms there is no drift to normalise and no source pathology to guard against.
--
-- Columns come from macros/tpcds_columns.sql so all four engines store the same columns in the same
-- order. `.github/scripts/test_tpcds_columns.py` pins that list against the generator's.
--
-- THE KEY IS `d_date_sk_1`, NOT `d_date_sk`, and the whole date side of the model turns on it. The
-- paper adds d_date_sk_1 = d_date_sk - 8401 days so the 1998-2003 sales keys land on 2021-2026
-- dates, replaces d_date_sk as the primary key, and drops every row outside that window -- 2,191
-- remain. Both facts' *_sold_date_sk join THIS column. d_date_sk is still carried, unused, because
-- the paper's table carries it.
--
-- A plain CTAS. dbt-fabric's `table` materialization builds an intermediate relation and renames
-- it, so a leading comment block is safe here -- unlike a dwh VIEW, which dbt-fabric wraps in
-- EXEC('create view ... as <sql>') where the same comment would swallow the SELECT.
{%- set cols = tpcds_columns('date_dim') -%}
{{ config(materialized='table', schema='mart') }}

SELECT
{%- for name in cols %}
  [{{ name }}]{{ "," if not loop.last }}
{%- endfor %}
FROM {{ openrowset_parquet(get_parquet_archive_path() ~ '/date_dim/*.parquet') }} AS src
