-- TPC-DS `store` -- the store dimension.
--
-- A PASS-THROUGH. The white paper's section 4.5 customisation happens at LAND time, in
-- download_tpcds.py, so the parquet under parquet_raw/store/ already IS the paper's table: the facts
-- have had every any-null row dropped and carry `cache_buster`, and date_dim carries `d_date_sk_1`
-- and only the 2021-2026 rows. This model selects the columns and nothing else -- no CAST, no
-- derived column, no filter. dsdgen emits one canonical schema at every scale factor, so unlike
-- nyc/green/cms there is no drift to normalise and no source pathology to guard against.
--
-- Columns come from macros/tpcds_columns.sql so all four engines store the same columns in the same
-- order. `.github/scripts/test_tpcds_columns.py` pins that list against the generator's.
{%- set cols = tpcds_columns('store') -%}
--
-- `incremental` rather than `table` for the reason every model in this tree is: the OneLake
-- Iceberg REST catalog cannot do the table materialization's temp-table RENAME, but it can do
-- CREATE TABLE AS + INSERT. delete+insert on the key, so a by-hand re-run replaces rather than
-- duplicates; under the per-run teardown the first-build branch is the one that runs.
{{ config(
    materialized='incremental',
    unique_key='s_store_sk',
    incremental_strategy='delete+insert'
) }}

SELECT
{%- for name in cols %}
  {{ name }}{{ "," if not loop.last }}
{%- endfor %}
FROM read_parquet('{{ get_parquet_archive_path() }}/store/*.parquet')
