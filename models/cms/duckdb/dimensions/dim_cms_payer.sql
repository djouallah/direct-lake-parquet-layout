-- The manufacturers and GPOs that make the payments (~1,000 live ids), from the lookup
-- download_cms_payments.py derives under parquet_raw/payer/.
--
-- DERIVED, NOT DOWNLOADED, and that is the one thing this differs from dim_green_zone in. CMS
-- publishes no manufacturer lookup file, so land_payers() builds it from the landed archive: one
-- row per payer id, newest program year winning, because manufacturers are RENAMED between years
-- and a plain DISTINCT over the four columns yields the same id twice with two names — which the
-- `unique` test below would then fail. It is built at land time rather than as a dbt model
-- because a model would have to scan the whole 88M-row fact to produce ~1,000 rows, on every
-- engine, every run.
--
-- Columns are renamed from CMS's 59-character source spellings, which is a departure from every
-- other dimension here and is deliberate: dim_green_zone mirrors a lookup TLC actually publishes,
-- so it keeps TLC's names, while this table is one this project constructs and has no source
-- spelling to be faithful to. The FACT keeps all 91 source names untouched.
--
-- ⚠️ payer_id IS A STRING JOIN KEY, so this dataset DOES need the whitespace assertion that nyc
-- and green do not. T-SQL pads on comparison ('X' = 'X ' is TRUE) where DuckDB and Spark do not,
-- so one trailing space puts a payer in dwh's join result and in no other engine's — the DUID
-- incident exactly. tests/cms/{duckdb,dwh,spark}/assert_payer_id_has_no_whitespace.sql is the
-- guard and all three copies are mandatory.
{{ config(
    materialized='incremental',
    unique_key='payer_id',
    incremental_strategy='delete+insert'
) }}

SELECT
  CAST("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID" AS VARCHAR) AS payer_id,
  CAST("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name" AS VARCHAR) AS payer_name,
  CAST("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State" AS VARCHAR) AS payer_state,
  CAST("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country" AS VARCHAR) AS payer_country
FROM read_parquet('{{ get_parquet_archive_path() }}/payer/cms_payers.parquet')
