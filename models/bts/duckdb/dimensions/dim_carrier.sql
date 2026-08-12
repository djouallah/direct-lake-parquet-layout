-- BTS's unique-carrier lookup (Code -> Description, landed as code/name), from the parquet
-- download_bts_flights.py lands under parquet_raw/carrier/. Small and slowly-changing, so it is
-- rebuilt whole rather than merged — delete+insert on the key matches dim_flight_date.
--
-- code is a STRING join key — the first one since AEMO's DUID — so this dataset carries the
-- whitespace assertion in all three dialects (tests/bts/*/assert_carrier_code_has_no_whitespace):
-- T-SQL pads on comparison, DuckDB and Spark do not, and a padded key splits the engines silently.
{{ config(
    materialized='incremental',
    unique_key='code',
    incremental_strategy='delete+insert'
) }}

SELECT
  CAST(code AS VARCHAR) AS code,
  CAST(name AS VARCHAR) AS name
FROM read_parquet('{{ get_parquet_archive_path() }}/carrier/carrier_lookup.parquet')
