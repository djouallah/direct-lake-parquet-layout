-- BTS's unique-carrier lookup, from the parquet the downloader lands under parquet_raw/carrier/.
--
-- PARQUET, not the CSV BTS serves: `csv.`path`` defaults header=false in Spark and would give
-- _c0.._c1, and the two routes around that are both closed here (an external CSV table with an
-- explicit schema is rejected by Fabric Spark, and the from_csv-over-text idiom costs an explicit
-- schema plus a header-row filter for a small dimension). The downloader converts once so all
-- three dialects read it with one plain statement — see land_carriers().
--
-- code is a STRING join key; the whitespace assertion covers it in all three dialects.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key='code',
    skip_matched_step=true
) }}

SELECT
  CAST(code AS STRING) AS code,
  CAST(name AS STRING) AS name
FROM parquet.`{{ get_parquet_archive_path() }}/carrier/carrier_lookup.parquet`
{% if is_incremental() %}
WHERE CAST(code AS STRING) NOT IN (SELECT code FROM {{ this }})
{% endif %}
