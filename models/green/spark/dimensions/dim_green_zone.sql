-- The 265 TLC taxi zones, from the lookup the downloader lands under parquet_raw/zone/.
--
-- PARQUET, not the CSV TLC serves: `csv.`path`` defaults header=false in Spark and would give
-- _c0.._c3, and the two routes around that are both closed here (an external CSV table with an
-- explicit schema is rejected by Fabric Spark, and the from_csv-over-text idiom the AEMO models use
-- costs an explicit schema plus a header-row filter for a 265-row dimension). The downloader
-- converts once so all three dialects read it with one plain statement — see land_zones().
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key='LocationID',
    skip_matched_step=true
) }}

SELECT
  CAST(LocationID AS INT) AS LocationID,
  CAST(Borough AS STRING) AS Borough,
  CAST(Zone AS STRING) AS Zone,
  CAST(service_zone AS STRING) AS service_zone
FROM parquet.`{{ get_parquet_archive_path() }}/zone/taxi_zone_lookup.parquet`
{% if is_incremental() %}
WHERE CAST(LocationID AS INT) NOT IN (SELECT LocationID FROM {{ this }})
{% endif %}
