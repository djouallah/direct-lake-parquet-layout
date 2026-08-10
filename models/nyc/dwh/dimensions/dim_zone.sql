{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['[LocationID]'],
    on_schema_change='sync_all_columns'
) }}

{#-- NOTE: do not add a leading `-- {{ ref(...) }}` dependency comment here. dbt-fabric wraps a
     model in EXEC('create view ... as <sql>'); the newline after such a line collapses and comments
     out the SELECT, producing "Incorrect syntax near ...". The downloader runs as a separate step
     before dbt anyway, so no ref dependency is needed. --#}

{#-- The 265 TLC taxi zones, from the lookup the downloader lands under parquet_raw/zone/. Parquet
     rather than the CSV TLC serves — it is converted once at land time so all three dialects read
     it with one plain statement; see the spark copy's header for whose limitation that is.
     Parquet is self-describing, so no ordinal WITH block is needed here, unlike the AEMO reports.

     merge on LocationID keeps the dimension current as zones are renamed; the source is 265 rows,
     so rebuilding and upserting every run costs nothing.

     LocationID is an INTEGER key, which is why this dataset carries no whitespace assertion: the
     T-SQL-pads-on-comparison pathology can only bite a STRING join key. --#}

SELECT
  TRY_CAST([LocationID] AS INT) AS [LocationID],
  CAST([Borough] AS VARCHAR(64)) AS [Borough],
  CAST([Zone] AS VARCHAR(128)) AS [Zone],
  CAST([service_zone] AS VARCHAR(32)) AS [service_zone]
FROM {{ openrowset_parquet(get_parquet_archive_path() ~ '/zone/taxi_zone_lookup.parquet') }} AS z
