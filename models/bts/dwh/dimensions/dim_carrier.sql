{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['[code]'],
    on_schema_change='sync_all_columns'
) }}

{#-- NOTE: do not add a leading `-- {{ ref(...) }}` dependency comment here. dbt-fabric wraps a
     model in EXEC('create view ... as <sql>'); the newline after such a line collapses and
     comments out the SELECT. The downloader runs as a separate step before dbt anyway. --#}

{#-- BTS's unique-carrier lookup, from the parquet the downloader lands under parquet_raw/carrier/.
     Parquet rather than the CSV BTS serves — converted once at land time so all three dialects
     read it with one plain statement; see the spark copy's header for whose limitation that is.

     merge on code keeps the dimension current as carriers are added; the source is small, so
     upserting every run costs nothing.

     code is a STRING join key — the exact shape of the DUID incident: T-SQL pads on comparison
     ('AA' = 'AA ' is TRUE here and FALSE on the other engines), so a padded code would join on
     this engine alone and split the parity table silently.
     tests/bts/dwh/assert_carrier_code_has_no_whitespace.sql is the load-bearing copy of the
     guard. --#}

SELECT
  CAST([code] AS VARCHAR(8)) AS [code],
  CAST([name] AS VARCHAR(256)) AS [name]
FROM {{ openrowset_parquet(get_parquet_archive_path() ~ '/carrier/carrier_lookup.parquet') }} AS c
