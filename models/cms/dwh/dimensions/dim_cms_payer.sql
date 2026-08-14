{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['[payer_id]'],
    on_schema_change='sync_all_columns'
) }}

{#-- NOTE: do not add a leading `-- {{ ref(...) }}` dependency comment here. dbt-fabric wraps a
     model in EXEC('create view ... as <sql>'); the newline after such a line collapses and comments
     out the SELECT, producing "Incorrect syntax near ...". The downloader runs as a separate step
     before dbt anyway, so no ref dependency is needed. --#}

{#-- The manufacturers and GPOs that make the payments (~1,000 live ids), from the lookup
     download_cms_payments.py DERIVES under parquet_raw/payer/ — CMS publishes no such file. See
     the duckdb copy's header for why it is built at land time and why the columns are renamed.

     merge on payer_id keeps the dimension current as manufacturers are renamed between program
     years; the source is ~1,000 rows, so rebuilding and upserting every run costs nothing.

     ⚠️ THIS IS THE ENGINE THE WHITESPACE ASSERTION EXISTS FOR. payer_id is a STRING join key and
     T-SQL pads on comparison, so a trailing space here matches in dwh and nowhere else. The declared
     VARCHAR(64) is measured, not guessed: the widest observed id is 12 characters over a 100 MB
     sample of PY2023. --#}

SELECT
  CAST([Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID] AS VARCHAR(64)) AS [payer_id],
  CAST([Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name] AS VARCHAR(256)) AS [payer_name],
  CAST([Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State] AS VARCHAR(32)) AS [payer_state],
  CAST([Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country] AS VARCHAR(64)) AS [payer_country]
FROM {{ openrowset_parquet(get_parquet_archive_path() ~ '/payer/cms_payers.parquet') }} AS p
