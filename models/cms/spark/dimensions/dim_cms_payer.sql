-- The manufacturers and GPOs that make the payments (~1,000 live ids), from the lookup
-- download_cms_payments.py DERIVES under parquet_raw/payer/ -- CMS publishes no such file. See the
-- duckdb copy's header for why it is built at land time, why the columns are renamed, and why
-- payer_id being a STRING join key means this dataset carries a whitespace assertion in all three
-- dialects where nyc and green carry none.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key='payer_id',
    skip_matched_step=true
) }}

SELECT
  CAST(Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID AS STRING) AS payer_id,
  CAST(Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name AS STRING) AS payer_name,
  CAST(Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State AS STRING) AS payer_state,
  CAST(Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country AS STRING) AS payer_country
FROM parquet.`{{ get_parquet_archive_path() }}/payer/cms_payers.parquet`
{% if is_incremental() %}
WHERE CAST(Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID AS STRING)
      NOT IN (SELECT payer_id FROM {{ this }})
{% endif %}
