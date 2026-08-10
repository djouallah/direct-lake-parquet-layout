-- Date dimension over the yellow-taxi archive's span. Spark builds the range with
-- sequence()+explode() (the DuckDB generate_series() equivalent). Insert-only merge on date, with
-- the NOT IN filter below kept so the merge source stays empty on a steady-state run.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key='date',
    skip_matched_step=true
) }}

SELECT
  CAST(d AS DATE) AS date,
  CAST(YEAR(d) AS INT) AS year,
  CAST(MONTH(d) AS INT) AS month,
  -- Spark's dayofweek() is 1=Sunday; DuckDB's EXTRACT(dow) is 0=Sunday and the dwh copy normalises
  -- to the same. All three engines must STORE the same value — a dimension whose values differ per
  -- engine would break the parity table for a reason that has nothing to do with the writer.
  CAST(dayofweek(d) - 1 AS INT) AS day_of_week
FROM (
  SELECT explode(sequence(to_date('2011-01-01'), to_date('2026-12-31'), interval 1 day)) AS d
)
{% if is_incremental() %}
WHERE CAST(d AS DATE) NOT IN (SELECT date FROM {{ this }})
{% endif %}
