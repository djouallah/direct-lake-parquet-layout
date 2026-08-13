-- Date dimension over the green-taxi archive's span. Named dim_green_date, not dim_date: a model
-- NAME may be patched by exactly one yml file project-wide (dbt raises DuplicatePatchPathError
-- otherwise, and it resolves patches by name regardless of which dataset is gated on), so the
-- datasets' dimensions must not collide. See models/green/_dimensions.yml.
--
-- The span stays 2011-01-01..2026-12-31 even though the archive starts 2014-01: green monthly
-- files carry misfiled pickup timestamps outside their own month, and the mart's date
-- relationship asserts referential integrity — narrowing the span would inner-join those rows
-- away in DAX. Same trade the yellow dataset already accepts.
--
-- day_of_week is here and hour is NOT. Time-of-day grouping belongs to the fact's own pickup
-- timestamp, and materialising an hour column to save a DAX expression is the month_key mistake —
-- a stored column nothing needs, in a benchmark whose subject is write cost.
{{ config(
    materialized='incremental',
    unique_key='date',
    incremental_strategy='delete+insert'
) }}

SELECT
  CAST(date AS DATE) as date,
  CAST(EXTRACT(year FROM date) AS INT) as year,
  CAST(EXTRACT(month FROM date) AS INT) as month,
  CAST(EXTRACT(dow FROM date) AS INT) as day_of_week
FROM (
  SELECT unnest(generate_series(
    CAST('2011-01-01' AS DATE),
    CAST('2026-12-31' AS DATE),
    INTERVAL 1 DAY
  )) as date
)
{% if is_incremental() %}
WHERE date NOT IN (SELECT date FROM {{ this }})
{% endif %}
