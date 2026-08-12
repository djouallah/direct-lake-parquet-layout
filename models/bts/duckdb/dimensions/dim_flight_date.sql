-- Date dimension over the on-time archive's span. Named dim_flight_date, not dim_date or
-- dim_calendar: a model NAME may be patched by exactly one yml file project-wide (dbt raises
-- DuplicatePatchPathError otherwise, and it resolves patches by name regardless of which dataset
-- is gated on), so the three datasets' dimensions must not collide. See models/bts/_dimensions.yml.
--
-- Same shape as nyc's dim_date — date, year, month, day_of_week — so the DAX suite's date tier
-- reads the same way across datasets. day_of_week here is the dimension's own 0=Sunday value,
-- derived from the date identically in all three dialects; the FACT also carries BTS's published
-- DayOfWeek, which is 1=Monday..7=Sunday and is stored verbatim as source data, not derived.
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
    CAST('1987-01-01' AS DATE),
    CAST('2026-12-31' AS DATE),
    INTERVAL 1 DAY
  )) as date
)
{% if is_incremental() %}
WHERE date NOT IN (SELECT date FROM {{ this }})
{% endif %}
