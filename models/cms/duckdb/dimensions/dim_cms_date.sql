-- Date dimension over the Open Payments archive's span. Named dim_cms_date, not dim_date: a model
-- NAME may be patched by exactly one yml file project-wide (dbt raises DuplicatePatchPathError
-- otherwise, and it resolves patches by name regardless of which dataset is gated on), so the
-- datasets' dimensions must not collide. See models/cms/_dimensions.yml.
--
-- The span is 2013-01-01..2027-12-31 even though the catalog only serves PY2019 onward. Two
-- reasons, and both are the same trade nyc and green already accept: CMS payments carry dates
-- outside their own program year (a documented source condition, which is why the downloader
-- buckets by the ACTUAL Date_of_Payment rather than clamping), and the mart's date relationship
-- asserts referential integrity — so a narrowed span would inner-join those rows away in DAX.
-- 2013 is where the programme itself starts, so it is the floor even if PY2013-2018 are added
-- later from CMS's archived downloads.
--
-- day_of_week is here and hour is NOT. Payments carry no time of day at all — Date_of_Payment is a
-- DATE in the source — so there is nothing an hour column could be derived from.
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
    CAST('2013-01-01' AS DATE),
    CAST('2027-12-31' AS DATE),
    INTERVAL 1 DAY
  )) as date
)
{% if is_incremental() %}
WHERE date NOT IN (SELECT date FROM {{ this }})
{% endif %}
