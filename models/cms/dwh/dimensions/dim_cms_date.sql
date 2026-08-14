{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['[date]']
) }}

-- One-off, fixed date dimension over the Open Payments archive's span. Built in full on the first
-- run; once the table exists every later run selects nothing (WHERE 1=0), which is dbt's idiom for
-- "create if not exists, otherwise skip". merge rather than append is uniformity with the rest of
-- the project rather than a fix: the source is empty on every incremental run, so the statement
-- matches nothing and costs nothing. The key is a one-element LIST, not a scalar -- a scalar routes
-- through dbt's equals() macro, the list form emits a plain `=` -- and it is bracketed because
-- `date` is reserved. The span stays 2013..2027 even though the catalog starts at PY2019 -- see
-- the duckdb copy's header.
--
-- DuckDB's generate_series(...INTERVAL 1 DAY) has no T-SQL equivalent that survives dbt-fabric's
-- subquery wrapping (OPTION(MAXRECURSION) cannot live in a derived table), so build a 0..9999 tally
-- by cross-joining digit tables and offset from the start date. Identical construction to
-- dim_green_date and the AEMO dim_calendar; only the span differs.
WITH digits(n) AS (
  SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
),
numbers AS (
  SELECT (d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n) AS num
  FROM digits d1 CROSS JOIN digits d2 CROSS JOIN digits d3 CROSS JOIN digits d4
),
calendar AS (
  SELECT DATEADD(DAY, num, CAST('2013-01-01' AS DATE)) AS d
  FROM numbers
  WHERE num <= DATEDIFF(DAY, CAST('2013-01-01' AS DATE), CAST('2027-12-31' AS DATE))
)
SELECT
  CAST(d AS DATE) AS [date],
  CAST(YEAR(d) AS INT) AS [year],
  CAST(MONTH(d) AS INT) AS [month],
  {#-- T-SQL DATEPART(weekday) is 1-based and DATEFIRST-dependent; DuckDB's EXTRACT(dow) and
       Spark's dayofweek()-1 are both 0=Sunday. Normalise to 0=Sunday here so the three engines
       store the SAME value — a dimension whose values differ per engine would break the parity
       table for a reason that has nothing to do with the writer. --#}
  CAST((DATEDIFF(DAY, CAST('1900-01-07' AS DATE), d) % 7) AS INT) AS [day_of_week]
FROM calendar
{% if is_incremental() %}
WHERE 1 = 0
{% endif %}
