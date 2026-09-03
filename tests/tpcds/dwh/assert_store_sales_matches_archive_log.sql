-- store_sales holds exactly the rows download_tpcds.py logged for it, and no fewer.
--
-- What it catches: a fact written twice (stored is a multiple of logged), a write that died partway
-- (stored is short), and a landing that never reached this engine at all (stored is zero while the
-- log has rows).
--
-- A TOTAL, NOT A PER-FILE COUNT, and that is the one way this differs from every other dataset's
-- copy of this test. The others compare each month against its own log row through the fact's
-- `file` column; this fact has no `file` column, because dsdgen emits a whole scale factor at once
-- and there is nothing to increment along. So the assertion is the sum over this table's log rows,
-- which still separates all three failures above from a healthy build.
--
-- IT READS TWO TABLES, a knowing exception to this project's one-table rule. The rule exists so a
-- test cannot go red because a SOURCE changed shape; the archive log is not a source in that sense,
-- it is the manifest this pipeline itself wrote at land time.
--
-- Full table, no window, not tagged `heavy` -- same reasoning as the AEMO grain test: a window would
-- encode an assumption about where a bad write lives, which is the knowledge the test exists to be
-- free of.
--
-- T-SQL (Fabric Warehouse) dialect. The other two dialect folders hold the same assertion in their own SQL and
-- `data_tests` in dbt_project.yml enables exactly one folder per (dataset, target). Keep the three
-- in step -- nothing reports which engine skipped what.

WITH stored AS (
  SELECT COUNT(*) AS n FROM {{ ref('store_sales') }}
),
landed AS (
  SELECT SUM([row_count]) AS n
  FROM {{ ref('stg_tpcds_archive_log') }}
  WHERE [source_type] = 'store_sales'
)
SELECT
  landed.n AS logged_rows,
  stored.n AS stored_rows
FROM landed CROSS JOIN stored
WHERE landed.n IS NULL OR stored.n <> landed.n
