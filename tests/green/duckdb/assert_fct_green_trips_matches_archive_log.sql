-- Every landed month appears in fct_green_trips exactly as many times as its parquet footer said,
-- and no month is missing. The ONLY assertion on this table, and the analogue of AEMO's
-- assert_fct_summary_grain -- but it has to be a different shape, because TLC trip records carry no
-- natural unique key (duplicate trips are a documented feature of the source), so there is no grain
-- to assert uniqueness on.
--
-- What it catches, which is exactly what the file-level incremental key exists to prevent:
--   a month inserted TWICE   -> stored count is a multiple of the logged one (a re-dispatch, or a
--                               `dbt retry` racing a scheduled run -- the race dwh is most exposed
--                               to, since Fabric Warehouse has no commit check)
--   a month PARTIALLY written -> stored count below the logged one (a write that died mid-file)
--   a month landed and never built -> present in the log, absent from the fact
--
-- IT READS TWO TABLES, and that is a knowing exception to this project's "no test reads more than
-- the one table it is about" rule. The rule exists so a test cannot go red because the SOURCE
-- changed shape; the archive log is not a source in that sense, it is the manifest of what this
-- pipeline itself landed, written from the parquet footer at land time and immutable afterwards.
-- A month's row count cannot drift the way an AEMO day's contents can. Without this join there is
-- no assertion available on this table at all.
--
-- Full table, no window and not tagged `heavy` -- same reasoning as the AEMO grain test: a window
-- would encode an assumption about where a bad write lives, which is the knowledge the test is
-- meant to be free of.
--
-- DuckDB dialect (duckrun + iceberg). tests/green/dwh and tests/green/spark hold the same
-- assertion in their own dialects; `data_tests` in dbt_project.yml enables exactly one folder per
-- (dataset, target). Keep the three in step.

WITH stored AS (
  SELECT file, COUNT(*) AS n
  FROM {{ ref('fct_green_trips') }}
  GROUP BY file
),
landed AS (
  SELECT file_stem, row_count
  FROM {{ ref('stg_green_archive_log') }}
  WHERE source_type = 'green'
)
SELECT
  landed.file_stem,
  landed.row_count AS logged_rows,
  stored.n AS stored_rows
FROM landed
LEFT JOIN stored ON stored.file = landed.file_stem
WHERE stored.n IS NULL OR stored.n <> landed.row_count
