-- Every landed month appears in fct_cms_payments exactly as many times as the archive log said,
-- and no month is missing.
--
-- What it catches:
--   a month inserted TWICE   -> stored count is a multiple of the logged one (a re-dispatch, or a
--                               `dbt retry` racing a scheduled run -- the race dwh is most exposed
--                               to, since Fabric Warehouse has no commit check)
--   a month PARTIALLY written -> stored count below the logged one (a write that died mid-file)
--   a month landed and never built -> present in the log, absent from the fact
--
-- THIS DATASET IS BETTER DEFENDED THAN nyc AND green, and this test is the weaker of its two
-- guards rather than the only one. CMS publishes Record_ID, so (file, Record_ID) is a genuine
-- unique key and the write is a real keyed merge on all four engines: a doubled month is caught at
-- WRITE time, not here. What is left for this test is the write that died halfway and the month
-- that landed but was never folded in.
--
-- WHAT THIS TEST DELIBERATELY DOES NOT DO: reconcile the months of a program year against the
-- source CSV they were split out of. download_cms_payments.py writes BOTH sides of that split — it
-- fetches one annual CSV and partitions it into ~12 monthly parquet files — so an assertion here
-- would be comparing the downloader against itself and would pass by construction. That check
-- lives in land_year() instead, where it can compare the partitions against a count of the source
-- CSV, and it refuses to log anything for a year that does not reconcile. Free, on a runner,
-- before any Fabric capacity is spent.
--
-- IT READS TWO TABLES, and that is a knowing exception to this project's "no test reads more than
-- the one table it is about" rule. The rule exists so a test cannot go red because the SOURCE
-- changed shape; the archive log is not a source in that sense, it is the manifest of what this
-- pipeline itself landed, written at land time and immutable afterwards.
--
-- Full table, no window and not tagged `heavy` -- same reasoning as the AEMO grain test: a window
-- would encode an assumption about where a bad write lives, which is the knowledge the test is
-- meant to be free of.
--
-- DuckDB dialect (duckrun + iceberg). tests/cms/dwh and tests/cms/spark hold the same assertion in
-- their own dialects; `data_tests` in dbt_project.yml enables exactly one folder per
-- (dataset, target). Keep the three in step.

WITH stored AS (
  SELECT file, COUNT(*) AS n
  FROM {{ ref('fct_cms_payments') }}
  GROUP BY file
),
landed AS (
  SELECT file_stem, row_count
  FROM {{ ref('stg_cms_archive_log') }}
  WHERE source_type = 'cms'
)
SELECT
  landed.file_stem,
  landed.row_count AS logged_rows,
  stored.n AS stored_rows
FROM landed
LEFT JOIN stored ON stored.file = landed.file_stem
WHERE stored.n IS NULL OR stored.n <> landed.row_count
