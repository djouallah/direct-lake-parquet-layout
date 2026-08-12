-- Every landed month appears in fct_flights exactly as many times as its parquet footer said, and
-- no month is missing. The analogue of nyc's assert_fct_trips_matches_archive_log, for the same
-- reason: BTS publishes no primary key for this data and duplicate rows exist in the source, so
-- there is no grain to assert uniqueness on and the file is the only unit the write can be graded
-- by.
--
-- What it catches, which is exactly what the file-level incremental key exists to prevent:
--   a month inserted TWICE      -> stored count is a multiple of the logged one (a re-dispatch, or
--                                  a `dbt retry` racing a scheduled run — the race dwh is most
--                                  exposed to, since Fabric Warehouse has no commit check)
--   a month PARTIALLY written   -> stored count below the logged one (a write that died mid-file)
--   a month landed, never built -> present in the log, absent from the fact
--
-- IT READS TWO TABLES — the same knowing exception nyc's copy documents: the archive log is not a
-- source that can change shape, it is the manifest of what this pipeline itself landed, written
-- from the parquet footer at land time and immutable afterwards. Without the join there is no
-- assertion available on this table at all.
--
-- Full table, no window, no `heavy` tag — a window would encode an assumption about where a bad
-- write lives, which is the knowledge the test is meant to be free of.
--
-- DuckDB dialect (duckrun + iceberg). tests/bts/dwh and tests/bts/spark hold the same assertion in
-- their own dialects; `data_tests` in dbt_project.yml enables exactly one folder per (dataset,
-- target). Keep the three in step.

WITH stored AS (
  SELECT file, COUNT(*) AS n
  FROM {{ ref('fct_flights') }}
  GROUP BY file
),
landed AS (
  SELECT file_stem, row_count
  FROM {{ ref('stg_flights_archive_log') }}
  WHERE source_type = 'flights'
)
SELECT
  landed.file_stem,
  landed.row_count AS logged_rows,
  stored.n AS stored_rows
FROM landed
LEFT JOIN stored ON stored.file = landed.file_stem
WHERE stored.n IS NULL OR stored.n <> landed.row_count
