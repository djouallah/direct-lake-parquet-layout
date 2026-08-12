-- Every landed month appears in fct_flights exactly as many times as its parquet footer said, and
-- no month is missing. Spark SQL dialect (Fabric Spark). Same assertion as
-- tests/bts/duckdb/assert_fct_flights_matches_archive_log.sql — read that one for why it takes
-- this shape rather than a grain check, and why it is allowed to read two tables.
--
-- Spark's write here is insert-only (merge with skip_matched_step), so a file already present
-- cannot be inserted again by a well-behaved run; what this catches on this engine is a Livy
-- session that died mid-file, and a month that landed but was never folded in.

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
