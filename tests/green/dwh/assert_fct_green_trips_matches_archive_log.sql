-- Every landed month appears in fct_green_trips exactly as many times as its parquet footer said,
-- and no month is missing. T-SQL dialect (Fabric Warehouse). Same assertion as
-- tests/green/duckdb/assert_fct_green_trips_matches_archive_log.sql — read that one for why it
-- takes this shape rather than a grain check, and why it is allowed to read two tables.
--
-- THIS IS THE COPY THAT MATTERS MOST, for the same reason the AEMO grain test's dwh copy does.
-- Fabric Warehouse is the one engine whose write path can genuinely duplicate: duckrun, iceberg and
-- spark check the commit and fail loudly on a real overlap, while under snapshot isolation two dwh
-- transactions overlapping in time can both write. And this fact is the one place dwh does not even
-- have a keyed merge to shrink that window with — dbt-fabric always emits WHEN MATCHED THEN UPDATE,
-- which a many-to-many match on [file] cannot survive, so the model uses delete+insert on [file]
-- instead. delete+insert is atomic per statement but two overlapping runs can still interleave, and
-- a doubled month is exactly what this reports.
--
-- Dialect differences from the DuckDB copy, both mandatory:
--   * [file] is a reserved word, so every reference is bracketed — the dwh model writes it under
--     exactly that name (unique_key=['[file]']);
--   * no GROUP BY ALL in T-SQL, so the key column is spelled out.

WITH stored AS (
  SELECT [file], COUNT(*) AS n
  FROM {{ ref('fct_green_trips') }}
  GROUP BY [file]
),
landed AS (
  SELECT [file_stem], [row_count]
  FROM {{ ref('stg_green_archive_log') }}
  WHERE [source_type] = 'green'
)
SELECT
  landed.[file_stem],
  landed.[row_count] AS logged_rows,
  stored.n AS stored_rows
FROM landed
LEFT JOIN stored ON stored.[file] = landed.[file_stem]
WHERE stored.n IS NULL OR stored.n <> landed.[row_count]
