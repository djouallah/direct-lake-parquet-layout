-- Every landed month appears in fct_flights exactly as many times as its parquet footer said, and
-- no month is missing. T-SQL dialect (Fabric Warehouse). Same assertion as
-- tests/bts/duckdb/assert_fct_flights_matches_archive_log.sql — read that one for why it takes
-- this shape rather than a grain check, and why it is allowed to read two tables.
--
-- THIS IS THE COPY THAT MATTERS MOST, exactly as nyc's dwh copy does: Fabric Warehouse is the one
-- engine whose write path can genuinely duplicate — under snapshot isolation two transactions
-- overlapping in time can both write, with no commit check to fail loudly — and this fact has no
-- keyed merge to shrink that window (delete+insert on [file] is atomic per statement, but two
-- overlapping runs can still interleave). A doubled month is exactly what this reports.
--
-- Dialect differences from the DuckDB copy, both mandatory:
--   * [file] is a reserved word, so every reference is bracketed — the dwh model writes it under
--     exactly that name (unique_key=['[file]']);
--   * no GROUP BY ALL in T-SQL, so the key column is spelled out.

WITH stored AS (
  SELECT [file], COUNT(*) AS n
  FROM {{ ref('fct_flights') }}
  GROUP BY [file]
),
landed AS (
  SELECT [file_stem], [row_count]
  FROM {{ ref('stg_flights_archive_log') }}
  WHERE [source_type] = 'flights'
)
SELECT
  landed.[file_stem],
  landed.[row_count] AS logged_rows,
  stored.n AS stored_rows
FROM landed
LEFT JOIN stored ON stored.[file] = landed.[file_stem]
WHERE stored.n IS NULL OR stored.n <> landed.[row_count]
