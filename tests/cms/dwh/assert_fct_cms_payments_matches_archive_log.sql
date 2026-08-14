-- Every landed month appears in fct_cms_payments exactly as many times as the archive log said,
-- and no month is missing. T-SQL dialect (Fabric Warehouse). Same assertion as
-- tests/cms/duckdb/assert_fct_cms_payments_matches_archive_log.sql — read that one for what it
-- catches, why the program-year reconciliation lives at land time instead, and why it is allowed
-- to read two tables.
--
-- Fabric Warehouse is still the engine most exposed here: duckrun, iceberg and spark check the
-- commit and fail loudly on a real overlap, while under snapshot isolation two dwh transactions
-- overlapping in time can both write. What differs from green is that on THIS dataset dwh has a
-- genuine keyed merge to shrink that window with — CMS publishes Record_ID, so ([file],
-- [Record_ID]) is unique and the many-to-many match that forces green onto delete+insert cannot
-- arise. This test is the detector for what the merge window still leaves.
--
-- Dialect differences from the DuckDB copy, both mandatory:
--   * [file] is a reserved word, so every reference is bracketed — the dwh model writes it under
--     exactly that name (unique_key=['[file]', '[Record_ID]']);
--   * no GROUP BY ALL in T-SQL, so the key column is spelled out.

WITH stored AS (
  SELECT [file], COUNT(*) AS n
  FROM {{ ref('fct_cms_payments') }}
  GROUP BY [file]
),
landed AS (
  SELECT [file_stem], [row_count]
  FROM {{ ref('stg_cms_archive_log') }}
  WHERE [source_type] = 'cms'
)
SELECT
  landed.[file_stem],
  landed.[row_count] AS logged_rows,
  stored.n AS stored_rows
FROM landed
LEFT JOIN stored ON stored.[file] = landed.[file_stem]
WHERE stored.n IS NULL OR stored.n <> landed.[row_count]
