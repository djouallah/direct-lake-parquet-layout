-- No carrier code may carry LEADING OR TRAILING whitespace. Spark SQL dialect (Fabric Spark).
-- See tests/bts/duckdb/assert_carrier_code_has_no_whitespace.sql for why this is deliberately
-- NARROWER than the DUID test — embedded spaces are legitimate here ('PA (1)' is Pan Am, real
-- data in every 1987 month), so only the EDGES are asserted.
--
-- Spark compares like DuckDB does ('AA' = 'AA ' is FALSE), so a padded key here loses rows
-- silently rather than gaining them — nothing errors, the carrier simply never appears.
--
-- Dialect note: RLIKE, not regexp_matches. '\\s' is a SQL string literal, and Spark's parser
-- turns \\ into a single backslash before the regex engine sees it (escapedStringLiterals
-- defaults to false), so the pattern that runs is ^\s|\s$. Writing '\s' would depend on that
-- setting; keep both backslashes.

SELECT
  code,
  length(code) AS len,
  concat('[', code, ']') AS boxed   -- makes the padding visible in the failure output
FROM {{ ref('dim_carrier') }}
WHERE code RLIKE '^\\s|\\s$'
