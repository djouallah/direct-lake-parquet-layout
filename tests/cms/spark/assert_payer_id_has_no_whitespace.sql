-- No payer_id may contain whitespace — leading, trailing, or embedded. Spark SQL dialect (Fabric
-- Spark). See tests/cms/duckdb/assert_payer_id_has_no_whitespace.sql for the incident this guards
-- and for why this dataset needs the assertion when nyc and green do not.
--
-- Spark compares like DuckDB does ('100000000123' = '100000000123 ' is FALSE), so a padded key here
-- loses rows silently rather than gaining them. That is the failure mode the original incident
-- produced on three of four engines, and it is invisible without this test: nothing errors, the
-- payer simply never appears.
--
-- Dialect note: RLIKE, not regexp_matches. '\\s' is a SQL string literal, and Spark's parser turns
-- \\ into a single backslash before the regex engine sees it (escapedStringLiterals defaults to
-- false), so the pattern that runs is \s — leading and trailing spaces, tabs, newlines, and
-- embedded whitespace. Writing '\s' would depend on that setting; keep both backslashes.

SELECT
  payer_id,
  length(payer_id) AS len,
  concat('[', payer_id, ']') AS boxed   -- makes the padding visible in the failure output
FROM {{ ref('dim_cms_payer') }}
WHERE payer_id RLIKE '\\s'
