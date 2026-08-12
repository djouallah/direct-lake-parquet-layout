-- No carrier code may carry LEADING OR TRAILING whitespace. T-SQL dialect (Fabric Warehouse).
-- See tests/bts/duckdb/assert_carrier_code_has_no_whitespace.sql for why this is deliberately
-- NARROWER than the DUID test — embedded spaces are legitimate here ('PA (1)' is Pan Am, real
-- data in every 1987 month), so only the EDGES are asserted.
--
-- This is the copy that had to exist. The bug is a *T-SQL* pathology — ANSI trailing-space padding
-- means 'AA' = 'AA ' is TRUE here and FALSE in DuckDB and Spark — so this is the one engine where
-- a padded key silently JOINS instead of silently dropping.
--
-- Two dialect spellings that are load-bearing, not stylistic (same as the DUID copy):
--
--   * LIKE with a character class, never `code <> LTRIM(RTRIM(code))` — LIKE does NOT pad, a
--     comparison DOES, so the comparison form is always FALSE for exactly the trailing-space case
--     this test exists to catch. Anchoring the class at the string's first and last position is
--     what narrows it to the edges.
--   * DATALENGTH, never LEN() — LEN() ignores trailing spaces and would print the padded and
--     clean values as the same length, which is the one thing the failure output needs to show.

SELECT
  code,
  DATALENGTH(code) AS len,
  '[' + code + ']' AS boxed   -- makes the padding visible in the failure output
FROM {{ ref('dim_carrier') }}
WHERE code LIKE '[' + CHAR(9) + CHAR(10) + CHAR(13) + ' ]%'
   OR code LIKE '%[' + CHAR(9) + CHAR(10) + CHAR(13) + ' ]'
