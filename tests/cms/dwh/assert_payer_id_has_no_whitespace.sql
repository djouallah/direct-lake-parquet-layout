-- No payer_id may contain whitespace — leading, trailing, or embedded. T-SQL dialect (Fabric
-- Warehouse). See tests/cms/duckdb/assert_payer_id_has_no_whitespace.sql for the incident this
-- guards and for why this dataset needs the assertion when nyc and green do not.
--
-- THIS IS THE COPY THAT HAD TO EXIST. The bug is a *T-SQL* pathology — ANSI trailing-space padding
-- means '100000000123' = '100000000123 ' is TRUE here and FALSE in DuckDB and Spark — so putting
-- the guard only on the duckdb targets would put it on the two engines that cannot exhibit it.
-- dim_cms_payer is built from the same landed parquet everywhere, so a dirty key does trip the
-- DuckDB copy too, but only this one runs against the table where the padding changes a join result.
--
-- Two dialect differences from the DuckDB copy, both mandatory:
--
--   * No regexp_matches. LIKE with a character class is the portable T-SQL equivalent, and it is
--     the right tool here specifically because LIKE does NOT pad: every character of the value is
--     significant, so a trailing space in payer_id is available to match. Do not "simplify" this to
--     `payer_id <> LTRIM(RTRIM(payer_id))` — that is a comparison, comparisons pad, and it is
--     therefore always FALSE for exactly the trailing-space case this test exists to catch.
--   * LEN() ignores trailing spaces, so it would report the padded and clean values as the same
--     length — the one thing the failure output needs to show. DATALENGTH counts them.

SELECT
  [payer_id],
  DATALENGTH([payer_id]) AS len,
  '[' + [payer_id] + ']' AS boxed   -- makes the padding visible in the failure output
FROM {{ ref('dim_cms_payer') }}
WHERE [payer_id] LIKE '%[' + CHAR(9) + CHAR(10) + CHAR(13) + ' ]%'
