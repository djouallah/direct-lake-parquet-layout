-- No carrier code may carry LEADING OR TRAILING whitespace. Deliberately NARROWER than the DUID
-- test it is copied from, and the narrowing is load-bearing: BTS dedups a reused code by
-- suffixing the earlier carrier with ' (1)', so EMBEDDED spaces are legitimate data — 'PA (1)' is
-- Pan Am, present in the real 1987 months at ~5,000 rows each, and forbidding embedded whitespace
-- would fail every leg on correct data. Verified against the landed 1987-10 file before this test
-- was written. Only the EDGES are the pathology.
--
-- Why the edges matter is the DUID incident exactly (see
-- tests/aemo/duckdb/assert_duid_has_no_whitespace.sql and LEARNINGS.md): code is the join key
-- from dim_carrier into fct_flights.Reporting_Airline, and the engines do not agree on equality
-- when one side is padded:
--
--   T-SQL   'AA' = 'AA '  ->  TRUE   (ANSI trailing-space padding on comparison)
--   DuckDB  'AA' = 'AA '  ->  FALSE
--   Spark   'AA' = 'AA '  ->  FALSE
--
-- A padded key means at minimum a silent cross-engine disagreement, and at worst a real carrier
-- missing from three of four outputs. Catch it at the dimension, where it enters.
--
-- DuckDB dialect (duckrun + iceberg). tests/bts/dwh and tests/bts/spark carry the same assertion;
-- the dwh one is the load-bearing copy, since T-SQL is the only dialect where a padded key joins
-- anyway.

SELECT
  code,
  length(code) AS len,
  '[' || code || ']' AS boxed   -- makes the padding visible in the failure output
FROM {{ ref('dim_carrier') }}
WHERE regexp_matches(code, '^\s|\s$')
