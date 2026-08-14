-- No payer_id may contain whitespace — leading, trailing, or embedded.
--
-- This is not cosmetic. payer_id is the join key from dim_cms_payer into fct_cms_payments, and the
-- engines do not agree on what equality means when one side is padded:
--
--   T-SQL   '100000000123' = '100000000123 '  ->  TRUE   (ANSI trailing-space padding)
--   DuckDB  '100000000123' = '100000000123 '  ->  FALSE
--   Spark   '100000000123' = '100000000123 '  ->  FALSE
--
-- The AEMO version of this test exists because exactly that split a real generating unit across the
-- four engines for a year, silently — dwh joined it, the other three dropped it, and the only
-- symptom was a row-count gap that accused the one engine that was correct. See LEARNINGS.md.
--
-- WHY THIS DATASET NEEDS IT WHEN nyc AND green DO NOT: their dimension key is an INTEGER
-- LocationID, and the padding pathology can only bite a STRING key. This is the first string join
-- key since DUID, so the rule that a new one needs all three dialect copies applies here.
--
-- THE GUARD IS THE FULL \s, NOT bts's LEADING/TRAILING NARROWING, and the difference is the data.
-- bts had to narrow its carrier-code guard because embedded spaces are legitimate there ('PA (1)'
-- is Pan Am, ~5K rows in every 1987 month), so a full guard would fail every leg on correct data.
-- CMS payer ids are numeric strings — 12 characters at the widest, no separators — so no whitespace
-- of any kind is legitimate and there is nothing to narrow for.
--
-- DuckDB dialect (duckrun + iceberg). tests/cms/dwh and tests/cms/spark carry the same assertion;
-- the dwh one is the load-bearing copy, since T-SQL is the only dialect where a padded key joins.

SELECT
  payer_id,
  length(payer_id) AS len,
  '[' || payer_id || ']' AS boxed   -- makes the padding visible in the failure output
FROM {{ ref('dim_cms_payer') }}
WHERE regexp_matches(payer_id, '\s')
