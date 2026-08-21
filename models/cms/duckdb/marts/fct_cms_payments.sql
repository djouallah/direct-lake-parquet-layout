-- The wide raw fact, and the table under layout test on this dataset — stats.py's MART for
-- DATASET=cms, the way mart.fct_green_trips is for green.
--
-- WHY THIS DATASET EXISTS. The other four vary SKEW at a roughly constant width (5, 17, 20, 22
-- columns). This one varies the WIDTH — 91 columns, 4x the widest of them — and adds a property
-- none of them has: SPARSITY. Measured on a 100 MB / 187,750-row sample of PY2023, 54 of the 91
-- columns are more than half NULL, because CMS models a one-to-many product list as five repeated
-- six-column groups plus a six-wide recipient-type/specialty group whose tail members run 83%,
-- 95%, 98% and 99% NULL. Sparse columns are where an encoding pass has the most to gain, and
-- nothing else here exercises that at all.
--
-- It also carries BOTH skew regimes in one table, which is what makes it a new point rather than a
-- wider bts: Nature_of_Payment 92% single-value, Form_of_Payment 86%, Dispute_Status 100% (nyc's
-- regime) beside Covered_Recipient_Specialty_1 at 302 competing values and the payer id at ~1,000
-- (bts's regime). nyc says nothing about the second and bts nothing about the first.
--
-- ⚠️ THIS MODEL RETURNS TO A REAL KEYED MERGE, and it is the only taxi-shaped fact here that can.
-- nyc's and green's fct_trips write with `append` because TLC publishes no natural unique key, so
-- the only candidate is the FILE and the source is not unique on it. CMS publishes `Record_ID`, a
-- system-generated unique identifier per payment record, so (file, Record_ID) is a genuine key:
-- duckrun's source-uniqueness assertion passes, the insert-only merge dedups at WRITE time rather
-- than relying on the pre_hook's file selection, and a re-dispatch racing a scheduled run cannot
-- double a month. Do NOT "simplify" this to append to match the taxi models — the reason they use
-- append does not hold here, and the guard is strictly better.
--
-- `Record_ID` STAYS A STRING. Every observed value is ten numeric digits and a BIGINT key would be
-- smaller, but CMS documents it as a string; see download_cms_payments.py's CANONICAL for why a
-- TRY_CAST that failed would be worse than a wide key.
--
-- The pending-file probe must run BEFORE config(): it feeds the has_files no-op gate and
-- incremental_predicates, and config() needs the latter.
{%- set pending_files_query -%}
SELECT file_stem FROM {{ ref('stg_cms_archive_log') }}
WHERE source_type = 'cms'
{%- if is_incremental() %}
AND file_stem NOT IN (SELECT DISTINCT file FROM {{ this }})
{%- endif -%}
{%- endset -%}

{%- if execute and flags.WHICH in ('run', 'build', 'retry') -%}
  {%- set files_result = run_query(pending_files_query) -%}
  {%- set pending_files = files_result.columns[0].values() | list if files_result else [] -%}
{%- else -%}
  {#-- Parse time: unknowable. `none` means "assume there is work", so the model renders its real
       body rather than the no-op branch — the compiled SQL a reviewer reads is the one that runs —
       and it means "do not narrow the merge". --#}
  {%- set pending_files = none -%}
{%- endif -%}
{%- set has_files = pending_files is none or pending_files | length > 0 -%}

{#-- Literal file names, one spelling for both adapters: only literals prune target files, and
     duckrun rewrites DBT_INTERNAL_DEST itself. See macros/pending_file_predicate.sql. `file` leads
     the unique_key, so this predicate is IMPLIED by the merge ON clause and removes no match the
     key would have made. --#}
{%- set file_predicate = pending_file_predicate(pending_files) -%}

{%- set cols = cms_payment_columns() -%}

{#-- The geometry knobs are the SAME dispatch inputs every other mart reads, so a layout question
     can be asked of any dataset with one workflow. The key this table WANTS is neither nyc's nor
     bts's: Date_of_Payment first (every composite query groups or filters THROUGH the date
     relationship), then Nature_of_Payment_or_Transfer_of_Value — which at 92% single-value is the
     cheapest column in the table to run-length encode and the one the composite queries slice on.
     That is a note for reading duckrun's choice, not something a dispatch can ask for any more.

     THE SORT IS `auto` OR NOTHING. There was a `sort_by` input taking a literal column list, and
     one field naming one key cannot serve five marts — its default was the aemo key, none of whose
     columns exist here, so the plan job carried a per-dataset column check purely to refuse the
     mismatch. duckrun's picker profiles the data and chooses per dataset instead. `auto` must be
     passed as a SCALAR — duckrun raises on `['auto']`, because a list means "these columns are the
     key".

     The comment sits ABOVE the tag, not inside it: dbt parses `config(...)` as an expression, and
     a Jinja comment between its arguments is `invalid syntax for function call expression` — an
     error that points at the `{{ config(` line and says nothing about comments. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    merge_clauses={'when_matched': [{'action': 'do_nothing'}]},
    unique_key=['file', 'Record_ID'],
    incremental_predicates=file_predicate,
    sort_by=('auto' if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
    max_row_group_size=(none if env_var('DUCKDB_ROW_GROUP_SIZE', 'auto').lower() == 'auto'
                        else env_var('DUCKDB_ROW_GROUP_SIZE', 'auto') | int),
    target_file_size_mb=(none if env_var('DUCKDB_FILE_SIZE_MB', 'auto').lower() == 'auto'
                         else env_var('DUCKDB_FILE_SIZE_MB', 'auto') | int),
    pre_hook="SET VARIABLE cms_paths = (SELECT COALESCE(NULLIF(list('{{ get_parquet_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_cms_archive_log') }} WHERE source_type = 'cms'{% if is_incremental() %} AND file_stem NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} ORDER BY archive_path))"
) }}

{% if has_files %}
{#-- A plain read, with no union_by_name and no schema merging, because the archive is HOMOGENEOUS
     by construction: download_cms_payments.py rewrites every program year to the canonical
     91-column schema before uploading it. Unlike nyc and green there was no drift to correct in
     the first place — the CSV header is byte-identical across PY2019-2025 — so the CASTs below are
     no-ops twice over, kept as the explicit declaration that all four engines store the same
     types.

     NO DERIVED DATE COLUMN, unlike nyc's and green's `pickup_date`. Date_of_Payment is already a
     DATE in the source, so the date dimension's join key ships in the file — the same way bts uses
     FlightDate. Adding a bridge column here would be the month_key mistake: a stored column
     nothing needs, in a benchmark whose subject is write cost. --#}
WITH payments AS (
  SELECT *
  FROM read_parquet(
    getvariable('cms_paths'),
    filename = 1,
    hive_partitioning = false
  )
)

SELECT
  {%- for name in cols %}
  CAST({{ cms_payment_value(name, name, 'duckdb') }} AS {{ cms_payment_type(name, 'duckdb') }}) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('filename') }} AS file
FROM payments
{% else %}
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
