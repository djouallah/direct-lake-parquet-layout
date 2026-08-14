-- Staging table over the archive log download_cms_payments.py writes to the landing lakehouse
-- (Files/parquet_raw_archive_log.parquet). One log; every engine reads it with SQL.
-- INCREMENTAL, not table/view, for the same reason as the AEMO staging model: the DuckDB Iceberg
-- catalog supports neither CREATE VIEW nor the table materialization's temp-table RENAME, but it
-- does CREATE TABLE AS + INSERT. Insert-only, keyed on (source_type, source_filename), spelled the
-- way both DuckDB targets accept verbatim -- see models/aemo/duckdb/marts/fct_price.sql for the
-- full reasoning behind that one config serving duckrun and iceberg alike.
--
-- ⚠️ THE KEY IS NOT UNIQUE ON THIS DATASET, and that is why it is spelled do_nothing rather than
-- relied on. Every other dataset writes one log row per source file, so (source_type,
-- source_filename) identifies a row. Here the watermark unit is the annual CSV and the landed unit
-- is the MONTH, so one program year writes ~12 rows sharing a source_filename and differing only in
-- file_stem. An insert-only merge is still correct — it never executes a matched branch, so a
-- non-unique key degenerates to "insert the rows not already present" — but the WHERE below is
-- what actually keeps the source small, and it must stay keyed on the same pair.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    merge_clauses={'when_matched': [{'action': 'do_nothing'}]},
    unique_key=['source_type', 'file_stem'],
    schema='landing'
) }}

SELECT
    source_type,
    source_filename,
    archive_path,
    archived_at,
    row_count,
    source_url,
    etag,
    file_stem,
    columns
FROM read_parquet('{{ get_root_path() }}/parquet_raw_archive_log.parquet')
{% if is_incremental() %}
WHERE (source_type, file_stem) NOT IN (SELECT source_type, file_stem FROM {{ this }})
{% endif %}
