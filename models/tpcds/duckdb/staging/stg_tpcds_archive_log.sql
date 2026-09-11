-- Staging table over the archive log download_tpcds.py writes to the landing lakehouse
-- (Files/parquet_raw_archive_log.parquet). One log; every engine reads it with SQL.
--
-- ONE ROW PER LANDED PARQUET FILE, and `source_type` is the TABLE name rather than a feed name --
-- ten distinct values, where every other dataset has one or two. `etag` carries `sf<N>`, which is
-- how the generator decides a scale factor is already landed and how a reader tells which scale
-- factor a run measured. The two singular tests reconcile each fact's stored row count against the
-- SUM of the row_count values logged for it.
--
-- INCREMENTAL, not table/view, for the same reason as every other staging model here: the DuckDB
-- Iceberg catalog supports neither CREATE VIEW nor the table materialization's temp-table RENAME,
-- but it does do CREATE TABLE AS + INSERT. Insert-only, keyed on (source_type, file_stem), spelled
-- the way both DuckDB targets accept verbatim.
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
