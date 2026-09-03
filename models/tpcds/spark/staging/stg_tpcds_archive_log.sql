-- Staging table over the archive log download_tpcds.py writes to the landing lakehouse
-- (Files/parquet_raw_archive_log.parquet). One log; every engine reads it with SQL.
--
-- ONE ROW PER LANDED PARQUET FILE, and `source_type` is the TABLE name rather than a feed name --
-- ten distinct values, where every other dataset has one or two. `etag` carries `sf<N>`, which is
-- how the generator decides a scale factor is already landed and how a reader tells which scale
-- factor a run measured. The two singular tests reconcile each fact's stored row count against the
-- SUM of the row_count values logged for it.
--
-- Fabric Spark 3.5 has no read_files(), so read parquet with the path datasource syntax.
-- Materialized as a TABLE (not a view) so it is a physical OneLake Delta table a neutral Delta
-- reader can see. `auto_optimize=false` for the reason stated in every model of this tree.
{{ config(materialized='table', schema='landing', auto_optimize=false) }}

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
FROM parquet.`{{ get_root_path() }}/parquet_raw_archive_log.parquet`
