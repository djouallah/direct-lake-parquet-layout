{# The write geometry for the ICEBERG leg, as Iceberg table properties: A ROW COUNT, AND NOTHING IN MB.

   ⚠️ **SINCE 2026-10-18 THE ICEBERG LEG IS SIZED BY ROWS ONLY, 4,000,000 BY DEFAULT.** Before that,
   `auto` (forced by `duckrun_auto`, on by default) emitted no geometry at all and the writer fell
   back to its 128 MB UNCOMPRESSED row group — nyc's mart came out as 729 row groups of ~812K rows on
   every default run, including run 39040729276 on the current pin. Rows are what VertiPaq segments
   are counted in, so rows are the one number this leg takes:

   - `row_group_size` pinned  -> that many rows;
   - `row_group_size` = `auto` -> `default_rows` below, 4,000,000.

   ONE ROW GROUP PER FILE: `write.parquet.row-groups-per-file = 1` (DuckDB's `ROW_GROUPS_PER_FILE`)
   closes the file after every row group, so a file is one 4M-row group — ~60 MB on disk on nyc.
   Without it the file target is the only thing that rotates files, and with the ceiling below
   that would be one multi-GB file. NOT a hard guarantee: DuckDB's docs say a file "may slightly
   exceed" the count when several threads write. Check `num_files` against `num_row_groups`.

   `file_size_mb` NO LONGER REACHES THIS LEG. It cannot: the writer closes a row group at whichever
   comes first, the row count or the byte budget, and CLAMPS the byte budget DOWN to the file target
   (`IcebergInsert::GetCopyOptions`: `batch_size_bytes > file_size_bytes -> file_size_bytes`). So a
   64 MB file target — the form default — caps every row group at 64 MB uncompressed whatever row
   count is asked for. Both byte limits are therefore set to one CEILING, `byte_ceiling_gb`, which
   is a limit and never a size: 4M rows is ~660 MB uncompressed on nyc's 17 columns (128 MB / 812K
   rows = ~165 B a row), so 2 GiB leaves room for ~500 B rows before the bytes matter again.

   ⚠️ **THE 128 MB IS UNCOMPRESSED, WHICH IS WHY IT DOES NOT LOOK LIKE IT IS BINDING.** nyc's iceberg
   mart is ~8,961 MB over 728 row groups — 12.3 MB per row group ON DISK. Read that as "nothing near
   128 MB" and the diagnosis inverts. Upstream still defaults it (`iceberg_insert.hpp`:
   `batch_size_bytes = 128ULL * 1024 * 1024`, checked on duckdb-iceberg main 2026-10-18).

   `write.parquet.row-group-size` counts ROWS — DuckDB's own key, not the Iceberg spec's
   `-bytes` one — and maps onto the same `batch_size` field DuckDB's `COPY … (ROW_GROUP_SIZE x)`
   resolves to. Multiplying it, or spelling it `-bytes`, would ask for an N-BYTE row group.

   THE PAGE SIZE IS ALSO EMITTED ON EVERY RUN: 1 MiB, duckrun's value. DuckDB otherwise splits a
   column chunk into data pages only at ~100 MB uncompressed, i.e. one page per chunk; duckdb#24645
   made that `data_page_size_limit` and duckdb-iceberg#1365 (merged 2026-09-03) maps
   `write.parquet.page-size-bytes` onto it. Confirmed binding on run 39040729276: 1-3 data pages per
   chunk (`layout.encodings.iceberg.<col>.data_pages`), where the default gives one.

   INTEGERS, NEVER '128MB'. duckdb-iceberg's `ParseByteSizeOptionallyFormatted` accepts human units
   and duckdb-iceberg#1150 records that it then writes the human STRING into the table metadata,
   where another engine reading the table may not parse it.

   ⚠️ MART ONLY, matching where duckrun's geometry keys sit. #}
{% macro iceberg_geometry() %}
  {%- set rg = env_var('DUCKDB_ROW_GROUP_SIZE', 'auto') -%}
  {#- The row count a default (`auto`) dispatch gets. -#}
  {%- set default_rows = 4000000 -%}
  {#- Both byte limits, so neither can close a row group before the row count does. A ceiling,
      never a size: with one row group per file, a file is as big as one row group on disk. -#}
  {%- set byte_ceiling_gb = 2 -%}
  {%- set ceiling = byte_ceiling_gb * 1073741824 -%}
  {{ return({
    'write.parquet.row-group-size': default_rows if rg | lower == 'auto' else rg | int,
    'write.parquet.row-groups-per-file': 1,
    'write.parquet.row-group-size-bytes': ceiling,
    'write.target-file-size-bytes': ceiling,
    'write.parquet.page-size-bytes': 1048576,
  }) }}
{% endmacro %}
