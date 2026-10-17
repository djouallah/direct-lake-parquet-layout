{# The write geometry for the ICEBERG leg, as Iceberg table properties: A ROW COUNT, AND NOTHING IN MB.

   ⚠️ **SINCE 2026-10-18 THE ICEBERG LEG IS SIZED BY ROWS ONLY, 4,000,000 BY DEFAULT.** Before that,
   `auto` (forced by `duckrun_auto`, on by default) emitted no geometry at all and the writer fell
   back to its 128 MB UNCOMPRESSED row group — nyc's mart came out as 729 row groups of ~812K rows on
   every default run, including run 39040729276 on the current pin. Rows are what VertiPaq segments
   are counted in, so rows are the one number this leg takes:

   - `row_group_size` pinned  -> that many rows;
   - `row_group_size` = `auto` -> `default_rows` below, 4,000,000.

   ⚠️ `write.parquet.row-groups-per-file = 1` IS SENT AND DOES NOTHING, upstream bug. Plain DuckDB's
   COPY binder resolves `ROW_GROUPS_PER_FILE` into `batches_per_file` on the copy operator
   (`bind_copy.cpp`); duckdb-iceberg special-cases `row_group_size(_bytes)` that way but lets
   `row_groups_per_file` fall through to the parquet options, which ignore it, and
   `IcebergCopyToFile::ApplyCopyOptions` never sets `batches_per_file`. Run 39042898921 (nyc, 2 GiB
   ceiling) wrote 149 row groups in 5 files. Kept so it takes effect the day upstream is fixed.

   `file_size_mb` NO LONGER REACHES THIS LEG. It cannot: the writer closes a row group at whichever
   comes first, the row count or the byte budget, and CLAMPS the byte budget DOWN to the file target
   (`IcebergInsert::GetCopyOptions`: `batch_size_bytes > file_size_bytes -> file_size_bytes`). So a
   64 MB file target — the form default — caps every row group at 64 MB uncompressed whatever row
   count is asked for. Both byte limits are therefore set to ONE value, `byte_ceiling_gb` = 1 GiB,
   and that makes the rule GENERIC rather than tuned to any dataset:

       a row group closes at 4M rows OR 1 GiB uncompressed, whichever comes first;
       a file closes at the first row-group boundary past 1 GiB written.

   So files are ~1 GiB (plus at most one row group) on every dataset, and a table whose rows are
   wider than ~268 B uncompressed gets row groups SMALLER than 4M — capped at 1 GiB of data — rather
   than a bigger file. That is the trade, stated once, for every table. The 2 GiB of run 39042898921
   gave ~1.85 GB files; 1 GiB is the file size asked for.

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
  {#- Both byte limits at one value: a row group closes at the row count or 1 GiB uncompressed,
      a file at the first row-group boundary past 1 GiB. See the block comment. -#}
  {%- set byte_ceiling_gb = 1 -%}
  {%- set ceiling = byte_ceiling_gb * 1073741824 -%}
  {{ return({
    'write.parquet.row-group-size': default_rows if rg | lower == 'auto' else rg | int,
    'write.parquet.row-groups-per-file': 1,
    'write.parquet.row-group-size-bytes': ceiling,
    'write.target-file-size-bytes': ceiling,
    'write.parquet.page-size-bytes': 1048576,
  }) }}
{% endmacro %}
