{# The write geometry for the ICEBERG leg, as Iceberg table properties.

   ONE DISPATCH DRIVES BOTH DuckDB WRITERS, and that is the point of this macro. duckrun takes
   `max_row_group_size` (ROWS) and `target_file_size_mb`; duckdb-iceberg takes
   `write.parquet.row-group-size` (ROWS — DuckDB's own extension to the spec, which is what makes
   the units line up) and `write.target-file-size-bytes`. So the SAME `row_group_size` /
   `file_size_mb` dispatch inputs now describe both, with no conversion on the row count and a
   plain MiB multiply on the file size. Two writers, one geometry, one number in the form — which
   is the comparison the duckrun/iceberg pair exists to make.

   ⚠️ **"WHY CAN'T IT JUST BE row group = x? PLAIN DuckDB DOES THAT FINE." IT IS THE SAME KNOB, AND
   THE DIFFERENCE IS A HARDCODED DEFAULT.** `write.parquet.row-group-size` maps onto `batch_size`,
   the exact field DuckDB's own `COPY … (ROW_GROUP_SIZE x)` resolves to —
   `IcebergInsert::GetCopyOptions` says so itself: *"DuckDB's parquet copy_to_bind ignores
   row_group_size(_bytes); the COPY binder resolves them into batch_size(_bytes) on the copy
   operator instead. Do the same here, since we bypass that binder."* What differs is that plain
   `COPY` carries NO byte threshold, so the row count binds, while duckdb-iceberg carries one
   ALWAYS. From `src/include/execution/operator/iceberg_insert.hpp:62-69`:

       //! default target file size: 512MB, per the Iceberg spec default for write.target-file-size-bytes
       idx_t file_size_bytes = 512ULL * 1024 * 1024;
       //! Row groups are flushed when either threshold is hit. Iceberg only defines a byte default
       //! (write.parquet.row-group-size-bytes = 128MB), so we leave batch_size unset unless the table
       //! sets write.parquet.row-group-size - otherwise DuckDB's 122880 row default would always win.
       optional_idx batch_size;
       optional_idx batch_size_bytes = 128ULL * 1024 * 1024;

   Both thresholds are live and the FIRST to hit wins, so a rows-only property is inert on any table
   that reaches 128 MB before it reaches x rows. That is exactly what run 33733500776 measured —
   nyc dispatched at `row_group_size=5000000` wrote **729 row groups at 811,700 rows**, i.e. the
   baseline's 728 at 812,815 and no change at all.

   **THE 128 MB IS UNCOMPRESSED, WHICH IS WHY IT DOES NOT LOOK LIKE IT IS BINDING.** nyc's iceberg
   mart is 8,961 MB over 728 row groups — **12.3 MB per row group ON DISK**. Read that as "nothing
   near 128 MB" and the whole diagnosis inverts; it is 128 MB in memory compressing ~10x. Every
   iceberg row-group count on record is that byte default rather than anything about the data.

   SO A PINNED ROW COUNT ALSO RAISES THE BYTE BUDGET, TO ONE FIXED CONSTANT — `row_group_budget_mb`
   below, 1 GiB. It is deliberately NOT derived from `file_size_mb`: one number in one place, so
   nothing about the row-group budget moves when someone changes a FILE setting.

   **1 GiB IS MEASURED FREE, WHICH IS WHY IT IS NOT A TRADE.** The obvious objection is that a
   bigger buffer must cost something, so it was measured locally — plain `COPY`, which resolves to
   the same `batch_size`/`batch_size_bytes` fields this macro reaches through the properties,
   20 BIGINT columns, `preserve_insertion_order` off:

       20 cols x 20M rows, asking 5,000,000 rows/row-group
         128 MB (the default)  -> 25 row groups @   827,392 rows   23.9s
         512 MB                ->  7 row groups @ 3,305,472 rows   22.7s
         1 GiB                 ->  4 row groups @ 5,001,216 rows   23.9s

       scaling, 128 MB against 1 GiB
         20M rows -> 23.9s vs 18.0s   (0.75x)
         60M rows -> 42.8s vs 48.9s   (1.14x)

   Both ratios are inside run-to-run variance and one run came out faster, so the budget is not a
   cost to be balanced — it is a correction. Note the first row: **827,392 rows** on a 20-column
   table, against nyc's real mart at **812,815**. That is the diagnosis confirmed on a second,
   independent table, and it is why 1 GiB is the right value rather than a cautious 512 MB: only at
   1 GiB does the DISPATCHED row count become the threshold that binds on a table this wide, which
   is the entire promise of the input.

   ⚠️ THE WRITER CLAMPS IT DOWN TO THE FILE TARGET, so the constant is a REQUEST and the effective
   budget can be smaller (`GetCopyOptions`): `if (result.batch_size_bytes.GetIndex() >
   result.file_size_bytes) result.batch_size_bytes = result.file_size_bytes;` — a file cannot hold
   less than one row group, and rotation only happens at row-group boundaries. **The consequence is
   easy to miss: a pinned row count with `file_size_mb` on `auto` gets 512 MB, not 1 GiB**, because
   the writer's own default file target is 512 MB. So `file_size_mb=1024` is part of asking for a
   5M-row group on a wide table, not incidental to it. Read `num_row_groups` back out of the record
   rather than assuming the dispatch got what it asked for, exactly as CLAUDE.md already says for
   duckrun's pair of knobs.

   INTEGERS, NEVER '128MB'. duckdb-iceberg's `ParseByteSizeOptionallyFormatted` accepts human
   units, and duckdb-iceberg#1150 records that it then writes the human STRING into the table
   metadata, where another engine reading the table may not parse it. The property is metadata
   other readers see, so it is spelled in bytes.

   `auto` on both inputs yields no property at all, so a default dispatch emits an EMPTY dict and
   `duckdb__create_table_as` falls back to the adapter's own SQL byte for byte. Absence is the
   writer's default (128 MB row groups / 512 MB files since duckdb-iceberg#1202, merged 2026-07-23),
   not zero.

   ⚠️ MART ONLY, matching where duckrun's geometry keys sit. Putting it on the facts would give
   iceberg a knob on tables duckrun writes at its default, which is the asymmetry this macro
   exists to remove. #}
{% macro iceberg_geometry() %}
  {%- set rg = env_var('DUCKDB_ROW_GROUP_SIZE', 'auto') -%}
  {%- set mb = env_var('DUCKDB_FILE_SIZE_MB', 'auto') -%}
  {#- The byte budget a pinned row count is given, so the ROW COUNT is the threshold that binds
      rather than the writer's 128 MB (which is ~827K rows on a 20-column table -- see the block
      comment for the measurement, and for why this is 1 GiB and not a cautious 512 MB). FIXED,
      never derived from `mb`. The writer clamps it down to the file target, so a dispatch on
      `file_size_mb=auto` effectively gets 512 MB. -#}
  {%- set row_group_budget_mb = 1024 -%}
  {%- set out = {} -%}
  {%- if mb | lower != 'auto' -%}
    {%- do out.update({'write.target-file-size-bytes': (mb | int) * 1048576}) -%}
  {%- endif -%}
  {%- if rg | lower != 'auto' -%}
    {%- do out.update({'write.parquet.row-group-size': rg | int}) -%}
    {#- Without this the line above is inert on anything wide — see the block comment. -#}
    {%- do out.update({'write.parquet.row-group-size-bytes': row_group_budget_mb * 1048576}) -%}
  {%- endif -%}
  {{ return(out) }}
{% endmacro %}
