{# The write geometry for the ICEBERG leg, as Iceberg table properties.

   ONE DISPATCH DRIVES BOTH DuckDB WRITERS, and that is the point of this macro. duckrun takes
   `max_row_group_size` (ROWS) and `target_file_size_mb`; duckdb-iceberg takes
   `write.parquet.row-group-size` (ROWS — DuckDB's own extension to the spec, which is what makes
   the units line up) and `write.target-file-size-bytes`. So the SAME `row_group_size` /
   `file_size_mb` dispatch inputs now describe both, with no conversion on the row count and a
   plain MiB multiply on the file size. Two writers, one geometry, one number in the form — which
   is the comparison the duckrun/iceberg pair exists to make.

   ⚠️ **THE ROW COUNT IS A CEILING, NOT A SETTING, AND ON ITS OWN IT DOES NOTHING.** DuckDB flushes
   a row group when EITHER threshold is reached, and duckdb-iceberg defaults the byte one to 128 MB
   (duckdb-iceberg#1202). Measured locally on 3M rows of four columns, `preserve_insertion_order`
   off, same data both times:

       ROW_GROUP_SIZE 5000000                             -> 1 row group, 3,000,000 rows
       ROW_GROUP_SIZE 5000000, ROW_GROUP_SIZE_BYTES 128MB -> 2 row groups, max 2,004,992 rows

   That is why run 33733500776 — nyc, dispatched at `row_group_size=5000000` — wrote **729 row
   groups at 811,700 rows**, i.e. the baseline's 728 at 812,815 and no change at all. 128 MB over
   nyc's 20 columns is ~812K rows and over aemo's 5 narrow ones ~2.7M, which is exactly what both
   datasets had always written: the leg has been byte-bound at the default the whole time, and a
   rows-only property is inert on any table wide enough to reach 128 MB first.

   SO A PINNED ROW COUNT ALSO RAISES THE BYTE BUDGET, to the file target. duckdb-iceberg caps its
   batch bytes at the file size anyway (a file cannot hold less than one row group), so the file
   target is the largest meaningful value and makes the ROW COUNT the binding threshold — which is
   what the dispatch input promises. It follows that **the file size still binds first when the
   rows would not fit**: ask for 5M rows of a 165-byte row in a 512 MB file and you get ~3.1M. Read
   `num_row_groups` back out of the record rather than assuming the dispatch got what it asked for,
   exactly as CLAUDE.md already says for duckrun's pair of knobs.

   INTEGERS, NEVER '128MB'. duckdb-iceberg's `ParseByteSizeOptionallyFormatted` accepts human
   units, and duckdb-iceberg#1150 records that it then writes the human STRING into the table
   metadata, where another engine reading the table may not parse it. The property is metadata
   other readers see, so it is spelled in bytes.

   `auto` on both inputs yields no property at all, so a default dispatch emits an EMPTY dict and
   `duckdb__create_table_as` falls back to the adapter's own SQL byte for byte. Absence is the
   writer's default (128 MB row groups / 512 MB files since duckdb-iceberg#1202), not zero.

   ⚠️ MART ONLY, matching where duckrun's geometry keys sit. Putting it on the facts would give
   iceberg a knob on tables duckrun writes at its default, which is the asymmetry this macro
   exists to remove. #}
{% macro iceberg_geometry() %}
  {%- set rg = env_var('DUCKDB_ROW_GROUP_SIZE', 'auto') -%}
  {%- set mb = env_var('DUCKDB_FILE_SIZE_MB', 'auto') -%}
  {#- The writer's own default target file size, and the ceiling on a row group's byte budget.
      Spelled here because a rows-only dispatch still has to raise that budget above 128 MB, and
      there is nothing else to raise it to. If duckdb-iceberg's default moves, move this with it. -#}
  {%- set default_file_mb = 512 -%}
  {%- set out = {} -%}
  {%- if mb | lower != 'auto' -%}
    {%- do out.update({'write.target-file-size-bytes': (mb | int) * 1048576}) -%}
  {%- endif -%}
  {%- if rg | lower != 'auto' -%}
    {%- do out.update({'write.parquet.row-group-size': rg | int}) -%}
    {#- Without this the line above is inert on anything wide — see the block comment. -#}
    {%- set budget_mb = (mb | int) if mb | lower != 'auto' else default_file_mb -%}
    {%- do out.update({'write.parquet.row-group-size-bytes': budget_mb * 1048576}) -%}
  {%- endif -%}
  {{ return(out) }}
{% endmacro %}
