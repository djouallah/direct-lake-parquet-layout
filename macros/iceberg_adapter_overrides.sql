{# Adapter overrides needed by the OneLake Iceberg REST catalog (the `iceberg` target).
   These dispatch on adapter type `duckdb`, so they apply only to the Iceberg target
   (type: duckdb); the Delta target is a different adapter (type: duckrun) and is
   unaffected. Even if it were reached, both overrides are benign for plain Delta. #}

{# get_columns_in_relation: Iceberg catalogs don't populate information_schema.columns,
   so use DESCRIBE and drop the Iceberg hidden "__" column. #}
{% macro duckdb__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          column_type as data_type,
          null as character_maximum_length,
          null as numeric_precision,
          null as numeric_scale
      from (describe {{ relation }})
      where column_name != '__'
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{# drop_relation: the DuckDB Iceberg extension does not support DROP TABLE ... CASCADE. #}
{% macro duckdb__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{# create_table_as: carry Iceberg TABLE PROPERTIES into the CTAS.

   THE WRITE GEOMETRY IS THE ONLY THING THIS ADDS, and `COPY`'s options are not how you set it.
   dbt-duckdb exposes no writer config at all — `ROW_GROUP_SIZE`, `DICTIONARY_SIZE_LIMIT` and
   friends are `COPY` options and no materialization emits them — so for years the iceberg leg
   wrote at whatever the DuckDB build defaulted to. duckdb-iceberg reads the geometry off ICEBERG
   TABLE PROPERTIES instead, and `CREATE TABLE <rel> WITH ('k' = 'v') AS SELECT …` is where those
   are set at CREATE time. Probed against the extension binary the leg's `duckdb==1.6.0.dev379`
   pin runs (v2.0.0-alpha): `write.parquet.row-group-size` (ROWS — DuckDB's own extension, not in
   the Iceberg spec), `write.parquet.row-group-size-bytes`, `write.parquet.row-groups-per-file`,
   `write.target-file-size-bytes`, `write.parquet.dict-size-bytes` and
   `write.parquet.compression-codec` / `-level`. The mapping is one static table,
   `ICEBERG_TABLE_PROPERTY_MAPPING` in duckdb-iceberg's `src/execution/operator/iceberg_insert.cpp`
   — read it before adding a key here; a property NOT in it is written to the table metadata and
   changes no parquet.

   ⚠️ `write.parquet.dict-size-bytes` IS NOT THE DICTIONARY KNOB THIS REPO WANTS, and treating it
   as one makes the encoding WORSE. It maps to `string_dictionary_page_size_limit`, which DuckDB
   already defaults to 1 GiB; Iceberg's spec default for it is 2 MB, so setting it to anything
   sane REDUCES how much stays dictionary-encoded. The knob that governs `mw` falling to `PLAIN`
   is `dictionary_size_limit` (default `row_group_size / 5` distinct values), and the one that
   would buy `RLE_DICTIONARY` + `DELTA_BINARY_PACKED` instead of `PLAIN_DICTIONARY` + `PLAIN` is
   `parquet_version` (V1 is the default and has no non-dictionary fallback but bare PLAIN).
   NEITHER IS MAPPED, on any version — so the encoding half of iceberg's cost is not reachable
   from here at all, and that is an upstream gap rather than something to configure around.

   ⚠️ IT MUST BE AT CREATE TIME, WHICH IS WHY THIS IS A MACRO AND NOT A post_hook.
   `set_iceberg_table_properties()` exists and would be one line in a `post_hook` — but a post_hook
   runs AFTER the model has written its files, and the teardown deletes the table at the end of
   every run, so the property would take effect on a write that never happens.

   ICEBERG-ONLY BY CONSTRUCTION, with no `target.name` anywhere. duckrun is a different adapter
   (`type: duckrun`), so a `duckdb__`-dispatched macro never reaches it — which is what lets the
   config live in model code both targets share, exactly as `sort_by` and the duckrun geometry
   keys already sit there inert on this side. See the header of
   models/aemo/duckdb/marts/fct_summary.sql for that argument in the other direction.

   Body copied from dbt-duckdb's `duckdb__create_table_as`
   (dbt/include/duckdb/macros/adapters.sql). The ONLY change is the `{{ iceberg_with_clause() }}`
   between the relation and `as (`; keep the rest in step when bumping the adapter. With no
   `iceberg_properties` config the clause is empty and the emitted SQL is byte-identical to the
   adapter's own — a default run is unchanged. #}
{% macro iceberg_with_clause() -%}
  {%- set props = config.get('iceberg_properties', {}) or {} -%}
  {%- if props -%}
    {#- Sorted so two runs of one config emit the same SQL; a dict's order is not a layout. -#}
    with ({% for k in props | sort %}'{{ k }}' = '{{ props[k] }}'{{ ", " if not loop.last }}{% endfor %})
  {%- endif -%}
{%- endmacro %}

{% macro duckdb__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if language == 'sql' -%}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(compiled_code) }}
    {% endif %}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    create {% if temporary: -%}temporary{%- endif %} table
      {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  {% if contract_config.enforced and not temporary %}
    {#-- DuckDB doesnt support constraints on temp tables --#}
    {{ get_table_columns_and_constraints() }} ;
    insert into {{ relation }} {{ get_column_names() }} (
      {{ get_select_subquery(compiled_code) }}
    );
  {% else %}
    {#-- The properties ride here and NOWHERE ELSE: a temp relation is a plain duckdb table, and
         the duckdb catalog refuses a WITH clause outright ("WITH clause is not supported for
         tables in a duckdb catalog"), so emitting one there would fail every temp write.

         WHICH BRANCH OF THE INCREMENTAL MATERIALIZATION LANDS HERE, since that is what decides
         whether the property reaches the table anyone queries (dbt-duckdb's incremental.sql):
           - existing_relation is none -> create_table_as(False, TARGET) : the clause lands on the
             real table. THE TEARDOWN MAKES THIS EVERY RUN, which is why a macro is enough.
           - the per-run temp source  -> create_table_as(temporary=True, temp) : suppressed here,
             and must be — `temporary` is `not is_motherduck()`, i.e. always true for this target.
           - full_refresh_mode       -> create_table_as(False, INTERMEDIATE) then a RENAME : the
             clause would land on `__dbt_tmp`. Untested, and it does not matter — `--full-refresh`
             already fails on this leg every time ("Table fct_summary__dbt_tmp does not exist"),
             which CLAUDE.md records. Do not read this branch as supported. --#}
    {% if not temporary %}{{ iceberg_with_clause() }}{% endif %} as (
      {{ compiled_code }}
    );
  {% endif %}
  {%- elif language == 'python' -%}
    {{ py_write_table(temporary=temporary, relation=relation, compiled_code=compiled_code) }}
  {%- else -%}
      {% do exceptions.raise_compiler_error("duckdb__create_table_as macro didn't get supported language, it got %s" % language) %}
  {%- endif -%}
{% endmacro %}
