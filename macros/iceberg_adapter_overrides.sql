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

   Body copied from **dbt-duckdb 1.11.0**'s `duckdb__create_table_as`. The ONLY change is the
   `{{ iceberg_with_clause() }}` between the relation and each `as (`; with no
   `iceberg_properties` config that renders empty and the SQL is byte-identical to the adapter's
   own, so a default run is unchanged.
   ⚠️ **KEEP IT ON THE VERSION CI RESOLVES, NOT THE ONE ON YOUR LAPTOP.** This was first copied
   from 1.10.1 and CI resolves 1.11.0, which added `partitioned_by=` / `sorted_by=` parameters
   AND PASSES THEM BY KEYWORD from `incremental.sql` — so the 1.10.1 signature would have failed
   the leg outright on its first build. `test_iceberg_geometry.py` pins the shared lines against
   whatever adapter is installed; that is what caught it, before any capacity was spent. #}
{% macro iceberg_with_clause() -%}
  {%- set props = config.get('iceberg_properties', {}) or {} -%}
  {%- if props -%}
    {#- Sorted so two runs of one config emit the same SQL; a dict's order is not a layout. -#}
    with ({% for k in props | sort %}'{{ k }}' = '{{ props[k] }}'{{ ", " if not loop.last }}{% endfor %})
  {%- endif -%}
{%- endmacro %}

{% macro duckdb__create_table_as(temporary, relation, compiled_code, language='sql', partitioned_by=none, sorted_by=none) -%}
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
    {#-- NO PROPERTIES ON THIS BRANCH, and it is unreachable here anyway: no model in this repo
         declares a contract. The column-list form puts WITH *after* the columns rather than after
         the relation, so the one insertion below would be in the wrong place — adding it here
         means testing it, and there is nothing to test it against. --#}
    {{ get_table_columns_and_constraints() }} ;
    {% if partitioned_by %}
      {{ duckdb__alter_table_set_partitioned_by(relation, partitioned_by) }}
    {% endif %}
    {% if sorted_by %}
      {{ duckdb__alter_table_set_sorted_by(relation, sorted_by) }}
    {% endif %}
    insert into {{ relation }} {{ get_column_names() }} (
      {{ get_select_subquery(compiled_code) }}
    );
  {% else %}
    {% if partitioned_by or sorted_by %}
    {% if not temporary %}{{ iceberg_with_clause() }}{% endif %} as (
      select * from (
        {{ compiled_code }}
      ) as model_subq
      limit 0
    );
    {% if partitioned_by %}
    {{ duckdb__alter_table_set_partitioned_by(relation, partitioned_by) }}
    {% endif %}
    {% if sorted_by %}
    {{ duckdb__alter_table_set_sorted_by(relation, sorted_by) }}
    {% endif %}
    insert into {{ relation }}
      select * from (
        {{ compiled_code }}
      ) as model_subq;
    {% else %}
    {#-- THE ONE INSERTION, and this is the branch every model here takes. --#}
    {% if not temporary %}{{ iceberg_with_clause() }}{% endif %} as (
      {{ compiled_code }}
    );
    {% endif %}
  {% endif %}
  {%- elif language == 'python' -%}
    {{ py_write_table(temporary=temporary, relation=relation, compiled_code=compiled_code, partitioned_by=partitioned_by, sorted_by=sorted_by) }}
  {%- else -%}
      {% do exceptions.raise_compiler_error("duckdb__create_table_as macro didn't get supported language, it got %s" % language) %}
  {%- endif -%}
{% endmacro %}
