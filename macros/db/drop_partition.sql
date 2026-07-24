{% macro list_partitions(database, table_name) %}
  {# Read-only: log active partitions (value, id, row count, part count) for a table.
     Use to verify the partition-key format before drop_partition, and to confirm a
     drop/rebuild landed. #}
  {% set sql %}
    SELECT partition, partition_id, sum(rows) AS rows, count() AS parts
    FROM system.parts
    WHERE active = 1
      AND database = '{{ database }}'
      AND table = '{{ table_name }}'
    GROUP BY partition, partition_id
    ORDER BY partition
  {% endset %}
  {% if execute %}
    {% set rows = run_query(sql).rows %}
    {% do log("partitions for " ~ database ~ "." ~ table_name ~ " (" ~ (rows | length) ~ " active):", info=True) %}
    {% for r in rows %}
      {% do log("  partition=" ~ r[0] ~ " id=" ~ r[1] ~ " rows=" ~ r[2] ~ " parts=" ~ r[3], info=True) %}
    {% endfor %}
  {% endif %}
{% endmacro %}


{% macro drop_partition(database, table_name, partition) %}
  {# Run `ALTER TABLE <db>.<table> DROP PARTITION '<p>'`. Physically removes a whole
     partition's parts (unlike delete+insert, which is unique_key-scoped and leaves
     sparse-dropped / stale rows behind). Use before a bounded rebuild when the corrected
     grain-state for some keys is "no row" (e.g. cumulative balance models where a holder
     fully exited within the window). alter_sync=2 waits for the change to apply. #}
  {% set sql %}
    ALTER TABLE {{ database }}.{{ table_name }}
    DROP PARTITION '{{ partition }}'
    SETTINGS alter_sync = 2
  {% endset %}
  {% do log("ALTER TABLE " ~ database ~ "." ~ table_name ~ " DROP PARTITION '" ~ partition ~ "' …", info=True) %}
  {% do run_query(sql) %}
  {% do log("DROP PARTITION '" ~ partition ~ "' on " ~ database ~ "." ~ table_name ~ " completed", info=True) %}
{% endmacro %}

{% macro drop_view(database, view_name) %}
  {# Drop a stale VIEW left behind after a model rename/retire (dbt does not
     drop removed models). Guarded: refuses anything that is not engine=View,
     so a mistyped name can never drop a physical table. #}
  {% set check_sql %}
    SELECT engine FROM system.tables
    WHERE database = '{{ database }}' AND name = '{{ view_name }}'
  {% endset %}
  {% if execute %}
    {% set rows = run_query(check_sql).rows %}
    {% if rows | length == 0 %}
      {% do log(database ~ "." ~ view_name ~ " does not exist — nothing to drop", info=True) %}
    {% elif rows[0][0] != 'View' %}
      {% do exceptions.raise_compiler_error(database ~ "." ~ view_name ~ " is engine=" ~ rows[0][0] ~ ", not a View — refusing to drop") %}
    {% else %}
      {% do run_query("DROP VIEW " ~ database ~ ".`" ~ view_name ~ "`") %}
      {% do log("Dropped view " ~ database ~ "." ~ view_name, info=True) %}
    {% endif %}
  {% endif %}
{% endmacro %}
