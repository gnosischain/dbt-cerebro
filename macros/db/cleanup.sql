{% macro clean_elementary_orphaned_tables() %}
    {# Drop orphaned Elementary tmp tables left behind by crashed runs.
       Elementary's on_run_end hook only cleans tables from the current run's
       cache — tables from interrupted runs persist and count toward the
       ClickHouse max_table_num_to_throw limit (default 1000). #}
    {% for db in ['elementary', 'dbt'] %}
        {% set orphan_query %}
            SELECT database, name
            FROM system.tables
            WHERE database = '{{ db }}'
              AND name LIKE '%__tmp_%'
        {% endset %}

        {% set orphans = run_query(orphan_query).rows %}
        {% if orphans | length > 0 %}
            {{ log("Found " ~ orphans | length ~ " orphaned tmp tables in " ~ db, info=True) }}
        {% endif %}

        {% for row in orphans %}
            {% set fqn = row[0] ~ '.`' ~ row[1] ~ '`' %}
            {{ log("Dropping orphaned tmp table: " ~ fqn, info=True) }}
            {% do run_query("DROP TABLE IF EXISTS " ~ fqn ~ " SYNC") %}
        {% endfor %}
    {% endfor %}
{% endmacro %}


{% macro drop_dbt_trash(database_name) %}
    {{ log("Dropping leftover dbt tables in " ~ database_name, info=True) }}

    -- 1) Gather the list of leftover tables
    {% set trash_tables_query %}
      SELECT name
      FROM system.tables
      WHERE database = '{{ database_name }}'
        AND name LIKE '%__dbt_%'
    {% endset %}

    {% set trash_tables = run_query(trash_tables_query).rows %}

    -- 2) Loop over each table name and DROP it
    {% for row in trash_tables %}
        {% set table_name = row[0] %}
        {{ log("Dropping table " ~ table_name, info=True) }}

        -- Here is where we actually run the DDL drop command
        {% do run_query("DROP TABLE IF EXISTS " ~ database_name ~ "." ~ table_name) %}
    {% endfor %}
{% endmacro %}
