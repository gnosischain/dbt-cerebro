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
