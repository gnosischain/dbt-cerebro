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


{% macro kill_failed_mutations() %}
    {# Kill ClickHouse mutations stuck in a failed state.
       Incremental models use DELETE + INSERT which creates mutations.
       If a previous run crashed, the mutation references a temp table
       (__dbt_new_data_*) that no longer exists, poisoning the table
       so all future writes fail with Code 341. #}
    {% set failed_query %}
        SELECT database, table, mutation_id,
               splitByChar('\n', latest_fail_reason)[1] as reason
        FROM system.mutations
        WHERE is_done = 0
          AND latest_fail_reason != ''
          AND database = 'dbt'
    {% endset %}

    {% set failed = run_query(failed_query).rows %}
    {% if failed | length > 0 %}
        {{ log("Found " ~ failed | length ~ " failed mutations to kill", info=True) }}
    {% endif %}

    {% for row in failed %}
        {% set db = row[0] %}
        {% set tbl = row[1] %}
        {% set mid = row[2] %}
        {{ log("Killing failed mutation " ~ mid ~ " on " ~ db ~ "." ~ tbl, info=True) }}
        {% do run_query("KILL MUTATION WHERE database = '" ~ db ~ "' AND table = '" ~ tbl ~ "' AND mutation_id = '" ~ mid ~ "'") %}
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
