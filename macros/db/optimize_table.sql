{% macro optimize_partition_final(database, table_name, partition) %}
  {# Run `OPTIMIZE TABLE <db>.<table> PARTITION '<p>' FINAL DEDUPLICATE`.
     Use after an append-mode rewrite of a single partition (typically the
     prices-gap recovery flow for `int_execution_tokens_balances_daily`).
     Cheaper than a full-table OPTIMIZE — touches only the named partition.
  #}
  {% set sql %}
    OPTIMIZE TABLE {{ database }}.{{ table_name }}
    PARTITION '{{ partition }}'
    FINAL DEDUPLICATE
    SETTINGS mutations_sync = 2
  {% endset %}
  {% do log("OPTIMIZE TABLE " ~ database ~ "." ~ table_name ~ " PARTITION '" ~ partition ~ "' FINAL DEDUPLICATE …", info=True) %}
  {% do run_query(sql) %}
  {% do log("OPTIMIZE TABLE " ~ database ~ "." ~ table_name ~ " PARTITION '" ~ partition ~ "' FINAL DEDUPLICATE completed", info=True) %}
{% endmacro %}


{% macro optimize_table_final(database, table_name) %}
  {# Run `OPTIMIZE TABLE <db>.<table> FINAL DEDUPLICATE` synchronously.
     Heavy: rewrites all parts of the table. Use sparingly — typically as a
     one-shot operator action after a large backfill. For routine
     housekeeping prefer `optimize_dbt_tables_by_threshold` which scopes
     to over-fragmented partitions only.
  #}
  {% set sql %}
    OPTIMIZE TABLE {{ database }}.{{ table_name }} FINAL DEDUPLICATE
    SETTINGS mutations_sync = 2
  {% endset %}
  {% do log("OPTIMIZE TABLE " ~ database ~ "." ~ table_name ~ " FINAL DEDUPLICATE …", info=True) %}
  {% do run_query(sql) %}
  {% do log("OPTIMIZE TABLE " ~ database ~ "." ~ table_name ~ " FINAL DEDUPLICATE completed", info=True) %}
{% endmacro %}


{% macro optimize_dbt_tables_by_threshold(
    threshold=50,
    dry_run=False,
    database='dbt',
    table_filter='%',
    max_partitions=200
) %}
  {# Weekly housekeeping: OPTIMIZE only the (table, partition) pairs whose
     active part count exceeds `threshold`. Per-partition OPTIMIZE is
     dramatically cheaper than full-table FINAL — it merges only that
     partition's parts, not the whole table.

     Args:
       threshold      - emit OPTIMIZE for any partition with > threshold parts.
                        Default 50 (a partition above this is a clear sign
                        background merges aren't keeping up).
       dry_run        - only log the candidates without executing.
       database       - target schema. Default 'dbt'.
       table_filter   - optional LIKE pattern to scope to specific tables.
       max_partitions - safety cap on how many OPTIMIZEs to issue per run;
                        prevents pathological cases from queueing too much
                        merge-pool work in one go.

     Failure mode notes:
       - CH 388 (background pool full): individual OPTIMIZE will raise and
         abort the run. Re-invoke later — anything not optimized this run
         will be picked up next time. Use --dry-run first to triage.

     Cron entry point: scripts/maintenance/optimize_dbt_tables.sh.
  #}
  {% set discover_sql %}
    SELECT database, table, partition_id, count() AS parts
    FROM system.parts
    WHERE active = 1
      AND database = '{{ database }}'
      AND table LIKE '{{ table_filter }}'
    GROUP BY database, table, partition_id
    HAVING parts > {{ threshold }}
    ORDER BY parts DESC
    LIMIT {{ max_partitions }}
  {% endset %}

  {% if execute %}
    {% set result = run_query(discover_sql) %}
    {% set rows = result.rows if (result and result.rows) else [] %}

    {% if rows | length == 0 %}
      {% do log("optimize_dbt_tables: no partitions exceed threshold=" ~ threshold ~ " in database=" ~ database, info=True) %}
    {% else %}
      {% do log("optimize_dbt_tables: " ~ (rows | length) ~ " partitions over threshold=" ~ threshold, info=True) %}
      {% set counter = namespace(ok=0) %}
      {% for row in rows %}
        {% set db = row[0] %}
        {% set tbl = row[1] %}
        {% set pid = row[2] %}
        {% set parts = row[3] %}
        {% set label = db ~ "." ~ tbl ~ " partition_id='" ~ pid ~ "' parts=" ~ parts %}
        {% if dry_run %}
          {% do log("[dry-run] " ~ label, info=True) %}
        {% else %}
          {% do log("OPTIMIZE " ~ label, info=True) %}
          {% set opt_sql %}
            OPTIMIZE TABLE {{ db }}.{{ tbl }}
            PARTITION ID '{{ pid }}'
            FINAL DEDUPLICATE
            SETTINGS mutations_sync = 2
          {% endset %}
          {% do run_query(opt_sql) %}
          {% set counter.ok = counter.ok + 1 %}
        {% endif %}
      {% endfor %}
      {% if not dry_run %}
        {% do log("optimize_dbt_tables: " ~ counter.ok ~ "/" ~ (rows | length) ~ " partitions optimized", info=True) %}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}
