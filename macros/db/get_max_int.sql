{% macro get_max_int(model_name, column_name) %}
  {# Sentinel-emitting macro consumed by the microbatch runner's range-template
     auto-discovery. Returns the maximum integer value of `column_name` from
     the given model. Used to detect when new buckets need to be synthesized
     (e.g. a new validator_index range that hasn't been declared in
     meta.full_refresh.stages yet).

     Output format (parsed by dbt_incremental_runner.maybe_extend_stages):
       MAX_INT_RESULT::<model_name>::<column_name>::<integer or NULL>
  #}
  {% if execute %}
    {% set rel = ref(model_name) %}
    {% set sql %}
      SELECT toString(max({{ column_name }}))
      FROM {{ rel }} FINAL
    {% endset %}
    {% set result = run_query(sql) %}
    {% set value = 'NULL' %}
    {% if result and result.rows and result.rows | length > 0 and result.rows[0][0] is not none %}
      {% set value = result.rows[0][0] %}
    {% endif %}
    {% do log("MAX_INT_RESULT::" ~ model_name ~ "::" ~ column_name ~ "::" ~ value, info=True) %}
  {% endif %}
{% endmacro %}


{% macro get_distinct_values(model_name, column_name, max_values=1000) %}
  {# Enumerate distinct values of `column_name` from the given model. Used by
     the runner to expand enum-keyed stages from a `range_template` block.

     Output (one sentinel line per value, deterministic order):
       DISTINCT_VALUE::<model_name>::<column_name>::<value>
     Followed by a sentinel marking end-of-stream:
       DISTINCT_VALUES_END::<model_name>::<column_name>::<count>

     `max_values` guards against accidentally enumerating high-cardinality
     columns (would produce thousands of stages). Default 1000 — runner will
     warn and refuse if the actual count exceeds this.
  #}
  {% if execute %}
    {% set rel = ref(model_name) %}
    {% set sql %}
      SELECT toString({{ column_name }}) AS v
      FROM {{ rel }} FINAL
      WHERE {{ column_name }} IS NOT NULL
      GROUP BY {{ column_name }}
      ORDER BY v
      LIMIT {{ max_values + 1 }}
    {% endset %}
    {% set result = run_query(sql) %}
    {% set rows = result.rows if (result and result.rows) else [] %}
    {% if rows | length > max_values %}
      {% do log("DISTINCT_VALUES_OVERFLOW::" ~ model_name ~ "::" ~ column_name ~ "::" ~ max_values, info=True) %}
    {% else %}
      {% for row in rows %}
        {% do log("DISTINCT_VALUE::" ~ model_name ~ "::" ~ column_name ~ "::" ~ row[0], info=True) %}
      {% endfor %}
      {% do log("DISTINCT_VALUES_END::" ~ model_name ~ "::" ~ column_name ~ "::" ~ rows | length, info=True) %}
    {% endif %}
  {% endif %}
{% endmacro %}


{% macro get_first_seen_date(model_name, date_column, where_filter='') %}
  {# Find the earliest `date_column` value matching an optional WHERE filter.
     Used by the runner to bootstrap a newly-synthesized stage from the moment
     its key range first appeared in the upstream — avoids "today" or "1970"
     bootstrap policies that lose history or backfill too aggressively.

     Output:
       FIRST_SEEN_DATE_RESULT::<model_name>::<date_column>::<YYYY-MM-DD or NULL>
  #}
  {% if execute %}
    {% set rel = ref(model_name) %}
    {% set sql %}
      SELECT toString(min(toDate({{ date_column }})))
      FROM {{ rel }} FINAL
      {% if where_filter %}
      WHERE {{ where_filter }}
      {% endif %}
    {% endset %}
    {% set result = run_query(sql) %}
    {% set value = 'NULL' %}
    {% if result and result.rows and result.rows | length > 0 and result.rows[0][0] is not none and result.rows[0][0] != '' %}
      {% set value = result.rows[0][0] %}
    {% endif %}
    {% do log("FIRST_SEEN_DATE_RESULT::" ~ model_name ~ "::" ~ date_column ~ "::" ~ value, info=True) %}
  {% endif %}
{% endmacro %}
