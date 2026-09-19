{% macro get_max_date(model_name, date_column, where_filter='') %}
  {# Emit a sentinel log line that the microbatch runner parses out of stdout.
     Avoids the need for a separate ClickHouse client in the runner — we reuse
     the dbt profile via `dbt run-operation`. Format:
       MAX_DATE_RESULT::<model_name>::<date_column>::<YYYY-MM-DD>

     `where_filter` is an optional SQL fragment without the WHERE keyword,
     used so the runner can read max(date) PER STAGE for staged models — e.g.
     "validator_index >= 0 AND validator_index < 100000". Without it, a model
     whose stages cover disjoint key ranges with different historical maxes
     would only see the global max, leading the runner to schedule slices
     that the per-stage macro filter then expands into a huge backfill.
  
     NOTE: deliberately NOT `FINAL`. This macro only reads max() of one column, and
     deduplication cannot change a maximum -- duplicate copies of a key carry the same
     or a lower value. `FINAL` forces a full merge-on-read instead: measured 2026-09-19
     on int_rpc_state_indexer_token_balances_daily (425M rows) it died with ClickHouse
     code 241 (memory limit exceeded) while the same query without it returned in 42 s.
     The runner swallowed that failure and silently re-bootstrapped 7 slices every night
     on both large balance models, which also masked the real watermark.
  #}
  {% if execute %}
    {% set rel = ref(model_name) %}
    {% set sql %}
      SELECT toString(coalesce(max(toDate({{ date_column }})), toDate('1970-01-01')))
      FROM {{ rel }}
      {% if where_filter %}
      WHERE {{ where_filter }}
      {% endif %}
    {% endset %}
    {% set result = run_query(sql) %}
    {% set value = '1970-01-01' %}
    {% if result and result.rows and result.rows | length > 0 %}
      {% set value = result.rows[0][0] %}
    {% endif %}
    {% do log("MAX_DATE_RESULT::" ~ model_name ~ "::" ~ date_column ~ "::" ~ value, info=True) %}
  {% endif %}
{% endmacro %}
