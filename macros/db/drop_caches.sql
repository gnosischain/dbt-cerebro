{% macro drop_clickhouse_caches() %}
  {% do run_query("SYSTEM DROP MARK CACHE") %}
  {% do run_query("SYSTEM DROP UNCOMPRESSED CACHE") %}
  {% do run_query("SYSTEM DROP COMPILED EXPRESSION CACHE") %}
  {% do log("Dropped MARK, UNCOMPRESSED, COMPILED EXPRESSION caches", info=True) %}
{% endmacro %}
