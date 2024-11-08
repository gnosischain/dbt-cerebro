{% macro get_postgres(pg_db,table_name) %}
    postgresql(
        '{{ env_var("POSTGRES_HOST", "postgres") }}:{{ env_var("POSTGRES_PORT", "5432") }}',
        '{{ pg_db }}',
        '{{ table_name }}',
        '{{ env_var("POSTGRES_USER") }}',
        '{{ env_var("POSTGRES_PASSWORD") }}'
    )
{% endmacro %}
