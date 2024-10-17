{% macro flexible_source(schema, table) %}
    {% set env_url = env_var('CLICKHOUSE_ENV_URL', '') %}
    {% set env_port = env_var('CLICKHOUSE_ENV_PORT', '8123') %}
    {% set env_user = env_var('CLICKHOUSE_ENV_USER', '') %}
    {% set env_password = env_var('CLICKHOUSE_ENV_PASSWORD', '') %}
    
    {% if env_url and env_url != env_var('CLICKHOUSE_URL') %}
        {% set remote_source = 'remote(\'' ~ env_url ~ ':' ~ env_port ~ '\', \'' ~ schema ~ '\', \'' ~ table ~ '\', \'' ~ env_user ~ '\', \'' ~ env_password ~ '\')' %}
        {{ remote_source }}
    {% else %}
        {{ source(schema, table) }}
    {% endif %}
{% endmacro %}