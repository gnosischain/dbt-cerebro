{% macro flexible_source(schema, table, env='dev') %}
    {% if env not in ['dev', 'prod'] %}
        {{ exceptions.raise_compiler_error("Invalid environment. Choose either 'dev' or 'prod'.") }}
    {% endif %}

    {% set env_prefix = 'CLICKHOUSE_' ~ env | upper ~ '_' %}

    {% set env_url = env_var(env_prefix ~ 'URL', '') %}
    {% set env_port = '9440' %}
    {% set env_user = env_var(env_prefix ~ 'USER', '') %}
    {% set env_password = env_var(env_prefix ~ 'PASSWORD', '') %}

    {% if env_url and env_url != env_var('CLICKHOUSE_URL') %}
        {% set remote_source = 'remoteSecure(\'' ~ env_url ~ ':' ~ env_port ~ '\', \'' ~ schema ~ '\', \'' ~ table ~ '\', \'' ~ env_user ~ '\', \'' ~ env_password ~ '\') 
            SETTINGS
                connect_timeout_with_failover_secure_ms = 60000,
                receive_timeout = 120000,                        
                send_timeout = 120000  ' %}
        {{ remote_source }}
    {% else %}
        {{ source(schema, table) }}
    {% endif %}
{% endmacro %}
