{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro generate_database_name(custom_database_name, node) -%}
    {%- set default_database = target.schema -%}
    {%- if custom_database_name is none -%}
        {{ default_database }}
    {%- else -%}
        {{ custom_database_name | trim }}
    {%- endif -%}
{%- endmacro %}