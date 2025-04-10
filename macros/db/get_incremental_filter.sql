{% macro get_incremental_filter() %}
    {% if is_incremental() %}
    last_partition AS (
        SELECT max(partition_month) as partition_month
        FROM {{ this }}
    ),
    {% endif %}
{% endmacro %}

{% macro apply_incremental_filter(timestamp_field, add_and=false) %}
    {% if is_incremental() %}
    {{ "AND " if add_and else "WHERE "}}toStartOfMonth({{ timestamp_field }}) >= (SELECT partition_month FROM last_partition)
    {% endif %}
{% endmacro %}

{% macro apply_monthly_incremental_filter(timestamp_field, add_and=false) %}
    {% if is_incremental() %}
        {{ "AND " if add_and else "WHERE " }}toStartOfMonth({{ timestamp_field }}) >= (
            SELECT max(toStartOfMonth({{ timestamp_field }})) FROM {{ this }}
        )
    {% endif %}
{% endmacro %}