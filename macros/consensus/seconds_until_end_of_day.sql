{% macro seconds_until_end_of_day(timestamp_column) %}
(
    86400 - (
        toUInt32(modulo(toUnixTimestamp({{ timestamp_column }}), 86400))
    )
)
{% endmacro %}