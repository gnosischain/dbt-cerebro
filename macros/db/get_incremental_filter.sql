{% macro apply_monthly_incremental_filter(source_field, destination_field=None, add_and=False) %}
  {% if is_incremental() %}
    {% if destination_field is none %}
      {% set dest_field = source_field %}
    {% else %}
      {% set dest_field = destination_field %}
    {% endif %}

   {{ "AND " if add_and else "WHERE " }}
    toStartOfMonth(toStartOfDay({{ source_field }})) >= (
      SELECT max(toStartOfMonth(x1.{{ dest_field }}))
      FROM {{ this }} AS x1
    )
    AND toStartOfDay({{ source_field }}) >= (
      SELECT max(toStartOfDay(x2.{{ dest_field }}, 'UTC'))
      FROM {{ this }} AS x2
    )
  {% endif %}
{% endmacro %}