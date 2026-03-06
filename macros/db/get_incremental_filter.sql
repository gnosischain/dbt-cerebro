{% macro apply_monthly_incremental_filter(source_field, destination_field=None, add_and=False, lookback_days=1, filters_sql='') %}
  {% if is_incremental() %}
    {% set dest_field = destination_field if destination_field is not none else source_field %}
    {% set lb_days = lookback_days - 1 %}

   {{ "AND " if add_and else "WHERE " }}
    toStartOfMonth(toDate({{ source_field }})) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.{{ dest_field }})), -{{ lb_days }}))
      FROM {{ this }} AS x1
      WHERE 1=1 {{ filters_sql }}
    )
    AND toDate({{ source_field }}) >= (
      SELECT addDays(max(toDate(x2.{{ dest_field }})), -{{ lb_days }})
      FROM {{ this }} AS x2
      WHERE 1=1 {{ filters_sql }}
    )
  {% endif %}
{% endmacro %}
