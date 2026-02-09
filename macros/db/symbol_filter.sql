{# ================================================================
   symbol_filter.sql - Flexible symbol filtering macro
   
   Handles both single values and comma-separated lists for
   symbol include/exclude filtering.
   
   Usage:
     {{ symbol_filter('symbol', symbol_var, 'include') }}
     {{ symbol_filter('symbol', symbol_exclude_var, 'exclude') }}
   
   Examples:
     symbol = "USDC"           -> AND symbol = 'USDC'
     symbol = "USDC,USDT,DAI"  -> AND symbol IN ('USDC', 'USDT', 'DAI')
     symbol_exclude = "A,B,C"  -> AND symbol NOT IN ('A', 'B', 'C')
================================================================ #}

{% macro symbol_filter(column, value, mode='include') %}
  {% if value is not none and value != '' %}
    {% set values = value.split(',') %}
    {% if mode == 'exclude' %}
      AND {{ column }} NOT IN (
        {% for v in values %}
          '{{ v | trim }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
      )
    {% elif values | length == 1 %}
      AND {{ column }} = '{{ values[0] | trim }}'
    {% else %}
      AND {{ column }} IN (
        {% for v in values %}
          '{{ v | trim }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
      )
    {% endif %}
  {% endif %}
{% endmacro %}