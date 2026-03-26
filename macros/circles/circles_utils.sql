{% macro circles_zero_address() -%}
'0x0000000000000000000000000000000000000000'
{%- endmacro %}

{% macro circles_chain_now_ts() -%}
(
    SELECT coalesce(max(timestamp), toUInt32(toUnixTimestamp(now())))
    FROM {{ source('execution', 'blocks') }}
)
{%- endmacro %}

{% macro circles_demurrage_factor(last_activity_ts, now_ts=none, inflation_day_zero=1602720000) -%}
{% if now_ts is none %}
  {% set now_ts = circles_chain_now_ts() %}
{% endif %}
pow(
  toDecimal256('0.9998013320085989574306481700129226782902039065082930593676448873', 64),
  intDiv({{ now_ts }} - {{ inflation_day_zero }}, 86400)
  - intDiv({{ last_activity_ts }} - {{ inflation_day_zero }}, 86400)
)
{%- endmacro %}
