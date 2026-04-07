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
toDecimal256(
  pow(
    toDecimal256('0.9998013320085989574306481700129226782902039065082930593676448873', 64),
    intDiv({{ now_ts }} - {{ inflation_day_zero }}, 86400)
    - intDiv({{ last_activity_ts }} - {{ inflation_day_zero }}, 86400)
  ),
  18
)
{%- endmacro %}

{% macro circles_strip_0x(value) -%}
if(
  startsWith(lower({{ value }}), '0x'),
  substring({{ value }}, 3),
  {{ value }}
)
{%- endmacro %}

{% macro circles_token_id_from_avatar(avatar) -%}
reinterpretAsUInt256(
  concat(
    reverse(
      unhex(
        lower({{ circles_strip_0x(avatar) }})
      )
    ),
    unhex(repeat('00', 12))
  )
)
{%- endmacro %}

{% macro circles_avatar_from_token_id(token_id) -%}
concat(
  '0x',
  leftPad(lower(hex(toUInt256({{ token_id }}))), 40, '0')
)
{%- endmacro %}

