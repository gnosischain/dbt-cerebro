{% macro bytea_to_bigint(bytea_input) %}
  cast(
    bitwise_left_shift(cast(from_base(to_hex(substr({{ bytea_input }}, 1, 1)), 16) as bigint), 56) +
    bitwise_left_shift(cast(from_base(to_hex(substr({{ bytea_input }}, 2, 1)), 16) as bigint), 48) +
    bitwise_left_shift(cast(from_base(to_hex(substr({{ bytea_input }}, 3, 1)), 16) as bigint), 40) +
    bitwise_left_shift(cast(from_base(to_hex(substr({{ bytea_input }}, 4, 1)), 16) as bigint), 32) +
    bitwise_left_shift(cast(from_base(to_hex(substr({{ bytea_input }}, 5, 1)), 16) as bigint), 24) +
    bitwise_left_shift(cast(from_base(to_hex(substr({{ bytea_input }}, 6, 1)), 16) as bigint), 16) +
    bitwise_left_shift(cast(from_base(to_hex(substr({{ bytea_input }}, 7, 1)), 16) as bigint), 8) +
    cast(from_base(to_hex(substr({{ bytea_input }}, 8, 1)), 16) as bigint)
  as bigint) 
{% endmacro %}
