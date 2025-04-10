{% macro decode_hex_split(column) %}
    arrayFilter(
      x -> x != '',
      splitByChar(
        ' ',
        arrayStringConcat(
          arrayMap(
            i -> if(
              reinterpretAsUInt8(substring(unhex({{ column }}), i, 1)) BETWEEN 32 AND 126,
              reinterpretAsString(substring(unhex({{ column }}), i, 1)),
              ' '
            ),
            range(1, length(unhex({{ column }})) + 1)
          ),
          ''
        )
      )
    )
{% endmacro %}
