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

{% macro decode_hex_tokens(column) %}
arrayFilter(
    x -> x != '',
    /* split on every “non word-ish” character (dash, @, space, etc.) */
    splitByRegexp(
        '[^A-Za-z0-9\\.]+',            -- ⇽ anything that isn’t a–z, 0–9 or “.”
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
