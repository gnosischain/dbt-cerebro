{% macro decode_graffiti(f_graffiti) %}
   -- reinterpretAsString(
        unhex(
            replaceRegexpOne(
                {{ f_graffiti }},
                '^\\\\x',
                ''
            )
        )
  --  )
{% endmacro %}
