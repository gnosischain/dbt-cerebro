{% macro get_chain_spec(spec_key) %}
(SELECT f_value FROM {{ get_postgres('gnosis_chaind', 't_chain_spec') }} WHERE f_key = '{{ spec_key }}' LIMIT 1)
{% endmacro %}