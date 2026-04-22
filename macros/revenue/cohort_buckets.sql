{#
    Cohort bucket expressions for the gnosis_revenue cohort marts.
    `amount_col` is the per-user fees value (rolling-52w or monthly).
    The bucket set is intentionally identical to the Dune definitions.
#}

{% macro cohort_bucket_yearly(amount_col, include_below_one=true) %}
    multiIf(
        {% if include_below_one %}
        {{ amount_col }} < 1,                       '<1',
        {% endif %}
        {{ amount_col }} < 3,                        '1-3',
        {{ amount_col }} < 6,                        '3-6',
        {{ amount_col }} < 10,                       '6-10',
        {{ amount_col }} < 100,                      '10-100',
        '>=100'
    )
{% endmacro %}

{% macro cohort_bucket_monthly(amount_col) %}
    multiIf(
        {{ amount_col }} < 0.01,                     '<0.01',
        {{ amount_col }} < 0.1,                      '0.01-0.1',
        {{ amount_col }} < 0.5,                      '0.1-0.5',
        {{ amount_col }} < 1,                        '0.5-1',
        {{ amount_col }} < 3,                        '1-3',
        {{ amount_col }} < 6,                        '3-6',
        {{ amount_col }} < 10,                       '6-10',
        {{ amount_col }} < 100,                      '10-100',
        '>=100'
    )
{% endmacro %}
