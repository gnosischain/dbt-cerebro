{% macro dedup_source(source_ref, partition_by, columns='*', pre_filter=none) %}
{#
  Deduplicates a source table using ROW_NUMBER with an optional pre-filter
  that runs BEFORE the window function, enabling ClickHouse partition pruning.

  Args:
    source_ref  : source() or ref() expression
    partition_by: comma-separated dedup key columns (e.g. 'block_number, log_index')
    columns     : columns to SELECT (default '*')
    pre_filter  : SQL WHERE clause body (without WHERE keyword), applied before ROW_NUMBER
#}
SELECT {{ columns }}
FROM (
    SELECT
        {{ columns }},
        ROW_NUMBER() OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY insert_version DESC
        ) AS _dedup_rn
    FROM {{ source_ref }}
    {% if pre_filter is not none and pre_filter | trim != '' %}
    WHERE {{ pre_filter }}
    {% endif %}
)
WHERE _dedup_rn = 1
{% endmacro %}
