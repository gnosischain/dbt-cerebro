{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)'
    )
}}


WITH

state_size_diff_daily AS (
    SELECT 
        date
        ,bytes_diff
    FROM 
        {{ ref('execution_state_size_diff_daily') }}
    {{ apply_monthly_incremental_filter('date','date') }}
),

{% if is_incremental() %}
last_partition_value AS (
    SELECT 
        bytes
    FROM 
        {{ this }}
    WHERE
        toStartOfMonth(date) = (
            SELECT addMonths(max(toStartOfMonth(date)), -1)
            FROM {{ this }}
        )
    ORDER BY date DESC
    LIMIT 1
),
{% endif %}

final AS (
    SELECT
        date
        ,SUM(bytes_diff) OVER (ORDER BY date ASC) 
        {% if is_incremental() %}
            + (SELECT bytes FROM last_partition_value)
        {% endif %}
        AS bytes
    FROM state_size_diff_daily
)

SELECT * FROM final

        
