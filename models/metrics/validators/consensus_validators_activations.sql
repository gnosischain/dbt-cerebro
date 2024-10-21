{{ 
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by='toYYYYMM(coalesce(month, toDate(\'1970-01-01\')))'
    ) 
}}

WITH validators_activations AS (
    SELECT 
        toDate(activation_time) AS day,
        toStartOfMonth(toDate(activation_time)) AS month,
        CAST(COUNT(*) AS Int64) AS cnt,
        'activations' AS label
    FROM {{ ref('consensus_validators_status') }}
    {% if is_incremental() %}
    WHERE toDate(activation_time) >= (SELECT max(day) FROM {{ this }})
    {% endif %}
    GROUP BY 1, 2
),

validators_exits AS (
    SELECT 
        toDate(withdrawable_time) AS day,
        toStartOfMonth(toDate(withdrawable_time)) AS month,
        -CAST(COUNT(*) AS Int64) AS cnt,
        'exits' AS label
    FROM {{ ref('consensus_validators_status') }}
    WHERE
        exit_time IS NOT NULL
    {% if is_incremental() %}
        AND toDate(exit_time) >= (SELECT max(day) FROM {{ this }})
    {% endif %}
    GROUP BY 1, 2
)

SELECT 
    day,
    coalesce(month, toDate('1970-01-01')) AS month,
    cnt,
    label
FROM validators_activations

UNION ALL

SELECT 
    day,
    coalesce(month, toDate('1970-01-01')) AS month,
    cnt,
    label
FROM validators_exits

ORDER BY day, label