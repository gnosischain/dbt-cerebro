{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        partition_by='partition_month',
        order_by='(day, label)',
        unique_key='(day, label)',
        settings={
            "allow_nullable_key": 1
        }
    ) 
}}

WITH 

{{ get_incremental_filter() }}

validators_activations AS (
    SELECT 
        toDate(activation_time) AS day
        ,CAST(COUNT(*) AS Int64) AS cnt
        ,'activations' AS label
    FROM {{ ref('consensus_validators_queue') }}
    {{ apply_incremental_filter('f_eth1_block_timestamp') }}
    GROUP BY 1, 3
),

validators_exits AS (
    SELECT 
        toDate(exit_time) AS day
        ,-CAST(COUNT(*) AS Int64) AS cnt
        ,'exits' AS label
    FROM {{ ref('consensus_validators_queue') }}
    WHERE
        exit_time IS NOT NULL
        {{ apply_incremental_filter('f_eth1_block_timestamp', add_and=true) }}
    GROUP BY 1, 3
),

final AS (
    SELECT 
        day,
        cnt,
        label
    FROM validators_activations
    WHERE day IS NOT NULL

    UNION ALL

    SELECT 
        day,
        cnt,
        label
    FROM validators_exits
    WHERE day IS NOT NULL
)

SELECT
    toStartOfMonth(day) AS partition_month
    ,day
    ,cnt
    ,label
FROM 
    final
WHERE
    day < (SELECT MAX(day) FROM final)
