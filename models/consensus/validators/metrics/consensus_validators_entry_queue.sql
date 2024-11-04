{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        partition_by='partition_month',
        order_by='(day)',
        unique_key='(day)',
        settings={
            "allow_nullable_key": 1
        }
    ) 
}}

WITH 

{{ get_incremental_filter() }}

validators_in_activation_queue AS (
    SELECT 
        toDate(f_eth1_block_timestamp) AS day
        ,CAST(COUNT(*) AS Int64) AS cnt
    FROM {{ ref('consensus_validators_queue') }}
    {{ apply_incremental_filter('f_eth1_block_timestamp') }}
    GROUP BY 1
),

validators_activated AS (
    SELECT 
        toDate(activation_time) AS day
        ,CAST(COUNT(*) AS Int64) AS cnt
    FROM {{ ref('consensus_validators_queue') }}
    {{ apply_incremental_filter('f_eth1_block_timestamp') }}
    GROUP BY 1
),

all_dates AS (
    SELECT arrayJoin(
        arrayMap(
            d -> toDate(min_date + d),
            range(0, dateDiff('day', min_date, max_date) + 1)
        )
    ) AS day
    FROM (
        SELECT 
            min(dates.day) as min_date,
            max(dates.day) as max_date
        FROM (
            SELECT day FROM validators_in_activation_queue
            UNION ALL
            SELECT day FROM validators_activated
        ) dates
        WHERE dates.day IS NOT NULL
    )
    WHERE min_date IS NOT NULL 
        AND max_date IS NOT NULL
),

daily_metrics AS (
    SELECT
        c.day AS day
        ,COALESCE(q.cnt, 0) AS validators_entered_queue
        ,COALESCE(a.cnt, 0) AS validators_activated
        ,COALESCE(q.cnt, 0) - COALESCE(a.cnt, 0) AS net_queue_change
    FROM all_dates c
    LEFT JOIN validators_in_activation_queue q ON c.day = q.day
    LEFT JOIN validators_activated a ON c.day = a.day
    WHERE c.day IS NOT NULL
)

SELECT
    toStartOfMonth(day) AS partition_month
    ,day
    ,validators_entered_queue
    ,validators_activated
    ,net_queue_change
FROM 
    daily_metrics
WHERE
    day < (SELECT MAX(day) FROM daily_metrics)