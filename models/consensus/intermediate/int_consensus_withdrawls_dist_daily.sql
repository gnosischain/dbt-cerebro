{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "withdrawals"]
    )
}}


SELECT
    date
    ,total_amount
    ,cnt
    ,q_amount[1] AS min
    ,q_amount[2] AS q05
    ,q_amount[3] AS q10
    ,q_amount[4] AS q25
    ,q_amount[5] AS q50
    ,q_amount[6] AS q75
    ,q_amount[7] AS q90
    ,q_amount[8] AS q95
    ,q_amount[9] AS max
FROM (
    SELECT
        toStartOfDay(slot_timestamp) AS date
        ,SUM(amount/POWER(10,9)) AS total_amount
        ,COUNT(*) AS cnt
        ,quantilesTDigest(
            0.0, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 1
        )(amount/POWER(10,9)) AS q_amount
    FROM {{ ref('stg_consensus__withdrawals') }}
    WHERE
        slot_timestamp < today()
        {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
    GROUP BY 1
)