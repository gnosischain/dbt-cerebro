{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
    ) 
}}

SELECT
    date,
    q_balance[1] AS q05,
    q_balance[2] AS q10,
    q_balance[3] AS q25,
    q_balance[4] AS q50,
    q_balance[5] AS q75,
    q_balance[6] AS q90,
    q_balance[7] AS q95
FROM (
    SELECT
        toStartOfDay(slot_timestamp) AS date,
       quantilesTDigest(-- quantilesExactExclusive(
            0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 1
        )(balance/POWER(10,9)) AS q_balance
    FROM {{ source('consensus', 'validators') }}
    WHERE 
        status = 'active_ongoing'
        AND
        slot_timestamp < today()
        {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
    GROUP BY date
)