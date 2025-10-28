{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(label, date)',
        unique_key='(label, date)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "deposits"]
    )
}}

WITH

deposists AS (
    SELECT 
        toStartOfDay(slot_timestamp) AS date
        ,SUM(amount) AS amount
        ,COUNT(*) AS cnt
    FROM {{ ref('stg_consensus__deposits') }}
    WHERE
        slot_timestamp < today()
        {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
    GROUP BY 1
),

deposists_requests AS (
    SELECT
        toStartOfDay(slot_timestamp) AS date
        ,SUM(toUInt64(JSONExtractString(deposit, 'amount'))) AS amount
        ,COUNT() AS cnt
    FROM {{ ref('stg_consensus__execution_requests') }}
    ARRAY JOIN JSONExtractArrayRaw(payload, 'deposits') AS deposit
    WHERE
        slot_timestamp < today()
        {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
    GROUP BY 1
),

withdrawals AS (
    SELECT 
        toStartOfDay(slot_timestamp) AS date 
        ,SUM(amount) AS amount
        ,COUNT(*) AS cnt
    FROM {{ ref('stg_consensus__withdrawals') }}
    WHERE
        slot_timestamp < today()
        {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
    GROUP BY 1
)


SELECT
    label
    ,date
    ,SUM(amount/POWER(10,9)) AS total_amount
    ,SUM(cnt) AS cnt
FROM (
    SELECT 'Deposits' AS label, * FROM deposists
    UNION ALL
    SELECT 'Deposits' AS label, * FROM deposists_requests
    UNION ALL
    SELECT 'Withdrawals' AS label, * FROM withdrawals
)
GROUP BY label, date 
