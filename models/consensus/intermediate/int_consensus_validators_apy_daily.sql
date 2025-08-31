{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "validators_apy"]
    ) 
}}

WITH

deposists AS (
    SELECT 
        toStartOfDay(slot_timestamp) AS date
        ,SUM(amount) AS amount
    FROM {{ ref('stg_consensus__deposits') }}
    WHERE 
        slot_timestamp < toDate('2025-04-30')
        {{ apply_monthly_incremental_filter(source_field='slot_timestamp',destination_field='date',add_and=true) }}
    GROUP BY 1

    UNION ALL 

    SELECT 
        date
        ,SUM(amount) AS amount
    FROM {{ ref('int_GBCDeposit_deposists_daily') }}
    WHERE 
        date >= toDate('2025-04-30')
        {{ apply_monthly_incremental_filter(source_field='date',destination_field='date',add_and=true) }}
    GROUP BY 1
),

deposists_requests AS (
    SELECT
        toStartOfDay(slot_timestamp) AS date,
        SUM(toUInt64(JSONExtractString(deposit, 'amount'))) AS amount
    FROM {{ ref('stg_consensus__execution_requests') }}
    ARRAY JOIN JSONExtractArrayRaw(payload, 'deposits') AS deposit
    {{ apply_monthly_incremental_filter(source_field='slot_timestamp',destination_field='date',add_and=false) }}
    GROUP BY 1
),


withdrawals AS (
    SELECT 
        toStartOfDay(slot_timestamp) AS date 
        ,SUM(amount) AS amount
    FROM {{ ref('stg_consensus__withdrawals') }}
    {{ apply_monthly_incremental_filter(source_field='slot_timestamp',destination_field='date',add_and=false) }}
    GROUP BY 1
),

withdrawals_requests AS (
    SELECT
        toStartOfDay(slot_timestamp) AS date,
        SUM(toUInt64(JSONExtractString(withdrawals, 'amount'))) AS amount
    FROM {{ ref('stg_consensus__execution_requests') }}
    ARRAY JOIN JSONExtractArrayRaw(payload, 'withdrawals') AS withdrawals
    {{ apply_monthly_incremental_filter(source_field='slot_timestamp',destination_field='date',add_and=false) }}
    GROUP BY 1
),

validators AS (
    SELECT
        date,
        balance,
        lagInFrame(balance, 1, balance) OVER (
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS prev_balance,
        balance - prev_balance AS balance_diff
    FROM (
        SELECT
            toStartOfDay(slot_timestamp) AS date,
            SUM(balance) AS balance
        FROM {{ ref('stg_consensus__validators') }}
        {{ apply_monthly_incremental_filter(source_field='slot_timestamp',destination_field='date',add_and=false) }}
        GROUP BY 1
    )
)

SELECT 
    t1.date AS date
    ,t1.balance AS balance
    ,t1.balance_diff AS balance_diff_original
    ,COALESCE(t2.amount,0)  AS deposited_amount
    ,COALESCE(t3.amount,0)  AS withdrawaled_amount
    ,COALESCE(t4.amount,0)  AS deposited_amount_request
    ,COALESCE(t5.amount,0)  AS withdrawaled_amount_request
    ,t1.balance_diff - COALESCE(t2.amount,0) - COALESCE(t4.amount,0) + COALESCE(t3.amount,0) + COALESCE(t5.amount,0) AS eff_balance_diff
    ,eff_balance_diff/t1.prev_balance AS rate
    ,ROUND((POWER((1+rate),365) - 1) * 100,2) AS apy
FROM validators t1
LEFT JOIN 
    deposists t2
    ON t2.date = t1.date
LEFT JOIN 
    withdrawals t3
    ON t3.date = t1.date
LEFT JOIN 
    deposists_requests t4
    ON t4.date = t1.date
LEFT JOIN 
    withdrawals_requests t5
    ON t5.date = t1.date
