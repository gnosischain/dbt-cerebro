{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        engine='ReplacingMergeTree()',
        order_by='(label, date)',
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


-- NOTE (2026-07): raw `amount` is Gwei-of-mGNO, matching the same convention
-- as int_consensus_validators_income_daily (Gnosis Beacon Chain mirrors
-- Ethereum's 32-unit-per-validator convention; 32 mGNO = 1 real GNO).
-- Confirmed empirically: the modal raw deposit amount in
-- stg_consensus__execution_requests is exactly 32,000,000,000, and every
-- common value is a clean multiple of it (real depositors making round
-- 1/2/9/10/12-GNO deposits). /POWER(10,9) alone only reaches mGNO scale; the
-- extra /32 below converts to real GNO.
SELECT
    label
    ,date
    ,SUM(amount/POWER(10,9)) / 32 AS total_amount
    ,SUM(cnt) AS cnt
FROM (
    SELECT 'Deposits' AS label, * FROM deposists
    UNION ALL
    SELECT 'Deposits' AS label, * FROM deposists_requests
    UNION ALL
    SELECT 'Withdrawals' AS label, * FROM withdrawals
)
GROUP BY label, date 
