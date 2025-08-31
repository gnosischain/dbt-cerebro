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

/* 1) Daily per-validator balance snapshot (already 1 row/day) */
daily_validator_balances AS (
    SELECT
        toStartOfDay(slot_timestamp) AS date,
        pubkey,
        validator_index,
        balance
    FROM {{ ref('stg_consensus__validators') }}
    WHERE 
        balance > 0
        AND
        toStartOfDay(slot_timestamp) >= DATE '2023-01-01'
        AND
        toStartOfDay(slot_timestamp) < DATE '2023-02-01'
),

/* 2) True previous-day balance per validator using a window function */
validator_with_prev AS (
    SELECT
        date,
        pubkey,
        validator_index,
        balance,
        -- default value = current balance on first day
        lagInFrame(balance, 1, balance) OVER (
            PARTITION BY pubkey, validator_index
            ORDER BY date
        ) AS prev_balance
    FROM daily_validator_balances
),

/* 3) Get date range to filter other tables - materialize this first */
date_range AS (
    SELECT 
        min(date) AS min_date,
        max(date) AS max_date
    FROM validator_with_prev
),

/* 4) Per-day deposits - direct pubkey and amount columns */
deposits AS (
    SELECT
        toStartOfDay(d.slot_timestamp) AS dep_date,
        d.pubkey AS dep_pubkey,
        sum(d.amount) AS dep_amount
    FROM {{ ref('stg_consensus__deposits') }} d
    CROSS JOIN date_range dr
    WHERE toStartOfDay(d.slot_timestamp) >= dr.min_date 
      AND toStartOfDay(d.slot_timestamp) <= dr.max_date
    GROUP BY toStartOfDay(d.slot_timestamp), d.pubkey
),

/* 5) Deposit requests - use JSON extraction with explicit date range */
deposit_requests AS (
    SELECT
        toStartOfDay(dr_table.slot_timestamp) AS dr_date,
        toString(JSONExtractString(deposit, 'pubkey')) AS dr_pubkey,
        sum(toUInt64(JSONExtractString(deposit, 'amount'))) AS dep_req_amount
    FROM {{ ref('stg_consensus__execution_requests') }} dr_table
    ARRAY JOIN JSONExtractArrayRaw(dr_table.payload, 'deposits') AS deposit
    CROSS JOIN date_range dr
    WHERE toStartOfDay(dr_table.slot_timestamp) >= dr.min_date 
      AND toStartOfDay(dr_table.slot_timestamp) <= dr.max_date
    GROUP BY toStartOfDay(dr_table.slot_timestamp), toString(JSONExtractString(deposit, 'pubkey'))
),

/* 6) Withdrawals - uses validator_index, not pubkey */
withdrawals AS (
    SELECT
        toStartOfDay(w.slot_timestamp) AS w_date,
        w.validator_index AS w_validator_index,
        sum(w.amount) AS wdr_amount
    FROM {{ ref('stg_consensus__withdrawals') }} w
    CROSS JOIN date_range dr
    WHERE toStartOfDay(w.slot_timestamp) >= dr.min_date 
      AND toStartOfDay(w.slot_timestamp) <= dr.max_date
    GROUP BY toStartOfDay(w.slot_timestamp), w.validator_index
),

/* 7) Withdrawal requests - use JSON extraction with explicit date range */
withdrawal_requests AS (
    SELECT
        toStartOfDay(wr_table.slot_timestamp) AS wr_date,
        toString(JSONExtractString(withdrawals, 'validator_pubkey')) AS wr_pubkey,
        sum(toUInt64(JSONExtractString(withdrawals, 'amount'))) AS wdr_req_amount
    FROM {{ ref('stg_consensus__execution_requests') }} wr_table
    ARRAY JOIN JSONExtractArrayRaw(wr_table.payload, 'withdrawals') AS withdrawals
    CROSS JOIN date_range dr
    WHERE toStartOfDay(wr_table.slot_timestamp) >= dr.min_date 
      AND toStartOfDay(wr_table.slot_timestamp) <= dr.max_date
    GROUP BY toStartOfDay(wr_table.slot_timestamp), toString(JSONExtractString(withdrawals, 'validator_pubkey'))
),

/* 8) Per-validator daily net change excluding external flows */
validator_rates AS (
    SELECT
        v.date,
        v.pubkey,
        v.validator_index,
        v.prev_balance,
        v.balance,
        (v.balance - v.prev_balance) AS raw_diff,

        coalesce(d.dep_amount, 0) AS deposits_amt,
        coalesce(dr.dep_req_amount, 0) AS deposit_req_amt,
        coalesce(w.wdr_amount, 0) AS withdrawals_amt,
        coalesce(wr.wdr_req_amount, 0) AS withdrawal_req_amt,

        /* Adjust for external flows (requests set to 0 unless you want them) */
        (
            (v.balance - v.prev_balance)
            - coalesce(d.dep_amount, 0)   -- deposits increase balance -> subtract to isolate rewards
            + coalesce(w.wdr_amount, 0)   -- withdrawals decrease balance -> add back
            - coalesce(dr.dep_req_amount, 0)
            + coalesce(wr.wdr_req_amount, 0)
        ) AS adjusted_diff,

        /* Daily rate per validator; guard against division by zero. */
        CASE 
            WHEN v.prev_balance > 0 
            THEN toFloat64(adjusted_diff) / toFloat64(v.prev_balance)
            ELSE toFloat64(0)
        END AS rate
    FROM validator_with_prev v
    LEFT JOIN deposits d ON d.dep_date = v.date AND d.dep_pubkey = v.pubkey
    LEFT JOIN deposit_requests dr ON dr.dr_date = v.date AND dr.dr_pubkey = v.pubkey
    LEFT JOIN withdrawals w ON w.w_date = v.date AND w.w_validator_index = v.validator_index
    LEFT JOIN withdrawal_requests wr ON wr.wr_date = v.date AND wr.wr_pubkey = v.pubkey
    WHERE v.prev_balance > 0  -- Only consider validators with positive previous balance
        AND v.date > DATE '2023-01-01'
)


SELECT
    date,
    ROUND(q_apy[1],2) AS q05,
    ROUND(q_apy[2],2) AS q10,
    ROUND(q_apy[3],2) AS q25,
    ROUND(q_apy[4],2) AS q50,
    ROUND(q_apy[5],2) AS q75,
    ROUND(q_apy[6],2) AS q90,
    ROUND(q_apy[7],2) AS q95 
FROM (
    SELECT
        toStartOfMonth(date) AS date
        ,quantilesTDigest(-- quantilesExactExclusive(
            0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95
        )((power(1 + rate, 365) - 1) * 100) AS q_apy
    FROM validator_rates
    GROUP BY 1
)
