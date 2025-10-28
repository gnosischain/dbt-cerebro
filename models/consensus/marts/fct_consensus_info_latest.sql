{{
    config(
        materialized='view',
        tags=["production", "consensus", "info"]
    )
}}

WITH

deposits_withdrawls_latest AS (
    SELECT
        label
        ,cnt
        ,total_amount
    FROM 
        {{ ref('int_consensus_deposits_withdrawals_daily') }}
    WHERE
        date = (SELECT MAX(date) FROM {{ ref('int_consensus_deposits_withdrawals_daily') }})
),

deposits_withdrawls_7d AS (
    SELECT
        label
        ,cnt
        ,total_amount
    FROM 
        {{ ref('int_consensus_deposits_withdrawals_daily') }}
    WHERE
        date = subtractDays((SELECT MAX(date) FROM {{ ref('int_consensus_deposits_withdrawals_daily') }}), 7)
),

apy_latest AS (
    SELECT
        q50_apy AS apy
    FROM 
        {{ ref('int_consensus_validators_dists_daily') }}
    WHERE
        date = (SELECT MAX(date) FROM {{ ref('int_consensus_validators_dists_daily') }})
),

apy_7d AS (
    SELECT
       q50_apy AS apy
    FROM 
        {{ ref('int_consensus_validators_dists_daily') }}
    WHERE
        date = subtractDays((SELECT MAX(date) FROM {{ ref('int_consensus_validators_dists_daily') }}), 7)
),

status_latest AS (
    SELECT
        status AS label
        ,CAST(COALESCE(cnt,0) AS Float64) AS value
    FROM 
        {{ ref('int_consensus_validators_status_daily') }}
    WHERE
        date = (SELECT MAX(date) FROM {{ ref('int_consensus_validators_status_daily') }})
),

status_7d AS (
    SELECT
        status AS label
        ,CAST(COALESCE(cnt,0) AS Float64) AS value
    FROM 
        {{ ref('int_consensus_validators_status_daily') }}
    WHERE
        date = subtractDays((SELECT MAX(date) FROM {{ ref('int_consensus_validators_status_daily') }}), 7)
),


staked_latest AS (
    SELECT
        'Staked' AS label
        ,effective_balance/32 AS value
    FROM 
        {{ ref('int_consensus_validators_balances_daily') }}
    WHERE
        date = (SELECT MAX(date) FROM {{ ref('int_consensus_validators_balances_daily') }})
),

staked_7d AS (
    SELECT
        'Staked' AS label
        ,effective_balance/32 AS value
    FROM 
        {{ ref('int_consensus_validators_balances_daily') }}
    WHERE
        date = subtractDays((SELECT MAX(date) FROM {{ ref('int_consensus_validators_balances_daily') }}), 7)
),


info_latest AS ( 
    SELECT
        'deposits_cnt' AS label
    ,COALESCE( (SELECT CAST(cnt AS Float64) FROM deposits_withdrawls_latest WHERE label = 'Deposits'), 0) AS value
    UNION ALL
    SELECT
        'withdrawls_cnt' AS label
    ,COALESCE( (SELECT CAST(cnt AS Float64) FROM deposits_withdrawls_latest WHERE label = 'Withdrawals'), 0) AS value
    UNION ALL
    SELECT
        'deposits_total_amount' AS label 
    ,COALESCE( (SELECT ROUND(total_amount,2) FROM deposits_withdrawls_latest WHERE label = 'Deposits'), 0) AS value
    UNION ALL
    SELECT
        'withdrawls_total_amount' AS label 
    ,COALESCE( (SELECT ROUND(total_amount,2) FROM deposits_withdrawls_latest WHERE label = 'Withdrawals'), 0) AS value
    UNION ALL
    SELECT
        'APY' AS label 
    ,COALESCE((SELECT ROUND(apy,2) FROM apy_latest),0) AS value
    UNION ALL
    SELECT
       label 
    ,  value
    FROM status_latest
    UNION ALL
    SELECT
       label 
    ,  value
    FROM staked_latest
),

info_7d AS ( 
    SELECT
        'deposits_cnt' AS label
    ,COALESCE( (SELECT CAST(cnt AS Float64) FROM deposits_withdrawls_7d WHERE label = 'Deposits'), 0) AS value
    UNION ALL
    SELECT
        'withdrawls_cnt' AS label
    ,COALESCE( (SELECT CAST(cnt AS Float64) FROM deposits_withdrawls_7d WHERE label = 'Withdrawals'), 0) AS value
    UNION ALL
    SELECT
        'deposits_total_amount' AS label 
    ,COALESCE( (SELECT ROUND(total_amount,2) FROM deposits_withdrawls_7d WHERE label = 'Deposits'), 0) AS value
    UNION ALL
     SELECT
        'withdrawls_total_amount' AS label 
    ,COALESCE( (SELECT ROUND(total_amount,2) FROM deposits_withdrawls_7d WHERE label = 'Withdrawals'), 0) AS value
    UNION ALL
    SELECT
        'APY' AS label 
    ,COALESCE((SELECT ROUND(apy,2) FROM apy_7d),0) AS value
    UNION ALL
    SELECT
       label 
    ,  value
    FROM status_7d
    UNION ALL
    SELECT
       label 
    ,  value
    FROM staked_7d
)

SELECT
    t1.label
    ,t1.value AS value
    ,IF(t1.value=0 AND t2.value=0, 0, ROUND(( COALESCE(t1.value / NULLIF(t2.value, 0), 0) - 1) * 100, 1)) AS change_pct
FROM info_latest t1
INNER JOIN info_7d t2
ON t2.label = t1.label
