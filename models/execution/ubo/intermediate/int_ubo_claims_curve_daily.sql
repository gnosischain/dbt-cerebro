{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, container_address, ubo_address, token_address)',
        unique_key='(date, container_address, ubo_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev','execution','ubo','claims','curve']
    )
}}

{% set pool_address = '0x7f90122bf0700f9e7e1f688fe926940e8839f353' %}

WITH

-- ─── LP TOKEN TRANSFER DELTAS ─────────────────────────────────────────────
-- Decode signed per-holder LP token deltas from ERC-20 Transfer events.
-- Mint = Transfer(from=0x0, to=holder); Burn = Transfer(from=holder, to=0x0).
-- We drop the zero address from both sides so total supply = sum of all holders.
lp_transfer_deltas AS (
    SELECT
        toDate(block_timestamp)                           AS date,
        lower(decoded_params['_to'])                     AS holder,
        toInt256OrNull(decoded_params['_value'])         AS delta
    FROM {{ ref('contracts_Curve3PoolLP_events') }}
    WHERE event_name = 'Transfer'
      AND decoded_params['_value'] IS NOT NULL
      AND lower(decoded_params['_to']) != '0x0000000000000000000000000000000000000000'
      AND block_timestamp < today()

    UNION ALL

    SELECT
        toDate(block_timestamp)                          AS date,
        lower(decoded_params['_from'])                   AS holder,
        -toInt256OrNull(decoded_params['_value'])        AS delta
    FROM {{ ref('contracts_Curve3PoolLP_events') }}
    WHERE event_name = 'Transfer'
      AND decoded_params['_value'] IS NOT NULL
      AND lower(decoded_params['_from']) != '0x0000000000000000000000000000000000000000'
      AND block_timestamp < today()
),

daily_lp_deltas AS (
    SELECT date, holder, sum(delta) AS daily_delta
    FROM lp_transfer_deltas
    GROUP BY date, holder
),

-- ─── CALENDAR (one row per holder per day, from first activity to yesterday)
calendar AS (
    SELECT
        holder,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            holder,
            min(date)                              AS min_date,
            dateDiff('day', min(date), yesterday()) AS num_days
        FROM daily_lp_deltas
        GROUP BY holder
    )
    ARRAY JOIN range(num_days + 1) AS offset
),

-- ─── CUMULATIVE LP BALANCE PER HOLDER PER DAY ─────────────────────────────
lp_balances AS (
    SELECT
        c.date,
        c.holder,
        sum(coalesce(d.daily_delta, toInt256(0))) OVER (
            PARTITION BY c.holder
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS lp_balance_raw
    FROM calendar c
    LEFT JOIN daily_lp_deltas d
        ON  d.holder = c.holder
        AND d.date   = c.date
),

active_lp_balances AS (
    SELECT date, holder, lp_balance_raw
    FROM lp_balances
    WHERE lp_balance_raw > 0
),

-- ─── TOTAL LP SUPPLY PER DAY ─────────────────────────────────────────────
-- Sum of all active positive LP balances = total circulating supply.
total_lp_supply AS (
    SELECT date, sum(lp_balance_raw) AS total_lp_raw
    FROM active_lp_balances
    GROUP BY date
),

-- ─── POOL RESERVES PER TOKEN PER DAY ─────────────────────────────────────
pool_reserves AS (
    SELECT
        date,
        lower(token_address) AS token_address,
        symbol,
        token_class,
        balance_raw,
        balance,
        balance_usd
    FROM {{ ref('int_execution_tokens_balances_daily') }}
    WHERE lower(address) = lower('{{ pool_address }}')
      AND balance > 0
      AND date < today()
)

-- ─── PROPORTIONAL CLAIMS ─────────────────────────────────────────────────
-- Each holder's share = (holder_lp_balance / total_lp_supply) × pool_reserve
SELECT
    alb.date                                                                    AS date,
    'Curve 3pool'                                                               AS protocol,
    lower('{{ pool_address }}')                                                 AS container_address,
    pr.token_address                                                            AS token_address,
    pr.symbol                                                                   AS symbol,
    pr.token_class                                                              AS token_class,
    lower(alb.holder)                                                           AS ubo_address,
    toInt256(round(
        toFloat64(alb.lp_balance_raw) / toFloat64(nullIf(ts.total_lp_raw, 0)) * toFloat64(pr.balance_raw)
    ))                                                                          AS balance_raw,
    toFloat64(alb.lp_balance_raw) / toFloat64(nullIf(ts.total_lp_raw, 0)) * pr.balance    AS balance,
    toFloat64(alb.lp_balance_raw) / toFloat64(nullIf(ts.total_lp_raw, 0)) * pr.balance_usd AS balance_usd
FROM active_lp_balances alb
INNER JOIN total_lp_supply ts ON ts.date = alb.date
INNER JOIN pool_reserves pr   ON pr.date = alb.date
