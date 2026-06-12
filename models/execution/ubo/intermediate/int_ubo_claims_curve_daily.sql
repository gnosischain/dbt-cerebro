{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, container_address, ubo_address, token_address)',
        unique_key='(date, container_address, ubo_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
            "SET max_bytes_before_external_group_by = 2000000000",
            "SET max_bytes_before_external_sort = 2000000000",
            "SET join_algorithm = 'grace_hash'"
        ],
        post_hook=[
            "SET max_bytes_before_external_group_by = 0",
            "SET max_bytes_before_external_sort = 0",
            "SET join_algorithm = 'default'"
        ],
        tags=['production','execution','ubo','claims','curve']
    )
}}

{% set pool_address  = '0x7f90122bf0700f9e7e1f688fe926940e8839f353' %}
{% set gauge_address = '0xb721cc32160ab0da2614cc6ab16ed822aeebc101' %}

WITH

-- ─── x3CRV LP TRANSFER DELTAS (all holders, including gauge contract) ─────────
lp_transfer_deltas AS (
    SELECT
        toDate(block_timestamp)                           AS date,
        lower(decoded_params['_to'])                      AS holder,
        toInt256OrNull(decoded_params['_value'])          AS delta
    FROM {{ ref('contracts_Curve3PoolLP_events') }}
    WHERE event_name = 'Transfer'
      AND decoded_params['_value'] IS NOT NULL
      AND lower(decoded_params['_to']) != '0x0000000000000000000000000000000000000000'
      AND block_timestamp < today()

    UNION ALL

    SELECT
        toDate(block_timestamp)                           AS date,
        lower(decoded_params['_from'])                    AS holder,
        -toInt256OrNull(decoded_params['_value'])         AS delta
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

lp_calendar AS (
    SELECT
        holder,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            holder,
            min(date)                               AS min_date,
            dateDiff('day', min(date), yesterday()) AS num_days
        FROM daily_lp_deltas
        GROUP BY holder
    )
    ARRAY JOIN range(num_days + 1) AS offset
),

lp_balances AS (
    SELECT
        c.date,
        c.holder,
        sum(coalesce(d.daily_delta, toInt256(0))) OVER (
            PARTITION BY c.holder
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS lp_balance_raw
    FROM lp_calendar c
    LEFT JOIN daily_lp_deltas d
        ON  d.holder = c.holder
        AND d.date   = c.date
),

-- ─── GAUGE TOKEN TRANSFER DELTAS ──────────────────────────────────────────────
gauge_transfer_deltas AS (
    SELECT
        toDate(block_timestamp)                           AS date,
        lower(decoded_params['_to'])                      AS holder,
        toInt256OrNull(decoded_params['_value'])          AS delta
    FROM {{ ref('contracts_CurveGauge_events') }}
    WHERE event_name = 'Transfer'
      AND decoded_params['_value'] IS NOT NULL
      AND lower(decoded_params['_to']) != '0x0000000000000000000000000000000000000000'
      AND block_timestamp < today()

    UNION ALL

    SELECT
        toDate(block_timestamp)                           AS date,
        lower(decoded_params['_from'])                    AS holder,
        -toInt256OrNull(decoded_params['_value'])         AS delta
    FROM {{ ref('contracts_CurveGauge_events') }}
    WHERE event_name = 'Transfer'
      AND decoded_params['_value'] IS NOT NULL
      AND lower(decoded_params['_from']) != '0x0000000000000000000000000000000000000000'
      AND block_timestamp < today()
),

daily_gauge_deltas AS (
    SELECT date, holder, sum(delta) AS daily_delta
    FROM gauge_transfer_deltas
    GROUP BY date, holder
),

gauge_calendar AS (
    SELECT
        holder,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            holder,
            min(date)                               AS min_date,
            dateDiff('day', min(date), yesterday()) AS num_days
        FROM daily_gauge_deltas
        GROUP BY holder
    )
    ARRAY JOIN range(num_days + 1) AS offset
),

gauge_balances AS (
    SELECT
        c.date,
        c.holder,
        sum(coalesce(d.daily_delta, toInt256(0))) OVER (
            PARTITION BY c.holder
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS gauge_balance_raw
    FROM gauge_calendar c
    LEFT JOIN daily_gauge_deltas d
        ON  d.holder = c.holder
        AND d.date   = c.date
),

active_gauge_balances AS (
    SELECT date, holder, gauge_balance_raw
    FROM gauge_balances
    WHERE gauge_balance_raw > 0
),

total_gauge_supply AS (
    SELECT date, sum(gauge_balance_raw) AS total_gauge_raw
    FROM active_gauge_balances
    GROUP BY date
),

-- ─── GAUGE'S OWN x3CRV BALANCE ────────────────────────────────────────────────
-- The gauge contract deposits x3CRV into itself; this is what gets split
-- proportionally among gauge depositors.
gauge_lp_balance AS (
    SELECT date, lp_balance_raw AS gauge_lp_raw
    FROM lp_balances
    WHERE lower(holder) = lower('{{ gauge_address }}')
      AND lp_balance_raw > 0
),

-- ─── EFFECTIVE x3CRV PER FINAL HOLDER ─────────────────────────────────────────
-- Direct holders (not the gauge): effective LP = their actual LP balance.
-- Gauge depositors: effective LP = (gauge_tokens / total_gauge_supply) × gauge_lp.
-- Summing both branches = total x3CRV supply, so no double-counting.
effective_lp AS (
    SELECT
        lb.date,
        lb.holder,
        toFloat64(lb.lp_balance_raw)  AS effective_lp_raw
    FROM lp_balances lb
    WHERE lb.lp_balance_raw > 0
      AND lower(lb.holder) != lower('{{ gauge_address }}')

    UNION ALL

    SELECT
        agb.date,
        agb.holder,
        toFloat64(agb.gauge_balance_raw)
            / toFloat64(nullIf(tgs.total_gauge_raw, 0))
            * toFloat64(glb.gauge_lp_raw)  AS effective_lp_raw
    FROM active_gauge_balances agb
    INNER JOIN total_gauge_supply tgs ON tgs.date = agb.date
    INNER JOIN gauge_lp_balance   glb ON glb.date = agb.date
),

-- ─── AGGREGATE (wallet may hold both direct LP and gauge tokens) ──────────────
per_holder_effective_lp AS (
    SELECT
        date,
        holder,
        sum(effective_lp_raw) AS effective_lp_raw
    FROM effective_lp
    GROUP BY date, holder
),

total_effective_lp AS (
    SELECT date, sum(effective_lp_raw) AS total_effective_raw
    FROM per_holder_effective_lp
    GROUP BY date
),

-- ─── POOL RESERVES PER TOKEN PER DAY ─────────────────────────────────────────
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

-- ─── PROPORTIONAL CLAIMS ──────────────────────────────────────────────────────
SELECT
    phel.date                                                                                  AS date,
    'Curve 3pool'                                                                              AS protocol,
    lower('{{ pool_address }}')                                                                AS container_address,
    pr.token_address                                                                           AS token_address,
    pr.symbol                                                                                  AS symbol,
    pr.token_class                                                                             AS token_class,
    lower(phel.holder)                                                                         AS ubo_address,
    toInt256(round(
        phel.effective_lp_raw / toFloat64(nullIf(tel.total_effective_raw, 0)) * toFloat64(pr.balance_raw)
    ))                                                                                         AS balance_raw,
    phel.effective_lp_raw / toFloat64(nullIf(tel.total_effective_raw, 0)) * pr.balance        AS balance,
    phel.effective_lp_raw / toFloat64(nullIf(tel.total_effective_raw, 0)) * pr.balance_usd    AS balance_usd
FROM per_holder_effective_lp phel
INNER JOIN total_effective_lp tel ON tel.date = phel.date
INNER JOIN pool_reserves pr        ON pr.date  = phel.date
