{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(metric)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'yields', 'overview', 'snapshot']
    )
}}

SELECT
    metric,
    value,
    change_pct,
    label
FROM (
    WITH
    
    pools_latest_date AS (
        SELECT max(date) AS max_date
        FROM {{ ref('fct_execution_yields_pools_daily') }}
        WHERE date < today()
    ),
    
    -- LP Total TVL (latest)
    lp_tvl_latest AS (
        SELECT sum(tvl_usd) AS tvl
        FROM {{ ref('fct_execution_yields_pools_daily') }} f
        CROSS JOIN pools_latest_date d
        WHERE f.date = d.max_date
    ),
    
    -- LP Total TVL (7 days ago)
    lp_tvl_7d_ago AS (
        SELECT sum(tvl_usd) AS tvl
        FROM {{ ref('fct_execution_yields_pools_daily') }} f
        CROSS JOIN pools_latest_date d
        WHERE f.date = d.max_date - INTERVAL 7 DAY
    ),
    
    -- LP Best APR (latest) with pool name
    lp_best_apr_latest AS (
        SELECT
            argMax(f.fee_apr_7d, f.fee_apr_7d) AS apr,
            argMax(f.pool, f.fee_apr_7d) AS pool_name
        FROM {{ ref('fct_execution_yields_pools_daily') }} f
        CROSS JOIN pools_latest_date d
        WHERE f.date = d.max_date
          AND f.fee_apr_7d IS NOT NULL
    ),
    
    -- LP Best APR (7 days ago)
    lp_best_apr_7d_ago AS (
        SELECT max(fee_apr_7d) AS apr
        FROM {{ ref('fct_execution_yields_pools_daily') }} f
        CROSS JOIN pools_latest_date d
        WHERE f.date = d.max_date - INTERVAL 7 DAY
          AND f.fee_apr_7d IS NOT NULL
    ),
    
    
    lending_latest_date AS (
        SELECT max(date) AS max_date
        FROM {{ ref('int_execution_yields_aave_daily') }}
        WHERE date < today()
    ),
    
    -- Best Lending APY (latest) with token name
    lending_best_apy_latest AS (
        SELECT
            argMax(a.apy_daily, a.apy_daily) AS apy,
            argMax(a.symbol, a.apy_daily) AS token_name
        FROM {{ ref('int_execution_yields_aave_daily') }} a
        CROSS JOIN lending_latest_date d
        WHERE a.date = d.max_date
          AND a.apy_daily IS NOT NULL
          AND a.apy_daily > 0
    ),
    
    -- Best Lending APY (7 days ago)
    lending_best_apy_7d_ago AS (
        SELECT max(apy_daily) AS apy
        FROM {{ ref('int_execution_yields_aave_daily') }} a
        CROSS JOIN lending_latest_date d
        WHERE a.date = d.max_date - INTERVAL 7 DAY
          AND a.apy_daily IS NOT NULL
    ),
    
    -- Active lenders (current): users with positive supply balance on latest date
    lending_active_lenders_latest AS (
        SELECT toUInt64(count(DISTINCT user_address)) AS lenders
        FROM {{ ref('int_execution_yields_aave_user_balances_daily') }} b
        CROSS JOIN lending_tvl_latest_date d
        WHERE b.date = d.max_date
          AND b.balance > 0
    ),

    -- Active lenders (7 days ago) for change calculation
    lending_active_lenders_7d_ago AS (
        SELECT toUInt64(count(DISTINCT user_address)) AS lenders
        FROM {{ ref('int_execution_yields_aave_user_balances_daily') }} b
        CROSS JOIN lending_tvl_latest_date d
        WHERE b.date = d.max_date - INTERVAL 7 DAY
          AND b.balance > 0
    ),
    
    
    sdai_latest_date AS (
        SELECT max(date) AS max_date
        FROM {{ ref('fct_yields_sdai_apy_daily') }}
        WHERE date < today()
          AND label = 'Daily'
    ),
    
    -- sDAI APY (latest)
    sdai_apy_latest AS (
        SELECT any(apy) AS apy
        FROM {{ ref('fct_yields_sdai_apy_daily') }} s
        CROSS JOIN sdai_latest_date d
        WHERE s.date = d.max_date
          AND s.label = 'Daily'
    ),
    
    -- sDAI APY (7 days ago)
    sdai_apy_7d_ago AS (
        SELECT any(apy) AS apy
        FROM {{ ref('fct_yields_sdai_apy_daily') }} s
        CROSS JOIN sdai_latest_date d
        WHERE s.date = d.max_date - INTERVAL 7 DAY
          AND s.label = 'Daily'
    ),
    
    -- sDAI Supply (latest)
    sdai_supply_latest AS (
        SELECT argMax(supply, date) AS supply
        FROM {{ ref('fct_execution_tokens_metrics_daily') }}
        WHERE upper(symbol) = 'SDAI'
          AND date < today()
    ),
    
    -- sDAI Supply (7 days ago)
    sdai_supply_7d_ago AS (
        SELECT any(supply) AS supply
        FROM {{ ref('fct_execution_tokens_metrics_daily') }}
        WHERE upper(symbol) = 'SDAI'
          AND date = (SELECT max(date) - INTERVAL 7 DAY FROM {{ ref('fct_execution_tokens_metrics_daily') }} WHERE upper(symbol) = 'SDAI' AND date < today())
    ),

    lending_tvl_latest_date AS (
        SELECT max(date) AS max_date
        FROM {{ ref('int_execution_yields_aave_user_balances_daily') }}
        WHERE date < today()
          AND balance_usd > 0
    ),

    lending_tvl_latest AS (
        SELECT coalesce(sum(balance_usd), 0) AS tvl
        FROM {{ ref('int_execution_yields_aave_user_balances_daily') }} b
        CROSS JOIN lending_tvl_latest_date d
        WHERE b.date = d.max_date
          AND b.balance_usd > 0
    ),

    lending_tvl_7d_ago AS (
        SELECT coalesce(sum(balance_usd), 0) AS tvl
        FROM {{ ref('int_execution_yields_aave_user_balances_daily') }} b
        CROSS JOIN lending_tvl_latest_date d
        WHERE b.date = d.max_date - INTERVAL 7 DAY
          AND b.balance_usd > 0
    )

    
    -- 1. LP Total TVL
    SELECT
        'lp_tvl_total' AS metric,
        l.tvl AS value,
        round(
            CASE
                WHEN p.tvl IS NULL OR p.tvl = 0 THEN NULL
                ELSE ((l.tvl / p.tvl) - 1) * 100
            END,
            2
        ) AS change_pct,
        NULL AS label
    FROM lp_tvl_latest l
    CROSS JOIN lp_tvl_7d_ago p
    
    UNION ALL
    
    -- 2. LP Best APR
    SELECT
        'lp_best_apr' AS metric,
        l.apr AS value,
        round(
            CASE
                WHEN p.apr IS NULL OR p.apr = 0 THEN NULL
                ELSE ((l.apr / p.apr) - 1) * 100
            END,
            2
        ) AS change_pct,
        l.pool_name AS label
    FROM lp_best_apr_latest l
    CROSS JOIN lp_best_apr_7d_ago p
    
    UNION ALL
    
    -- 3. Lending Best APY
    SELECT
        'lending_best_apy' AS metric,
        l.apy AS value,
        round(
            CASE
                WHEN p.apy IS NULL OR p.apy = 0 THEN NULL
                ELSE ((l.apy / p.apy) - 1) * 100
            END,
            2
        ) AS change_pct,
        l.token_name AS label
    FROM lending_best_apy_latest l
    CROSS JOIN lending_best_apy_7d_ago p
    
    UNION ALL
    
    -- 4. Active Lenders (current open positions)
    SELECT
        'lending_lenders_total' AS metric,
        toFloat64(c.lenders) AS value,
        round(
            CASE
                WHEN p.lenders IS NULL OR p.lenders = 0 THEN NULL
                ELSE ((toFloat64(c.lenders) / toFloat64(p.lenders)) - 1) * 100
            END,
            2
        ) AS change_pct,
        NULL AS label
    FROM lending_active_lenders_latest c
    CROSS JOIN lending_active_lenders_7d_ago p
    
    UNION ALL
    
    -- 5. sDAI APY
    SELECT
        'sdai_apy' AS metric,
        l.apy AS value,
        round(
            CASE
                WHEN p.apy IS NULL OR p.apy = 0 THEN NULL
                ELSE ((l.apy / p.apy) - 1) * 100
            END,
            2
        ) AS change_pct,
        NULL AS label
    FROM sdai_apy_latest l
    CROSS JOIN sdai_apy_7d_ago p
    
    UNION ALL
    
    -- 6. sDAI Total Supply
    SELECT
        'sdai_supply_total' AS metric,
        l.supply AS value,
        round(
            CASE
                WHEN p.supply IS NULL OR p.supply = 0 THEN NULL
                ELSE ((l.supply / p.supply) - 1) * 100
            END,
            2
        ) AS change_pct,
        NULL AS label
    FROM sdai_supply_latest l
    CROSS JOIN sdai_supply_7d_ago p
    
    UNION ALL
    
    -- 7. Lending TVL Total
    SELECT
        'lending_tvl_total' AS metric,
        l.tvl AS value,
        round(
            CASE
                WHEN p.tvl IS NULL OR p.tvl = 0 THEN NULL
                ELSE ((l.tvl / p.tvl) - 1) * 100
            END,
            2
        ) AS change_pct,
        NULL AS label
    FROM lending_tvl_latest l
    CROSS JOIN lending_tvl_7d_ago p
)
