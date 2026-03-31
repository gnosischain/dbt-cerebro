{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(type, name)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'yields', 'opportunities', 'latest']
    )
}}

SELECT
    type,
    token,
    name,
    address,
    yield_pct,
    yield_label,
    borrow_apy,
    tvl,
    total_supplied,
    total_borrowed,
    fees_7d,
    lvr_apr_7d,
    net_apr_7d,
    utilization_rate,
    protocol,
    fee_pct
FROM (
    WITH
    
    pool_fee_tiers AS (
        SELECT pool_address, fee_tier_ppm / 10000.0 AS fee_pct
        FROM {{ ref('stg_pools__v3_pool_registry') }}
        WHERE protocol = 'Uniswap V3'
          AND fee_tier_ppm IS NOT NULL

        UNION ALL

        SELECT
            concat('0x', lower(contract_address)) AS pool_address,
            argMax(toUInt32OrNull(decoded_params['fee']), block_timestamp) / 10000.0 AS fee_pct
        FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }}
        WHERE event_name = 'Fee'
          AND decoded_params['fee'] IS NOT NULL
        GROUP BY pool_address

        UNION ALL

        SELECT
            lower(decoded_params['pool']) AS pool_address,
            argMax(toFloat64OrNull(decoded_params['swapFeePercentage']), block_timestamp) / 1e16 AS fee_pct
        FROM {{ ref('contracts_BalancerV3_Vault_events') }}
        WHERE event_name = 'SwapFeePercentageChanged'
          AND decoded_params['pool'] IS NOT NULL
          AND decoded_params['swapFeePercentage'] IS NOT NULL
        GROUP BY pool_address
    ),

    pools_latest_date AS (
        SELECT max(date) AS max_date
        FROM {{ ref('fct_execution_pools_daily') }}
        WHERE date < today()
    ),
    
    lp_pool_fees_7d AS (
        SELECT
            f.pool,
            sum(f.fees_usd_daily) AS fees_7d
        FROM {{ ref('fct_execution_pools_daily') }} f
        CROSS JOIN pools_latest_date d
        WHERE f.date > d.max_date - INTERVAL 7 DAY
          AND f.date <= d.max_date
          AND f.pool IS NOT NULL
        GROUP BY f.pool
    ),
    
    lp_pools AS (
        SELECT
            'LP' AS type,
            f.token AS token,
            replaceOne(f.pool, concat(' • ', f.protocol), '') AS name,
            f.pool_address AS address,
            f.fee_apr_7d AS yield_pct,
            'APR' AS yield_label,
            NULL AS borrow_apy,
            f.tvl_usd AS tvl,
            NULL AS total_supplied,
            NULL AS total_borrowed,
            pf.fees_7d AS fees_7d,
            f.lvr_apr_7d AS lvr_apr_7d,
            f.net_apr_7d AS net_apr_7d,
            NULL AS utilization_rate,
            f.protocol AS protocol,
            ft.fee_pct AS fee_pct
        FROM {{ ref('fct_execution_pools_daily') }} f
        CROSS JOIN pools_latest_date d
        LEFT JOIN lp_pool_fees_7d pf ON pf.pool = f.pool
        LEFT JOIN pool_fee_tiers ft ON ft.pool_address = f.pool_address
        WHERE f.date = d.max_date
          AND f.fee_apr_7d IS NOT NULL
          AND f.pool IS NOT NULL
    ),
    
    lp_pools_dedup AS (
        SELECT
            type,
            token,
            name,
            address,
            yield_pct,
            yield_label,
            borrow_apy,
            tvl,
            total_supplied,
            total_borrowed,
            fees_7d,
            lvr_apr_7d,
            net_apr_7d,
            utilization_rate,
            protocol,
            fee_pct
        FROM (
            SELECT
                *,
                row_number() OVER (PARTITION BY name ORDER BY tvl DESC) AS rn
            FROM lp_pools
        )
        WHERE rn = 1
    ),
    
    
    lending_latest_date AS (
        SELECT max(date) AS max_date
        FROM {{ ref('int_execution_lending_aave_daily') }}
        WHERE date < today()
    ),

    lending_cumulative_latest AS (
        SELECT
            token_address,
            argMax(cumulative_scaled_supply, date) AS cumulative_scaled_supply,
            argMax(cumulative_scaled_borrow, date) AS cumulative_scaled_borrow,
            argMax(utilization_rate, date) AS latest_utilization_rate
        FROM {{ ref('int_execution_lending_aave_utilization_daily') }}
        WHERE utilization_rate IS NOT NULL
        GROUP BY token_address
    ),

    lending_markets AS (
        SELECT
            'Lending' AS type,
            a.symbol AS token,
            a.symbol AS name,
            rm.atoken_address AS address,
            a.apy_daily AS yield_pct,
            'APY' AS yield_label,
            a.borrow_apy_variable_daily AS borrow_apy,
            NULL AS tvl,
            (lc.cumulative_scaled_supply * a.liquidity_index / 1e27)
                / power(10, rm.decimals) * coalesce(pr.price, 0) AS total_supplied,
            (lc.cumulative_scaled_borrow * a.variable_borrow_index / 1e27)
                / power(10, rm.decimals) * coalesce(pr.price, 0) AS total_borrowed,
            NULL AS fees_7d,
            NULL AS lvr_apr_7d,
            NULL AS net_apr_7d,
            lc.latest_utilization_rate AS utilization_rate,
            a.protocol AS protocol,
            NULL AS fee_pct
        FROM {{ ref('int_execution_lending_aave_daily') }} a
        CROSS JOIN lending_latest_date d
        LEFT JOIN lending_cumulative_latest lc
            ON lc.token_address = a.token_address
        INNER JOIN {{ ref('atoken_reserve_mapping') }} rm
            ON lower(rm.reserve_address) = a.token_address
        LEFT JOIN {{ ref('int_execution_token_prices_daily') }} pr
            ON pr.symbol = a.symbol
           AND pr.date = a.date
        WHERE a.date = d.max_date
          AND a.apy_daily IS NOT NULL
          AND a.apy_daily > 0
    )
    
    
    SELECT type, token, name, address, yield_pct, yield_label, borrow_apy, tvl, total_supplied, total_borrowed, fees_7d, lvr_apr_7d, net_apr_7d, utilization_rate, protocol, fee_pct
    FROM lp_pools_dedup
    
    UNION ALL
    
    SELECT type, token, name, address, yield_pct, yield_label, borrow_apy, tvl, total_supplied, total_borrowed, fees_7d, lvr_apr_7d, net_apr_7d, utilization_rate, protocol, fee_pct
    FROM lending_markets
)
ORDER BY yield_pct DESC
