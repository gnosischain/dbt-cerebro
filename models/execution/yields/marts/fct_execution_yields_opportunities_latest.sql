{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(type, protocol, name, address)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'yields', 'opportunities', 'latest']
    )
}}

SELECT
    type,
    token,
    name,
    address,
    pool_key,
    rate_trend_14d,
    yield_apr,
    yield_apy,
    borrow_apy,
    tvl,
    total_supplied,
    total_borrowed,
    fees_7d,
    volume_usd_7d,
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

    lp_pool_volume_7d AS (
        SELECT
            f.pool,
            sum(f.volume_usd_daily) AS volume_usd_7d
        FROM {{ ref('fct_execution_pools_daily') }} f
        CROSS JOIN pools_latest_date d
        WHERE f.date > d.max_date - INTERVAL 7 DAY
          AND f.date <= d.max_date
          AND f.pool IS NOT NULL
        GROUP BY f.pool
    ),

    lp_trend_source AS (
        SELECT
            f.date AS date,
            f.pool_address AS pool_address,
            any(f.pool) AS pool_key,
            max(toFloat64(f.fee_apr_7d)) AS fee_apr_7d
        FROM {{ ref('fct_execution_pools_daily') }} f
        CROSS JOIN pools_latest_date d
        WHERE f.date <= d.max_date
          AND f.fee_apr_7d IS NOT NULL
          AND f.pool_address IS NOT NULL
          AND f.pool IS NOT NULL
        GROUP BY f.date, f.pool_address
    ),

    lp_trends AS (
        SELECT
            pool_address,
            argMax(pool_key, date) AS pool_key,
            arrayMap(
                point -> point.2,
                arraySort(point -> point.1, groupArray((date, fee_apr_7d)))
            ) AS rate_trend_14d
        FROM (
            SELECT
                pool_address,
                pool_key,
                date,
                fee_apr_7d,
                row_number() OVER (PARTITION BY pool_address ORDER BY date DESC) AS rn
            FROM lp_trend_source
        )
        WHERE rn <= 14
        GROUP BY pool_address
    ),

    lp_pools AS (
        SELECT
            'LP' AS type,
            f.token AS token,
            replaceOne(f.pool, concat(' • ', f.protocol), '') AS name,
            f.pool_address AS address,
            f.fee_apr_7d AS yield_apr,
            NULL AS yield_apy,
            NULL AS borrow_apy,
            f.tvl_usd AS tvl,
            NULL AS total_supplied,
            NULL AS total_borrowed,
            pf.fees_7d AS fees_7d,
            pv.volume_usd_7d AS volume_usd_7d,
            f.net_apr_7d AS net_apr_7d,
            NULL AS utilization_rate,
            f.protocol AS protocol,
            ft.fee_pct AS fee_pct
        FROM {{ ref('fct_execution_pools_daily') }} f
        CROSS JOIN pools_latest_date d
        LEFT JOIN lp_pool_fees_7d pf ON pf.pool = f.pool
        LEFT JOIN lp_pool_volume_7d pv ON pv.pool = f.pool
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
            yield_apr,
            yield_apy,
            borrow_apy,
            tvl,
            total_supplied,
            total_borrowed,
            fees_7d,
            volume_usd_7d,
            net_apr_7d,
            utilization_rate,
            protocol,
            fee_pct
        FROM (
            SELECT
                *,
                row_number() OVER (PARTITION BY address ORDER BY tvl DESC, token ASC) AS rn
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
            protocol,
            token_address,
            argMax(cumulative_scaled_supply, date) AS cumulative_scaled_supply,
            argMax(cumulative_scaled_borrow, date) AS cumulative_scaled_borrow,
            argMax(utilization_rate, date) AS latest_utilization_rate
        FROM {{ ref('int_execution_lending_aave_utilization_daily') }}
        WHERE utilization_rate IS NOT NULL
        GROUP BY protocol, token_address
    ),

    lending_trend_source AS (
        SELECT
            a.date AS date,
            a.protocol AS protocol,
            a.symbol AS symbol,
            toFloat64(a.apy_daily) AS apy_daily
        FROM {{ ref('int_execution_lending_aave_daily') }} a
        CROSS JOIN lending_latest_date d
        WHERE a.date <= d.max_date
          AND a.apy_daily IS NOT NULL
    ),

    lending_trends AS (
        SELECT
            protocol,
            symbol,
            arrayMap(
                point -> point.2,
                arraySort(point -> point.1, groupArray((date, apy_daily)))
            ) AS rate_trend_14d
        FROM (
            SELECT
                protocol,
                symbol,
                date,
                apy_daily,
                row_number() OVER (PARTITION BY protocol, symbol ORDER BY date DESC) AS rn
            FROM lending_trend_source
        )
        WHERE rn <= 14
        GROUP BY protocol, symbol
    ),

    lending_markets AS (
        SELECT
            'Lending' AS type,
            a.symbol AS token,
            a.symbol AS name,
            rm.supply_token_address AS address,
            NULL AS yield_apr,
            a.apy_daily AS yield_apy,
            a.borrow_apy_variable_daily AS borrow_apy,
            NULL AS tvl,
            (toFloat64(lc.cumulative_scaled_supply) * a.liquidity_index / 1e27)
                / power(10, rm.decimals) * coalesce(pr.price, 0) AS total_supplied,
            (toFloat64(lc.cumulative_scaled_borrow) * a.variable_borrow_index / 1e27)
                / power(10, rm.decimals) * coalesce(pr.price, 0) AS total_borrowed,
            NULL AS fees_7d,
            NULL AS volume_usd_7d,
            NULL AS net_apr_7d,
            lc.latest_utilization_rate AS utilization_rate,
            a.protocol AS protocol,
            NULL AS fee_pct
        FROM {{ ref('int_execution_lending_aave_daily') }} a
        CROSS JOIN lending_latest_date d
        LEFT JOIN lending_cumulative_latest lc
            ON  lc.protocol      = a.protocol
           AND  lc.token_address = a.token_address
        INNER JOIN {{ ref('lending_market_mapping') }} rm
            ON  rm.protocol              = a.protocol
           AND lower(rm.reserve_address) = a.token_address
        LEFT JOIN {{ ref('int_execution_token_prices_daily') }} pr
            ON pr.symbol = a.symbol
           AND pr.date   = a.date
        WHERE a.date = d.max_date
          AND a.apy_daily IS NOT NULL
          AND a.apy_daily > 0
    )

    SELECT
        lp.type,
        lp.token,
        lp.name,
        lp.address,
        lt.pool_key AS pool_key,
        ifNull(lt.rate_trend_14d, CAST([], 'Array(Float64)')) AS rate_trend_14d,
        lp.yield_apr,
        lp.yield_apy,
        lp.borrow_apy,
        lp.tvl,
        lp.total_supplied,
        lp.total_borrowed,
        lp.fees_7d,
        lp.volume_usd_7d,
        lp.net_apr_7d,
        lp.utilization_rate,
        lp.protocol,
        lp.fee_pct
    FROM lp_pools_dedup lp
    LEFT JOIN lp_trends lt
        ON lt.pool_address = lp.address

    UNION ALL

    SELECT
        lm.type,
        lm.token,
        lm.name,
        lm.address,
        CAST(NULL, 'Nullable(String)') AS pool_key,
        ifNull(ltr.rate_trend_14d, CAST([], 'Array(Float64)')) AS rate_trend_14d,
        lm.yield_apr,
        lm.yield_apy,
        lm.borrow_apy,
        lm.tvl,
        lm.total_supplied,
        lm.total_borrowed,
        lm.fees_7d,
        lm.volume_usd_7d,
        lm.net_apr_7d,
        lm.utilization_rate,
        lm.protocol,
        lm.fee_pct
    FROM lending_markets lm
    LEFT JOIN lending_trends ltr
        ON  ltr.protocol = lm.protocol
       AND  ltr.symbol   = lm.token
)
ORDER BY COALESCE(yield_apr, yield_apy) DESC
