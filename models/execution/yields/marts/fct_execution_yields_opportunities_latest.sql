{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(type, name)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'yields', 'opportunities', 'latest']
    )
}}

SELECT
    type,
    token,
    name,
    yield_pct,
    yield_label,
    borrow_apy,
    tvl,
    fees_7d,
    protocol
FROM (
    WITH
    
    pools_latest_date AS (
        SELECT max(date) AS max_date
        FROM {{ ref('fct_execution_yields_pools_daily') }}
        WHERE date < today()
    ),
    
    lp_pool_fees_7d AS (
        SELECT
            f.pool,
            sum(f.fees_usd_daily) AS fees_7d
        FROM {{ ref('fct_execution_yields_pools_daily') }} f
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
            f.fee_apr_7d AS yield_pct,
            'APR' AS yield_label,
            NULL AS borrow_apy,
            f.tvl_usd AS tvl,
            pf.fees_7d AS fees_7d,
            f.protocol AS protocol
        FROM {{ ref('fct_execution_yields_pools_daily') }} f
        CROSS JOIN pools_latest_date d
        LEFT JOIN lp_pool_fees_7d pf ON pf.pool = f.pool
        WHERE f.date = d.max_date
          AND f.fee_apr_7d IS NOT NULL
          AND f.pool IS NOT NULL
    ),
    
    lp_pools_dedup AS (
        SELECT
            type,
            token,
            name,
            yield_pct,
            yield_label,
            borrow_apy,
            tvl,
            fees_7d,
            protocol
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
        FROM {{ ref('int_execution_yields_aave_daily') }}
        WHERE date < today()
    ),
    
    lending_scaled_supply AS (
        SELECT
            token_address,
            sum(net_scaled_supply_change_daily) AS total_scaled_supply
        FROM {{ ref('int_execution_yields_aave_scaled_supply_daily') }}
        GROUP BY token_address
    ),
    
    lending_latest_index AS (
        SELECT
            token_address,
            argMax(liquidity_index, date) AS latest_liquidity_index
        FROM {{ ref('int_execution_yields_aave_daily') }}
        WHERE liquidity_index IS NOT NULL
        GROUP BY token_address
    ),
    
    latest_prices AS (
        SELECT
            nullIf(upper(trimBoth(symbol)), '') AS token,
            argMax(toFloat64(price), date) AS price_usd
        FROM {{ ref('int_execution_token_prices_daily') }}
        WHERE date < today()
        GROUP BY token
    ),
    
    lending_markets AS (
        SELECT
            'Lending' AS type,
            a.symbol AS token,
            a.symbol AS name,
            a.apy_daily AS yield_pct,
            'APY' AS yield_label,
            a.borrow_apy_variable_daily AS borrow_apy,
            CASE
                WHEN ss.total_scaled_supply IS NOT NULL AND ss.total_scaled_supply > 0
                     AND li.latest_liquidity_index IS NOT NULL
                THEN (ss.total_scaled_supply * li.latest_liquidity_index / 1e27)
                     / POWER(10, COALESCE(w.decimals, 18))
                     * coalesce(lp.price_usd, 0)
                ELSE NULL
            END AS tvl,
            NULL AS fees_7d,
            a.protocol AS protocol
        FROM {{ ref('int_execution_yields_aave_daily') }} a
        CROSS JOIN lending_latest_date d
        LEFT JOIN lending_scaled_supply ss
            ON ss.token_address = a.token_address
        LEFT JOIN lending_latest_index li
            ON li.token_address = a.token_address
        LEFT JOIN (
            SELECT lower(address) AS token_address, any(decimals) AS decimals
            FROM {{ ref('tokens_whitelist') }}
            GROUP BY lower(address)
        ) w ON w.token_address = a.token_address
        LEFT JOIN latest_prices lp
            ON lp.token = nullIf(upper(trimBoth(a.symbol)), '')
        WHERE a.date = d.max_date
          AND a.apy_daily IS NOT NULL
          AND a.apy_daily > 0
    )
    
    
    SELECT type, token, name, yield_pct, yield_label, borrow_apy, tvl, fees_7d, protocol
    FROM lp_pools_dedup
    
    UNION ALL
    
    SELECT type, token, name, yield_pct, yield_label, borrow_apy, tvl, fees_7d, protocol
    FROM lending_markets
)
ORDER BY yield_pct DESC
