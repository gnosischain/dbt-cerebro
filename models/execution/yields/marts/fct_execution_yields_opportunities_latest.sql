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
    name,
    yield_pct,
    borrow_apy,
    tvl,
    protocol
FROM (
    WITH
    
    pools_latest_date AS (
        SELECT max(date) AS max_date
        FROM {{ ref('fct_execution_yields_pools_daily') }}
        WHERE date < today()
    ),
    
    lp_pools AS (
        SELECT
            'LP' AS type,
            f.pool AS name,
            f.fee_apr_7d AS yield_pct,
            NULL AS borrow_apy,
            f.tvl_usd AS tvl,
            f.protocol AS protocol
        FROM {{ ref('fct_execution_yields_pools_daily') }} f
        CROSS JOIN pools_latest_date d
        WHERE f.date = d.max_date
          AND f.fee_apr_7d IS NOT NULL
          AND f.pool IS NOT NULL
    ),
    
    lp_pools_dedup AS (
        SELECT
            type,
            name,
            yield_pct,
            borrow_apy,
            tvl,
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
    
    lending_markets AS (
        SELECT
            'Lending' AS type,
            a.symbol AS name,
            a.apy_daily AS yield_pct,
            a.borrow_apy_variable_daily AS borrow_apy,
            NULL AS tvl,
            a.protocol AS protocol
        FROM {{ ref('int_execution_yields_aave_daily') }} a
        CROSS JOIN lending_latest_date d
        WHERE a.date = d.max_date
          AND a.apy_daily IS NOT NULL
          AND a.apy_daily > 0
    )
    
    
    SELECT type, name, yield_pct, borrow_apy, tvl, protocol
    FROM lp_pools_dedup
    
    UNION ALL
    
    SELECT type, name, yield_pct, borrow_apy, tvl, protocol
    FROM lending_markets
)
ORDER BY yield_pct DESC
