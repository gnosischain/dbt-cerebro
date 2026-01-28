{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'yields', 'pools', 'daily']
    )
}}

WITH
balances_base AS (
    SELECT
        toDate(b.date) AS date,
        b.protocol AS protocol,
        lower(b.pool_address) AS pool_address_raw,
        lower(b.token_address) AS token_address,
        b.token_amount AS token_amount
    FROM {{ ref('int_execution_pools_balances_daily') }} b
    WHERE b.date < today()
),

balances_canon AS (
    SELECT
        date,
        protocol,
        multiIf(
            startsWith(pool_address_raw, '0x'),
            pool_address_raw,
            concat('0x', pool_address_raw)
        ) AS pool_address,
        replaceAll(
            multiIf(
                startsWith(pool_address_raw, '0x'),
                pool_address_raw,
                concat('0x', pool_address_raw)
            ),
            '0x',
            ''
        ) AS pool_address_no0x,
        token_address,
        token_amount
    FROM balances_base
),

token_meta AS (
    SELECT
        lower(address) AS token_address,
        nullIf(upper(trimBoth(symbol)), '') AS token,
        decimals,
        date_start,
        date_end
    FROM {{ ref('tokens_whitelist') }}
),

prices AS (
    SELECT
        toDate(date) AS date,
        nullIf(upper(trimBoth(symbol)), '') AS token,
        toFloat64(price) AS price_usd
    FROM {{ ref('int_execution_token_prices_daily') }}
    WHERE date < today()
),

balances_enriched AS (
    SELECT
        b.date AS date,
        b.protocol AS protocol,
        b.pool_address AS pool_address,
        b.pool_address_no0x AS pool_address_no0x,
        b.token_address AS token_address,
        tm.token AS token,
        b.token_amount AS token_amount,
        p.price_usd AS price_usd,
        b.token_amount * coalesce(p.price_usd, 0) AS tvl_component_usd
    FROM balances_canon b
    LEFT JOIN token_meta tm
      ON tm.token_address = b.token_address
     AND b.date >= toDate(tm.date_start)
     AND (tm.date_end IS NULL OR b.date < toDate(tm.date_end))
    LEFT JOIN prices p
      ON p.date = b.date
     AND p.token = tm.token
),

token_pool_tvl_daily AS (
    SELECT
        date,
        protocol,
        pool_address,
        token_address,
        token,
        sum(tvl_component_usd) AS token_tvl_usd
    FROM balances_enriched
    WHERE protocol IN ('Uniswap V3', 'Swapr V3')
      AND token IS NOT NULL
      AND token != ''
    GROUP BY date, protocol, pool_address, token_address, token
),

token_pool_tvl_scored AS (
    SELECT
        date,
        protocol,
        pool_address,
        token_address,
        token,
        token_tvl_usd,
        avg(token_tvl_usd) OVER (
            PARTITION BY protocol, pool_address, token_address
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS token_tvl_usd_30d_avg
    FROM token_pool_tvl_daily
),

top_pools_by_token AS (
    SELECT
        token,
        protocol,
        pool_address,
        token_address
    FROM (
        SELECT
            token,
            protocol,
            pool_address,
            token_address,
            token_tvl_usd_30d_avg,
            row_number() OVER (
                PARTITION BY token
                ORDER BY token_tvl_usd_30d_avg DESC, protocol, pool_address
            ) AS pool_rank
        FROM (
            SELECT
                *,
                max(date) OVER (PARTITION BY token) AS latest_date_for_token
            FROM token_pool_tvl_scored
        )
        WHERE date = latest_date_for_token
          AND token_tvl_usd_30d_avg >= 1000
    )
    WHERE pool_rank <= 5
),

pool_tvl_daily AS (
    SELECT
        date,
        protocol,
        pool_address,
        any(pool_address_no0x) AS pool_address_no0x,
        sum(tvl_component_usd) AS tvl_usd
    FROM balances_enriched
    GROUP BY date, protocol, pool_address
),

uniswap_v3_pools AS (
    SELECT DISTINCT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address_no0x,
        lower(decoded_params['token0']) AS token0_address,
        lower(decoded_params['token1']) AS token1_address,
        'Uniswap V3' AS protocol
    FROM {{ ref('contracts_UniswapV3_Factory_events') }}
    WHERE event_name = 'PoolCreated'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['token0'] IS NOT NULL
      AND decoded_params['token1'] IS NOT NULL
),

swapr_v3_pools AS (
    SELECT DISTINCT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address_no0x,
        lower(decoded_params['token0']) AS token0_address,
        lower(decoded_params['token1']) AS token1_address,
        'Swapr V3' AS protocol
    FROM {{ ref('contracts_Swapr_v3_AlgebraFactory_events') }}
    WHERE event_name = 'Pool'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['token0'] IS NOT NULL
      AND decoded_params['token1'] IS NOT NULL
),

v3_pool_meta AS (
    SELECT * FROM uniswap_v3_pools
    UNION ALL
    SELECT * FROM swapr_v3_pools
),

pool_symbol_lists AS (
    SELECT
        protocol,
        pool_address,
        arraySort(groupUniqArray(token)) AS tokens_sorted,
        countDistinct(token) AS tokens_cnt
    FROM (
        SELECT
            protocol,
            pool_address,
            token
        FROM balances_enriched
        WHERE token IS NOT NULL
          AND token != ''
    )
    GROUP BY protocol, pool_address
),

pool_labels AS (
    SELECT
        p.protocol AS protocol,
        p.pool_address AS pool_address,
        -- Default label for Balancer or unknown: sym1/sym2/sym3(+N) • protocol • 0x…suffix
        multiIf(
            p.protocol IN ('Uniswap V3', 'Swapr V3'),
            concat(
                coalesce(t0.token, 'UNK'),
                '/',
                coalesce(t1.token, 'UNK'),
                ' • ',
                p.protocol,
                ' • ',
                right(p.pool_address, 6)
            ),
            concat(
                arrayStringConcat(arraySlice(sl.tokens_sorted, 1, 3), '/'),
                if(sl.tokens_cnt > 3, concat('(+', toString(sl.tokens_cnt - 3), ')'), ''),
                ' • ',
                p.protocol,
                ' • ',
                right(p.pool_address, 6)
            )
        ) AS pool
    FROM (
        SELECT DISTINCT protocol, pool_address FROM balances_canon
    ) p
    LEFT JOIN v3_pool_meta m
      ON m.protocol = p.protocol
     AND m.pool_address_no0x = replaceAll(lower(p.pool_address), '0x', '')
    LEFT JOIN token_meta t0
      ON t0.token_address = m.token0_address
    LEFT JOIN token_meta t1
      ON t1.token_address = m.token1_address
    LEFT JOIN pool_symbol_lists sl
      ON sl.protocol = p.protocol
     AND sl.pool_address = p.pool_address
),

-- Accrued fees (gross) from Swap + Flash events (Uniswap V3 + Swapr V3 only)
fees_usd_daily AS (
    SELECT
        date,
        protocol,
        pool_address,
        sum(fees_usd) AS fees_usd_daily
    FROM {{ ref('int_execution_yields_pools_fees_daily') }}
    WHERE date < today()
    GROUP BY date, protocol, pool_address
),

pool_metrics_daily AS (
    SELECT
        t.date,
        t.protocol,
        t.pool_address,
        t.tvl_usd,
        coalesce(f.fees_usd_daily, 0) AS fees_usd_daily,
        sum(coalesce(f.fees_usd_daily, 0)) OVER (
            PARTITION BY t.protocol, t.pool_address
            ORDER BY t.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS fees_usd_7d,
        avg(t.tvl_usd) OVER (
            PARTITION BY t.protocol, t.pool_address
            ORDER BY t.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS tvl_usd_7d_avg
    FROM pool_tvl_daily t
    LEFT JOIN fees_usd_daily f
      ON f.date = t.date
     AND f.protocol = t.protocol
     AND f.pool_address = t.pool_address
),

pool_metrics_final AS (
    SELECT
        date,
        protocol,
        pool_address,
        tvl_usd,
        fees_usd_daily,
        multiIf(
            protocol IN ('Uniswap V3', 'Swapr V3') AND tvl_usd_7d_avg > 0,
            (fees_usd_7d / tvl_usd_7d_avg) * (365.0 / 7.0) * 100.0,
            NULL
        ) AS fee_apr_7d
    FROM pool_metrics_daily
),

final AS (
    SELECT
        b.date AS date,
        b.protocol AS protocol,
        b.pool_address AS pool_address,
        pl.pool AS pool,
        b.token_address AS token_address,
        b.token AS token,
        pm.tvl_usd AS tvl_usd,
        pm.fees_usd_daily AS fees_usd_daily,
        pm.fee_apr_7d AS fee_apr_7d
    FROM (
        SELECT DISTINCT
            date,
            protocol,
            pool_address,
            token_address,
            token
        FROM balances_enriched
        WHERE token IS NOT NULL
          AND token != ''
    ) b
    INNER JOIN top_pools_by_token tp
      ON tp.token = b.token
     AND tp.protocol = b.protocol
     AND tp.pool_address = b.pool_address
     AND tp.token_address = b.token_address
    LEFT JOIN pool_labels pl
      ON pl.protocol = b.protocol
     AND pl.pool_address = b.pool_address
    LEFT JOIN pool_metrics_final pm
      ON pm.date = b.date
     AND pm.protocol = b.protocol
     AND pm.pool_address = b.pool_address
    WHERE b.protocol IN ('Uniswap V3', 'Swapr V3')
)

SELECT
    date,
    protocol,
    pool_address,
    pool,
    token_address,
    token,
    tvl_usd,
    fees_usd_daily,
    fee_apr_7d
FROM final
WHERE date < today()
