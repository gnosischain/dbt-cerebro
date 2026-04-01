{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'daily']
    )
}}

WITH

{# Balancer V3 pools where ALL tokens have metadata (symbol + price).
   Pools with partial coverage are excluded to avoid misleading TVL/APR. #}
balancer_v3_complete_pools AS (
    SELECT pool_address
    FROM (
        SELECT
            pool_address,
            count(DISTINCT token_address) AS total_tokens,
            count(DISTINCT CASE WHEN token IS NOT NULL AND token != '' THEN token_address END) AS known_tokens
        FROM {{ ref('int_execution_pools_enriched_daily') }}
        WHERE protocol = 'Balancer V3'
        GROUP BY pool_address
    )
    WHERE total_tokens = known_tokens
),

{# Resolve token symbols from the pool registry without date_end filtering.
   Covers V3 pools where migrated tokens (e.g. EURe, GBPe) have date_end in
   the whitelist but still exist in active pools. Each address appears once
   in the whitelist, so no duplication risk. #}
registry_pool_tokens AS (
    SELECT protocol, pool_address, token_address, token
    FROM (
        SELECT
            m.protocol,
            m.pool_address,
            m.token0_address AS token_address,
            t.token AS token
        FROM {{ ref('stg_pools__v3_pool_registry') }} m
        INNER JOIN {{ ref('stg_pools__tokens_meta') }} t
          ON t.token_address = m.token0_address
        WHERE m.protocol IN ('Uniswap V3', 'Swapr V3')

        UNION ALL

        SELECT
            m.protocol,
            m.pool_address,
            m.token1_address AS token_address,
            t.token AS token
        FROM {{ ref('stg_pools__v3_pool_registry') }} m
        INNER JOIN {{ ref('stg_pools__tokens_meta') }} t
          ON t.token_address = m.token1_address
        WHERE m.protocol IN ('Uniswap V3', 'Swapr V3')
    )
    WHERE token IS NOT NULL AND token != ''

    UNION ALL

    SELECT DISTINCT
        protocol,
        pool_address,
        token_address,
        token
    FROM {{ ref('int_execution_pools_enriched_daily') }}
    WHERE protocol = 'Balancer V3'
      AND token IS NOT NULL AND token != ''
      AND pool_address IN (SELECT pool_address FROM balancer_v3_complete_pools)
),

{# Pool-level TVL from enriched (respects date_end for price safety). #}
pool_tvl_daily AS (
    SELECT
        date,
        protocol,
        pool_address,
        sum(tvl_component_usd) AS pool_tvl_usd
    FROM {{ ref('int_execution_pools_enriched_daily') }}
    WHERE protocol IN ('Uniswap V3', 'Swapr V3', 'Balancer V3')
      AND (protocol != 'Balancer V3' OR pool_address IN (SELECT pool_address FROM balancer_v3_complete_pools))
    GROUP BY date, protocol, pool_address
),

token_pool_tvl_daily AS (
    SELECT
        p.date AS date,
        p.protocol AS protocol,
        p.pool_address AS pool_address,
        r.token_address AS token_address,
        r.token AS token,
        p.pool_tvl_usd AS token_tvl_usd
    FROM pool_tvl_daily p
    INNER JOIN registry_pool_tokens r
      ON r.protocol = p.protocol
     AND r.pool_address = p.pool_address
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

pool_symbol_lists AS (
    SELECT
        protocol,
        pool_address,
        arraySort(groupUniqArray(token)) AS tokens_sorted,
        countDistinct(token) AS tokens_cnt
    FROM (
        SELECT DISTINCT protocol, pool_address, token
        FROM registry_pool_tokens
    )
    GROUP BY protocol, pool_address
),

pool_labels AS (
    SELECT
        p.protocol AS protocol,
        p.pool_address AS pool_address,
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
        SELECT DISTINCT protocol, pool_address
        FROM {{ ref('int_execution_pools_enriched_daily') }}
    ) p
    LEFT JOIN {{ ref('stg_pools__v3_pool_registry') }} m
      ON m.protocol = p.protocol
     AND m.pool_address = lower(p.pool_address)
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} t0
      ON t0.token_address = m.token0_address
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} t1
      ON t1.token_address = m.token1_address
    LEFT JOIN pool_symbol_lists sl
      ON sl.protocol = p.protocol
     AND sl.pool_address = p.pool_address
),

{# Dates × pools from enriched, crossed with registry tokens so that
   migrated tokens (e.g. old EURe) still appear as filter options. #}
distinct_token_pool_dates AS (
    SELECT
        d.date AS date,
        d.protocol AS protocol,
        d.pool_address AS pool_address,
        r.token_address AS token_address,
        r.token AS token
    FROM (
        SELECT DISTINCT date, protocol, pool_address
        FROM {{ ref('int_execution_pools_enriched_daily') }}
        WHERE protocol IN ('Uniswap V3', 'Swapr V3', 'Balancer V3')
    ) d
    INNER JOIN registry_pool_tokens r
      ON r.protocol = d.protocol
     AND r.pool_address = d.pool_address
)

SELECT
    b.date AS date,
    b.protocol AS protocol,
    b.pool_address AS pool_address,
    pl.pool AS pool,
    b.token_address AS token_address,
    b.token AS token,
    pm.tvl_usd AS tvl_usd,
    pm.fees_usd_daily AS fees_usd_daily,
    pm.volume_usd_daily AS volume_usd_daily,
    pm.swap_count AS swap_count,
    pm.fee_apr_7d AS fee_apr_7d,
    il.lvr_apr_7d AS lvr_apr_7d,
    CASE
        WHEN pm.fee_apr_7d IS NOT NULL AND il.lvr_apr_7d IS NOT NULL
        THEN pm.fee_apr_7d + il.lvr_apr_7d
        ELSE NULL
    END AS net_apr_7d
FROM distinct_token_pool_dates b
INNER JOIN top_pools_by_token tp
  ON tp.token = b.token
 AND tp.protocol = b.protocol
 AND tp.pool_address = b.pool_address
 AND tp.token_address = b.token_address
LEFT JOIN pool_labels pl
  ON pl.protocol = b.protocol
 AND pl.pool_address = b.pool_address
LEFT JOIN {{ ref('int_execution_pools_metrics_daily') }} pm
  ON pm.date = b.date
 AND pm.protocol = b.protocol
 AND pm.pool_address = b.pool_address
LEFT JOIN {{ ref('fct_execution_pools_il_daily') }} il
  ON il.date = b.date
 AND il.protocol = b.protocol
 AND il.pool_address = b.pool_address
WHERE b.protocol IN ('Uniswap V3', 'Swapr V3', 'Balancer V3')
  AND b.date < today()
