{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(pool_address, provider)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'yields', 'user_portfolio', 'intermediate']
    )
}}

-- True Balancer LP fees per (provider, pool), replacing the
-- capital_out - capital_in PnL proxy in int_execution_yields_user_lp_positions
-- (model review EXECUTIONYIELDS-C12).
--
-- Method (contribution-based, value-weighted, monthly): for each month a pool
-- earns swap fees (int_execution_pools_fees_daily, which now carries Balancer V2
-- and V3), take the fees NET of the Balancer protocol-fee cut (50% post ~2023-03,
-- 0% before) and split that net across LPs in proportion to each LP's cumulative
-- net contributed USD (mint - burn) as of month-end. Sum over months.
-- Attributes to the wallet that provided the liquidity (the Join/Exit `provider`),
-- which is the right target for user analytics even when BPT is later staked in a
-- gauge. Conserves: sum over LPs of fees_usd == pool fees (shares sum to 1 each
-- month with positive contribution). Monthly grain keeps the build tractable;
-- intra-month membership changes are immaterial for lifetime totals.

WITH

monthly_net AS (
    SELECT
        toStartOfMonth(block_timestamp)                            AS month,
        pool_address,
        provider,
        sumIf(amount_usd, event_type = 'mint')
          - sumIf(amount_usd, event_type = 'burn')                 AS net_usd
    FROM {{ ref('int_execution_pools_dex_liquidity_events') }}
    WHERE protocol IN ('Balancer V2', 'Balancer V3')
      AND provider IS NOT NULL
      AND provider != ''
      AND event_type IN ('mint', 'burn')
    GROUP BY month, pool_address, provider
),

provider_cum AS (
    SELECT
        pool_address,
        provider,
        month,
        sum(net_usd) OVER (
            PARTITION BY pool_address, provider
            ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_usd
    FROM monthly_net
),

fee_months_gross AS (
    SELECT
        pool_address,
        protocol,
        toStartOfMonth(date) AS month,
        sum(fees_usd)        AS pool_fee_usd
    FROM {{ ref('int_execution_pools_fees_daily') }}
    WHERE protocol IN ('Balancer V2', 'Balancer V3')
      AND fees_usd > 0
    GROUP BY pool_address, protocol, month
),

-- Global Balancer V2 swap protocol-fee timeline (feeType 0). LPs receive swap
-- fees NET of this cut (0% before ~2023-03, 50% after). The protocol fee is a
-- global param the pools cache, so a single timeline models it well. Applied to
-- V2 only; V3 uses a different aggregate-fee mechanism and is left gross here
-- (~4% of Balancer fees, immaterial).
protocol_fee_global AS (
    SELECT month, max(protocol_fraction) AS protocol_fraction
    FROM (
        SELECT
            toStartOfMonth(block_timestamp)                                 AS month,
            toFloat64OrNull(decoded_params['protocolFeePercentage']) / 1e18 AS protocol_fraction
        FROM {{ ref('contracts_BalancerV2_Pool_events') }}
        WHERE event_name = 'ProtocolFeePercentageCacheUpdated'
          AND decoded_params['feeType'] = '0'
          AND decoded_params['protocolFeePercentage'] IS NOT NULL
    )
    GROUP BY month
),

-- net-to-LP pool fees: gross swap fees x (1 - prevailing protocol fee), ASOF month
fee_months AS (
    SELECT
        fmg.pool_address                                                                 AS pool_address,
        fmg.month                                                                        AS month,
        fmg.pool_fee_usd
          * if(fmg.protocol = 'Balancer V2', 1 - coalesce(g.protocol_fraction, 0), 1)    AS pool_fee_usd
    FROM (SELECT *, toUInt8(1) AS jk FROM fee_months_gross) fmg
    ASOF LEFT JOIN (
        SELECT toUInt8(1) AS jk, month, protocol_fraction
        FROM protocol_fee_global
        ORDER BY jk, month
    ) g
        ON fmg.jk = g.jk AND fmg.month >= g.month
),

pool_providers AS (
    SELECT DISTINCT pool_address, provider FROM monthly_net
),

-- one row per (pool, fee-month, provider) carrying the provider's cumulative
-- contribution as of that month (ASOF: latest cum at or before the fee month)
contrib AS (
    SELECT
        g.pool_address                          AS pool_address,
        g.month                                 AS month,
        g.provider                              AS provider,
        g.pool_fee_usd                          AS pool_fee_usd,
        greatest(coalesce(pc.cum_usd, 0), 0)    AS cum_usd
    FROM (
        SELECT fm.pool_address, fm.month, fm.pool_fee_usd, pp.provider
        FROM fee_months fm
        INNER JOIN pool_providers pp ON pp.pool_address = fm.pool_address
    ) g
    ASOF LEFT JOIN (
        SELECT pool_address, provider, month, cum_usd
        FROM provider_cum
        ORDER BY pool_address, provider, month
    ) pc
        ON  g.pool_address = pc.pool_address
        AND g.provider     = pc.provider
        AND g.month       >= pc.month
),

pool_tot AS (
    SELECT pool_address, month, sum(cum_usd) AS pool_cum_usd
    FROM contrib
    GROUP BY pool_address, month
)

SELECT
    c.pool_address                                                      AS pool_address,
    c.provider                                                          AS provider,
    sum(c.pool_fee_usd * c.cum_usd / nullIf(pt.pool_cum_usd, 0))        AS fees_usd
FROM contrib c
INNER JOIN pool_tot pt
    ON pt.pool_address = c.pool_address
   AND pt.month        = c.month
GROUP BY c.pool_address, c.provider
