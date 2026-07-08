{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:stablecoin_transfers', 'granularity:quarterly'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": true,
                "parameters": [
                    {"name": "quarter_from", "column": "quarter", "operator": ">=", "type": "date", "description": "Inclusive lower bound on quarter start date (e.g. 2024-01-01 for 2024-Q1)"},
                    {"name": "quarter_to", "column": "quarter", "operator": "<=", "type": "date", "description": "Inclusive upper bound on quarter start date"},
                    {"name": "peg_class", "column": "peg_class", "operator": "=", "type": "string", "description": "Filter by peg class: USD-pegged or non-USD"}
                ],
                "pagination": {"enabled": true, "default_limit": 200, "max_limit": 1000, "response": "envelope"},
                "sort": [{"column": "quarter", "direction": "DESC"}]
            }
        }
    )
}}

WITH per_class AS (
    SELECT
        toStartOfQuarter(date) AS quarter,
        CASE
            WHEN symbol IN ('WxDAI', 'sDAI', 'USDC', 'USDC.e', 'USDT')
            THEN 'USD-pegged'
            ELSE 'non-USD'
        END AS peg_class,
        sum(transfer_count) AS transfers,
        sum(volume_usd)     AS volume_usd
    FROM {{ ref('fct_execution_tokens_metrics_daily') }}
    WHERE token_class = 'STABLECOIN'
      AND symbol NOT IN ('BRZ')
    GROUP BY quarter, peg_class
)

-- Emits the two per-class rows plus a 'total' row (plain sum across classes, which
-- is the exact grand-total count/volume). Wrapped in a subquery so the trailing
-- ORDER BY applies to the whole UNION, not just its last arm.
SELECT * FROM (
    SELECT
        quarter,
        peg_class,
        transfers,
        volume_usd,
        CASE
            WHEN peg_class = 'USD-pegged' THEN 'WxDAI, sDAI, USDC, USDC.e, USDT'
            ELSE 'EURe, GBPe, BRLA, ZCHF, svZCHF'
        END AS tokens_included
    FROM per_class

    UNION ALL

    SELECT
        quarter,
        'total'          AS peg_class,
        sum(transfers)   AS transfers,
        sum(volume_usd)  AS volume_usd,
        'ALL (USD-pegged + non-USD, excl BRZ)' AS tokens_included
    FROM per_class
    GROUP BY quarter
)
ORDER BY quarter, peg_class
