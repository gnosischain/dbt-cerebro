

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
    FROM `dbt`.`fct_execution_tokens_metrics_daily`
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