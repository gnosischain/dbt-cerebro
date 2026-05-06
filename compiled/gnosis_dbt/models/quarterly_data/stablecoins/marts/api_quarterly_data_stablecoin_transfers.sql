

SELECT
    toStartOfQuarter(date) AS quarter,
    CASE
        WHEN symbol IN ('WxDAI', 'sDAI', 'USDC', 'USDC.e', 'USDT')
        THEN 'USD-pegged'
        ELSE 'non-USD'
    END AS peg_class,
    sum(transfer_count) AS transfers,
    sum(volume_usd)     AS volume_usd,
    CASE
        WHEN symbol IN ('WxDAI', 'sDAI', 'USDC', 'USDC.e', 'USDT')
        THEN 'WxDAI, sDAI, USDC, USDC.e, USDT'
        ELSE 'EURe, GBPe, BRLA, ZCHF, svZCHF'
    END AS tokens_included
FROM `dbt`.`fct_execution_tokens_metrics_daily`
WHERE token_class = 'STABLECOIN'
  AND symbol NOT IN ('BRZ')
GROUP BY quarter, peg_class, tokens_included
ORDER BY quarter, peg_class