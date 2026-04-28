{{
  config(
    materialized='view',
    tags=['production','execution','dao_treasury','tier1',
          'api:dao_treasury_holdings_by_class_ts','granularity:daily']
  )
}}

SELECT
    date,
    CASE
        WHEN symbol IN ('GNO', 'sGNO', 'spGNO', 'aGnoGNO') THEN 'GNO'
        WHEN token_class = 'STABLECOIN' OR (position_type = 'lending' AND symbol IN ('WxDAI','USDC.e','USDC','USDT','EURe','GBPe','BRLA','BRZ','sDAI')) THEN 'Stablecoins'
        WHEN token_class = 'RWA' THEN 'RWA'
        WHEN symbol IN ('WETH') OR (position_type = 'lending' AND symbol = 'WETH') THEN 'ETH'
        WHEN symbol IN ('WBTC') THEN 'BTC'
        ELSE 'Other'
    END AS label,
    round(sum(balance_usd), 0) AS value
FROM {{ ref('int_dao_treasury_holdings_daily') }}
GROUP BY date, label
HAVING value > 0
ORDER BY date, label
