{{
  config(
    materialized='view',
    tags=['production','execution','dao_treasury','tier1',
          'api:dao_treasury_allocation_latest','granularity:latest']
  )
}}

WITH latest AS (
    SELECT max(date) AS d FROM {{ ref('int_dao_treasury_holdings_daily') }}
),
per_token AS (
    SELECT
        symbol,
        CASE
            WHEN symbol IN ('GNO', 'sGNO', 'spGNO', 'aGnoGNO') THEN 'GNO'
            WHEN token_class = 'STABLECOIN' OR (position_type = 'lending' AND symbol IN ('WxDAI','USDC.e','USDC','USDT','EURe','GBPe','BRLA','BRZ','sDAI')) THEN 'Stablecoins'
            WHEN token_class = 'RWA' THEN 'RWA'
            WHEN symbol IN ('WETH') OR (position_type = 'lending' AND symbol = 'WETH') THEN 'ETH'
            WHEN symbol IN ('WBTC') THEN 'BTC'
            ELSE 'Other'
        END AS token_class,
        round(sum(balance_usd), 2) AS value_usd
    FROM {{ ref('int_dao_treasury_holdings_daily') }}
    WHERE date = (SELECT d FROM latest)
    GROUP BY symbol, token_class
    HAVING value_usd > 100
),
total AS (
    SELECT sum(value_usd) AS t FROM per_token
)
SELECT
    token_class,
    symbol AS token,
    value_usd,
    round(value_usd / nullIf((SELECT t FROM total), 0) * 100, 2) AS percentage
FROM per_token
ORDER BY value_usd DESC
