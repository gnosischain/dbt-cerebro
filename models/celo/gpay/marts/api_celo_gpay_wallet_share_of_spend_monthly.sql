{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_wallet_share_of_spend', 'granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "destination", "column": "label", "operator": "=", "type": "string", "description": "to_card or to_elsewhere"},
          {"name": "start_date", "column": "date", "operator": ">=", "type": "date", "description": "Inclusive start date"},
          {"name": "end_date", "column": "date", "operator": "<=", "type": "date", "description": "Inclusive end date"}
        ],
        "sort": [{"column": "date", "direction": "DESC"}]
      }
    }
  )
}}

-- Monthly share of cardholder stablecoin outflow reaching the GP card vs going
-- elsewhere. `value` is the RETAIL number (transfers under $1,000); value_all is the
-- raw flow including large treasury-scale movements, which a handful of addresses
-- dominate. Read fct_celo_gpay_wallet_share_of_spend_daily before quoting either.
SELECT
    toStartOfMonth(date)              AS date,
    destination                       AS label,
    round(sum(usd_excl_large), 2)     AS value,
    round(sum(usd_total), 2)          AS value_all,
    sum(n_transfers)                  AS transfers,
    max(n_wallets)                    AS peak_daily_wallets
FROM {{ ref('fct_celo_gpay_wallet_share_of_spend_daily') }}
GROUP BY date, label
ORDER BY date, label
