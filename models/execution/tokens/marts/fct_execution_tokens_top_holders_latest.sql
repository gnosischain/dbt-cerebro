{{
  config(
    materialized='table',
    tags=['production','execution','tokens','top_holders']
  )
}}

WITH

prev_balances AS (
    SELECT
        token_address,
        address,
        balance_usd AS balance_usd_7d_ago
    FROM {{ ref('int_execution_tokens_balances_daily') }}
    WHERE date = (
        SELECT max(date) - 7
        FROM {{ ref('int_execution_tokens_balances_daily') }}
        WHERE date < today() AND balance > 0
    )
      AND balance > 0
      AND (token_address, address) IN (
          SELECT token_address, address
          FROM {{ ref('fct_execution_tokens_top_holders_ranked') }}
      )
)

SELECT
    r.rank,
    r.token_address AS token_address,
    r.symbol,
    r.token_class,
    r.address AS address,
    l.project AS label,
    l.sector AS label_sector,
    r.balance,
    r.balance_usd,
    round(r.pct_of_total, 4) AS pct_of_total,
    round(sum(r.pct_of_total) OVER (
        PARTITION BY r.token_address ORDER BY r.rank
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 4) AS cumulative_pct,
    r.balance_usd - coalesce(p.balance_usd_7d_ago, 0) AS change_usd_7d
FROM {{ ref('fct_execution_tokens_top_holders_ranked') }} r
LEFT JOIN prev_balances p
    ON r.address = p.address
   AND r.token_address = p.token_address
LEFT JOIN {{ ref('int_crawlers_data_labels') }} l
    ON lower(l.address) = lower(r.address)
ORDER BY r.token_address, r.rank
