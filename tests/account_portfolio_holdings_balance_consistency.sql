-- Data-quality check: the latest-day total balance must be reconcilable
-- with token-grain holdings. An address that shows total_balance_usd > 0 in
-- int_execution_account_balance_history_daily on its latest date but has
-- ZERO token rows in fct_execution_account_token_balances_latest is a
-- pipeline inconsistency that the dashboard would render as
--   "$149 total" + "No token balances"
-- which is exactly the contradiction we want to surface.
--
-- Returns offending rows; a passing test returns zero rows.

WITH latest_history AS (
  SELECT
    address,
    argMax(total_balance_usd, date) AS total_balance_usd,
    max(date)                       AS latest_date,
    argMax(tokens_held, date)       AS tokens_held_history
  FROM {{ ref('int_execution_account_balance_history_daily') }}
  WHERE
    {% if var('test_full_refresh', false) %}1=1
    {% else %}toDate(date) >= today() - {{ var('test_lookback_days', 7) }}
    {% endif %}
  GROUP BY address
),

holdings AS (
  SELECT
    address,
    count() AS rows_in_holdings,
    sum(balance_usd) AS sum_balance_usd
  FROM {{ ref('fct_execution_account_token_balances_latest') }}
  GROUP BY address
)

SELECT
  h.address                                        AS address,
  h.latest_date                                    AS latest_balance_date,
  h.total_balance_usd                              AS history_total_balance_usd,
  h.tokens_held_history                            AS history_tokens_held,
  coalesce(o.rows_in_holdings, 0)                  AS holdings_rows,
  coalesce(o.sum_balance_usd, 0)                   AS holdings_sum_balance_usd
FROM latest_history h
LEFT JOIN holdings o ON o.address = h.address
WHERE h.total_balance_usd > 1
  AND coalesce(o.rows_in_holdings, 0) = 0
LIMIT 100
