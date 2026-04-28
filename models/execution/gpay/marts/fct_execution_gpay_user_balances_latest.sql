{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(wallet_address, token)',
    unique_key='(wallet_address, token)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production', 'execution', 'gpay', 'user_portfolio', 'balances', 'granularity:latest']
  )
}}

{# Aliasing the aggregate `max(date)` as `date` (the source column name)
   confused ClickHouses predicate-pushdown optimizer into reporting
   "Aggregate function max(date) AS date is found in WHERE" (CH 184).
   The fix has two parts:
     - the CTE renames its aggregate to `max_date`, so the WHERE subquery
       references a non-shadowing column;
     - the outer aggregation runs inside a subquery with `as_of_date`, then
       the wrapping SELECT renames it back to `date` so downstream consumers
       (e.g. api_execution_gpay_user_balances_latest) see the same shape. #}
WITH latest_date AS (
  SELECT max(date) AS max_date
  FROM {{ ref('int_execution_gpay_balances_daily') }}
  WHERE date < today()
),
agg AS (
  SELECT
    address AS wallet_address,
    symbol AS token,
    sum(round(toFloat64(balance_usd), 2)) AS value_usd,
    sum(round(toFloat64(balance), 6)) AS value_native,
    max(date) AS as_of_date
  FROM {{ ref('int_execution_gpay_balances_daily') }}
  WHERE date = (SELECT max_date FROM latest_date)
  GROUP BY wallet_address, token
)

SELECT
  wallet_address,
  token,
  value_usd,
  value_native,
  as_of_date AS date
FROM agg
