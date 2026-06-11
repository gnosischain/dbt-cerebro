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

{# USER-HOLDINGS SEMANTICS (June 2026 Safe migration): this mart reads
   int_execution_gpay_balances_user_daily, NOT the raw per-Safe balances.
   Refunded ("lost") old Safes are therefore excluded from their
   first_refund_at onward - residuals still physically sitting on them are
   recovery-entitled, not user holdings - so this mart stays consistent
   with the gpay balance aggregates (fct_execution_gpay_balances_by_token_daily).
   For raw on-chain per-Safe balances (forensics, chain-state inspection)
   query int_execution_gpay_balances_daily directly.

   Aliasing the aggregate `max(date)` as `date` (the source column name)
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
  FROM {{ ref('int_execution_gpay_balances_user_daily') }}
  WHERE date < today()
),
agg AS (
  SELECT
    address AS wallet_address,
    symbol AS token,
    sum(round(toFloat64(balance_usd), 2)) AS value_usd,
    sum(round(toFloat64(balance), 6)) AS value_native,
    max(date) AS as_of_date
  FROM {{ ref('int_execution_gpay_balances_user_daily') }}
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
