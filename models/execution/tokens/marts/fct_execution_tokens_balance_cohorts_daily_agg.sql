{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, balance_bucket)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, balance_bucket)',
    settings={ 'allow_nullable_key': 1 },
    tags=['dev','execution','tokens','balance_cohorts_daily_agg']
  )
}}

SELECT
    date,
    token_address,
    symbol,
    token_class,
    balance_bucket,
    sum(holders_in_bucket)   AS holders_in_bucket,
    sum(value_usd_in_bucket) AS value_usd_in_bucket
FROM {{ ref('fct_execution_tokens_balance_cohorts_daily') }}   -- sharded fact
WHERE date < today()
GROUP BY
    date,
    token_address,
    symbol,
    token_class,
    balance_bucket