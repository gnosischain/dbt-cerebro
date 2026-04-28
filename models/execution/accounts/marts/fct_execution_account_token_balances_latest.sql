{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(address, balance_usd, token_address)',
    unique_key='(address, token_address)',
    settings={ 'allow_nullable_key': 1 },
    pre_hook=[
      "SET max_threads = 1",
      "SET max_block_size = 8192",
      "SET max_memory_usage = 10000000000",
      "SET max_bytes_before_external_group_by = 100000000",
      "SET max_bytes_before_external_sort = 100000000"
    ],
    post_hook=[
      "SET max_threads = 0",
      "SET max_block_size = 65505",
      "SET max_memory_usage = 0",
      "SET max_bytes_before_external_group_by = 0",
      "SET max_bytes_before_external_sort = 0"
    ],
    tags=['production', 'execution', 'accounts', 'portfolio', 'balances', 'granularity:latest']
  )
}}

-- Bound the max(date) scan to a small recent window so ClickHouse only reads
-- a couple of monthly partitions instead of every partition since 2020.
-- Without this, max(date) triggers a full-tree scan that OOMs on the 10.8 GiB
-- cluster cap.
WITH latest_date AS (
  SELECT max(date) AS max_date
  FROM {{ ref('int_execution_tokens_balances_daily') }}
  WHERE date >= today() - 14
    AND date < today()
),

latest_balances AS (
  SELECT
    lower(address) AS address,
    date,
    lower(token_address) AS token_address,
    symbol,
    token_class,
    balance_raw,
    balance,
    ifNull(balance_usd, 0) AS balance_usd
  FROM {{ ref('int_execution_tokens_balances_daily') }}
  WHERE date >= today() - 14
    AND date = (SELECT max_date FROM latest_date)
    AND address IS NOT NULL
    AND address != ''
    AND balance > 0
)

SELECT
  address,
  date,
  token_address,
  symbol,
  token_class,
  balance_raw,
  balance,
  balance_usd
FROM latest_balances

