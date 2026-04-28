{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(address)',
    unique_key='address',
    settings={ 'allow_nullable_key': 1 },
    pre_hook=[
      "SET max_threads = 1",
      "SET max_block_size = 8192",
      "SET max_memory_usage = 10000000000",
      "SET join_algorithm = 'grace_hash'",
      "SET grace_hash_join_initial_buckets = 256",
      "SET max_bytes_in_join = 500000000",
      "SET max_bytes_before_external_sort = 100000000"
    ],
    post_hook=[
      "SET max_threads = 0",
      "SET max_block_size = 65505",
      "SET max_memory_usage = 0",
      "SET join_algorithm = 'default'",
      "SET max_bytes_in_join = 0",
      "SET max_bytes_before_external_sort = 0"
    ],
    tags=['production', 'execution', 'accounts', 'portfolio', 'gnosis_app', 'fct:gnosis_app_user_profile', 'granularity:latest']
  )
}}

SELECT
  lower(u.address) AS address,
  u.first_seen_at,
  u.last_seen_at,
  u.heuristic_hits,
  u.heuristic_kinds,
  u.n_distinct_heuristics,
  g.pay_wallet AS controlled_gpay_wallet,
  g.is_currently_ga_owned,
  g.n_ga_owners_current,
  g.n_total_owners_current,
  g.onboarding_class
FROM {{ ref('int_execution_gnosis_app_users_current') }} u
LEFT JOIN {{ ref('int_execution_gnosis_app_gpay_wallets') }} g
  ON lower(g.first_ga_owner_address) = lower(u.address)
