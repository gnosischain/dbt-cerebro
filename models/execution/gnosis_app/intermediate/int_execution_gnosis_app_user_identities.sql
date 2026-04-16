{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(user_pseudonym)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gnosis_app']
  )
}}


SELECT
    {{ pseudonymize_address('address') }} AS user_pseudonym,
    first_seen_at,
    last_seen_at,
    heuristic_kinds,
    heuristic_hits,
    n_distinct_heuristics
FROM {{ ref('int_execution_gnosis_app_users_current') }}
