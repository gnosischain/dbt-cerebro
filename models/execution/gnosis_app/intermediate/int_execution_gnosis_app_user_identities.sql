{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(user_pseudonym)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gnosis_app']
  )
}}

-- Pseudonymization boundary for the Gnosis App sector. Projects the
-- internal-only bridge (which holds raw addresses) down to pseudonym-only
-- columns so marts and downstream consumers can read this safely. The
-- bridge is the single source of truth for the (address, pseudonym) pair
-- and the heuristic snapshot; this model just drops the raw address
-- column so the hash function is computed exactly once across the lineage.

SELECT
    user_pseudonym,
    first_seen_at,
    last_seen_at,
    heuristic_kinds,
    heuristic_hits,
    n_distinct_heuristics
FROM {{ ref('int_execution_gnosis_app_user_identity_bridge') }}
