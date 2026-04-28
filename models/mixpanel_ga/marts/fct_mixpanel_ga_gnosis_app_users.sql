{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(user_pseudonym)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','mixpanel_ga','gnosis_app']
  )
}}

WITH mp_users AS (
    SELECT DISTINCT user_id_hash
    FROM {{ ref('stg_mixpanel_ga__events') }}
    WHERE is_production = 1
      AND is_identified = 1
)

SELECT
    id.user_pseudonym,
    if(mp.user_id_hash IS NOT NULL, 1, 0)              AS matched_mp,
    mp.user_id_hash                                    AS mp_user_id_hash,
    id.first_seen_at,
    id.last_seen_at,
    id.heuristic_kinds,
    id.heuristic_hits,
    id.n_distinct_heuristics
FROM {{ ref('int_execution_gnosis_app_user_identities') }} id
LEFT JOIN mp_users mp
    ON id.user_pseudonym = mp.user_id_hash
