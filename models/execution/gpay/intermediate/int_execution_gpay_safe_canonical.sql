{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='address',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gpay']
  )
}}

-- Canonical resolution for the June 2026 post-exploit Safe migration
-- (gp_migrated_safes seed). One row per OLD Safe pointing to its NEW
-- (canonical) Safe. The seed has no chained migrations (a new Safe never
-- appears as an old Safe), so a single hop resolves fully. The GROUP BY
-- absorbs the one exact-duplicate row present in the source export.

SELECT
    lower(old_safe_address)                                AS address,
    lower(any(new_safe_address))                           AS canonical_address,
    any(userId)                                            AS gp_user_id,
    min(toDateTime(parseDateTimeBestEffort(completedAt)))  AS migrated_at
FROM {{ ref('gp_migrated_safes') }}
GROUP BY lower(old_safe_address)
