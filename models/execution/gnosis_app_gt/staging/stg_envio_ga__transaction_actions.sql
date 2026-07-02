{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(address)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'execution', 'gnosis_app_gt', 'stretch', 'internal_only', 'privacy:tier_internal'],
    meta={
        'expose_to_mcp': false,
        'privacy_tier': 'internal',
        'api': {'exclude_from_api': true},
        'grain': 'avatar_identity',
        'guard': 'STRETCH — full-scans envio_ga.transaction_action (208M, no pruning). Reduced to per-avatar first/last action timestamp + count. avatar_id spans ALL of Circles since 2020 (~626k) — a recency enrichment, NEVER a user count on its own.'
    }
) }}

-- STRETCH: exactly ONE full scan of envio_ga.transaction_action (208M rows;
-- id-sorted, so `timestamp`/`_synced_block` do NOT prune — EXPLAIN ESTIMATE reads
-- all 208M). Reduces the per-action feed to per-identity recency: first/last
-- action time + total action count. This is the missing on-chain timestamp for
-- the ground-truth suite (envio_ga entity tables have none).
--
-- avatar_id covers the WHOLE Circles ecosystem back to 2020 (~626k avatars), so
-- this table alone is NOT a user metric — int_execution_gnosis_app_gt_user_activity
-- joins it to registry/avatar identities only.
SELECT
    lower(avatar_id)            AS address,
    toDateTime(min(timestamp))  AS first_action_at,
    toDateTime(max(timestamp))  AS last_action_at,
    count()                     AS n_actions
FROM {{ source('envio_ga', 'transaction_action') }}
WHERE _deleted = 0
  AND avatar_id != ''
GROUP BY lower(avatar_id)
